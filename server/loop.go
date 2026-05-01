package server

import (
	"encoding/json"
	"fmt"
	"time"
	"vdc/api"
	"vdc/jobspec"

	"github.com/google/uuid"
)

// RunLoop is the server's main scan loop. It schedules new runs and, after
// startupGrace has elapsed, detects lost machines and reschedules their tasks.
// The startup grace period gives reconnecting machines time to reclaim their
// tasks before they are declared lost.
func (s *Server) RunLoop(heartbeatTimeout, startupGrace time.Duration) {
	lostDetectionStart := time.Now().Add(startupGrace)
	lostCheckInterval := heartbeatTimeout / 3
	var lastLostCheck time.Time

	for {
		time.Sleep(100 * time.Millisecond)
		s.scheduleNewRuns()
		if time.Now().After(lostDetectionStart) {
			if time.Since(lastLostCheck) >= lostCheckInterval {
				s.detectAndRescheduleLost(heartbeatTimeout)
				lastLostCheck = time.Now()
			}
		}
	}
}

func (s *Server) scheduleNewRuns() {
	rows, err := s.db.Query(`SELECT run_id FROM runs WHERE status = ?`, int(api.RunScheduling))
	if err != nil {
		s.logger.Printf("scheduleNewRuns: %v", err)
		return
	}
	var runIDs []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		runIDs = append(runIDs, id)
	}
	rows.Close()

	for _, runID := range runIDs {
		if err := s.tryScheduleRun(runID); err != nil {
			s.logger.Printf("tryScheduleRun %s: %v", runID, err)
		}
	}
}

func (s *Server) tryScheduleRun(runID string) error {
	var specJSON, hashesJSON string
	if err := s.db.QueryRow(`SELECT spec_json, package_hashes_json FROM runs WHERE run_id = ?`, runID).
		Scan(&specJSON, &hashesJSON); err != nil {
		return fmt.Errorf("get run: %w", err)
	}

	var spec jobspec.JobSpec
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		return fmt.Errorf("unmarshal spec: %w", err)
	}
	var hashes map[string]string
	if err := json.Unmarshal([]byte(hashesJSON), &hashes); err != nil {
		return fmt.Errorf("unmarshal hashes: %w", err)
	}

	needRAM := uint64(spec.Requirements.RAM)
	needDisk := uint64(spec.Requirements.Disk)

	// Build free-capacity map; each assignment this tick is reflected immediately
	// so we don't over-commit a machine within the same scheduling pass.
	free, err := s.freeCapacities()
	if err != nil {
		return fmt.Errorf("compute capacities: %w", err)
	}

	s.logger.Printf("schedule run=%s  need ram=%d disk=%d  machines=%d", runID, needRAM, needDisk, len(free))
	for machID, c := range free {
		s.logger.Printf("  machine %s  free ram=%d disk=%d", machID, c.ram, c.disk)
	}

	rows, err := s.db.Query(`SELECT task_run_id, task_number FROM tasks WHERE run_id = ? AND machine_id IS NULL ORDER BY task_number`, runID)
	if err != nil {
		return fmt.Errorf("query unassigned tasks: %w", err)
	}
	type unassigned struct {
		taskRunID  string
		taskNumber int
	}
	var tasks []unassigned
	for rows.Next() {
		var t unassigned
		rows.Scan(&t.taskRunID, &t.taskNumber)
		tasks = append(tasks, t)
	}
	rows.Close()

	s.logger.Printf("  unassigned tasks: %d", len(tasks))

	if len(tasks) == 0 {
		// All tasks already have machines; promote run to Pending.
		_, err = s.db.Exec(`UPDATE runs SET status = ? WHERE run_id = ?`, int(api.RunPending), runID)
		return err
	}

	// Greedily assign tasks to machines that have enough free capacity.
	// Tasks that don't fit stay unassigned and are retried on the next tick.
	type assignment struct {
		taskRunID  string
		taskNumber int
		machineID  string
	}
	var assignments []assignment
	for _, task := range tasks {
		machineID := pickMachine(free, needRAM, needDisk)
		if machineID == "" {
			s.logger.Printf("  task %d: no machine has ram=%d disk=%d free", task.taskNumber, needRAM, needDisk)
			break // if one task can't fit, none will (same requirements)
		}
		free[machineID].ram -= needRAM
		free[machineID].disk -= needDisk
		assignments = append(assignments, assignment{task.taskRunID, task.taskNumber, machineID})
	}

	s.logger.Printf("  assigned %d/%d tasks this tick", len(assignments), len(tasks))

	if len(assignments) == 0 {
		return nil // nothing fits this tick; retry later
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, a := range assignments {
		if _, err := tx.Exec(`UPDATE tasks SET machine_id = ? WHERE task_run_id = ?`, a.machineID, a.taskRunID); err != nil {
			return err
		}
		for _, pkg := range spec.Packages {
			if err := insertCommand(tx, a.machineID, api.CmdFetchPackage, &api.FetchPackageDetails{
				PackageName: pkg.Name,
				PackageHash: hashes[pkg.Name],
			}); err != nil {
				return err
			}
		}
		if err := insertCommand(tx, a.machineID, api.CmdRunBinary, &api.RunBinaryDetails{
			RunID:       a.taskRunID,
			PackageName: spec.Binary.Package,
			BinaryPath:  spec.Binary.Path,
			Args:        expandArgs(spec.Args, a.taskNumber),
		}); err != nil {
			return err
		}
	}

	// Only promote to RunPending once every task has a machine.
	if len(assignments) == len(tasks) {
		if _, err := tx.Exec(`UPDATE runs SET status = ? WHERE run_id = ?`, int(api.RunPending), runID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Server) detectAndRescheduleLost(heartbeatTimeout time.Duration) {
	cutoff := time.Now().Add(-heartbeatTimeout).UnixNano()

	rows, err := s.db.Query(`SELECT id FROM machines WHERE last_heartbeat < ?`, cutoff)
	if err != nil {
		s.logger.Printf("detectAndRescheduleLost: %v", err)
		return
	}
	var lostMachines []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		lostMachines = append(lostMachines, id)
	}
	rows.Close()

	for _, machineID := range lostMachines {
		if err := s.handleLostMachine(machineID); err != nil {
			s.logger.Printf("handleLostMachine %s: %v", machineID, err)
		}
	}
	if err := s.rescheduleLostTasks(); err != nil {
		s.logger.Printf("rescheduleLostTasks: %v", err)
	}
}

func (s *Server) handleLostMachine(machineID string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM machines WHERE id = ?`, machineID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM commands WHERE machine_id = ?`, machineID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE tasks SET status = ? WHERE machine_id = ? AND status IN (?, ?, ?)`,
		int(api.TaskLost), machineID,
		int(api.TaskPending), int(api.TaskFetching), int(api.TaskRunning)); err != nil {
		return err
	}

	// Collect affected run IDs (within tx, sees updated task statuses).
	rows, err := tx.Query(`SELECT DISTINCT run_id FROM tasks WHERE machine_id = ?`, machineID)
	if err != nil {
		return err
	}
	var runIDs []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		runIDs = append(runIDs, id)
	}
	rows.Close()

	for _, runID := range runIDs {
		if err := updateRunStatus(tx, runID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Server) rescheduleLostTasks() error {
	rows, err := s.db.Query(`
		SELECT t.task_run_id, t.run_id, t.task_number, r.spec_json, r.package_hashes_json
		FROM tasks t JOIN runs r ON t.run_id = r.run_id
		WHERE t.status = ?`, int(api.TaskLost))
	if err != nil {
		return err
	}
	type lostTask struct {
		taskRunID, runID     string
		taskNumber           int
		specJSON, hashesJSON string
	}
	var lost []lostTask
	for rows.Next() {
		var t lostTask
		rows.Scan(&t.taskRunID, &t.runID, &t.taskNumber, &t.specJSON, &t.hashesJSON)
		lost = append(lost, t)
	}
	rows.Close()

	for _, t := range lost {
		if err := s.rescheduleTask(t.taskRunID, t.runID, t.taskNumber, t.specJSON, t.hashesJSON); err != nil {
			s.logger.Printf("rescheduleTask %s: %v", t.taskRunID, err)
		}
	}
	return nil
}

func (s *Server) rescheduleTask(taskRunID, runID string, taskNumber int, specJSON, hashesJSON string) error {
	var spec jobspec.JobSpec
	if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
		return err
	}
	var hashes map[string]string
	if err := json.Unmarshal([]byte(hashesJSON), &hashes); err != nil {
		return err
	}

	free, err := s.freeCapacities()
	if err != nil {
		return fmt.Errorf("compute capacities: %w", err)
	}
	needRAM := uint64(spec.Requirements.RAM)
	needDisk := uint64(spec.Requirements.Disk)

	machineID := pickMachine(free, needRAM, needDisk)
	if machineID == "" {
		return nil // no capacity yet; retry next tick
	}

	newTaskRunID := uuid.New().String()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM tasks WHERE task_run_id = ?`, taskRunID); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO tasks (task_run_id, run_id, task_number, machine_id, status) VALUES (?, ?, ?, ?, ?)`,
		newTaskRunID, runID, taskNumber, machineID, int(api.TaskPending)); err != nil {
		return err
	}

	for _, pkg := range spec.Packages {
		if err := insertCommand(tx, machineID, api.CmdFetchPackage, &api.FetchPackageDetails{
			PackageName: pkg.Name,
			PackageHash: hashes[pkg.Name],
		}); err != nil {
			return err
		}
	}
	if err := insertCommand(tx, machineID, api.CmdRunBinary, &api.RunBinaryDetails{
		RunID:       newTaskRunID,
		PackageName: spec.Binary.Package,
		BinaryPath:  spec.Binary.Path,
		Args:        expandArgs(spec.Args, taskNumber),
	}); err != nil {
		return err
	}

	if err := updateRunStatus(tx, runID); err != nil {
		return err
	}

	return tx.Commit()
}

// capacity tracks the free RAM and disk on a machine.
type capacity struct{ ram, disk uint64 }

// freeCapacities returns the available RAM and disk for every registered
// machine, after subtracting resources consumed by tasks that are currently
// pending, fetching, or running.
func (s *Server) freeCapacities() (map[string]*capacity, error) {
	free := make(map[string]*capacity)

	rows, err := s.db.Query(`SELECT id, ram, disk FROM machines`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		var ram, disk uint64
		rows.Scan(&id, &ram, &disk)
		free[id] = &capacity{ram, disk}
	}
	rows.Close()

	// Subtract resources held by active tasks.
	rows, err = s.db.Query(`
		SELECT t.machine_id, r.spec_json
		FROM tasks t JOIN runs r ON t.run_id = r.run_id
		WHERE t.machine_id IS NOT NULL AND t.status IN (?, ?, ?)`,
		int(api.TaskPending), int(api.TaskFetching), int(api.TaskRunning))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var machineID, specJSON string
		rows.Scan(&machineID, &specJSON)
		var spec jobspec.JobSpec
		if err := json.Unmarshal([]byte(specJSON), &spec); err != nil {
			continue
		}
		if c, ok := free[machineID]; ok {
			needRAM := uint64(spec.Requirements.RAM)
			needDisk := uint64(spec.Requirements.Disk)
			if c.ram >= needRAM {
				c.ram -= needRAM
			} else {
				c.ram = 0
			}
			if c.disk >= needDisk {
				c.disk -= needDisk
			} else {
				c.disk = 0
			}
		}
	}
	rows.Close()

	return free, nil
}

// pickMachine returns the ID of the machine with the most free RAM that still
// satisfies the requirements, or "" if none qualifies. Preferring the most
// free machine spreads load rather than packing tasks onto a single box.
func pickMachine(free map[string]*capacity, needRAM, needDisk uint64) string {
	var best string
	var bestRAM uint64
	for id, c := range free {
		if c.ram >= needRAM && c.disk >= needDisk && c.ram > bestRAM {
			best = id
			bestRAM = c.ram
		}
	}
	return best
}
