package server

import (
	"encoding/json"
	"fmt"
	"log"
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
		log.Printf("scheduleNewRuns: %v", err)
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
			log.Printf("tryScheduleRun %s: %v", runID, err)
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

	rows, err := s.db.Query(`SELECT id FROM machines WHERE ram >= ? AND disk >= ?`, needRAM, needDisk)
	if err != nil {
		return fmt.Errorf("query machines: %w", err)
	}
	var eligible []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		eligible = append(eligible, id)
	}
	rows.Close()
	if len(eligible) == 0 {
		return nil // no machines yet; retry next tick
	}

	rows, err = s.db.Query(`SELECT task_run_id, task_number FROM tasks WHERE run_id = ? AND machine_id IS NULL ORDER BY task_number`, runID)
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

	if len(tasks) == 0 {
		// All tasks already assigned; promote run to Pending.
		_, err = s.db.Exec(`UPDATE runs SET status = ? WHERE run_id = ?`, int(api.RunPending), runID)
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for i, task := range tasks {
		machineID := eligible[i%len(eligible)]
		if _, err := tx.Exec(`UPDATE tasks SET machine_id = ? WHERE task_run_id = ?`, machineID, task.taskRunID); err != nil {
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
			RunID:       task.taskRunID,
			PackageName: spec.Binary.Package,
			BinaryPath:  spec.Binary.Path,
			Args:        expandArgs(spec.Args, task.taskNumber),
		}); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(`UPDATE runs SET status = ? WHERE run_id = ?`, int(api.RunPending), runID); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Server) detectAndRescheduleLost(heartbeatTimeout time.Duration) {
	cutoff := time.Now().Add(-heartbeatTimeout).UnixNano()

	rows, err := s.db.Query(`SELECT id FROM machines WHERE last_heartbeat < ?`, cutoff)
	if err != nil {
		log.Printf("detectAndRescheduleLost: %v", err)
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
			log.Printf("handleLostMachine %s: %v", machineID, err)
		}
	}
	if err := s.rescheduleLostTasks(); err != nil {
		log.Printf("rescheduleLostTasks: %v", err)
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
			log.Printf("rescheduleTask %s: %v", t.taskRunID, err)
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

	needRAM := uint64(spec.Requirements.RAM)
	needDisk := uint64(spec.Requirements.Disk)

	rows, err := s.db.Query(`SELECT id FROM machines WHERE ram >= ? AND disk >= ?`, needRAM, needDisk)
	if err != nil {
		return err
	}
	var eligible []string
	for rows.Next() {
		var id string
		rows.Scan(&id)
		eligible = append(eligible, id)
	}
	rows.Close()
	if len(eligible) == 0 {
		return nil // no machines available; try again next tick
	}

	machineID := eligible[0]
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
