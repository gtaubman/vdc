package server

import (
	"archive/tar"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"vdc/api"

	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS machines (
	id TEXT PRIMARY KEY,
	ram INTEGER NOT NULL,
	disk INTEGER NOT NULL,
	last_heartbeat INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS package_names (
	hash TEXT PRIMARY KEY,
	name TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
	run_id TEXT PRIMARY KEY,
	job_name TEXT NOT NULL,
	spec_json TEXT NOT NULL,
	package_hashes_json TEXT NOT NULL,
	status INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
	task_run_id TEXT PRIMARY KEY,
	run_id TEXT NOT NULL,
	task_number INTEGER NOT NULL,
	machine_id TEXT,
	status INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	machine_id TEXT NOT NULL,
	type TEXT NOT NULL,
	payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pull_requests (
	request_id TEXT PRIMARY KEY,
	total INTEGER NOT NULL,
	tar_data BLOB
);
CREATE TABLE IF NOT EXISTS pull_results (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	request_id TEXT NOT NULL,
	task_number INTEGER NOT NULL,
	filename TEXT NOT NULL,
	data BLOB,
	err TEXT NOT NULL DEFAULT ''
);
`

// Config holds server configuration.
type Config struct {
	PackageDir string
	DBPath     string // SQLite path; "" uses ":memory:"
	LogPath    string // log file path; "" logs to stderr
}

// RunSummary is a brief description of a run for the status display.
type RunSummary struct {
	RunID    string
	JobName  string
	Status   api.RunStatus
	Replicas int
}

// PackageInfo pairs a package name with its content hash.
type PackageInfo struct {
	Name string
	Hash string
}

// StatusSnapshot is a point-in-time view of the datacenter for the leader display.
type StatusSnapshot struct {
	Machines         []api.MachineInfo
	MachineJobCounts map[string]int
	Packages         []PackageInfo
	Runs             []RunSummary
}

type pullFileResult struct {
	TaskNumber int
	Filename   string
	Data       []byte
}

// Server is the VDC leader server.
type Server struct {
	cfg    Config
	db     *sql.DB
	logger *log.Logger
}

// New creates a new Server. If cfg.PackageDir is empty, a temp dir is created.
// If cfg.DBPath is empty, an in-memory SQLite database is used.
func New(cfg Config) (*Server, error) {
	if cfg.PackageDir == "" {
		dir, err := os.MkdirTemp("", "vdc-packages-*")
		if err != nil {
			return nil, fmt.Errorf("create package dir: %w", err)
		}
		cfg.PackageDir = dir
	} else if err := os.MkdirAll(cfg.PackageDir, 0755); err != nil {
		return nil, fmt.Errorf("create package dir: %w", err)
	}

	dbPath := cfg.DBPath
	if dbPath == "" {
		dbPath = ":memory:"
	}
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("WAL mode: %w", err)
	}
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}

	var logger *log.Logger
	if cfg.LogPath != "" {
		lf, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("open log file: %w", err)
		}
		logger = log.New(lf, "", log.LstdFlags)
	} else {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}

	return &Server{cfg: cfg, db: db, logger: logger}, nil
}

// PackageDir returns the directory used to store packages.
func (s *Server) PackageDir() string { return s.cfg.PackageDir }

// LogPath returns the path of the log file, or "" if logging to stderr.
func (s *Server) LogPath() string { return s.cfg.LogPath }

// RegisterMachine registers a new machine with the datacenter.
// If the machine reports ActiveTaskIDs from a previous connection, tasks that
// were rescheduled (no longer present in the DB) receive a CancelTask command,
// and Lost tasks that haven't been rescheduled are reactivated.
func (s *Server) RegisterMachine(args *api.RegisterRequest, reply *api.RegisterReply) error {
	id := args.MachineID
	if id == "" {
		id = uuid.New().String()
	}
	now := time.Now().UnixNano()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`INSERT OR REPLACE INTO machines (id, ram, disk, last_heartbeat) VALUES (?, ?, ?, ?)`,
		id, args.Spec.RAM, args.Spec.Disk, now); err != nil {
		return fmt.Errorf("insert machine: %w", err)
	}

	for _, taskRunID := range args.ActiveTaskIDs {
		var taskStatus int
		err := tx.QueryRow(`SELECT status FROM tasks WHERE task_run_id = ?`, taskRunID).Scan(&taskStatus)
		if err == sql.ErrNoRows {
			// Task was rescheduled (old task_run_id deleted) — tell machine to cancel.
			if err2 := insertCommand(tx, id, api.CmdCancelTask, &api.CancelTaskDetails{RunID: taskRunID}); err2 != nil {
				return err2
			}
			continue
		}
		if err != nil {
			return err
		}
		if api.TaskStatus(taskStatus) == api.TaskLost {
			// Machine reconnected before reschedule — reactivate task under new machine ID.
			var runID string
			if err := tx.QueryRow(`SELECT run_id FROM tasks WHERE task_run_id = ?`, taskRunID).Scan(&runID); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE tasks SET machine_id = ?, status = ? WHERE task_run_id = ?`,
				id, int(api.TaskRunning), taskRunID); err != nil {
				return err
			}
			if err := updateRunStatus(tx, runID); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.logger.Printf("RegisterMachine %s  ram=%d disk=%d  active=%d",
		id, args.Spec.RAM, args.Spec.Disk, len(args.ActiveTaskIDs))
	reply.MachineID = id
	return nil
}

// Heartbeat updates the last-seen time for a machine.
func (s *Server) Heartbeat(args *api.HeartbeatRequest, reply *api.HeartbeatReply) error {
	res, err := s.db.Exec(`UPDATE machines SET last_heartbeat = ? WHERE id = ?`,
		time.Now().UnixNano(), args.MachineID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("unknown machine: %s", args.MachineID)
	}
	return nil
}

// HasPackage reports whether a package with the given content hash is stored.
func (s *Server) HasPackage(args *api.HasPackageRequest, reply *api.HasPackageReply) error {
	_, err := os.Stat(s.packagePath(args.Hash))
	reply.Exists = err == nil
	return nil
}

// UploadPackage stores a tar archive keyed by its content hash.
func (s *Server) UploadPackage(args *api.UploadPackageRequest, reply *api.UploadPackageReply) error {
	if err := os.WriteFile(s.packagePath(args.Hash), args.Data, 0644); err != nil {
		return fmt.Errorf("write package: %w", err)
	}
	return nil
}

// FetchPackage returns the tar archive for the given content hash.
func (s *Server) FetchPackage(args *api.FetchPackageRequest, reply *api.FetchPackageReply) error {
	data, err := os.ReadFile(s.packagePath(args.Hash))
	if err != nil {
		return fmt.Errorf("read package %s: %w", args.Hash, err)
	}
	reply.Data = data
	return nil
}

// SubmitJob records the job in the database; the scan loop assigns machines.
func (s *Server) SubmitJob(args *api.SubmitJobRequest, reply *api.SubmitJobReply) error {
	specJSON, err := json.Marshal(args.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}
	hashesJSON, err := json.Marshal(args.PackageHashes)
	if err != nil {
		return fmt.Errorf("marshal hashes: %w", err)
	}

	runID := uuid.New().String()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`INSERT INTO runs (run_id, job_name, spec_json, package_hashes_json, status) VALUES (?, ?, ?, ?, ?)`,
		runID, args.Spec.Name, string(specJSON), string(hashesJSON), int(api.RunScheduling)); err != nil {
		return err
	}

	for i := range args.Spec.Replicas {
		taskRunID := uuid.New().String()
		if _, err := tx.Exec(`INSERT INTO tasks (task_run_id, run_id, task_number, machine_id, status) VALUES (?, ?, ?, NULL, ?)`,
			taskRunID, runID, i, int(api.TaskPending)); err != nil {
			return err
		}
	}

	for name, hash := range args.PackageHashes {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO package_names (hash, name) VALUES (?, ?)`, hash, name); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	reply.RunID = runID
	return nil
}

// ReportTaskStatus is called by a machine to update its task's lifecycle state.
func (s *Server) ReportTaskStatus(args *api.ReportTaskStatusRequest, reply *api.ReportTaskStatusReply) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var runID string
	err = tx.QueryRow(`SELECT run_id FROM tasks WHERE task_run_id = ?`, args.TaskRunID).Scan(&runID)
	if err == sql.ErrNoRows {
		// Task was rescheduled; silently ignore stale status reports.
		return tx.Commit()
	}
	if err != nil {
		return err
	}

	if _, err := tx.Exec(`UPDATE tasks SET status = ? WHERE task_run_id = ?`,
		int(args.Status), args.TaskRunID); err != nil {
		return err
	}
	if err := updateRunStatus(tx, runID); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.logger.Printf("ReportTaskStatus task=%s status=%s", args.TaskRunID, args.Status)
	return nil
}

// GetRunStatus returns the current lifecycle state of a run.
func (s *Server) GetRunStatus(args *api.GetRunStatusRequest, reply *api.GetRunStatusReply) error {
	var status int
	err := s.db.QueryRow(`SELECT status FROM runs WHERE run_id = ?`, args.RunID).Scan(&status)
	if err == sql.ErrNoRows {
		return fmt.Errorf("unknown run ID: %s", args.RunID)
	}
	if err != nil {
		return err
	}
	reply.Status = api.RunStatus(status)
	return nil
}

// GetCommand retrieves and clears the pending command queue for a machine.
// It reads all commands into memory, parses them, then deletes and commits on success.
func (s *Server) GetCommand(args *api.GetCommandRequest, reply *api.GetCommandReply) error {
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM machines WHERE id = ?`, args.MachineID).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("unknown machine: %s", args.MachineID)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT id, type, payload_json FROM commands WHERE machine_id = ? ORDER BY id`, args.MachineID)
	if err != nil {
		return err
	}
	type dbRow struct {
		id      int64
		cmdType string
		payload string
	}
	var dbRows []dbRow
	for rows.Next() {
		var r dbRow
		if err := rows.Scan(&r.id, &r.cmdType, &r.payload); err != nil {
			rows.Close()
			return err
		}
		dbRows = append(dbRows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	// Parse all commands; roll back if any are malformed.
	cmds := make([]api.Command, 0, len(dbRows))
	for _, r := range dbRows {
		cmd, err := parseCommand(r.cmdType, r.payload)
		if err != nil {
			return err
		}
		cmds = append(cmds, cmd)
	}

	// Parsing succeeded — delete and commit.
	for _, r := range dbRows {
		if _, err := tx.Exec(`DELETE FROM commands WHERE id = ?`, r.id); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	if len(cmds) > 0 {
		s.logger.Printf("GetCommand machine=%s returning %d commands", args.MachineID, len(cmds))
	}
	reply.Commands = cmds
	return nil
}

// PullFiles enqueues SendFile commands for the relevant task(s) and returns a request ID.
func (s *Server) PullFiles(args *api.PullFilesRequest, reply *api.PullFilesReply) error {
	var exists int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id = ?`, args.RunID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return fmt.Errorf("unknown run ID: %s", args.RunID)
	}

	var query string
	var queryArgs []any
	if args.TaskNumber >= 0 {
		query = `SELECT task_run_id, task_number, machine_id FROM tasks WHERE run_id = ? AND task_number = ? AND machine_id IS NOT NULL`
		queryArgs = []any{args.RunID, args.TaskNumber}
	} else {
		query = `SELECT task_run_id, task_number, machine_id FROM tasks WHERE run_id = ? AND machine_id IS NOT NULL`
		queryArgs = []any{args.RunID}
	}

	rows, err := s.db.Query(query, queryArgs...)
	if err != nil {
		return err
	}
	type task struct {
		taskRunID  string
		taskNumber int
		machineID  string
	}
	var tasks []task
	for rows.Next() {
		var t task
		if err := rows.Scan(&t.taskRunID, &t.taskNumber, &t.machineID); err != nil {
			rows.Close()
			return err
		}
		tasks = append(tasks, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	if len(tasks) == 0 {
		return fmt.Errorf("no assigned tasks found for run %s", args.RunID)
	}

	requestID := uuid.New().String()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`INSERT INTO pull_requests (request_id, total) VALUES (?, ?)`, requestID, len(tasks)); err != nil {
		return err
	}
	for _, t := range tasks {
		if err := insertCommand(tx, t.machineID, api.CmdSendFile, &api.SendFileDetails{
			RequestID:  requestID,
			TaskRunID:  t.taskRunID,
			TaskNumber: t.taskNumber,
			Filename:   args.Filename,
		}); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	reply.RequestID = requestID
	reply.Total = len(tasks)
	return nil
}

// UploadFile is called by a machine to deliver a requested file to the server.
func (s *Server) UploadFile(args *api.UploadFileRequest, reply *api.UploadFileReply) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`INSERT INTO pull_results (request_id, task_number, filename, data, err) VALUES (?, ?, ?, ?, ?)`,
		args.RequestID, args.TaskNumber, args.Filename, args.Data, args.Err); err != nil {
		return err
	}

	var total, received int
	if err := tx.QueryRow(`SELECT total FROM pull_requests WHERE request_id = ?`, args.RequestID).Scan(&total); err != nil {
		return fmt.Errorf("pull request %s: %w", args.RequestID, err)
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pull_results WHERE request_id = ?`, args.RequestID).Scan(&received); err != nil {
		return err
	}

	if received == total {
		rows, err := tx.Query(`SELECT task_number, filename, data FROM pull_results WHERE request_id = ? AND err = ''`, args.RequestID)
		if err != nil {
			return err
		}
		var results []pullFileResult
		for rows.Next() {
			var r pullFileResult
			if err := rows.Scan(&r.TaskNumber, &r.Filename, &r.Data); err != nil {
				rows.Close()
				return err
			}
			results = append(results, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		tarData, err := buildResultTar(results)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE pull_requests SET tar_data = ? WHERE request_id = ?`, tarData, args.RequestID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// GetPullResult returns the current status of a pull request.
func (s *Server) GetPullResult(args *api.GetPullResultRequest, reply *api.GetPullResultReply) error {
	var total int
	var tarData []byte
	err := s.db.QueryRow(`SELECT total, tar_data FROM pull_requests WHERE request_id = ?`, args.RequestID).Scan(&total, &tarData)
	if err == sql.ErrNoRows {
		return fmt.Errorf("unknown request ID: %s", args.RequestID)
	}
	if err != nil {
		return err
	}

	var received int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM pull_results WHERE request_id = ?`, args.RequestID).Scan(&received); err != nil {
		return err
	}

	rows, err := s.db.Query(`SELECT task_number, err FROM pull_results WHERE request_id = ? AND err != ''`, args.RequestID)
	if err != nil {
		return err
	}
	var errs []api.PullFileError
	for rows.Next() {
		var e api.PullFileError
		if err := rows.Scan(&e.TaskNumber, &e.Err); err != nil {
			rows.Close()
			return err
		}
		errs = append(errs, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	reply.Received = received
	reply.Total = total
	reply.Done = received == total
	reply.TarData = tarData
	reply.Errors = errs
	return nil
}

// Status returns a point-in-time snapshot of the datacenter for display.
func (s *Server) Status() StatusSnapshot {
	var machines []api.MachineInfo
	if rows, err := s.db.Query(`SELECT id, ram, disk, last_heartbeat FROM machines`); err == nil {
		for rows.Next() {
			var m api.MachineInfo
			var lastBeat int64
			rows.Scan(&m.ID, &m.Spec.RAM, &m.Spec.Disk, &lastBeat)
			m.LastHeartbeat = time.Unix(0, lastBeat)
			machines = append(machines, m)
		}
		rows.Close()
	}

	jobCounts := make(map[string]int)
	if rows, err := s.db.Query(`SELECT machine_id, COUNT(*) FROM tasks WHERE status = ? AND machine_id IS NOT NULL GROUP BY machine_id`, int(api.TaskRunning)); err == nil {
		for rows.Next() {
			var machineID string
			var count int
			rows.Scan(&machineID, &count)
			jobCounts[machineID] = count
		}
		rows.Close()
	}

	entries, _ := os.ReadDir(s.cfg.PackageDir)
	var packages []PackageInfo
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tar") {
			hash := strings.TrimSuffix(e.Name(), ".tar")
			var name string
			s.db.QueryRow(`SELECT name FROM package_names WHERE hash = ?`, hash).Scan(&name)
			packages = append(packages, PackageInfo{Name: name, Hash: hash})
		}
	}

	var runs []RunSummary
	rows, err := s.db.Query(`
		SELECT r.run_id, r.job_name, r.status, COUNT(t.task_run_id)
		FROM runs r LEFT JOIN tasks t ON r.run_id = t.run_id
		GROUP BY r.run_id`)
	if err == nil {
		for rows.Next() {
			var r RunSummary
			var status int
			rows.Scan(&r.RunID, &r.JobName, &status, &r.Replicas)
			r.Status = api.RunStatus(status)
			runs = append(runs, r)
		}
		rows.Close()
	}

	return StatusSnapshot{
		Machines:         machines,
		MachineJobCounts: jobCounts,
		Packages:         packages,
		Runs:             runs,
	}
}

// ListenAndServe starts the RPC server on the given port.
func (s *Server) ListenAndServe(port int) error {
	if err := rpc.Register(s); err != nil {
		return fmt.Errorf("rpc.Register: %w", err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("net.Listen: %w", err)
	}
	rpc.Accept(ln)
	return nil
}

func (s *Server) packagePath(hash string) string {
	return filepath.Join(s.cfg.PackageDir, hash+".tar")
}

// updateRunStatus recomputes and stores the run's aggregate status from its tasks.
// Must be called within a transaction.
func updateRunStatus(tx *sql.Tx, runID string) error {
	// RunScheduling is exclusively managed by the scheduler (tryScheduleRun /
	// rescheduleTask). Status reports and lost-machine handling must not
	// interfere with it; any unscheduled tasks will be picked up on the next
	// scheduling tick.
	var currentStatus int
	if err := tx.QueryRow(`SELECT status FROM runs WHERE run_id = ?`, runID).Scan(&currentStatus); err != nil {
		return err
	}
	if api.RunStatus(currentStatus) == api.RunScheduling {
		return nil
	}

	rows, err := tx.Query(`SELECT status FROM tasks WHERE run_id = ?`, runID)
	if err != nil {
		return err
	}
	var statuses []api.TaskStatus
	for rows.Next() {
		var s int
		rows.Scan(&s)
		statuses = append(statuses, api.TaskStatus(s))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	status := computeRunStatus(statuses)
	_, err = tx.Exec(`UPDATE runs SET status = ? WHERE run_id = ?`, int(status), runID)
	return err
}

type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// insertCommand marshals payload as JSON and inserts a command row.
func insertCommand(db execer, machineID string, cmdType api.CommandType, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = db.Exec(`INSERT INTO commands (machine_id, type, payload_json) VALUES (?, ?, ?)`,
		machineID, string(cmdType), string(data))
	return err
}

// parseCommand deserializes a command from its DB representation.
func parseCommand(cmdType, payloadJSON string) (api.Command, error) {
	cmd := api.Command{Type: api.CommandType(cmdType)}
	switch cmd.Type {
	case api.CmdFetchPackage:
		var d api.FetchPackageDetails
		if err := json.Unmarshal([]byte(payloadJSON), &d); err != nil {
			return cmd, err
		}
		cmd.FetchPackage = &d
	case api.CmdRunBinary:
		var d api.RunBinaryDetails
		if err := json.Unmarshal([]byte(payloadJSON), &d); err != nil {
			return cmd, err
		}
		cmd.RunBinary = &d
	case api.CmdSendFile:
		var d api.SendFileDetails
		if err := json.Unmarshal([]byte(payloadJSON), &d); err != nil {
			return cmd, err
		}
		cmd.SendFile = &d
	case api.CmdCancelTask:
		var d api.CancelTaskDetails
		if err := json.Unmarshal([]byte(payloadJSON), &d); err != nil {
			return cmd, err
		}
		cmd.CancelTask = &d
	default:
		return cmd, fmt.Errorf("unknown command type: %s", cmdType)
	}
	return cmd, nil
}

func computeRunStatus(statuses []api.TaskStatus) api.RunStatus {
	var running, pending, fetching, complete, failed, lost, cancelled int
	for _, s := range statuses {
		switch s {
		case api.TaskRunning:
			running++
		case api.TaskPending:
			pending++
		case api.TaskFetching:
			fetching++
		case api.TaskComplete:
			complete++
		case api.TaskFailed:
			failed++
		case api.TaskLost:
			lost++
		case api.TaskCancelled:
			cancelled++
		}
	}
	total := len(statuses)
	switch {
	case running > 0:
		return api.RunRunning
	case pending+fetching+lost > 0:
		// Lost tasks are awaiting reschedule — the run is still active.
		return api.RunPending
	case complete == total:
		return api.RunComplete
	case cancelled == total:
		return api.RunCancelled
	case complete > 0:
		return api.RunPartialFailure
	default:
		return api.RunFailed
	}
}

func expandArgs(args []string, taskNumber int) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = strings.ReplaceAll(a, "%TASKID%", strconv.Itoa(taskNumber))
	}
	return out
}

func buildResultTar(results []pullFileResult) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, r := range results {
		name := fmt.Sprintf("%d.%s", r.TaskNumber, r.Filename)
		hdr := &tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(r.Data)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(r.Data); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
