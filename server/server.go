package server

import (
	"archive/tar"
	"bytes"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"vdc/api"

	"github.com/google/uuid"
)

// Config holds server configuration.
type Config struct {
	// PackageDir is where uploaded packages are stored.
	// If empty, a temporary directory is created.
	PackageDir string
}

type taskRecord struct {
	TaskNumber int
	MachineID  string
	TaskRunID  string
	Status     api.TaskStatus
}

type taskRunRef struct {
	task  *taskRecord
	runID string
}

type runRecord struct {
	RunID   string
	JobName string
	Tasks   []*taskRecord
	Status  api.RunStatus
}

// RunSummary is a brief description of a run for the status display.
type RunSummary struct {
	RunID    string
	JobName  string
	Status   api.RunStatus
	Replicas int
}

// StatusSnapshot is a point-in-time view of the datacenter for the leader display.
type StatusSnapshot struct {
	Machines         []api.MachineInfo
	MachineJobCounts map[string]int // machine ID -> number of currently running tasks
	PackageHashes    []string
	Runs             []RunSummary
}

type pullFileResult struct {
	TaskNumber int
	Filename   string
	Data       []byte
}

type pullState struct {
	mu       sync.Mutex
	total    int
	received int // successes + errors
	results  []pullFileResult
	errors   []api.PullFileError
	tarData  []byte // built when received == total
}

// Server is the VDC leader server.
type Server struct {
	cfg           Config
	mu            sync.Mutex
	machines      map[string]*api.MachineInfo
	runs          map[string]*runRecord
	taskRunIndex  map[string]taskRunRef // task run ID -> (task pointer, run ID)
	machineQueues map[string][]api.Command
	pullRequests  map[string]*pullState
}

// New creates a new Server. If cfg.PackageDir is empty, a temp dir is created.
func New(cfg Config) (*Server, error) {
	if cfg.PackageDir == "" {
		dir, err := os.MkdirTemp("", "vdc-packages-*")
		if err != nil {
			return nil, fmt.Errorf("create package dir: %w", err)
		}
		cfg.PackageDir = dir
	}
	return &Server{
		cfg:           cfg,
		machines:      make(map[string]*api.MachineInfo),
		runs:          make(map[string]*runRecord),
		taskRunIndex:  make(map[string]taskRunRef),
		machineQueues: make(map[string][]api.Command),
		pullRequests:  make(map[string]*pullState),
	}, nil
}

// PackageDir returns the directory used to store packages.
func (s *Server) PackageDir() string { return s.cfg.PackageDir }

// RegisterMachine registers a new machine with the datacenter.
func (s *Server) RegisterMachine(args *api.RegisterRequest, reply *api.RegisterReply) error {
	id := uuid.New().String()
	s.mu.Lock()
	s.machines[id] = &api.MachineInfo{
		ID:            id,
		Spec:          args.Spec,
		LastHeartbeat: time.Now(),
	}
	s.machineQueues[id] = nil
	s.mu.Unlock()
	reply.MachineID = id
	return nil
}

// Heartbeat updates the last-seen time for a machine.
func (s *Server) Heartbeat(args *api.HeartbeatRequest, reply *api.HeartbeatReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.machines[args.MachineID]
	if !ok {
		return fmt.Errorf("unknown machine: %s", args.MachineID)
	}
	m.LastHeartbeat = time.Now()
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

// SubmitJob validates capacity, selects machines, enqueues commands, and records the job.
func (s *Server) SubmitJob(args *api.SubmitJobRequest, reply *api.SubmitJobReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	spec := args.Spec
	needRAM := uint64(spec.Requirements.RAM)
	needDisk := uint64(spec.Requirements.Disk)

	// Collect all eligible machines, then assign replicas round-robin so that
	// different machines are preferred but a single machine can run multiple tasks.
	var eligible []string
	for id, m := range s.machines {
		if m.Spec.RAM >= needRAM && m.Spec.Disk >= needDisk {
			eligible = append(eligible, id)
		}
	}
	if len(eligible) == 0 {
		return fmt.Errorf("no machines available with RAM>=%d disk>=%d", needRAM, needDisk)
	}

	selected := make([]string, spec.Replicas)
	for i := range selected {
		selected[i] = eligible[i%len(eligible)]
	}

	runID := uuid.New().String()

	tasks := make([]*taskRecord, len(selected))
	for i, machineID := range selected {
		taskRunID := uuid.New().String()
		task := &taskRecord{
			TaskNumber: i,
			MachineID:  machineID,
			TaskRunID:  taskRunID,
			Status:     api.TaskPending,
		}
		tasks[i] = task
		s.taskRunIndex[taskRunID] = taskRunRef{task: task, runID: runID}

		for _, pkg := range spec.Packages {
			s.machineQueues[machineID] = append(s.machineQueues[machineID], api.Command{
				Type: api.CmdFetchPackage,
				FetchPackage: &api.FetchPackageDetails{
					PackageName: pkg.Name,
					PackageHash: args.PackageHashes[pkg.Name],
				},
			})
		}
		s.machineQueues[machineID] = append(s.machineQueues[machineID], api.Command{
			Type: api.CmdRunBinary,
			RunBinary: &api.RunBinaryDetails{
				RunID:       taskRunID,
				PackageName: spec.Binary.Package,
				BinaryPath:  spec.Binary.Path,
				Args:        spec.Args,
			},
		})
	}

	s.runs[runID] = &runRecord{
		RunID:   runID,
		JobName: spec.Name,
		Tasks:   tasks,
		Status:  api.RunPending,
	}

	reply.RunID = runID
	return nil
}

// ReportTaskStatus is called by a machine to update its task's lifecycle state.
func (s *Server) ReportTaskStatus(args *api.ReportTaskStatusRequest, reply *api.ReportTaskStatusReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref, ok := s.taskRunIndex[args.TaskRunID]
	if !ok {
		return fmt.Errorf("unknown task run ID: %s", args.TaskRunID)
	}
	ref.task.Status = args.Status
	s.runs[ref.runID].Status = computeRunStatus(s.runs[ref.runID].Tasks)
	return nil
}

// GetRunStatus returns the current lifecycle state of a run.
func (s *Server) GetRunStatus(args *api.GetRunStatusRequest, reply *api.GetRunStatusReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[args.RunID]
	if !ok {
		return fmt.Errorf("unknown run ID: %s", args.RunID)
	}
	reply.Status = run.Status
	return nil
}

// GetCommand returns and clears the pending command queue for a machine.
func (s *Server) GetCommand(args *api.GetCommandRequest, reply *api.GetCommandReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.machines[args.MachineID]; !ok {
		return fmt.Errorf("unknown machine: %s", args.MachineID)
	}
	reply.Commands = s.machineQueues[args.MachineID]
	s.machineQueues[args.MachineID] = nil
	return nil
}

// PullFiles enqueues SendFile commands for the relevant task(s) and returns a request ID.
func (s *Server) PullFiles(args *api.PullFilesRequest, reply *api.PullFilesReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[args.RunID]
	if !ok {
		return fmt.Errorf("unknown run ID: %s", args.RunID)
	}

	tasks := run.Tasks
	if args.TaskNumber >= 0 {
		var found *taskRecord
		for _, t := range tasks {
			if t.TaskNumber == args.TaskNumber {
				found = t
				break
			}
		}
		if found == nil {
			return fmt.Errorf("task %d not found in run %s", args.TaskNumber, args.RunID)
		}
		tasks = []*taskRecord{found}
	}

	requestID := uuid.New().String()
	s.pullRequests[requestID] = &pullState{total: len(tasks)}

	for _, task := range tasks {
		s.machineQueues[task.MachineID] = append(s.machineQueues[task.MachineID], api.Command{
			Type: api.CmdSendFile,
			SendFile: &api.SendFileDetails{
				RequestID:  requestID,
				TaskRunID:  task.TaskRunID,
				TaskNumber: task.TaskNumber,
				Filename:   args.Filename,
			},
		})
	}

	reply.RequestID = requestID
	reply.Total = len(tasks)
	return nil
}

// UploadFile is called by a machine to deliver a requested file to the server.
func (s *Server) UploadFile(args *api.UploadFileRequest, reply *api.UploadFileReply) error {
	s.mu.Lock()
	ps, ok := s.pullRequests[args.RequestID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown request ID: %s", args.RequestID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.received++
	if args.Err != "" {
		ps.errors = append(ps.errors, api.PullFileError{TaskNumber: args.TaskNumber, Err: args.Err})
	} else {
		ps.results = append(ps.results, pullFileResult{
			TaskNumber: args.TaskNumber,
			Filename:   args.Filename,
			Data:       args.Data,
		})
	}
	if ps.received == ps.total {
		var err error
		ps.tarData, err = buildResultTar(ps.results)
		if err != nil {
			return fmt.Errorf("build result tar: %w", err)
		}
	}
	return nil
}

// GetPullResult returns the current status of a pull request.
func (s *Server) GetPullResult(args *api.GetPullResultRequest, reply *api.GetPullResultReply) error {
	s.mu.Lock()
	ps, ok := s.pullRequests[args.RequestID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown request ID: %s", args.RequestID)
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	reply.Received = ps.received
	reply.Total = ps.total
	reply.Done = ps.received == ps.total
	reply.TarData = ps.tarData
	reply.Errors = ps.errors
	return nil
}

// Machines returns a snapshot of all registered machines.
func (s *Server) Machines() []api.MachineInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]api.MachineInfo, 0, len(s.machines))
	for _, m := range s.machines {
		out = append(out, *m)
	}
	return out
}

// Status returns a point-in-time snapshot of the datacenter for display.
func (s *Server) Status() StatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	machines := make([]api.MachineInfo, 0, len(s.machines))
	for _, m := range s.machines {
		machines = append(machines, *m)
	}

	jobCounts := make(map[string]int)
	for _, run := range s.runs {
		for _, task := range run.Tasks {
			if task.Status == api.TaskRunning {
				jobCounts[task.MachineID]++
			}
		}
	}

	entries, _ := os.ReadDir(s.cfg.PackageDir)
	var hashes []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".tar") {
			hashes = append(hashes, strings.TrimSuffix(e.Name(), ".tar"))
		}
	}

	runs := make([]RunSummary, 0, len(s.runs))
	for _, run := range s.runs {
		runs = append(runs, RunSummary{
			RunID:    run.RunID,
			JobName:  run.JobName,
			Status:   run.Status,
			Replicas: len(run.Tasks),
		})
	}

	return StatusSnapshot{
		Machines:         machines,
		MachineJobCounts: jobCounts,
		PackageHashes:    hashes,
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

func computeRunStatus(tasks []*taskRecord) api.RunStatus {
	anyRunning, anyPending, anyFailed := false, false, false
	for _, t := range tasks {
		switch t.Status {
		case api.TaskRunning:
			anyRunning = true
		case api.TaskPending:
			anyPending = true
		case api.TaskFailed:
			anyFailed = true
		}
	}
	switch {
	case anyRunning:
		return api.RunRunning
	case anyPending:
		return api.RunPending
	case anyFailed:
		return api.RunFailed
	default:
		return api.RunComplete
	}
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
