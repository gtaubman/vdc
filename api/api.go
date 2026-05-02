package api

import (
	"fmt"
	"time"
	"vdc/jobspec"
)

// MachineSpec describes the resources a machine contributes to the datacenter.
type MachineSpec struct {
	RAM  uint64
	Disk uint64
}

// MachineInfo is the server's record of a connected machine.
type MachineInfo struct {
	ID            string
	Spec          MachineSpec
	LastHeartbeat time.Time
}

// RegisterRequest is the argument to the RegisterMachine RPC.
type RegisterRequest struct {
	MachineID     string   // if non-empty, reuse this ID (reconnect after server restart)
	Spec          MachineSpec
	ActiveTaskIDs []string // task run IDs the machine was executing before reconnecting
}

// RegisterReply is the reply from the RegisterMachine RPC.
type RegisterReply struct {
	MachineID string
}

// HeartbeatRequest is the argument to the Heartbeat RPC.
type HeartbeatRequest struct {
	MachineID string
}

// HeartbeatReply is the reply from the Heartbeat RPC.
type HeartbeatReply struct{}

// HasPackageRequest asks whether the server already has a package by content hash.
type HasPackageRequest struct {
	Hash string
}

// HasPackageReply is the reply from the HasPackage RPC.
type HasPackageReply struct {
	Exists bool
}

// UploadPackageRequest sends a tar archive to the server, keyed by content hash.
type UploadPackageRequest struct {
	Hash string
	Data []byte // tar archive
}

// UploadPackageReply is the reply from the UploadPackage RPC.
type UploadPackageReply struct{}

// SubmitJobRequest submits a job to the server for scheduling.
type SubmitJobRequest struct {
	Spec          jobspec.JobSpec
	PackageHashes map[string]string // package name -> content hash
}

// SubmitJobReply is the reply from the SubmitJob RPC.
type SubmitJobReply struct {
	RunID string
}

// TaskStatus is the lifecycle state of a single task replica.
type TaskStatus int

const (
	TaskPending   TaskStatus = iota // assigned to a machine, not yet started
	TaskFetching                    // machine is downloading the package
	TaskRunning                     // binary is executing
	TaskComplete                    // binary exited successfully
	TaskFailed                      // binary exited with an error
	TaskLost                        // machine stopped heartbeating; outcome unknown
	TaskCancelled                   // cancelled before or during execution
)

func (s TaskStatus) String() string {
	switch s {
	case TaskPending:
		return "pending"
	case TaskFetching:
		return "fetching"
	case TaskRunning:
		return "running"
	case TaskComplete:
		return "complete"
	case TaskFailed:
		return "failed"
	case TaskLost:
		return "lost"
	case TaskCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("TaskStatus(%d)", int(s))
	}
}

// RunStatus is the aggregate lifecycle state of a job run.
type RunStatus int

const (
	RunScheduling     RunStatus = iota // tasks are being assigned to machines
	RunPending                         // all tasks assigned; none running yet
	RunRunning                         // at least one task is executing
	RunComplete                        // all tasks completed successfully
	RunPartialFailure                  // some tasks completed, at least one failed or was lost
	RunFailed                          // all tasks finished; none succeeded
	RunCancelled                       // all tasks were cancelled
)

func (s RunStatus) String() string {
	switch s {
	case RunScheduling:
		return "scheduling"
	case RunPending:
		return "pending"
	case RunRunning:
		return "running"
	case RunComplete:
		return "complete"
	case RunPartialFailure:
		return "partial_failure"
	case RunFailed:
		return "failed"
	case RunCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("RunStatus(%d)", int(s))
	}
}

// ReportTaskStatusRequest is sent by a machine to update its task's lifecycle state.
type ReportTaskStatusRequest struct {
	TaskRunID string
	Status    TaskStatus
	ExitCode  *int // non-nil when Status is TaskComplete or TaskFailed
}

// ReportTaskStatusReply is the reply from the ReportTaskStatus RPC.
type ReportTaskStatusReply struct{}

// GetRunStatusRequest asks the server for the current status of a run.
type GetRunStatusRequest struct {
	RunID string
}

// GetRunStatusReply is the reply from the GetRunStatus RPC.
type GetRunStatusReply struct {
	Status RunStatus
}

// CommandType identifies the kind of work a machine should perform.
type CommandType string

const (
	CmdFetchPackage CommandType = "FetchPackage"
	CmdRunBinary    CommandType = "RunBinary"
	CmdSendFile     CommandType = "SendFile"
	CmdCancelTask   CommandType = "CancelTask"
)

// FetchPackageDetails carries the parameters for a FetchPackage command.
type FetchPackageDetails struct {
	PackageName string
	PackageHash string
}

// RunBinaryDetails carries the parameters for a RunBinary command.
type RunBinaryDetails struct {
	RunID       string   // unique ID for this task execution
	PackageName string
	PackageHash string   // content hash; used to locate the package directory
	BinaryPath  string
	Args        []string
}

// SendFileDetails carries the parameters for a SendFile command.
type SendFileDetails struct {
	RequestID  string
	TaskRunID  string
	TaskNumber int
	Filename   string
}

// CancelTaskDetails carries the parameters for a CancelTask command.
type CancelTaskDetails struct {
	RunID string // task run ID to kill
}

// Command is a unit of work dispatched to a machine.
type Command struct {
	Type         CommandType
	FetchPackage *FetchPackageDetails // non-nil when Type == CmdFetchPackage
	RunBinary    *RunBinaryDetails    // non-nil when Type == CmdRunBinary
	SendFile     *SendFileDetails     // non-nil when Type == CmdSendFile
	CancelTask   *CancelTaskDetails   // non-nil when Type == CmdCancelTask
}

// GetCommandRequest is the argument to the GetCommand RPC.
type GetCommandRequest struct {
	MachineID string
}

// GetCommandReply is the reply from the GetCommand RPC.
type GetCommandReply struct {
	Commands []Command
}

// FetchPackageRequest asks the server to send a package's tar archive.
type FetchPackageRequest struct {
	Hash string
}

// FetchPackageReply is the reply from the FetchPackage RPC.
type FetchPackageReply struct {
	Data []byte // tar archive
}

// PullFilesRequest asks the server to collect a file from one or all tasks of a run.
type PullFilesRequest struct {
	RunID      string
	TaskNumber int    // -1 means all tasks
	Filename   string
}

// PullFilesReply is the reply from the PullFiles RPC.
type PullFilesReply struct {
	RequestID string
	Total     int
}

// UploadFileRequest is sent by a machine to deliver a requested file to the server.
type UploadFileRequest struct {
	RequestID  string
	TaskNumber int
	Filename   string
	Data       []byte
	Err        string // non-empty if the machine could not read the file
}

// UploadFileReply is the reply from the UploadFile RPC.
type UploadFileReply struct{}

// PullFileError records a per-task error during a pull.
type PullFileError struct {
	TaskNumber int
	Err        string
}

// GetPullResultRequest polls for the status of a pull request.
type GetPullResultRequest struct {
	RequestID string
}

// GetPullResultReply is the reply from the GetPullResult RPC.
type GetPullResultReply struct {
	Received int
	Total    int
	Done     bool
	TarData  []byte          // tar of <taskNumber>.<filename> entries; populated when Done
	Errors   []PullFileError // per-task errors, if any
}

// TaskSummary is a brief description of one task within a run.
type TaskSummary struct {
	TaskNumber int
	TaskRunID  string
	MachineID  string
	Status     TaskStatus
	StartedAt  time.Time // zero if task hasn't started
	FinishedAt time.Time // zero if task hasn't finished
	ExitCode   *int      // nil if task hasn't finished
}

// RunSummary is a brief description of a run.
type RunSummary struct {
	RunID     string
	JobName   string
	Status    RunStatus
	Replicas  int
	Binary    string    // executable path from job spec
	Package   string    // package name from job spec
	StartedAt time.Time // zero if run hasn't started yet
	Tasks     []TaskSummary
}

// PackageInfo pairs a package name with its content hash.
type PackageInfo struct {
	Name string
	Hash string
}

// GetStatusRequest is the argument to the GetStatus RPC.
type GetStatusRequest struct{}

// GetStatusReply is a point-in-time view of the datacenter.
type GetStatusReply struct {
	Machines         []MachineInfo
	MachineJobCounts map[string]int
	Packages         []PackageInfo
	Runs             []RunSummary
}
