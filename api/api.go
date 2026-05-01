package api

import (
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
	Spec MachineSpec
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
type TaskStatus string

const (
	TaskPending  TaskStatus = "pending"
	TaskRunning  TaskStatus = "running"
	TaskComplete TaskStatus = "complete"
	TaskFailed   TaskStatus = "failed"
)

// RunStatus is the lifecycle state of an entire job run.
type RunStatus string

const (
	RunPending  RunStatus = "pending"
	RunRunning  RunStatus = "running"
	RunComplete RunStatus = "complete"
	RunFailed   RunStatus = "failed"
)

// ReportTaskStatusRequest is sent by a machine to update its task's status.
type ReportTaskStatusRequest struct {
	TaskRunID string
	Status    TaskStatus
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
)

// FetchPackageDetails carries the parameters for a FetchPackage command.
type FetchPackageDetails struct {
	PackageName string
	PackageHash string
}

// RunBinaryDetails carries the parameters for a RunBinary command.
type RunBinaryDetails struct {
	RunID       string   // unique ID for this task execution; used as the run directory name
	PackageName string   // package containing the binary
	BinaryPath  string   // path to the binary within the package
	Args        []string // commandline arguments
}

// SendFileDetails carries the parameters for a SendFile command.
type SendFileDetails struct {
	RequestID  string // pull request to satisfy
	TaskRunID  string // run directory on this machine
	TaskNumber int
	Filename   string
}

// Command is a unit of work dispatched to a machine.
type Command struct {
	Type         CommandType
	FetchPackage *FetchPackageDetails // non-nil when Type == CmdFetchPackage
	RunBinary    *RunBinaryDetails    // non-nil when Type == CmdRunBinary
	SendFile     *SendFileDetails     // non-nil when Type == CmdSendFile
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
	Total     int // number of files to expect
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

