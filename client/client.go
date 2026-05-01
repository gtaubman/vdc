package client

import (
	"fmt"
	"net/rpc"
	"vdc/api"
	"vdc/jobspec"
)

// Client is a VDC leader client.
type Client struct {
	rpc *rpc.Client
}

// Dial connects to a VDC leader at host:port.
func Dial(host string, port int) (*Client, error) {
	c, err := rpc.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("rpc.Dial: %w", err)
	}
	return &Client{rpc: c}, nil
}

// RegisterMachine registers this machine with the leader.
// machineID should be the machine's existing ID on reconnect, or "" on first join.
// activeTaskIDs should list any task run IDs the machine was executing before a disconnect.
func (c *Client) RegisterMachine(machineID string, spec api.MachineSpec, activeTaskIDs []string) (string, error) {
	args := &api.RegisterRequest{MachineID: machineID, Spec: spec, ActiveTaskIDs: activeTaskIDs}
	reply := &api.RegisterReply{}
	if err := c.rpc.Call("Server.RegisterMachine", args, reply); err != nil {
		return "", err
	}
	return reply.MachineID, nil
}

// Heartbeat sends a heartbeat to the leader.
func (c *Client) Heartbeat(machineID string) error {
	args := &api.HeartbeatRequest{MachineID: machineID}
	reply := &api.HeartbeatReply{}
	return c.rpc.Call("Server.Heartbeat", args, reply)
}

// HasPackage reports whether the server already has a package with the given hash.
func (c *Client) HasPackage(hash string) (bool, error) {
	args := &api.HasPackageRequest{Hash: hash}
	reply := &api.HasPackageReply{}
	if err := c.rpc.Call("Server.HasPackage", args, reply); err != nil {
		return false, err
	}
	return reply.Exists, nil
}

// UploadPackage sends a tar archive to the server.
func (c *Client) UploadPackage(hash string, data []byte) error {
	args := &api.UploadPackageRequest{Hash: hash, Data: data}
	reply := &api.UploadPackageReply{}
	return c.rpc.Call("Server.UploadPackage", args, reply)
}

// SubmitJob submits a job to the server and returns the run ID.
func (c *Client) SubmitJob(spec jobspec.JobSpec, packageHashes map[string]string) (string, error) {
	args := &api.SubmitJobRequest{Spec: spec, PackageHashes: packageHashes}
	reply := &api.SubmitJobReply{}
	if err := c.rpc.Call("Server.SubmitJob", args, reply); err != nil {
		return "", err
	}
	return reply.RunID, nil
}

// GetCommand retrieves and clears the pending command queue for a machine.
func (c *Client) GetCommand(machineID string) ([]api.Command, error) {
	args := &api.GetCommandRequest{MachineID: machineID}
	reply := &api.GetCommandReply{}
	if err := c.rpc.Call("Server.GetCommand", args, reply); err != nil {
		return nil, err
	}
	return reply.Commands, nil
}

// FetchPackage downloads a package tar archive from the server.
func (c *Client) FetchPackage(hash string) ([]byte, error) {
	args := &api.FetchPackageRequest{Hash: hash}
	reply := &api.FetchPackageReply{}
	if err := c.rpc.Call("Server.FetchPackage", args, reply); err != nil {
		return nil, err
	}
	return reply.Data, nil
}

// PullFiles asks the server to collect a file from one or all tasks of a run.
// taskNumber -1 means all tasks.
func (c *Client) PullFiles(runID string, taskNumber int, filename string) (requestID string, total int, err error) {
	args := &api.PullFilesRequest{RunID: runID, TaskNumber: taskNumber, Filename: filename}
	reply := &api.PullFilesReply{}
	if err := c.rpc.Call("Server.PullFiles", args, reply); err != nil {
		return "", 0, err
	}
	return reply.RequestID, reply.Total, nil
}

// UploadFile delivers a file to the server on behalf of a machine.
func (c *Client) UploadFile(requestID string, taskNumber int, filename string, data []byte, errMsg string) error {
	args := &api.UploadFileRequest{
		RequestID:  requestID,
		TaskNumber: taskNumber,
		Filename:   filename,
		Data:       data,
		Err:        errMsg,
	}
	reply := &api.UploadFileReply{}
	return c.rpc.Call("Server.UploadFile", args, reply)
}

// GetPullResult polls the status of a pull request.
func (c *Client) GetPullResult(requestID string) (received, total int, done bool, tarData []byte, errors []api.PullFileError, err error) {
	args := &api.GetPullResultRequest{RequestID: requestID}
	reply := &api.GetPullResultReply{}
	if err := c.rpc.Call("Server.GetPullResult", args, reply); err != nil {
		return 0, 0, false, nil, nil, err
	}
	return reply.Received, reply.Total, reply.Done, reply.TarData, reply.Errors, nil
}

// ReportTaskStatus reports a task's lifecycle state to the server.
func (c *Client) ReportTaskStatus(taskRunID string, status api.TaskStatus) error {
	args := &api.ReportTaskStatusRequest{TaskRunID: taskRunID, Status: status}
	reply := &api.ReportTaskStatusReply{}
	return c.rpc.Call("Server.ReportTaskStatus", args, reply)
}

// GetRunStatus returns the current lifecycle state of a run.
func (c *Client) GetRunStatus(runID string) (api.RunStatus, error) {
	args := &api.GetRunStatusRequest{RunID: runID}
	reply := &api.GetRunStatusReply{}
	if err := c.rpc.Call("Server.GetRunStatus", args, reply); err != nil {
		return 0, err
	}
	return reply.Status, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.rpc.Close()
}
