package server

import (
	"testing"
	"vdc/api"
	"vdc/bytesize"
	"vdc/jobspec"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	srv, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

func mustRegister(t *testing.T, srv *Server, spec api.MachineSpec) string {
	t.Helper()
	reply := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{Spec: spec}, reply); err != nil {
		t.Fatal(err)
	}
	return reply.MachineID
}

func mustSubmit(t *testing.T, srv *Server, spec jobspec.JobSpec) string {
	t.Helper()
	hashes := make(map[string]string, len(spec.Packages))
	for _, pkg := range spec.Packages {
		hashes[pkg.Name] = pkg.Name + "-hash"
	}
	reply := &api.SubmitJobReply{}
	if err := srv.SubmitJob(&api.SubmitJobRequest{Spec: spec, PackageHashes: hashes}, reply); err != nil {
		t.Fatal(err)
	}
	return reply.RunID
}

func mustGetCommand(t *testing.T, srv *Server, machineID string) []api.Command {
	t.Helper()
	reply := &api.GetCommandReply{}
	if err := srv.GetCommand(&api.GetCommandRequest{MachineID: machineID}, reply); err != nil {
		t.Fatal(err)
	}
	return reply.Commands
}

func TestComputeRunStatus(t *testing.T) {
	tests := []struct {
		name     string
		statuses []api.TaskStatus
		want     api.RunStatus
	}{
		{
			"all pending",
			[]api.TaskStatus{api.TaskPending, api.TaskPending},
			api.RunPending,
		},
		{
			"fetching counts as pending",
			[]api.TaskStatus{api.TaskFetching, api.TaskPending},
			api.RunPending,
		},
		{
			"running beats pending",
			[]api.TaskStatus{api.TaskRunning, api.TaskPending},
			api.RunRunning,
		},
		{
			"all complete",
			[]api.TaskStatus{api.TaskComplete, api.TaskComplete},
			api.RunComplete,
		},
		{
			"all cancelled",
			[]api.TaskStatus{api.TaskCancelled, api.TaskCancelled},
			api.RunCancelled,
		},
		{
			"some complete some failed is partial failure",
			[]api.TaskStatus{api.TaskComplete, api.TaskFailed, api.TaskComplete},
			api.RunPartialFailure,
		},
		{
			"all failed",
			[]api.TaskStatus{api.TaskFailed, api.TaskFailed},
			api.RunFailed,
		},
		{
			"running beats failed",
			[]api.TaskStatus{api.TaskRunning, api.TaskFailed},
			api.RunRunning,
		},
		{
			"pending beats failed",
			[]api.TaskStatus{api.TaskPending, api.TaskFailed},
			api.RunPending,
		},
		{
			"lost counts as pending (awaiting reschedule)",
			[]api.TaskStatus{api.TaskLost, api.TaskComplete},
			api.RunPending,
		},
		{
			"all lost",
			[]api.TaskStatus{api.TaskLost, api.TaskLost},
			api.RunPending,
		},
		{
			"some complete some lost is pending since they may be rescheduled",
			[]api.TaskStatus{api.TaskComplete, api.TaskLost},
			api.RunPending,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeRunStatus(tt.statuses)
			if got != tt.want {
				t.Errorf("computeRunStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSubmitJobScheduledByLoop(t *testing.T) {
	srv := newTestServer(t)
	machineID := mustRegister(t, srv, api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30})

	spec := jobspec.JobSpec{
		Name:     "test-job",
		Packages: []jobspec.Package{{Name: "mypkg", Files: []string{"somefile"}}},
		Binary:   jobspec.Binary{Package: "mypkg", Path: "bin"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(64 << 20),
			Disk: bytesize.ByteSize(64 << 20),
		},
		Replicas: 3,
	}
	mustSubmit(t, srv, spec)
	srv.scheduleNewRuns()

	cmds := mustGetCommand(t, srv, machineID)
	var runCount int
	for _, cmd := range cmds {
		if cmd.Type == api.CmdRunBinary {
			runCount++
		}
	}
	if runCount != spec.Replicas {
		t.Errorf("RunBinary count = %d, want %d", runCount, spec.Replicas)
	}
}

func TestLostTaskRescheduledOnNewMachine(t *testing.T) {
	srv := newTestServer(t)
	m1 := mustRegister(t, srv, api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30})
	m2 := mustRegister(t, srv, api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30})

	spec := jobspec.JobSpec{
		Name:     "test-job",
		Packages: []jobspec.Package{{Name: "pkg", Files: []string{"f"}}},
		Binary:   jobspec.Binary{Package: "pkg", Path: "bin"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(64 << 20),
			Disk: bytesize.ByteSize(64 << 20),
		},
		Replicas: 1,
	}
	runID := mustSubmit(t, srv, spec)
	srv.scheduleNewRuns()

	var originalMachine, originalTaskRunID string
	srv.db.QueryRow(`SELECT machine_id, task_run_id FROM tasks WHERE run_id = ?`, runID).
		Scan(&originalMachine, &originalTaskRunID)

	if err := srv.handleLostMachine(originalMachine); err != nil {
		t.Fatal(err)
	}
	if err := srv.rescheduleLostTasks(); err != nil {
		t.Fatal(err)
	}

	var newMachine, newTaskRunID string
	var taskStatus int
	srv.db.QueryRow(`SELECT machine_id, task_run_id, status FROM tasks WHERE run_id = ?`, runID).
		Scan(&newMachine, &newTaskRunID, &taskStatus)

	if api.TaskStatus(taskStatus) != api.TaskPending {
		t.Errorf("task status = %v, want Pending", api.TaskStatus(taskStatus))
	}
	if newTaskRunID == originalTaskRunID {
		t.Error("task run ID was not updated after reschedule")
	}

	otherMachine := m2
	if originalMachine == m2 {
		otherMachine = m1
	}
	if newMachine != otherMachine {
		t.Errorf("task assigned to %q, want %q", newMachine, otherMachine)
	}

	var fetchCount, runCount int
	for _, cmd := range mustGetCommand(t, srv, otherMachine) {
		switch cmd.Type {
		case api.CmdFetchPackage:
			fetchCount++
		case api.CmdRunBinary:
			runCount++
		}
	}
	if fetchCount != 1 || runCount != 1 {
		t.Errorf("rescheduled machine queue: FetchPackage=%d RunBinary=%d, want 1 and 1", fetchCount, runCount)
	}
}

func TestReconnectingMachineGetsCancelForRescheduledTask(t *testing.T) {
	srv := newTestServer(t)
	mustRegister(t, srv, api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30})
	mustRegister(t, srv, api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30})

	spec := jobspec.JobSpec{
		Name:     "cancel-test",
		Packages: []jobspec.Package{{Name: "pkg", Files: []string{"f"}}},
		Binary:   jobspec.Binary{Package: "pkg", Path: "bin"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(64 << 20),
			Disk: bytesize.ByteSize(64 << 20),
		},
		Replicas: 1,
	}
	runID := mustSubmit(t, srv, spec)
	srv.scheduleNewRuns()

	var originalMachine, oldTaskRunID string
	srv.db.QueryRow(`SELECT machine_id, task_run_id FROM tasks WHERE run_id = ?`, runID).
		Scan(&originalMachine, &oldTaskRunID)

	if err := srv.handleLostMachine(originalMachine); err != nil {
		t.Fatal(err)
	}
	if err := srv.rescheduleLostTasks(); err != nil {
		t.Fatal(err)
	}

	// Original machine reconnects and reports the old task run ID.
	reconnectReply := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec:          api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
		ActiveTaskIDs: []string{oldTaskRunID},
	}, reconnectReply); err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, cmd := range mustGetCommand(t, srv, reconnectReply.MachineID) {
		if cmd.Type == api.CmdCancelTask && cmd.CancelTask.RunID == oldTaskRunID {
			found = true
		}
	}
	if !found {
		t.Error("reconnecting machine did not receive CancelTask for rescheduled run ID")
	}
}
