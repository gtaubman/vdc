package server

import (
	"testing"
	"time"
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

// TestLostMachineTasksRescheduledViaHeartbeat exercises the full heartbeat
// detection path: two machines each receive one task; one machine's heartbeat
// is backdated so detectAndRescheduleLost declares it lost; the surviving
// machine should then hold both tasks.
func TestLostMachineTasksRescheduledViaHeartbeat(t *testing.T) {
	srv := newTestServer(t)

	// Two machines, each with enough RAM for two tasks so the survivor can
	// absorb the rescheduled task once the other machine is declared lost.
	m1 := mustRegister(t, srv, api.MachineSpec{RAM: 512 << 20, Disk: 10 << 30})
	m2 := mustRegister(t, srv, api.MachineSpec{RAM: 512 << 20, Disk: 10 << 30})

	spec := jobspec.JobSpec{
		Name:     "lost-heartbeat-job",
		Packages: []jobspec.Package{{Name: "pkg", Files: []string{"bin"}}},
		Binary:   jobspec.Binary{Package: "pkg", Path: "bin"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(256 << 20),
			Disk: bytesize.ByteSize(1 << 30),
		},
		Replicas: 2,
	}
	runID := mustSubmit(t, srv, spec)
	srv.scheduleNewRuns()

	// Confirm one task landed on each machine.
	taskCount := func(machineID string) int {
		t.Helper()
		var n int
		srv.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE run_id = ? AND machine_id = ?`, runID, machineID).Scan(&n)
		return n
	}
	if taskCount(m1) != 1 || taskCount(m2) != 1 {
		t.Fatalf("initial distribution: m1=%d m2=%d, want 1 each", taskCount(m1), taskCount(m2))
	}

	// Drain the initial commands for both machines (simulate them polling once)
	// so the queue is empty before m1 goes silent.
	mustGetCommand(t, srv, m1)
	mustGetCommand(t, srv, m2)

	// Backdate m1's heartbeat so it looks stale, leave m2 current.
	if _, err := srv.db.Exec(`UPDATE machines SET last_heartbeat = ? WHERE id = ?`,
		time.Now().Add(-2*time.Minute).UnixNano(), m1); err != nil {
		t.Fatal(err)
	}

	// detectAndRescheduleLost with a 1-minute timeout: m1 is 2 minutes silent → lost.
	srv.detectAndRescheduleLost(1 * time.Minute)

	// m1 must be removed from the machines table.
	var machineExists int
	srv.db.QueryRow(`SELECT COUNT(*) FROM machines WHERE id = ?`, m1).Scan(&machineExists)
	if machineExists != 0 {
		t.Error("lost machine was not removed from machines table")
	}

	// The rescheduled task must now sit on m2.
	if got := taskCount(m2); got != 2 {
		t.Errorf("surviving machine task count = %d, want 2", got)
	}

	// m2's command queue must contain FetchPackage + RunBinary for the new task.
	var fetchCount, runCount int
	for _, cmd := range mustGetCommand(t, srv, m2) {
		switch cmd.Type {
		case api.CmdFetchPackage:
			fetchCount++
		case api.CmdRunBinary:
			runCount++
		}
	}
	if fetchCount != 1 || runCount != 1 {
		t.Errorf("rescheduled commands: FetchPackage=%d RunBinary=%d, want 1 and 1", fetchCount, runCount)
	}
}

// TestIncrementalScheduling verifies that tasks are assigned as capacity
// becomes available rather than all at once.
//
// Setup: 2 machines × 1MB each, 5 tasks × 500KB each.
// Round 1: 4 tasks fit (2 per machine); 1 task stays unassigned.
// After one task completes, round 2: the freed slot allows the 5th task.
func TestIncrementalScheduling(t *testing.T) {
	srv := newTestServer(t)

	mustRegister(t, srv, api.MachineSpec{RAM: 1 << 20, Disk: 1 << 20})
	mustRegister(t, srv, api.MachineSpec{RAM: 1 << 20, Disk: 1 << 20})

	spec := jobspec.JobSpec{
		Name:     "sleep-job",
		Packages: []jobspec.Package{{Name: "pkg", Files: []string{"sleep"}}},
		Binary:   jobspec.Binary{Package: "pkg", Path: "sleep"},
		Args:     []string{"1"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(500 << 10),
			Disk: bytesize.ByteSize(500 << 10),
		},
		Replicas: 5,
	}
	runID := mustSubmit(t, srv, spec)
	srv.scheduleNewRuns()

	countTasks := func(whereClause string) int {
		t.Helper()
		var n int
		srv.db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE run_id = ? AND `+whereClause, runID).Scan(&n)
		return n
	}

	// Round 1: each machine holds 2 tasks (2 × 500KB = 1000KB < 1MB); 1 left over.
	if got := countTasks("machine_id IS NOT NULL"); got != 4 {
		t.Fatalf("after first schedule: assigned = %d, want 4", got)
	}
	if got := countTasks("machine_id IS NULL"); got != 1 {
		t.Fatalf("after first schedule: unassigned = %d, want 1", got)
	}

	// Simulate one task completing — frees 500KB on its machine.
	var doneTaskRunID string
	srv.db.QueryRow(`SELECT task_run_id FROM tasks WHERE run_id = ? AND machine_id IS NOT NULL LIMIT 1`, runID).
		Scan(&doneTaskRunID)
	if err := srv.ReportTaskStatus(
		&api.ReportTaskStatusRequest{TaskRunID: doneTaskRunID, Status: api.TaskComplete},
		&api.ReportTaskStatusReply{},
	); err != nil {
		t.Fatal(err)
	}

	// Round 2: 500KB is now free; the remaining task should be assigned.
	srv.scheduleNewRuns()

	if got := countTasks("machine_id IS NOT NULL"); got != 5 {
		t.Fatalf("after second schedule: assigned = %d, want 5", got)
	}
	if got := countTasks("machine_id IS NULL"); got != 0 {
		t.Fatalf("after second schedule: unassigned = %d, want 0", got)
	}

	// With all tasks assigned the run should advance to Pending.
	var runStatus int
	srv.db.QueryRow(`SELECT status FROM runs WHERE run_id = ?`, runID).Scan(&runStatus)
	if api.RunStatus(runStatus) != api.RunPending {
		t.Errorf("run status = %v, want Pending", api.RunStatus(runStatus))
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
