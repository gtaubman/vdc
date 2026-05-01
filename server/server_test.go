package server

import (
	"testing"
	"vdc/api"
	"vdc/bytesize"
	"vdc/jobspec"
)

func TestComputeRunStatus(t *testing.T) {
	tests := []struct {
		name  string
		tasks []*taskRecord
		want  api.RunStatus
	}{
		{
			"all pending",
			[]*taskRecord{{Status: api.TaskPending}, {Status: api.TaskPending}},
			api.RunPending,
		},
		{
			"fetching counts as pending",
			[]*taskRecord{{Status: api.TaskFetching}, {Status: api.TaskPending}},
			api.RunPending,
		},
		{
			"running beats pending",
			[]*taskRecord{{Status: api.TaskRunning}, {Status: api.TaskPending}},
			api.RunRunning,
		},
		{
			"all complete",
			[]*taskRecord{{Status: api.TaskComplete}, {Status: api.TaskComplete}},
			api.RunComplete,
		},
		{
			"all cancelled",
			[]*taskRecord{{Status: api.TaskCancelled}, {Status: api.TaskCancelled}},
			api.RunCancelled,
		},
		{
			"some complete some failed is partial failure",
			[]*taskRecord{{Status: api.TaskComplete}, {Status: api.TaskFailed}, {Status: api.TaskComplete}},
			api.RunPartialFailure,
		},
		{
			"all failed",
			[]*taskRecord{{Status: api.TaskFailed}, {Status: api.TaskFailed}},
			api.RunFailed,
		},
		{
			"running beats failed",
			[]*taskRecord{{Status: api.TaskRunning}, {Status: api.TaskFailed}},
			api.RunRunning,
		},
		{
			"pending beats failed",
			[]*taskRecord{{Status: api.TaskPending}, {Status: api.TaskFailed}},
			api.RunPending,
		},
		{
			"lost counts as pending (awaiting reschedule)",
			[]*taskRecord{{Status: api.TaskLost}, {Status: api.TaskComplete}},
			api.RunPending,
		},
		{
			"all lost",
			[]*taskRecord{{Status: api.TaskLost}, {Status: api.TaskLost}},
			api.RunPending,
		},
		{
			"some complete some lost is partial failure once rescheduled tasks finish",
			// This state is reached after lost tasks fail their reschedule attempt.
			// For now lost still shows as pending since they may be rescheduled.
			[]*taskRecord{{Status: api.TaskComplete}, {Status: api.TaskLost}},
			api.RunPending,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeRunStatus(tt.tasks)
			if got != tt.want {
				t.Errorf("computeRunStatus() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestServerEnqueuesOneRunBinaryPerReplica(t *testing.T) {
	srv, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	regReply := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec: api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
	}, regReply); err != nil {
		t.Fatal(err)
	}
	machineID := regReply.MachineID

	spec := jobspec.JobSpec{
		Name: "test-job",
		Packages: []jobspec.Package{
			{Name: "mypkg", Files: []string{"somefile"}},
		},
		Binary: jobspec.Binary{Package: "mypkg", Path: "bin"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(64 << 20),
			Disk: bytesize.ByteSize(64 << 20),
		},
		Replicas: 3,
	}

	submitReply := &api.SubmitJobReply{}
	if err := srv.SubmitJob(&api.SubmitJobRequest{
		Spec:          spec,
		PackageHashes: map[string]string{"mypkg": "abc123hash"},
	}, submitReply); err != nil {
		t.Fatal(err)
	}

	var runCount int
	for _, cmd := range srv.machineQueues[machineID] {
		if cmd.Type == api.CmdRunBinary {
			runCount++
		}
	}
	if runCount != spec.Replicas {
		t.Errorf("RunBinary count = %d, want %d", runCount, spec.Replicas)
	}
}

func TestLostTaskRescheduledOnNewMachine(t *testing.T) {
	srv, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	// Register two machines; first gets the job, then goes lost.
	reg1 := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec: api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
	}, reg1); err != nil {
		t.Fatal(err)
	}
	reg2 := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec: api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
	}, reg2); err != nil {
		t.Fatal(err)
	}

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
	submitReply := &api.SubmitJobReply{}
	if err := srv.SubmitJob(&api.SubmitJobRequest{
		Spec:          spec,
		PackageHashes: map[string]string{"pkg": "hash1"},
	}, submitReply); err != nil {
		t.Fatal(err)
	}

	// Find which machine got the task.
	srv.mu.Lock()
	run := srv.runs[submitReply.RunID]
	task := run.Tasks[0]
	originalMachine := task.MachineID
	originalTaskRunID := task.TaskRunID
	srv.mu.Unlock()

	// Simulate the machine going lost.
	srv.mu.Lock()
	srv.handleLostMachine(originalMachine)
	srv.mu.Unlock()

	if task.Status != api.TaskLost {
		t.Fatalf("task status = %v, want Lost", task.Status)
	}

	// Reschedule onto the surviving machine.
	srv.mu.Lock()
	srv.rescheduleLostTasks()
	srv.mu.Unlock()

	if task.Status != api.TaskPending {
		t.Fatalf("task status after reschedule = %v, want Pending", task.Status)
	}
	if task.TaskRunID == originalTaskRunID {
		t.Error("task run ID was not updated after reschedule")
	}
	otherMachine := reg1.MachineID
	if originalMachine == reg1.MachineID {
		otherMachine = reg2.MachineID
	}
	if task.MachineID != otherMachine {
		t.Errorf("task assigned to %q, want %q", task.MachineID, otherMachine)
	}

	// The surviving machine should have received FetchPackage + RunBinary.
	var fetchCount, runCount int
	for _, cmd := range srv.machineQueues[otherMachine] {
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
	srv, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	reg1 := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec: api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
	}, reg1); err != nil {
		t.Fatal(err)
	}
	reg2 := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec: api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
	}, reg2); err != nil {
		t.Fatal(err)
	}

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
	submitReply := &api.SubmitJobReply{}
	if err := srv.SubmitJob(&api.SubmitJobRequest{
		Spec:          spec,
		PackageHashes: map[string]string{"pkg": "hash1"},
	}, submitReply); err != nil {
		t.Fatal(err)
	}

	srv.mu.Lock()
	run := srv.runs[submitReply.RunID]
	task := run.Tasks[0]
	originalMachine := task.MachineID
	oldTaskRunID := task.TaskRunID
	srv.handleLostMachine(originalMachine)
	srv.rescheduleLostTasks()
	srv.mu.Unlock()

	// Original machine reconnects and reports the old task run ID.
	reconnectReply := &api.RegisterReply{}
	if err := srv.RegisterMachine(&api.RegisterRequest{
		Spec:          api.MachineSpec{RAM: 1 << 30, Disk: 10 << 30},
		ActiveTaskIDs: []string{oldTaskRunID},
	}, reconnectReply); err != nil {
		t.Fatal(err)
	}

	// Its queue should contain a CancelTask for the old run ID.
	var found bool
	for _, cmd := range srv.machineQueues[reconnectReply.MachineID] {
		if cmd.Type == api.CmdCancelTask && cmd.CancelTask.RunID == oldTaskRunID {
			found = true
		}
	}
	if !found {
		t.Error("reconnecting machine did not receive CancelTask for rescheduled run ID")
	}
}
