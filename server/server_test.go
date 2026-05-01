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
			"running beats pending",
			[]*taskRecord{{Status: api.TaskRunning}, {Status: api.TaskPending}},
			api.RunRunning,
		},
		{
			"all complete",
			[]*taskRecord{{Status: api.TaskComplete}, {Status: api.TaskComplete}, {Status: api.TaskComplete}},
			api.RunComplete,
		},
		{
			"any failed with rest complete",
			[]*taskRecord{{Status: api.TaskComplete}, {Status: api.TaskFailed}, {Status: api.TaskComplete}},
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

// TestServerEnqueuesOneRunBinaryPerReplica verifies that a machine assigned
// multiple replicas receives one RunBinary command per replica.
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
