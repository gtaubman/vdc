package machine

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
	"vdc/api"
	"vdc/client"
)

// Machine represents this device's participation in the datacenter.
type Machine struct {
	ID      string
	BaseDir string
	Spec    api.MachineSpec
	client  *client.Client

	fetchMu sync.Mutex
	// fetchDone is keyed by package hash. A closed channel means the fetch
	// completed successfully. A nil entry means not yet started.
	fetchDone map[string]chan struct{}
}

// Join connects to the leader, registers this machine, and creates a basedir for job execution.
func Join(host string, port int, spec api.MachineSpec) (*Machine, error) {
	c, err := client.Dial(host, port)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}

	id, err := c.RegisterMachine(spec)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("register: %w", err)
	}

	baseDir, err := os.MkdirTemp("", "vdc-machine-*")
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("create basedir: %w", err)
	}

	return &Machine{
		ID:              id,
		BaseDir:         baseDir,
		Spec:            spec,
		client:          c,
		fetchDone: make(map[string]chan struct{}),
	}, nil
}

// RunHeartbeats sends heartbeats to the leader on the given interval.
// It runs until the machine is closed; errors are reported via errFn.
func (m *Machine) RunHeartbeats(interval time.Duration, errFn func(error)) {
	for {
		time.Sleep(interval)
		if err := m.client.Heartbeat(m.ID); err != nil {
			errFn(err)
		}
	}
}

// RunCommandLoop polls the leader for commands every second and executes them.
// CmdRunBinary is launched in a goroutine so the poll loop stays live during execution.
// All other commands run synchronously to preserve ordering within a batch.
func (m *Machine) RunCommandLoop(errFn func(error)) {
	for {
		time.Sleep(1 * time.Second)
		cmds, err := m.client.GetCommand(m.ID)
		if err != nil {
			errFn(err)
			continue
		}
		for _, cmd := range cmds {
			if cmd.Type == api.CmdRunBinary {
				go func(cmd api.Command) {
					if err := m.execute(cmd); err != nil {
						errFn(err)
					}
				}(cmd)
			} else {
				if err := m.execute(cmd); err != nil {
					errFn(err)
				}
			}
		}
	}
}

func (m *Machine) execute(cmd api.Command) error {
	switch cmd.Type {
	case api.CmdFetchPackage:
		return m.fetchPackage(cmd.FetchPackage)
	case api.CmdRunBinary:
		return m.runBinary(cmd.RunBinary)
	case api.CmdSendFile:
		return m.sendFile(cmd.SendFile)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (m *Machine) fetchPackage(details *api.FetchPackageDetails) error {
	m.fetchMu.Lock()
	if done, ok := m.fetchDone[details.PackageHash]; ok {
		m.fetchMu.Unlock()
		<-done // wait for the in-progress fetch to complete
		return nil
	}
	done := make(chan struct{})
	m.fetchDone[details.PackageHash] = done
	m.fetchMu.Unlock()

	defer close(done)

	data, err := m.client.FetchPackage(details.PackageHash)
	if err != nil {
		return fmt.Errorf("fetch package %q: %w", details.PackageName, err)
	}
	destDir := filepath.Join(m.BaseDir, "packages", details.PackageName)
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("mkdir %q: %w", destDir, err)
	}
	if err := extractTar(data, destDir); err != nil {
		return fmt.Errorf("extract package %q: %w", details.PackageName, err)
	}
	return nil
}

func (m *Machine) runBinary(details *api.RunBinaryDetails) error {
	if err := m.client.ReportTaskStatus(details.RunID, api.TaskRunning); err != nil {
		return fmt.Errorf("report task running: %w", err)
	}

	runDir := filepath.Join(m.BaseDir, "runs", details.RunID)
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return fmt.Errorf("mkdir run dir: %w", err)
	}

	stdout, err := os.Create(filepath.Join(runDir, "STDOUT"))
	if err != nil {
		return err
	}
	defer stdout.Close()

	stderr, err := os.Create(filepath.Join(runDir, "STDERR"))
	if err != nil {
		return err
	}
	defer stderr.Close()

	binaryPath := filepath.Join(m.BaseDir, "packages", details.PackageName, details.BinaryPath)
	cmd := exec.Command(binaryPath, details.Args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	runErr := cmd.Run()

	status := api.TaskComplete
	if runErr != nil {
		status = api.TaskFailed
	}
	if err := m.client.ReportTaskStatus(details.RunID, status); err != nil {
		return fmt.Errorf("report task complete: %w", err)
	}
	return runErr
}

func (m *Machine) sendFile(details *api.SendFileDetails) error {
	path := filepath.Join(m.BaseDir, "runs", details.TaskRunID, details.Filename)
	data, err := os.ReadFile(path)
	if err != nil {
		return m.client.UploadFile(details.RequestID, details.TaskNumber, details.Filename, nil, err.Error())
	}
	return m.client.UploadFile(details.RequestID, details.TaskNumber, details.Filename, data, "")
}

func extractTar(data []byte, destDir string) error {
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		path := filepath.Join(destDir, hdr.Name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
		if err != nil {
			return err
		}
		_, err = io.Copy(f, tr)
		f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close cleans up the machine's basedir and closes the leader connection.
func (m *Machine) Close() error {
	os.RemoveAll(m.BaseDir)
	return m.client.Close()
}
