package main

import (
	"archive/tar"
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
	"vdc/client"
)

func cmdPull(args []string) {
	fs := flag.NewFlagSet("pull", flag.ExitOnError)
	leader := fs.String("leader", "localhost", "leader host")
	port := fs.Int("port", 8080, "leader port")
	fs.Parse(args)

	if fs.NArg() < 2 {
		fmt.Fprintln(os.Stderr, "usage: vdc pull [flags] <runid>[/<tasknumber>] <filename>")
		os.Exit(1)
	}

	runID, taskNumber := parseRunSpec(fs.Arg(0))
	filename := fs.Arg(1)

	c, err := client.Dial(*leader, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	status, err := c.GetRunStatus(runID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get run status: %v\n", err)
		os.Exit(1)
	}
	if status == "pending" || status == "running" {
		if !confirm(fmt.Sprintf("Run is still %s. Pull anyway?", status)) {
			fmt.Println("Aborted.")
			return
		}
	}

	requestID, total, err := c.PullFiles(runID, taskNumber, filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pull: %v\n", err)
		os.Exit(1)
	}

	taskDesc := "all tasks"
	if taskNumber >= 0 {
		taskDesc = fmt.Sprintf("task %d", taskNumber)
	}
	fmt.Printf("Pulling %q from run %s (%s)...\n", filename, shortID(runID), taskDesc)

	var tarData []byte
	for {
		time.Sleep(500 * time.Millisecond)
		received, _, done, data, errs, err := c.GetPullResult(requestID)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nerror polling: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("  %d/%d received\r", received, total)
		if done {
			fmt.Printf("  %d/%d received\n", received, total)
			tarData = data
			for _, e := range errs {
				fmt.Fprintf(os.Stderr, "  task %d error: %s\n", e.TaskNumber, e.Err)
			}
			break
		}
	}

	written, err := extractPullTar(tarData)
	if err != nil {
		fmt.Fprintf(os.Stderr, "extract: %v\n", err)
		os.Exit(1)
	}
	if len(written) > 0 {
		fmt.Printf("Wrote: %s\n", strings.Join(written, ", "))
	}
}

// parseRunSpec splits "runid/tasknumber" into its parts.
// taskNumber is -1 if no task number was given.
func parseRunSpec(s string) (runID string, taskNumber int) {
	if i := strings.LastIndex(s, "/"); i >= 0 {
		if n, err := strconv.Atoi(s[i+1:]); err == nil {
			return s[:i], n
		}
	}
	return s, -1
}

func extractPullTar(data []byte) ([]string, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	var written []string
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
		f, err := os.Create(hdr.Name)
		if err != nil {
			return written, err
		}
		_, err = io.Copy(f, tr)
		f.Close()
		if err != nil {
			return written, err
		}
		written = append(written, hdr.Name)
	}
	return written, nil
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
