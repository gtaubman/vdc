package main

import (
	"archive/tar"
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
	"vdc/client"
)

//go:embed webstatic/index.html
var indexHTML []byte

func cmdWeb(args []string) {
	fs := flag.NewFlagSet("web", flag.ExitOnError)
	leader  := fs.String("leader",   "localhost", "leader host")
	port    := fs.Int("port",    8080,        "leader RPC port")
	webPort := fs.Int("web-port", 8081,        "web UI HTTP port")
	fs.Parse(args)

	c, err := client.Dial(*leader, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		snap, err := c.GetStatus()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	})

	// /api/log?runID=<runID>&task=<taskNumber>&file=STDOUT|STDERR
	// Issues a pull for a single task file and streams the content back.
	mux.HandleFunc("/api/log", func(w http.ResponseWriter, r *http.Request) {
		runID    := r.URL.Query().Get("runID")
		taskStr  := r.URL.Query().Get("task")
		filename := r.URL.Query().Get("file")

		taskNum, convErr := strconv.Atoi(taskStr)
		if runID == "" || filename == "" || convErr != nil {
			http.Error(w, "usage: ?runID=...&task=N&file=STDOUT|STDERR", http.StatusBadRequest)
			return
		}

		requestID, _, err := c.PullFiles(runID, taskNum, filename)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		deadline := time.Now().Add(30 * time.Second)
		for {
			if time.Now().After(deadline) {
				http.Error(w, "timed out waiting for file from machine", http.StatusGatewayTimeout)
				return
			}
			_, _, done, tarData, pullErrs, err := c.GetPullResult(requestID)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if !done {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if len(pullErrs) > 0 {
				http.Error(w, "machine error: "+pullErrs[0].Err, http.StatusInternalServerError)
				return
			}
			content, err := firstTarEntry(tarData)
			if err != nil {
				http.Error(w, "extract: "+err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.Write(content)
			return
		}
	})

	addr := fmt.Sprintf(":%d", *webPort)
	fmt.Printf("VDC web UI → http://localhost%s  (leader: %s:%d)\n", addr, *leader, *port)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "web server: %v\n", err)
		os.Exit(1)
	}
}

func firstTarEntry(data []byte) ([]byte, error) {
	tr := tar.NewReader(bytes.NewReader(data))
	if _, err := tr.Next(); err != nil {
		return nil, fmt.Errorf("empty archive: %w", err)
	}
	return io.ReadAll(tr)
}
