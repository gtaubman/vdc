package main

import (
	"archive/tar"
	"bytes"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
	"vdc/client"
)

//go:embed webstatic/index.html
var indexHTML []byte

// sharedClient wraps a client.Client with reconnect-on-error for the status endpoint.
type sharedClient struct {
	mu     sync.Mutex
	c      *client.Client
	leader string
	port   int
}

func (sc *sharedClient) reconnect() error {
	if sc.c != nil {
		sc.c.Close()
		sc.c = nil
	}
	c, err := client.Dial(sc.leader, sc.port)
	if err != nil {
		return err
	}
	sc.c = c
	log.Printf("reconnected to leader %s:%d", sc.leader, sc.port)
	return nil
}

func (sc *sharedClient) getStatus() (interface{}, error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	snap, err := sc.c.GetStatus()
	if err != nil {
		log.Printf("GetStatus error: %v — reconnecting", err)
		if rerr := sc.reconnect(); rerr != nil {
			return nil, fmt.Errorf("reconnect: %w", rerr)
		}
		snap, err = sc.c.GetStatus()
	}
	return snap, err
}

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

	sc := &sharedClient{c: c, leader: *leader, port: *port}

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		snap, err := sc.getStatus()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(snap)
	})

	// /api/log?runID=<runID>&task=<taskNumber>&file=STDOUT|STDERR
	// Dials a fresh connection per request so a stale shared connection
	// (e.g. after a leader restart) doesn't cause it to hang.
	mux.HandleFunc("/api/log", func(w http.ResponseWriter, r *http.Request) {
		runID    := r.URL.Query().Get("runID")
		taskStr  := r.URL.Query().Get("task")
		filename := r.URL.Query().Get("file")

		taskNum, convErr := strconv.Atoi(taskStr)
		if runID == "" || filename == "" || convErr != nil {
			http.Error(w, "usage: ?runID=...&task=N&file=STDOUT|STDERR", http.StatusBadRequest)
			return
		}

		log.Printf("/api/log runID=%s task=%d file=%s — dialing leader", runID, taskNum, filename)
		lc, err := client.Dial(*leader, *port)
		if err != nil {
			log.Printf("/api/log dial error: %v", err)
			http.Error(w, "cannot reach leader: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer lc.Close()

		log.Printf("/api/log calling PullFiles")
		requestID, _, err := lc.PullFiles(runID, taskNum, filename)
		if err != nil {
			log.Printf("/api/log PullFiles error: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("/api/log PullFiles ok requestID=%s — polling", requestID)

		deadline := time.Now().Add(30 * time.Second)
		for {
			if time.Now().After(deadline) {
				log.Printf("/api/log timed out waiting for file")
				http.Error(w, "timed out waiting for file from machine", http.StatusGatewayTimeout)
				return
			}
			received, total, done, tarData, pullErrs, err := lc.GetPullResult(requestID)
			if err != nil {
				log.Printf("/api/log GetPullResult error: %v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log.Printf("/api/log poll: received=%d total=%d done=%v", received, total, done)
			if !done {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if len(pullErrs) > 0 {
				log.Printf("/api/log machine error: %s", pullErrs[0].Err)
				http.Error(w, "machine error: "+pullErrs[0].Err, http.StatusInternalServerError)
				return
			}
			content, err := firstTarEntry(tarData)
			if err != nil {
				log.Printf("/api/log extract error: %v", err)
				http.Error(w, "extract: "+err.Error(), http.StatusInternalServerError)
				return
			}
			log.Printf("/api/log success: %d bytes", len(content))
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
