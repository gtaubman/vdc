package main

import (
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
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

	addr := fmt.Sprintf(":%d", *webPort)
	fmt.Printf("VDC web UI → http://localhost%s  (leader: %s:%d)\n", addr, *leader, *port)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintf(os.Stderr, "web server: %v\n", err)
		os.Exit(1)
	}
}
