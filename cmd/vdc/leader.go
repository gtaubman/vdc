package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"vdc/api"
	"vdc/server"
)

func cmdLeader(args []string) {
	fs := flag.NewFlagSet("leader", flag.ExitOnError)
	port := fs.Int("port", 8080, "port to listen on")
	packageDir := fs.String("package_dir", "vdc-packages", "directory to store packages")
	dbPath := fs.String("db", "vdc-leader.db", "SQLite database path")
	logPath := fs.String("log", "vdc-leader.log", "log file path")
	heartbeatTimeout := fs.Duration("heartbeat_timeout", 30*time.Second, "time before a silent machine is considered lost")
	fs.Parse(args)

	srv, err := server.New(server.Config{PackageDir: *packageDir, DBPath: *dbPath, LogPath: *logPath})
	if err != nil {
		fmt.Fprintf(os.Stderr, "server init: %v\n", err)
		os.Exit(1)
	}

	go func() {
		if err := srv.ListenAndServe(*port); err != nil {
			fmt.Fprintf(os.Stderr, "server error: %v\n", err)
			os.Exit(1)
		}
	}()
	go srv.RunLoop(*heartbeatTimeout, 60*time.Second)

	fmt.Printf("VDC leader listening on :%d  (packages: %s  log: %s)\n", *port, srv.PackageDir(), srv.LogPath())
	printStatus(srv)
}

func printStatus(srv *server.Server) {
	for {
		snap := srv.Status()

		fmt.Print("\033[H\033[2J") // clear screen

		fmt.Printf("Log: %s\n\n", srv.LogPath())

		// Machines
		fmt.Printf("MACHINES (%d)\n", len(snap.Machines))
		fmt.Printf("  %-36s  %8s  %8s  %14s  %s\n", "ID", "RAM", "DISK", "LAST SEEN", "JOBS RUNNING")
		fmt.Println("  " + repeat("-", 82))
		for _, m := range snap.Machines {
			fmt.Printf("  %-36s  %8s  %8s  %14s  %d\n",
				m.ID,
				formatBytes(m.Spec.RAM),
				formatBytes(m.Spec.Disk),
				formatAgo(time.Since(m.LastHeartbeat)),
				snap.MachineJobCounts[m.ID],
			)
		}
		if len(snap.Machines) == 0 {
			fmt.Println("  (none)")
		}

		// Packages
		fmt.Printf("\nPACKAGES (%d)\n", len(snap.Packages))
		for _, p := range snap.Packages {
			fmt.Printf("  %-20s  %s\n", p.Name, p.Hash)
		}
		if len(snap.Packages) == 0 {
			fmt.Println("  (none)")
		}

		// Runs
		fmt.Printf("\nRUNS (%d)\n", len(snap.Runs))
		if len(snap.Runs) == 0 {
			fmt.Println("  (none)")
		}
		for _, r := range snap.Runs {
			fmt.Printf("  %s  %-20s  %s\n",
				shortID(r.RunID),
				truncate(r.JobName, 20),
				r.Status,
			)
			tasks := r.Tasks
			truncated := r.Status == api.RunComplete && len(tasks) > 3
			if truncated {
				tasks = tasks[:3]
			}
			for _, t := range tasks {
				machine := shortID(t.MachineID)
				if t.MachineID == "" {
					machine = "(unassigned)"
				}
				fmt.Printf("    task %-4d  %-12s  %s\n", t.TaskNumber, t.Status, machine)
			}
			if truncated {
				fmt.Printf("    ... (%d more)\n", len(r.Tasks)-3)
			}
		}

		time.Sleep(2 * time.Second)
	}
}

func formatAgo(d time.Duration) string {
	d = d.Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%ds ago", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%dm ago", int(d.Hours()), int(d.Minutes())%60)
	}
}

func formatBytes(b uint64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fG", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fM", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1fK", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

func repeat(s string, n int) string {
	out := ""
	for i := 0; i < n; i++ {
		out += s
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
