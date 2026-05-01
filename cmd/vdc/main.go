package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "leader":
		cmdLeader(os.Args[2:])
	case "join":
		cmdJoin(os.Args[2:])
	case "run":
		cmdRun(os.Args[2:])
	case "pull":
		cmdPull(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: vdc <command> [flags]")
	fmt.Fprintln(os.Stderr, "commands: leader, join, run, pull")
}
