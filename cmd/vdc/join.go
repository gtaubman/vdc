package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
	"vdc/api"
	"vdc/machine"
)

func cmdJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	leader := fs.String("leader", "localhost", "leader host")
	port := fs.Int("port", 8080, "leader port")

	ram := byteSize(512 << 20)
	disk := byteSize(5 << 30)
	fs.Var(&ram, "ram", "RAM to contribute (e.g. 512M, 2G)")
	fs.Var(&disk, "disk", "disk to contribute (e.g. 5G, 1T)")

	heartbeat := fs.Duration("heartbeat_duration", 10*time.Second, "heartbeat interval")
	fs.Parse(args)

	m, err := machine.Join(*leader, *port, api.MachineSpec{RAM: uint64(ram), Disk: uint64(disk)})
	if err != nil {
		fmt.Fprintf(os.Stderr, "join: %v\n", err)
		os.Exit(1)
	}
	defer m.Close()

	errFn := func(err error) {
		log.Printf("error: %v", err)
	}

	go m.RunHeartbeats(*heartbeat, errFn)
	m.RunCommandLoop(errFn)
}
