# Virtual Datacenter (VDC)

This document describes the virtual datacenter project.  This project will be written using the Go programming language.  There will be several packages and binaries in this directory:

- server: This package contains the code for the leader server of the datacenter.
- client: This package contains code that can make RPCs to the vdc server to interact with it, for example creating jobs.
- api: This package contains API definitions shared between the server and client.
- jobspec: This package contains information about the requirements of a job that runs in the datacenter.
- machine: This package contains the code that a machine runs to join the datacenter.
- apps/mr: This package contains the code for running a mapreduce using the machines connected to the vdc.
- cmd: This package contains sub-directories for different binaries.
- cmd/vdc: This package contains the main binary that will be used to issue commands like `vdc leader` to run the leader, `vdc join` which joins the current device to the datacenter, `vdc run <jobspec file>` which will run a job, and `vdc web` which will run a webserver to show/interact with the leader.

## Example commands

Here are some example commands that will be implemented:

### `vdc leader`

This has an optional commandline flag of --port which defaults to 8080.

Once the server has started, this prints status information about the leader, such as a table of all the machines connected, a brief summary of their specs (Total RAM, Total Disk, Last Heartbeat).

### `vdc join`

This the following optional commandline flags:

- `--leader` which defaults to localhost.
- `--port` which defaults to 8080.
- `--ram` which defaults to 512M which is how many RAM to contribute to the virtual datacenter from this machine.
- `--disk` which defaults to 5G which is how much disk to contribute to the virtual datacenter from this machine.
- `--heartbeat_duration` which defaults to 10 seconds which is how often to check in with the server.

Joining will create a net/rpc connection to the leader server and call `RegisterMachine()` with the specs of this server.

## Server Design

The server will have 
