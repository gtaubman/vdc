package jobspec

import "vdc/bytesize"

// Package describes a collection of files to be distributed to machines.
type Package struct {
	Name  string   `json:"name"`
	Files []string `json:"files"`
}

// Binary identifies the executable to run, by package and path within that package.
type Binary struct {
	Package string `json:"package"`
	Path    string `json:"path"`
}

// Requirements describes the resources needed to run a single task replica.
type Requirements struct {
	RAM  bytesize.ByteSize `json:"ram"`
	Disk bytesize.ByteSize `json:"disk"`
}

// JobSpec describes a job to be run in the datacenter.
type JobSpec struct {
	Name         string       `json:"name"`
	Packages     []Package    `json:"packages"`
	Binary       Binary       `json:"binary"`
	Requirements Requirements `json:"requirements"`
	Replicas     int          `json:"replicas"`
	Args         []string     `json:"args"`
}
