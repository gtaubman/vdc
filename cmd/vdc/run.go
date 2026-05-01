package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"vdc/client"
	"vdc/jobspec"
)

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	leader := fs.String("leader", "localhost", "leader host")
	port := fs.Int("port", 8080, "leader port")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: vdc run [flags] <jobspec.json>")
		os.Exit(1)
	}
	specFile := fs.Arg(0)

	spec, err := parseJobSpec(specFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "jobspec: %v\n", err)
		os.Exit(1)
	}
	if err := validateJobSpec(spec); err != nil {
		fmt.Fprintf(os.Stderr, "invalid jobspec: %v\n", err)
		os.Exit(1)
	}

	printSummary(spec)
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	c, err := client.Dial(*leader, *port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	packageHashes, err := uploadPackages(c, spec.Packages, ".")
	if err != nil {
		fmt.Fprintf(os.Stderr, "packages: %v\n", err)
		os.Exit(1)
	}

	runID, err := c.SubmitJob(spec, packageHashes)
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Run ID: %s\n", runID)
}

func parseJobSpec(path string) (jobspec.JobSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return jobspec.JobSpec{}, err
	}
	defer f.Close()
	var spec jobspec.JobSpec
	if err := json.NewDecoder(f).Decode(&spec); err != nil {
		return jobspec.JobSpec{}, err
	}
	return spec, nil
}

func validateJobSpec(spec jobspec.JobSpec) error {
	if spec.Name == "" {
		return fmt.Errorf("name is required")
	}
	if spec.Replicas <= 0 {
		return fmt.Errorf("replicas must be > 0")
	}
	if spec.Binary.Package == "" || spec.Binary.Path == "" {
		return fmt.Errorf("binary.package and binary.path are required")
	}
	if len(spec.Packages) == 0 {
		return fmt.Errorf("at least one package is required")
	}
	pkgNames := make(map[string]bool, len(spec.Packages))
	for _, p := range spec.Packages {
		if p.Name == "" {
			return fmt.Errorf("all packages must have a name")
		}
		if len(p.Files) == 0 {
			return fmt.Errorf("package %q has no files", p.Name)
		}
		pkgNames[p.Name] = true
	}
	if !pkgNames[spec.Binary.Package] {
		return fmt.Errorf("binary.package %q not found in packages", spec.Binary.Package)
	}
	return nil
}

func printSummary(spec jobspec.JobSpec) {
	totalRAM := uint64(spec.Requirements.RAM) * uint64(spec.Replicas)
	totalDisk := uint64(spec.Requirements.Disk) * uint64(spec.Replicas)
	fmt.Printf("Job:      %s\n", spec.Name)
	fmt.Printf("Binary:   %s/%s\n", spec.Binary.Package, spec.Binary.Path)
	fmt.Printf("Replicas: %d\n", spec.Replicas)
	fmt.Printf("Per task: RAM %s  Disk %s\n", spec.Requirements.RAM, spec.Requirements.Disk)
	fmt.Printf("Total:    RAM %s  Disk %s\n", byteSize(totalRAM), byteSize(totalDisk))
	fmt.Printf("Packages: %d\n", len(spec.Packages))
	for _, p := range spec.Packages {
		fmt.Printf("  %s (%d file(s))\n", p.Name, len(p.Files))
	}
	if len(spec.Args) > 0 {
		fmt.Printf("Args:     %s\n", strings.Join(spec.Args, " "))
	}
}

func confirm(prompt string) bool {
	fmt.Printf("%s [y/N] ", prompt)
	s, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	s = strings.TrimSpace(strings.ToLower(s))
	return s == "y" || s == "yes"
}

// uploadPackages builds a tar for each package, checks if the server already has
// it by hash, uploads if not, and returns a map of package name -> hash.
func uploadPackages(c *client.Client, packages []jobspec.Package, baseDir string) (map[string]string, error) {
	hashes := make(map[string]string, len(packages))
	for _, pkg := range packages {
		data, err := buildPackageTar(pkg, baseDir)
		if err != nil {
			return nil, fmt.Errorf("build package %q: %w", pkg.Name, err)
		}

		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])

		exists, err := c.HasPackage(hash)
		if err != nil {
			return nil, fmt.Errorf("check package %q: %w", pkg.Name, err)
		}
		if exists {
			fmt.Printf("Package %q: already on server (%s)\n", pkg.Name, hash[:12])
		} else {
			fmt.Printf("Package %q: uploading %s... ", pkg.Name, byteSize(uint64(len(data))))
			if err := c.UploadPackage(hash, data); err != nil {
				return nil, fmt.Errorf("upload package %q: %w", pkg.Name, err)
			}
			fmt.Println("done")
		}
		hashes[pkg.Name] = hash
	}
	return hashes, nil
}

// buildPackageTar creates an in-memory tar archive of the package files.
// Each file is stored under its base name so that binary.path is always simple.
func buildPackageTar(pkg jobspec.Package, baseDir string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	for _, f := range pkg.Files {
		absPath := f
		if !filepath.IsAbs(f) {
			absPath = filepath.Join(baseDir, f)
		}

		info, err := os.Stat(absPath)
		if err != nil {
			return nil, fmt.Errorf("stat %q: %w", f, err)
		}

		tarPath := filepath.Base(absPath)

		hdr := &tar.Header{
			Name: tarPath,
			Mode: int64(info.Mode()),
			Size: info.Size(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}

		fh, err := os.Open(absPath)
		if err != nil {
			return nil, fmt.Errorf("open %q: %w", f, err)
		}
		_, err = io.Copy(tw, fh)
		fh.Close()
		if err != nil {
			return nil, fmt.Errorf("read %q: %w", f, err)
		}
	}

	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
