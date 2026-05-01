// Driver for the wordcount MapReduce job.
//
// Build the worker binaries first, then run:
//
//	./mapreduce/run.sh [-mappers N] [-reducers R]
//
// Or manually:
//
//	go build -o /tmp/mapper ./mapreduce/mapper/
//	go build -o /tmp/reducer ./mapreduce/reducer/
//	go run ./mapreduce/driver/ -mapper /tmp/mapper -reducer /tmp/reducer
package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"vdc/api"
	"vdc/bytesize"
	"vdc/client"
	"vdc/jobspec"
)

func main() {
	mapperBin  := flag.String("mapper",  "",                   "path to compiled mapper binary (required)")
	reducerBin := flag.String("reducer", "",                   "path to compiled reducer binary (required)")
	inputFile  := flag.String("input",   "data/shakespeare.txt", "input text file")
	numMappers  := flag.Int("mappers",  4,    "number of mapper tasks")
	numReducers := flag.Int("reducers", 3,    "number of reducer tasks")
	leader      := flag.String("leader", "localhost", "VDC leader host")
	port        := flag.Int("port",     8080, "VDC leader port")
	topN        := flag.Int("top",      20,   "number of top words to print")
	flag.Parse()

	if *mapperBin == "" || *reducerBin == "" {
		fmt.Fprintln(os.Stderr, "usage: driver -mapper <bin> -reducer <bin> [-input <file>] [-mappers N] [-reducers R]")
		os.Exit(1)
	}

	totalStart := time.Now()

	header("WordCount MapReduce",
		fmt.Sprintf("%d mappers  ·  %d reducers  ·  %s:%d", *numMappers, *numReducers, *leader, *port))

	c, err := client.Dial(*leader, *port)
	if err != nil {
		fatalf("dial %s:%d: %v", *leader, *port, err)
	}
	defer c.Close()

	// ── 1. Split ────────────────────────────────────────────────────────────

	section("1/5", "Split input")
	inputInfo, err := os.Stat(*inputFile)
	if err != nil {
		fatalf("stat %s: %v", *inputFile, err)
	}
	logf("Input   %s  (%s)", *inputFile, mib(inputInfo.Size()))

	chunks, totalLines, err := splitFile(*inputFile, *numMappers)
	if err != nil {
		fatalf("split: %v", err)
	}
	logf("Lines   %s total  →  split into %d chunks", commas(totalLines), *numMappers)
	for i, chunk := range chunks {
		lines := strings.Count(string(chunk), "\n")
		logf("  chunk-%-3d  %6s lines   %s", i, commas(lines), mib(int64(len(chunk))))
	}

	// ── 2. Map package ──────────────────────────────────────────────────────

	section("2/5", "Build & upload mapper package")
	mapPkg, err := buildMapPackage(*mapperBin, chunks)
	if err != nil {
		fatalf("build mapper package: %v", err)
	}
	mapHash := sha256hex(mapPkg)
	logf("Package size  %s  (hash %s…)", mib(int64(len(mapPkg))), mapHash[:12])
	if err := uploadPackage(c, "mapper", mapHash, mapPkg); err != nil {
		fatalf("upload mapper: %v", err)
	}

	// ── 3. Map phase ────────────────────────────────────────────────────────

	section("3/5", "Map phase")
	mapSpec := jobspec.JobSpec{
		Name:     "wordcount-map",
		Packages: []jobspec.Package{{Name: "mapper", Files: []string{}}},
		Binary:   jobspec.Binary{Package: "mapper", Path: "mapper"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(128 << 20),
			Disk: bytesize.ByteSize(256 << 20),
		},
		Replicas: *numMappers,
		Args:     []string{"--task-id", "%TASKID%", "--num-reducers", strconv.Itoa(*numReducers)},
	}
	mapRunID, err := c.SubmitJob(mapSpec, map[string]string{"mapper": mapHash})
	if err != nil {
		fatalf("submit map job: %v", err)
	}
	logf("Run ID  %s  (%d replicas)", shortID(mapRunID), *numMappers)

	mapDur, err := waitForRun(c, mapRunID)
	if err != nil {
		fatalf("map phase: %v", err)
	}
	logf("Completed in %s", mapDur.Round(time.Millisecond*100))

	// ── 4. Collect intermediate data ────────────────────────────────────────

	section("4/5", "Collect intermediate data")
	mapOutputs := make(map[int]map[int][]byte, *numReducers)
	for k := range *numReducers {
		mapOutputs[k] = make(map[int][]byte)
	}

	var totalIntermediate int
	for k := range *numReducers {
		filename := fmt.Sprintf("part-%d.txt", k)
		reqID, total, err := c.PullFiles(mapRunID, -1, filename)
		if err != nil {
			fatalf("PullFiles %s: %v", filename, err)
		}
		tarData, pullErrs, err := waitForPull(c, reqID, total)
		if err != nil {
			fatalf("pull %s: %v", filename, err)
		}
		for _, e := range pullErrs {
			fmt.Fprintf(os.Stderr, "  warning: mapper %d %s: %s\n", e.TaskNumber, filename, e.Err)
		}
		entries, err := unpackTar(tarData)
		if err != nil {
			fatalf("unpack %s tar: %v", filename, err)
		}
		var partBytes int
		for name, data := range entries {
			taskNumStr := strings.SplitN(name, ".", 2)[0]
			taskNum, _ := strconv.Atoi(taskNumStr)
			mapOutputs[k][taskNum] = data
			partBytes += len(data)
		}
		totalIntermediate += partBytes
		logf("  part-%-3d  %d mappers  →  %s", k, len(entries), kib(partBytes))
	}
	logf("Total intermediate  %s  across %d files",
		kib(totalIntermediate), *numMappers**numReducers)

	// ── 5. Reduce phase ─────────────────────────────────────────────────────

	section("5/5", "Build & upload reducer package")
	redPkg, err := buildReducePackage(*reducerBin, mapOutputs, *numMappers, *numReducers)
	if err != nil {
		fatalf("build reducer package: %v", err)
	}
	redHash := sha256hex(redPkg)
	logf("Package size  %s  (hash %s…)", mib(int64(len(redPkg))), redHash[:12])
	if err := uploadPackage(c, "reducer", redHash, redPkg); err != nil {
		fatalf("upload reducer: %v", err)
	}

	section("5/5", "Reduce phase")
	redSpec := jobspec.JobSpec{
		Name:     "wordcount-reduce",
		Packages: []jobspec.Package{{Name: "reducer", Files: []string{}}},
		Binary:   jobspec.Binary{Package: "reducer", Path: "reducer"},
		Requirements: jobspec.Requirements{
			RAM:  bytesize.ByteSize(128 << 20),
			Disk: bytesize.ByteSize(256 << 20),
		},
		Replicas: *numReducers,
		Args:     []string{"--partition", "%TASKID%"},
	}
	redRunID, err := c.SubmitJob(redSpec, map[string]string{"reducer": redHash})
	if err != nil {
		fatalf("submit reduce job: %v", err)
	}
	logf("Run ID  %s  (%d replicas)", shortID(redRunID), *numReducers)

	redDur, err := waitForRun(c, redRunID)
	if err != nil {
		fatalf("reduce phase: %v", err)
	}
	logf("Completed in %s", redDur.Round(time.Millisecond*100))

	// ── 6. Pull and display results ─────────────────────────────────────────

	section("", "Results")
	reqID, total, err := c.PullFiles(redRunID, -1, "output.txt")
	if err != nil {
		fatalf("PullFiles output.txt: %v", err)
	}
	tarData, _, err := waitForPull(c, reqID, total)
	if err != nil {
		fatalf("pull output: %v", err)
	}
	logf("Pulled output.txt from %d reducers  (%s)", *numReducers, kib(len(tarData)))

	entries, err := unpackTar(tarData)
	if err != nil {
		fatalf("unpack output: %v", err)
	}

	counts := make(map[string]int)
	for _, data := range entries {
		scanner := bufio.NewScanner(bytes.NewReader(data))
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), "\t", 2)
			if len(parts) == 2 {
				n, _ := strconv.Atoi(parts[1])
				counts[parts[0]] += n
			}
		}
	}

	type wc struct{ word string; count int }
	all := make([]wc, 0, len(counts))
	for w, n := range counts {
		all = append(all, wc{w, n})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].count != all[j].count {
			return all[i].count > all[j].count
		}
		return all[i].word < all[j].word
	})

	fmt.Printf("\n  Top %d words:\n\n", *topN)
	fmt.Printf("  %4s  %-22s  %8s\n", "RANK", "WORD", "COUNT")
	fmt.Printf("  %s\n", strings.Repeat("─", 40))
	for i, w := range all {
		if i >= *topN {
			break
		}
		fmt.Printf("  %4d  %-22s  %8s\n", i+1, w.word, commas(w.count))
	}

	totalTime := time.Since(totalStart).Round(time.Millisecond * 100)
	fmt.Printf("\n  %s unique words  ·  map %s  ·  reduce %s  ·  total %s\n\n",
		commas(len(counts)),
		mapDur.Round(time.Millisecond*100),
		redDur.Round(time.Millisecond*100),
		totalTime,
	)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func header(title, subtitle string) {
	bar := strings.Repeat("─", len(title)+4)
	fmt.Printf("\n  ┌%s┐\n  │  %s  │\n  └%s┘\n  %s\n\n", bar, title, bar, subtitle)
}

func section(step, name string) {
	if step != "" {
		fmt.Printf("\n── %s  %s %s\n", step, name, strings.Repeat("─", max(0, 50-len(step)-len(name)-2)))
	} else {
		fmt.Printf("\n── %s %s\n", name, strings.Repeat("─", max(0, 52-len(name))))
	}
}

func logf(format string, args ...any) {
	fmt.Printf("  "+format+"\n", args...)
}

func uploadPackage(c *client.Client, name, hash string, data []byte) error {
	exists, err := c.HasPackage(hash)
	if err != nil {
		return err
	}
	if exists {
		logf("Package %q already on server — skipping upload", name)
		return nil
	}
	fmt.Printf("  Uploading %q...", name)
	start := time.Now()
	if err := c.UploadPackage(hash, data); err != nil {
		fmt.Println()
		return err
	}
	fmt.Printf(" done (%s)\n", time.Since(start).Round(time.Millisecond*10))
	return nil
}

func waitForRun(c *client.Client, runID string) (time.Duration, error) {
	start := time.Now()
	var lastStatus api.RunStatus = -1
	var transitions []string
	for {
		status, err := c.GetRunStatus(runID)
		if err != nil {
			fmt.Println()
			return 0, fmt.Errorf("get run status: %w", err)
		}
		if status != lastStatus {
			lastStatus = status
			transitions = append(transitions, status.String())
		}
		elapsed := time.Since(start).Round(time.Millisecond * 100)
		fmt.Printf("\r  [%6s]  %s", elapsed, strings.Join(transitions, " → "))
		switch status {
		case api.RunComplete:
			fmt.Println()
			return time.Since(start), nil
		case api.RunFailed, api.RunPartialFailure, api.RunCancelled:
			fmt.Println()
			return time.Since(start), fmt.Errorf("run ended with status %s", status)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForPull(c *client.Client, reqID string, total int) ([]byte, []api.PullFileError, error) {
	for {
		received, _, done, tarData, errs, err := c.GetPullResult(reqID)
		if err != nil {
			return nil, nil, err
		}
		if done {
			return tarData, errs, nil
		}
		_ = received
		time.Sleep(250 * time.Millisecond)
	}
}

// splitFile returns numChunks line-balanced byte slices and the total line count.
func splitFile(path string, numChunks int) ([][]byte, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, 0, err
	}

	chunks := make([][]byte, numChunks)
	size := (len(lines) + numChunks - 1) / numChunks
	for i := range chunks {
		start := i * size
		end := min(start+size, len(lines))
		if start >= len(lines) {
			chunks[i] = []byte{}
		} else {
			chunks[i] = []byte(strings.Join(lines[start:end], "\n") + "\n")
		}
	}
	return chunks, len(lines), nil
}

func buildMapPackage(binPath string, chunks [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := addFileTar(tw, "mapper", binPath); err != nil {
		return nil, fmt.Errorf("add mapper binary: %w", err)
	}
	for i, chunk := range chunks {
		name := fmt.Sprintf("chunk-%d.txt", i)
		hdr := &tar.Header{Name: name, Mode: 0644, Size: int64(len(chunk))}
		if err := tw.WriteHeader(hdr); err != nil {
			return nil, err
		}
		if _, err := tw.Write(chunk); err != nil {
			return nil, err
		}
	}
	return finishTar(tw, &buf)
}

func buildReducePackage(binPath string, mapOutputs map[int]map[int][]byte, numMappers, numReducers int) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := addFileTar(tw, "reducer", binPath); err != nil {
		return nil, fmt.Errorf("add reducer binary: %w", err)
	}
	for k := range numReducers {
		for m := range numMappers {
			data := mapOutputs[k][m]
			name := fmt.Sprintf("mapper-%d-part-%d.txt", m, k)
			hdr := &tar.Header{Name: name, Mode: 0644, Size: int64(len(data))}
			if err := tw.WriteHeader(hdr); err != nil {
				return nil, err
			}
			if _, err := tw.Write(data); err != nil {
				return nil, err
			}
		}
	}
	return finishTar(tw, &buf)
}

func addFileTar(tw *tar.Writer, name, path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	hdr := &tar.Header{Name: name, Mode: int64(info.Mode()), Size: info.Size()}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(tw, f)
	return err
}

func finishTar(tw *tar.Writer, buf *bytes.Buffer) ([]byte, error) {
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func unpackTar(data []byte) (map[string][]byte, error) {
	out := make(map[string][]byte)
	tr := tar.NewReader(bytes.NewReader(data))
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		out[hdr.Name] = b
	}
	return out, nil
}

func sha256hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func mib(n int64) string {
	return fmt.Sprintf("%.1f MiB", float64(n)/float64(1<<20))
}

func kib(n int) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/float64(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func commas(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nerror: "+format+"\n", args...)
	os.Exit(1)
}
