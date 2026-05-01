// Mapper for the wordcount MapReduce job.
//
// The binary and its input chunk live in the same directory (the VDC package
// directory). Output partition files are written to the working directory (the
// VDC run directory), where PullFiles can retrieve them.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	taskID := flag.Int("task-id", 0, "which input chunk to process")
	numReducers := flag.Int("num-reducers", 1, "number of reduce partitions")
	flag.Parse()

	// Input chunk lives next to this binary in the package directory.
	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executable: %v\n", err)
		os.Exit(1)
	}
	pkgDir := filepath.Dir(exePath)
	inputPath := filepath.Join(pkgDir, fmt.Sprintf("chunk-%d.txt", *taskID))

	log.Printf("mapper starting  task=%d  input=%s  reducers=%d", *taskID, inputPath, *numReducers)

	f, err := os.Open(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", inputPath, err)
		os.Exit(1)
	}
	defer f.Close()

	log.Printf("sleeping 5s to simulate a slow task...")
	for i := 1; i <= 5; i++ {
		time.Sleep(1 * time.Second)
		log.Printf("  %ds / 5s", i)
	}

	log.Printf("counting words...")
	counts := make(map[string]int)
	var lineCount, wordCount int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lineCount++
		for _, word := range strings.FieldsFunc(scanner.Text(), func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			counts[strings.ToLower(word)]++
			wordCount++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}
	log.Printf("read %d lines, %d words, %d unique", lineCount, wordCount, len(counts))

	// Open one output file per reducer partition.
	log.Printf("writing %d partition file(s)...", *numReducers)
	outs := make([]*os.File, *numReducers)
	for i := range outs {
		outs[i], err = os.Create(fmt.Sprintf("part-%d.txt", i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "create part-%d.txt: %v\n", i, err)
			os.Exit(1)
		}
		defer outs[i].Close()
	}

	partCounts := make([]int, *numReducers)
	for word, count := range counts {
		p := int(hashWord(word)) % *numReducers
		fmt.Fprintf(outs[p], "%s\t%d\n", word, count)
		partCounts[p]++
	}

	for i, n := range partCounts {
		log.Printf("  part-%d.txt: %d entries", i, n)
	}
	log.Printf("mapper done")
}

func hashWord(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
