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
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

func main() {
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

	f, err := os.Open(inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open %s: %v\n", inputPath, err)
		os.Exit(1)
	}
	defer f.Close()

	counts := make(map[string]int)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		for _, word := range strings.FieldsFunc(scanner.Text(), func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r)
		}) {
			counts[strings.ToLower(word)]++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scan: %v\n", err)
		os.Exit(1)
	}

	// Open one output file per reducer partition.
	outs := make([]*os.File, *numReducers)
	for i := range outs {
		outs[i], err = os.Create(fmt.Sprintf("part-%d.txt", i))
		if err != nil {
			fmt.Fprintf(os.Stderr, "create part-%d.txt: %v\n", i, err)
			os.Exit(1)
		}
		defer outs[i].Close()
	}

	for word, count := range counts {
		p := int(hashWord(word)) % *numReducers
		fmt.Fprintf(outs[p], "%s\t%d\n", word, count)
	}
}

func hashWord(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
