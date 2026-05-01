// Reducer for the wordcount MapReduce job.
//
// The binary and its input files live in the same directory (the VDC package
// directory). The driver names each input file "mapper-M-part-K.txt" where M
// is the mapper task number and K is this reducer's partition. The reducer
// reads all files matching its partition, sums word counts, and writes a
// sorted output.txt to the working directory (the VDC run directory).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func main() {
	partition := flag.Int("partition", 0, "which partition to reduce")
	flag.Parse()

	exePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "executable: %v\n", err)
		os.Exit(1)
	}
	pkgDir := filepath.Dir(exePath)

	pattern := filepath.Join(pkgDir, fmt.Sprintf("mapper-*-part-%d.txt", *partition))
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "glob: %v\n", err)
		os.Exit(1)
	}

	counts := make(map[string]int)
	for _, path := range files {
		if err := readCounts(path, counts); err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
			os.Exit(1)
		}
	}

	words := make([]string, 0, len(counts))
	for w := range counts {
		words = append(words, w)
	}
	sort.Strings(words)

	out, err := os.Create("output.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	for _, w := range words {
		fmt.Fprintf(out, "%s\t%d\n", w, counts[w])
	}
}

func readCounts(path string, counts map[string]int) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 {
			continue
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		counts[parts[0]] += n
	}
	return scanner.Err()
}
