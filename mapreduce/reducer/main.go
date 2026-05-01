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
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime)

	partition := flag.Int("partition", 0, "which partition to reduce")
	flag.Parse()

	log.Printf("reducer starting  partition=%d", *partition)

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
	log.Printf("found %d input file(s) matching %s", len(files), pattern)

	counts := make(map[string]int)
	for _, path := range files {
		n, err := readCounts(path, counts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "read %s: %v\n", path, err)
			os.Exit(1)
		}
		log.Printf("  read %d entries from %s", n, filepath.Base(path))
	}
	log.Printf("merged to %d unique words", len(counts))

	words := make([]string, 0, len(counts))
	for w := range counts {
		words = append(words, w)
	}
	sort.Strings(words)

	// Log the top 10 words by count.
	type wc struct{ word string; count int }
	top := make([]wc, 0, len(counts))
	for _, w := range words {
		top = append(top, wc{w, counts[w]})
	}
	sort.Slice(top, func(i, j int) bool { return top[i].count > top[j].count })
	n := 10
	if len(top) < n {
		n = len(top)
	}
	log.Printf("top %d words:", n)
	for _, e := range top[:n] {
		log.Printf("  %-20s %d", e.word, e.count)
	}

	out, err := os.Create("output.txt")
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	for _, w := range words {
		fmt.Fprintf(out, "%s\t%d\n", w, counts[w])
	}
	log.Printf("wrote output.txt (%d words)", len(words))
	log.Printf("reducer done")
}

func readCounts(path string, counts map[string]int) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var n int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "\t", 2)
		if len(parts) != 2 {
			continue
		}
		v, err := strconv.Atoi(parts[1])
		if err != nil {
			continue
		}
		counts[parts[0]] += v
		n++
	}
	return n, scanner.Err()
}
