package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

var (
	dir         = flag.String("d", "", "Use a directory path to iterate through all included CSV files.")
	input       = flag.String("i", "", "Use a file with the name file-name as an input.")
	output      = flag.String("o", "", "Use a file with the name file-name as an output.")
	fieldSort   = flag.Int("f", 0, "Sort input lines by value number N.")
	reverseSort = flag.Bool("r", false, "Sort input lines in reverse order.")
)

func main() {
	files := make(chan string)
	var buffer [][]string

	// Starting
	fmt.Println("-----STARTED-----")
	flag.Parse()

	if *dir != "" && *input != "" {
		log.Fatal("ERROR: You can't use -d and -i options at the same time!")
	}

	// Parsing current Dir or selected Dir
	if *dir == "" {
		if *input != "" {
			files = findFiles(*input)
		} else {
			currentDirectory, err := os.Getwd()
			if err != nil {
				log.Fatal(err)
			}
			files = findFiles(currentDirectory)
		}
	} else {
		files = findFiles(*dir)
	}

	// Collecting all rows and splitting them to fields
	fields := readFilesStaging(files, runtime.NumCPU()) // numbers of go routines is giving from CPU cores quantity

	// Collecting fields from chan to buffer
	for row := range fields {
		buffer = append(buffer, row)
	}

	// Sorting buffer
	sortFields(buffer, *fieldSort, *reverseSort)

	// Writing to Console
	if *output != "" {
		writeFile(buffer, *output)
	} else {
		writeConsole(buffer)
	}

	// Finishing
	fmt.Println("-----FINISHED-----")
}

// Parsing passed directory
func findFiles(path string) (files chan string) {
	files = make(chan string)

	go func() {
		defer close(files)

		filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Fatalf(err.Error())
			}
			if filepath.Ext(path) == ".csv" {
				// Uncomment the line below for logging found files
				//fmt.Printf("File Name: %s\n", info.Name())
				files <- path
			}
			return nil
		})
	}()

	return files
}

func readFilesStaging(files chan string, n int) (allFields chan []string) {
	fields := make([]chan []string, n)
	allFields = make(chan []string)

	// fan-out
	for i := 0; i < n; i++ {
		fields[i] = make(chan []string)
		readFiles(files, fields[i])
	}

	// fan-in
	go func() {
		defer close(allFields)
		wg := &sync.WaitGroup{}

		for i := range fields {
			wg.Add(1)
			go func(ch chan []string) {
				defer wg.Done()
				for field := range ch {
					allFields <- field
				}
			}(fields[i])
		}

		wg.Wait()
	}()

	return allFields
}

func readFiles(files chan string, fields chan []string) {
	go func() {
		defer close(fields)
		for file := range files {
			readFile(file, fields)
		}
	}()
}

// Reading passed file
func readFile(fileName string, fieldsChan chan []string) {
	readFile, openErr := os.Open(fileName)
	if openErr != nil {
		log.Fatal(openErr)
	}
	fmt.Println("READFILE")
	s := bufio.NewScanner(readFile)
	s.Split(bufio.ScanLines)

	for s.Scan() {
		row := s.Text()
		fields := strings.Split(row, ",")
		if s.Err() != nil {
			log.Fatal(s.Err())
		}
		fieldsChan <- fields
	}

	closeErr := readFile.Close()
	if closeErr != nil {
		log.Fatal(closeErr)
	}
}

// Sorting Func
func sortFields(values [][]string, field int, reverse bool) {
	if reverse {
		sort.Slice(values, func(i, j int) bool { return values[i][field] > values[j][field] })
	} else {
		sort.Slice(values, func(i, j int) bool { return values[i][field] < values[j][field] })
	}
}

// Outputing results to console
func writeConsole(fields [][]string) {
	fmt.Println("-----RESULT-----")
	for _, row := range fields {
		fmt.Println(strings.Join(row, ","))
	}
}

// Outputing results to file
func writeFile(fields [][]string, fileName string) {
	writeFile, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}

	for _, s := range fields {
		writeFile.WriteString(strings.Join(s, ",") + "\n")
	}

	closeErr := writeFile.Close()
	if closeErr != nil {
		log.Fatal(closeErr)
	}
}
