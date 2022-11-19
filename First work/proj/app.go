package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
)

var content [][]string
var headerText string
var length int

// Declaring Flags
var (
	input       = flag.String("i", "", "Use a file with the name file-name as an input.")
	output      = flag.String("o", "", "Use a file with the name file-name as an output.")
	headerUse   = flag.Bool("h", false, "The first line is a header that must be ignored during sorting but included in the output.")
	fieldSort   = flag.Int("f", 0, "Sort input lines by value number N.")
	reverseSort = flag.Bool("r", false, "Sort input lines in reverse order.")
	typeSort    = flag.Int("a", 1, "1 - Default sort, 2 - Tree sort.")
)

func main() {
	// Starting
	fmt.Println("-----STARTED-----")
	flag.Parse()

	// Reading
	if *input != "" {
		readFile(*input)
	} else {
		readConsole()
	}

	// Sorting
	if *fieldSort < length {
		switch *typeSort {
		case 1:
			sortDef(content, *fieldSort)
		case 2:
			sortTree(content, *fieldSort)
		default:
			log.Fatal("ERROR 3: Selected wrong type of sort. Use only 1 or 2!")
		}
	} else {
		log.Fatal("ERROR 2: Col num of sorting is greater than cols quantity")
	}

	// Writing to Console
	if *output != "" {
		writeFile(*output)
	} else {
		writeConsole()
	}

	// Finishing
	fmt.Println("-----FINISHED-----")
}

// Sorting Func
func sortDef(values [][]string, field int) {
	if *reverseSort {
		sort.Slice(content, func(i, j int) bool { return content[i][field] > content[j][field] })
	} else {
		sort.Slice(content, func(i, j int) bool { return content[i][field] < content[j][field] })
	}
}

// Console Func
func writeConsole() {
	fmt.Println("-----RESULT-----")
	if *headerUse {
		fmt.Println(headerText)
	}
	for _, row := range content {
		fmt.Println(strings.Join(row, ","))
	}
}

func readConsole() {
	s := bufio.NewScanner(os.Stdin)
	length = 0
	if *headerUse {
		fmt.Println("Input header:")
		s.Scan()
		headerText = s.Text()
	}

	fmt.Println("Input lines:")
	for s.Scan() {
		row := s.Text()
		fields := strings.Split(row, ",")
		if row == "" {
			break
		}
		if s.Err() != nil {
			log.Fatal(s.Err())
		}
		if length == 0 {
			length = len(fields)
		}
		if len(fields) != length {
			log.Fatal("ERROR 1: Length of each lines must be identical")
		}
		content = append(content, fields)
	}
}

// File Func

func writeFile(fileName string) {
	writeFile, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}

	if *headerUse {
		writeFile.WriteString(headerText + "\n")
	}
	for _, s := range content {
		writeFile.WriteString(strings.Join(s, ",") + "\n")
	}

	closeErr := writeFile.Close()
	if closeErr != nil {
		log.Fatal(closeErr)
	}
}

func readFile(fileName string) {
	length = 0
	readFile, openErr := os.Open(fileName)
	if openErr != nil {
		log.Fatal(openErr)
	}

	s := bufio.NewScanner(readFile)
	s.Split(bufio.ScanLines)

	if *headerUse {
		s.Scan()
		headerText = s.Text()
	}

	for s.Scan() {
		row := s.Text()
		fields := strings.Split(row, ",")
		if s.Err() != nil {
			log.Fatal(s.Err())
		}
		if length == 0 {
			length = len(fields)
		}
		if len(fields) != length {
			log.Fatal("ERROR 1: Length of each lines must be identical")
		}
		content = append(content, fields)
	}

	closeErr := readFile.Close()
	if closeErr != nil {
		log.Fatal(closeErr)
	}
}

// TREE SORTING IMPLEMENTATION

// TreeNode is structure for Tree Sort algorithm
type TreeNode struct {
	value       []string
	left, right *TreeNode
}

// Sort sorts given array by k`s value
func sortTree(values [][]string, key int) {
	var root *TreeNode

	for _, v := range values {
		root = add(root, v, key)
	}
	appendValues(values[:0][:0], root)
}

// appendValues adds the elements of t to the values in order
// and returns the resulting fragment.
func appendValues(values [][]string, t *TreeNode) [][]string {
	if t != nil {
		values = appendValues(values, t.left)
		values = append(values, t.value)
		values = appendValues(values, t.right)
	}
	return values
}

func add(t *TreeNode, value []string, k int) *TreeNode {
	if t == nil {
		t = new(TreeNode)
		t.value = value
		return t
	}
	if *reverseSort {
		if value[k] > t.value[k] {
			t.left = add(t.left, value, k)
		} else {
			t.right = add(t.right, value, k)
		}
	} else {
		if value[k] < t.value[k] {
			t.left = add(t.left, value, k)
		} else {
			t.right = add(t.right, value, k)
		}
	}
	return t
}
