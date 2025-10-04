package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	baseCommit := os.Getenv("BASE_SHA")
	headCommit := os.Getenv("HEAD_SHA")

	if baseCommit == "" || headCommit == "" {
		fmt.Println("Warning: BASE_SHA or HEAD_SHA not set. Falling back to 'HEAD~1...HEAD'.")
		baseCommit = "HEAD~1"
		headCommit = "HEAD"
	}

	fmt.Printf("Checking for new test cases between %s and %s\n", baseCommit, headCommit)

	diffCmd := exec.Command("git", "diff", "--name-only",
		baseCommit, headCommit, "--", "*_test.go")
	diffOutput, err := diffCmd.Output()
	if err != nil {
		log.Fatalf("Failed to run git diff: %v", err)
	}

	changedTestFiles := strings.Split(strings.TrimSpace(string(diffOutput)), "\n")
	if len(changedTestFiles) == 0 || changedTestFiles[0] == "" {
		fmt.Println("No `_test.go` files were changed in this pull request.")
		setOutput("new_tests", "")
		os.Exit(0)
	}
	fmt.Printf("Found changed test files: %v\n", changedTestFiles)

	var newTests []string

	for _, file := range changedTestFiles {
		oldContent, _ := getFileContentAtCommit(baseCommit, file)
		newContent, err := getFileContentAtCommit(headCommit, file)
		if err != nil {
			fmt.Printf("Could not get new content for %s: %v\n", file, err)
			continue
		}

		oldTests, _ := getTestFunctions(oldContent)
		newTestsMap, err := getTestFunctions(newContent)
		if err != nil {
			fmt.Printf("Could not parse new content for %s: %v\n", file, err)
			continue
		}

		testPackage := filepath.Dir(file)

		for testName := range newTestsMap {
			if !oldTests[testName] {
				fmt.Printf("Found new test: '%s' in file '%s'\n", testName, file)
				newTests = append(newTests, fmt.Sprintf("%s:%s", testPackage, testName))
			}
		}
	}

	if len(newTests) > 0 {
		setOutput("new_tests", strings.Join(newTests, " "))
	} else {
		fmt.Println("No new test functions were found in the changed files.")
		setOutput("new_tests", "")
	}
}

// setOutput appends a key-value pair to the GITHUB_OUTPUT file.
func setOutput(name, value string) {
	outputFile := os.Getenv("GITHUB_OUTPUT")
	if outputFile == "" {
		fmt.Println("GITHUB_OUTPUT not set. Skipping setting output.")
		return
	}
	f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open GITHUB_OUTPUT file: %v", err)
	}
	defer f.Close()
	if _, err := f.WriteString(fmt.Sprintf("%s=%s\n", name, value)); err != nil {
		log.Fatalf("Failed to write to GITHUB_OUTPUT file: %v", err)
	}
}

// getFileContentAtCommit retrieves the content of a file at a specific git commit.
func getFileContentAtCommit(commitHash, filePath string) (string, error) {
	cmd := exec.Command("git", "show", fmt.Sprintf("%s:%s", commitHash, filePath))
	output, err := cmd.Output()
	if err != nil {
		return "", nil
	}
	return string(output), nil
}

// getTestFunctions parses Go source code and returns a map of test function names.
func getTestFunctions(source string) (map[string]bool, error) {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "src.go", source, 0)
	if err != nil {
		return nil, err
	}
	tests := make(map[string]bool)
	ast.Inspect(node, func(n ast.Node) bool {
		fn, ok := n.(*ast.FuncDecl)
		if ok && strings.HasPrefix(fn.Name.Name, "Test") {
			tests[fn.Name.Name] = true
		}
		return true
	})
	return tests, nil
}
