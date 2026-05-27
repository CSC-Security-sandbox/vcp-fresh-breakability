package unitTest

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

var UnitTestCmd = &cobra.Command{
	Use:   "unit-test",
	Short: "A cli used to control unit-test functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		if err := runUnitTests(filtered); err != nil {
			return err
		}
		return nil
	},
}

// workspaceModules lists every module participating in go.work. Keep this in
// sync with go.work and the WORKSPACE_MODULES variable in the root Makefile.
var workspaceModules = []string{".", "cicd", "core", "database", "hyperscaler", "lib", "vcp-core"}

const coverageFile = "coverage.out"
const excludeFile = "./cicd/cmd/unit-test/exclude-from-code-coverage"

func runUnitTests(filtered bool) error {
	err := runGoTests()
	if err != nil {
		log.Println("Error running Go tests:", err)
		return err
	}
	log.Println("Go unit tests completed successfully.")

	if filtered {
		if err := filterUnitTestFile(); err != nil {
			log.Println("Error filtering unit tests:", err)
			os.Exit(1)
		}
	}

	return nil
}

func runGoTests() error {
	log.Println("Running Go unit tests with coverage across workspace modules...")

	if err := os.Remove(coverageFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to clear %s: %w", coverageFile, err)
	}

	var combined bytes.Buffer
	combined.WriteString("mode: set\n")

	for _, mod := range workspaceModules {
		log.Printf("==> testing module %q", mod)
		listCmd := exec.Command("go", "list", "./...")
		listCmd.Dir = mod
		var listOut, listErr bytes.Buffer
		listCmd.Stdout = &listOut
		listCmd.Stderr = &listErr
		if err := listCmd.Run(); err != nil {
			return fmt.Errorf("go list failed in %s: %w; stderr: %s", mod, err, listErr.String())
		}
		pkgs := strings.Fields(listOut.String())
		if len(pkgs) == 0 {
			log.Printf("    (no packages in %q, skipping)", mod)
			continue
		}
		modCoverage := "coverage.tmp.out"
		var stdout, stderr bytes.Buffer
		cmd := exec.Command("go", "test", "-tags=test_exclude", "./...", "-cover", "-coverprofile="+modCoverage)
		cmd.Dir = mod
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		cmd.Env = append(os.Environ(), "GOEXPERIMENT=boringcrypto")

		if err := cmd.Run(); err != nil {
			log.Printf("Error running Go tests in %q: %v", mod, err)
			log.Println("Stdout output:", stdout.String())
			log.Println("Stderr output:", stderr.String())
			return fmt.Errorf("go test failed in %s: %w", mod, err)
		}

		modPath := mod + string(os.PathSeparator) + modCoverage
		data, err := os.ReadFile(modPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read %s: %w", modPath, err)
		}
		// Drop the per-module "mode:" header; keep only entries.
		for i, line := range strings.SplitAfter(string(data), "\n") {
			if i == 0 && strings.HasPrefix(line, "mode:") {
				continue
			}
			combined.WriteString(line)
		}
		_ = os.Remove(modPath)
	}

	if err := os.WriteFile(coverageFile, combined.Bytes(), 0o644); err != nil {
		return fmt.Errorf("failed to write merged %s: %w", coverageFile, err)
	}

	log.Println("Go tests completed successfully.")
	return nil
}

func filterUnitTestFile() error {
	log.Println("Filtering files from coverage report...")

	// Open the exclude patterns file
	excludeFile, err := os.Open(excludeFile)
	if err != nil {
		return fmt.Errorf("failed to open exclude patterns file: %w", err)
	}
	defer func() {
		if err := excludeFile.Close(); err != nil {
			log.Printf("Error closing exclude file: %v", err)
		}
	}()

	// Read exclude patterns into a slice
	var excludePatterns []string
	scanner := bufio.NewScanner(excludeFile)
	for scanner.Scan() {
		excludePatterns = append(excludePatterns, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading exclude patterns file: %w", err)
	}

	// Open the coverage file
	inputFile, err := os.Open(coverageFile)
	if err != nil {
		return fmt.Errorf("failed to open coverage file: %w", err)
	}
	defer func() {
		if err := inputFile.Close(); err != nil {
			log.Printf("Error closing input file: %v", err)
		}
	}()

	// Filter lines based on exclude patterns
	var buffer bytes.Buffer
	scanner = bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		exclude := false
		for _, pattern := range excludePatterns {
			if strings.Contains(line, pattern) {
				exclude = true
				break
			}
		}
		if !exclude {
			buffer.WriteString(line + "\n")
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading coverage file: %w", err)
	}

	// Write the filtered content back to the coverage file
	return os.WriteFile(coverageFile, buffer.Bytes(), 0644)
}

func init() {
	UnitTestCmd.Flags().BoolVarP(&filtered, "filtered", "f", false, "Filter the coverage report")
	UnitTestCmd.AddCommand(CoverageCmd)
}
