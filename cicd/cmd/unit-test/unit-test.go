package unitTest

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/exec"
	"strings"
)

var UnitTestCmd = &cobra.Command{
	Use:   "unit-test",
	Short: "A cli used to control unit-test functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := runUnitTests(filtered); err != nil {
			return err
		}
		return nil
	},
}

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
	log.Println("Running Go unit tests with coverage...")
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("go", "test", "./...", "-cover", "-coverprofile="+coverageFile)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Set GOEXPERIMENT env var in addition to the current env
	cmd.Env = append(os.Environ(), "GOEXPERIMENT=boringcrypto,nocoverageredesign")

	log.Println("Running gotest command..")
	if err := cmd.Run(); err != nil {
		log.Println("Error running Go tests:", err)
		log.Println("Stdout output:", stdout.String())
		log.Println("Stderr output:", stderr.String())
		return err
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
