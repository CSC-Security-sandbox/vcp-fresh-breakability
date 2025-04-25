package coverage

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var CoverageCmd = &cobra.Command{
	Use:   "coverage",
	Short: "A cli used to control all coverage functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Call RunTestsWithCoverage with the value of the filtered flag
		if err := RunTestsWithCoverage(filtered); err != nil {
			return err
		}
		return nil
	},
}

const coverageFile = "coverage.out"
const excludeFile = "./exclude-from-code-coverage"

var filtered bool

func RunTestsWithCoverage(filtered bool) error {
	if err := runGoTests(); err != nil {
		log.Println("Error running Go tests:", err)
		os.Exit(1)
	}
	log.Println("Go unit tests completed successfully.")
	if filtered {
		if err := filterCoverageFile(); err != nil {
			log.Println("Error filtering coverage file:", err)
			os.Exit(1)
		}
	}
	log.Println("Generating coverage report...")
	if _, err := os.Stat(coverageFile); os.IsNotExist(err) {
		log.Println("Error: failed to generate coverage report")
		os.Exit(1)
	}

	overallCoverage, err := extractOverallCoverage()
	if err != nil {
		log.Println("Error extracting overall coverage:", err)
		os.Exit(1)
	}

	// Parse the coverage threshold from the environment variable
	coverageThresholdStr := os.Getenv("COVERAGE_THRESHOLD")
	if coverageThresholdStr == "" {
		log.Println("Error: COVERAGE_THRESHOLD environment variable is not set")
		os.Exit(1)
	}

	coverageThreshold, err := strconv.Atoi(coverageThresholdStr)
	if err != nil {
		log.Println("Error parsing COVERAGE_THRESHOLD:", err)
		os.Exit(1)
	}

	// Convert overallCoverage to a float
	overallCoverageFloat, err := strconv.ParseFloat(overallCoverage, 64)
	if err != nil {
		log.Println("Error parsing overall coverage percentage:", err)
		os.Exit(1)
	}

	// Compare coverage with the threshold
	if overallCoverageFloat < float64(coverageThreshold) {
		log.Printf("Error: coverage %.2f%% is below the threshold of %d%%\n", overallCoverageFloat, coverageThreshold)
		os.Exit(1)
	}

	log.Printf("Coverage %.2f%% meets the threshold of %d%%.\n", overallCoverageFloat, coverageThreshold)

	if err := os.Remove(coverageFile); err != nil {
		log.Println("Error removing coverage file:", err)
		os.Exit(1)
	}

	return nil
}

func runGoTests() error {
	log.Println("Running Go unit tests with coverage...")
	cmd := exec.Command("go", "test", "./...", "-cover", "-coverprofile="+coverageFile)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func filterCoverageFile() error {
	log.Println("Filtering files from coverage report...")

	// Open the exclude patterns file
	excludeFile, err := os.Open(excludeFile)
	if err != nil {
		return fmt.Errorf("failed to open exclude patterns file: %w", err)
	}
	defer excludeFile.Close()

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
	defer inputFile.Close()

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

func extractOverallCoverage() (string, error) {
	cmd := exec.Command("go", "tool", "cover", "-func="+coverageFile)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to extract coverage percentage: %w", err)
	}

	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "total:") {
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				return strings.TrimSuffix(fields[2], "%"), nil
			}
		}
	}

	return "", fmt.Errorf("failed to extract overall coverage percentage")
}

func init() {
	// Add the filtered flag to the CoverageCmd
	CoverageCmd.Flags().BoolVarP(&filtered, "filtered", "f", false, "Filter the coverage report")
}
