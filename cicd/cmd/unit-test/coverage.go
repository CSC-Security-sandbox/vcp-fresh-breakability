package unitTest

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var UnitTestCmd = &cobra.Command{
	Use:   "unit-test",
	Short: "A cli used to control all coverage functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := RunTestsWithCoverage(filtered, coverage); err != nil {
			return err
		}
		return nil
	},
}

const coverageFile = "coverage.out"
const excludeFile = "./cicd/cmd/unit-test/exclude-from-code-coverage"

var filtered bool
var coverage bool

func RunTestsWithCoverage(filtered bool, coverage bool) error {
	if err := runGoTests(); err != nil {
		log.Println("Error running Go tests:", err)
		os.Exit(1)
	}

	if !coverage {
		return nil
	}

	log.Println("Go unit tests completed successfully.")

	if filtered {
		if err := filterCoverageFile(); err != nil {
			log.Println("Error filtering coverage file:", err)
			os.Exit(1)
		}
	}

	if err := generateCoverageReport(); err != nil {
		log.Println("Error generating coverage report:", err)
		os.Exit(1)
	}

	return nil
}

func generateCoverageReport() error {
	log.Println("Generating coverage report...")

	if _, err := os.Stat(coverageFile); os.IsNotExist(err) {
		return fmt.Errorf("failed to generate coverage report")
	}

	overallCoverage, err := extractOverallCoverage()
	if err != nil {
		return fmt.Errorf("error extracting overall coverage: %w", err)
	}

	coverageThreshold, err := getCoverageThreshold()
	if err != nil {
		return err
	}

	if err := compareCoverageWithThreshold(overallCoverage, coverageThreshold); err != nil {
		return err
	}

	if err := os.Remove(coverageFile); err != nil {
		return fmt.Errorf("error removing coverage file: %w", err)
	}

	return nil
}

func getCoverageThreshold() (int, error) {
	coverageThresholdStr := os.Getenv("COVERAGE_THRESHOLD")
	if coverageThresholdStr == "" {
		return 0, fmt.Errorf("COVERAGE_THRESHOLD environment variable is not set")
	}

	coverageThreshold, err := strconv.Atoi(coverageThresholdStr)
	if err != nil {
		return 0, fmt.Errorf("error parsing COVERAGE_THRESHOLD: %w", err)
	}

	return coverageThreshold, nil
}

func compareCoverageWithThreshold(overallCoverage string, coverageThreshold int) error {
	overallCoverageFloat, err := strconv.ParseFloat(overallCoverage, 64)
	if err != nil {
		return fmt.Errorf("error parsing overall coverage percentage: %w", err)
	}

	if overallCoverageFloat < float64(coverageThreshold) {
		return fmt.Errorf("coverage %.2f%% is below the threshold of %d%%", overallCoverageFloat, coverageThreshold)
	}

	log.Printf("Coverage %.2f%% meets the threshold of %d%%.\n", overallCoverageFloat, coverageThreshold)
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
	UnitTestCmd.Flags().BoolVarP(&filtered, "filtered", "f", false, "Filter the coverage report")
	UnitTestCmd.Flags().BoolVarP(&coverage, "coverage", "u", false, "Run unit tests only")
}
