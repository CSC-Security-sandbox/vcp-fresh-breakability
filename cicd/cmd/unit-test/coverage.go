package unitTest

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

var CoverageCmd = &cobra.Command{
	Use:   "coverage",
	Short: "A cli used to control code coverage functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := RunTestsWithCoverage(filtered); err != nil {
			return err
		}
		return nil
	},
}

var filtered bool

func RunTestsWithCoverage(filtered bool) error {
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
	CoverageCmd.Flags().BoolVarP(&filtered, "filtered", "f", false, "Filter the coverage report")
}
