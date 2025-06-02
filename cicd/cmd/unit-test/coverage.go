package unitTest

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var CoverageCmd = &cobra.Command{
	Use:   "coverage",
	Short: "A cli used to control code coverage functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := generateCoverageReport(); err != nil {
			log.Println("Error generating coverage report:", err)
			os.Exit(1)
			return err
		}
		return nil
	},
}

var filtered bool

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
