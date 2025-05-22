package unitTest

import (
	"bytes"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/exec"
)

var UnitTestCmd = &cobra.Command{
	Use:   "unit-test",
	Short: "A cli used to control unit-test functionalities",
	RunE: func(cmd *cobra.Command, args []string) error {
		if err := runGoTests(); err != nil {
			return err
		}
		return nil
	},
}

const coverageFile = "coverage.out"
const excludeFile = "./cicd/cmd/unit-test/exclude-from-code-coverage"

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

func init() {
	UnitTestCmd.AddCommand(CoverageCmd)
}
