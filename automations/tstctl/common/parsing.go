package common

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/tstctl/structs"
)

func ReadOutputXML(path string) (string, bool, error) {
	testResult := true
	failure := ""
	details := ""
	message := "" // will accumulate all suite summaries

	xmlFile, err := os.Open(path)
	if err != nil {
		return "", false, fmt.Errorf("open xml: %w", err)
	}
	defer xmlFile.Close()

	byteValue, err := io.ReadAll(xmlFile)
	if err != nil {
		return "", false, fmt.Errorf("read xml: %w", err)
	}

	var testSuites structs.TestSuites
	if err := xml.Unmarshal(byteValue, &testSuites); err != nil {
		return "", false, fmt.Errorf("unmarshal xml: %w", err)
	}

	for _, suite := range testSuites.TestSuites {
		// reuse outer message (no :=)
		line := fmt.Sprintf("Suite: %s | Tests: %d | Failures: %d | Errors: %d | Skipped: %d\n",
			suite.Name, suite.Tests, suite.Failures, suite.Errors, suite.Skipped)
		log.Print(line)
		message += line

		if suite.Tests == suite.Skipped || suite.Failures > 0 || suite.Errors > 0 {
			testResult = false
		}
		log.Printf("  Overall result: %v", testResult)

		for _, tc := range suite.TestCases {
			log.Printf("  Test: %s.%s (%.2fs)", tc.ClassName, tc.Name, tc.Time)
			if tc.Failure != nil {
				failureLine := fmt.Sprintf("%s.%s: %s", tc.ClassName, tc.Name, tc.Failure.Message)
				failure += failureLine + "\n"
				if tc.Failure.Text != "" {
					details += fmt.Sprintf("Details for %s.%s:\n%s\n", tc.ClassName, tc.Name, tc.Failure.Text)
				}
				log.Printf("    ❌ Failed: %s", tc.Failure.Message)
				log.Printf("    Details: %s", tc.Failure.Text)
			} else {
				log.Println("    ✅ Passed")
			}
		}
	}

	return message, testResult, nil
}
