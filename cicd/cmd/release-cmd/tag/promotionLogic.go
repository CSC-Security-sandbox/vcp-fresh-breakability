package tag

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
)

var promotionCmd = &cobra.Command{
	Use:   "promotional",
	Short: "Command to handle release creation logic",
	RunE: func(cmd *cobra.Command, args []string) error {
		PromotionalFunc()
		return nil
	},
}

func PromotionalFunc() {
	if strings.Contains(SetupConfigobject.BaseRelease, "-RC.") {
		PromotionalCreation()
	}
}

func PromotionalCreation() {
	errFetch := FetchTagsPrune()
	if errFetch != nil {
		log.Printf("Failed to fetch tags: %v", errFetch)
		return
	}

	err := CleanWorkingDirectory()
	if err != nil {
		log.Fatalf("Error cleaning working directory: %v", err)
	}

	errGitTag := CreateGitTag(SetupConfigobject.FinalRelease, SetupConfigobject.BaseRelease)
	if errGitTag != nil {
		log.Fatalf("Error creating git tag: %v", errGitTag)
	}

	errGitPush := GitPush(SetupConfigobject.FinalRelease)
	if errGitPush != nil {
		log.Fatalf("Error pushing git tag: %v", errGitPush)
	}

	if errWrite := os.WriteFile(os.Getenv("GITHUB_OUTPUT"), []byte(fmt.Sprintf("FINAL_TAG=%s\n", SetupConfigobject.FinalRelease)), 0644); errWrite != nil {
		log.Fatalf("Error writing to stdout: %v", errWrite)
	}
}

func init() {
	TagCmd.AddCommand(promotionCmd)
}
