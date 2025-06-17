package tag

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var promotionCmd = &cobra.Command{
	Use:   "promotional",
	Short: "Command to handle release creation logic",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		SetupConfigobject.PromotionalFunc()
		return nil
	},
}

func (o *SetupConfig) PromotionalFunc() {
	errFetch := FetchTagsPrune()
	if errFetch != nil {
		log.Printf("Failed to fetch tags: %v", errFetch)
		return
	}

	FinalTagCheck := FetchTag(o.FinalRelease)
	if FinalTagCheck == "" {
		log.Printf("Final Tag %s does not exist", o.FinalRelease)
	} else {
		log.Printf("Final Tag %s exists, cannot perform operation", o.FinalRelease)
	}

	BaseTagCheck := FetchTag(o.BaseRelease)
	if BaseTagCheck == "" {
		log.Printf("Base tag %s does not exist, cannot perform operation", o.BaseRelease)
	} else {
		log.Printf("Base Tag %s exists", o.BaseRelease)
	}

	if o.OperationType == "FinalTag" {
		if BaseTagCheck != "" && strings.Contains(o.BaseRelease, "-RC.") && strings.Contains(o.BaseRelease, o.FinalRelease) && FinalTagCheck == "" {
			log.Println("Base release is a valid RC tag, proceeding with promotion.")
			PromotionalCreation()
		} else {
			log.Fatalf("Base release %s is not a valid RC tag or Final Release has a naming error %s", o.BaseRelease, o.FinalRelease)
			return
		}
	} else if o.OperationType == "HFFirstRelease" {
		if BaseTagCheck != "" && CheckForHFfinalName() && FinalTagCheck == "" {
			log.Println("Base release is a valid HF tag, proceeding with promotion.")
			PromotionalCreation()
		} else {
			log.Fatalf("Base release %s is not a valid HF tag or Final Release has a naming error %s", o.BaseRelease, o.FinalRelease)
			return
		}
	} else {
		log.Fatalf("Invalid operation type: %s. Operation can only be FinalTag or HFFirstRelease. ", o.OperationType)
		return
	}
}

func PromotionalCreation() {
	file, err := os.OpenFile(os.Getenv("GITHUB_OUTPUT"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error opening file for appending: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Error closing file: %v", err)
		}
	}()

	errFetch := FetchTagsPrune()
	if errFetch != nil {
		log.Printf("Failed to fetch tags: %v", errFetch)
		return
	}

	errClean := CleanWorkingDirectory()
	if errClean != nil {
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
	sprint := strings.Split(SetupConfigobject.FinalRelease, ".")[0]

	branchName := "release/" + sprint

	if _, errWrite := file.WriteString(fmt.Sprintf("FINAL_TAG=%s\n", SetupConfigobject.FinalRelease)); errWrite != nil {
		log.Fatalf("Error writing FINAL_TAG: %v", errWrite)
	}

	if _, errWrite := file.WriteString(fmt.Sprintf("FINAL_BRANCH=%s\n", branchName)); errWrite != nil {
		log.Fatalf("Error writing FINAL_BRANCH: %v", errWrite)
	}
}

func init() {
	TagCmd.AddCommand(promotionCmd)
}
