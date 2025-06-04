package tag

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var releaseCmd = &cobra.Command{
	Use:   "release",
	Short: "Command to handle release creation logic",
	RunE: func(cmd *cobra.Command, args []string) error {
		ReleaseFunc()
		return nil
	},
}

func ReleaseFunc() {
	file, err := os.OpenFile(os.Getenv("GITHUB_OUTPUT"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error opening file for appending: %v", err)
	}
	defer file.Close()

	errFetch := FetchTagsPrune()
	if errFetch != nil {
		log.Printf("Failed to fetch tags: %v", errFetch)
		return
	}
	CurrentTag := FetchDevTag()
	sprint := strings.Split(CurrentTag, ".")[0]
	log.Printf("Checking if branch exists for release --> %s\n", sprint)

	// Finding if a branch exists for a weekly release
	err_fetch := FetchTagsPrune()
	if err_fetch != nil {
		log.Printf("Failed to fetch tags: %v", err_fetch)
		return
	}

	BranchCheck, err1 := FetchReleaseBranch(sprint)
	if err1 != nil {
		log.Fatalf("Error fetching release branch: %v", err1)
	}

	// If it does not exist, then creating a new branch for that weekly release
	if BranchCheck == "" {
		err := CleanWorkingDirectory()
		if err != nil {
			log.Fatalf("Error cleaning working directory: %v", err)
		}

		DevTagCreation(CurrentTag, file)

		latestSprintBranch := "release/" + sprint
		log.Printf("Creating the main - %s\n", sprint)
		if err := GitCheckout(latestSprintBranch, CurrentTag); err != nil {
			log.Fatalf("Error creating main: %v", err)
		}
		if err := GitPush(latestSprintBranch); err != nil {
			log.Fatalf("Error pushing main: %v", err)
		}

		rcTag := sprint + ".0.0-RC.1"
		errCreateGit := CreateGitTag(rcTag, "")
		if errCreateGit != nil {
			log.Fatalf("Error creating tag: %v", errCreateGit)
		}
		errGitPush := GitPush(rcTag)
		if errGitPush != nil {
			log.Fatalf("Error pushing new tag: %v", errGitPush)
		}
		if _, errWrite := file.WriteString(fmt.Sprintf("RC_TAG=%s\n", rcTag)); errWrite != nil {
			log.Fatalf("Error writing RC_TAG: %v", errWrite)
		}
	} else {
		log.Printf("Branch exists")
	}
}

func DevTagCreation(currTag string, file *os.File) {
	NewSprint := NewSprintName()
	devTag := NewSprint + ".0.0-DEV.1"

	errGitTag := CreateGitTag(devTag, currTag)
	if errGitTag != nil {
		log.Fatalf("Error creating git tag: %v", errGitTag)
	}
	errGitDevPush := GitPush(devTag)
	if errGitDevPush != nil {
		log.Fatalf("Error pushing git tag: %v", errGitDevPush)
	}
	if _, errWrite := file.WriteString(fmt.Sprintf("DEV_TAG=%s\n", devTag)); errWrite != nil {
		log.Fatalf("Error writing DEV_TAG: %v", errWrite)
	}
}
