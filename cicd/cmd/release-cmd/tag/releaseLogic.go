package tag

import (
	"fmt"
	"github.com/spf13/cobra"
	"log"
	"os"
	"strings"
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

		DevTagCreation(CurrentTag)

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
		if errWrite := os.WriteFile(os.Getenv("GITHUB_OUTPUT"), []byte(fmt.Sprintf("RC_TAG=%s\n", rcTag)), 0644); errWrite != nil {
			log.Fatalf("Error writing to stdout: %v", errWrite)
		}
		ReleaseGithub(true, rcTag)
	} else {
		log.Printf("Branch exists")
	}
}

func DevTagCreation(currTag string) {
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
	if errWrite := os.WriteFile(os.Getenv("GITHUB_OUTPUT"), []byte(fmt.Sprintf("DEV_TAG=%s\n", devTag)), 0644); errWrite != nil {
		log.Fatalf("Error writing to stdout: %v", errWrite)
	}
	ReleaseGithub(true, devTag)
}
