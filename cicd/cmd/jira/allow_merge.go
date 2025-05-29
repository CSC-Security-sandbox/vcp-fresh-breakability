package jira

import (
	"github.com/spf13/cobra"
	"log"
	ghutils "main/cmd/github"
	"os"
	"strings"
)

var githubBaseRef string

var allowMergeCmd = &cobra.Command{
	Use:   "allow_merge",
	Short: "Command to check if a PR is allowed to be merged with basic jira validations",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := allowMerge()
		if err != nil {
			return err
		}
		return nil
	},
}

func allowMerge() error {
	// Get Jira Creds
	_, credentials := GetJiraUrlCredentials()

	jiraID, err := ExtractJiraID(ghutils.PrTitle)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}
	client, err := GetJiraClient(credentials, defaultUrl)
	if err != nil {
		log.Println("Error creating jira client:", err)
		os.Exit(1)
	}

	issue, err := GetJiraIssue(jiraID, client)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	err = CheckIssueStatus(issue, []string{"In Development", "In Progress"}, "Expected in state In Development or In Progress.")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = ValidateAssigneeEmail(issue)
	if err != nil {
		log.Println("Error in Validating email: ", err)
		os.Exit(1)
	}

	if githubBaseRef == "main" {
		err := allowMain(issue)
		if err != nil {
			return err
		}
	} else if strings.HasPrefix(githubBaseRef, "release/") {
		err := allowRelease(issue)
		if err != nil {
			return err
		}
	} else {
		log.Println("Error: Invalid base branch. Allowed branches are main and release/*")
		os.Exit(1)
	}
	return nil
}

func init() {
	githubBaseRef = os.Getenv("BASE_BRANCH")
}
