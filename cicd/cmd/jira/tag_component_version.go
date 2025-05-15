package jira

import (
	"github.com/spf13/cobra"
	"log"
	ghutils "main/cmd/github"
	"os"
)

var (
	newTag    string
	ghToken   string
	owner     string
	repo      string
	commitSHA string
)

var tagJiraComponentVersionCmd = &cobra.Command{
	Use:   "tag_component_version",
	Short: "Command tag component version",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := updateComponentVersionTag()
		if err != nil {
			return err
		}
		return nil
	},
}

func updateComponentVersionTag() error {
	log.Println("Updating jira component tag to:", newTag)

	_, credentials := GetJiraUrlCredentials()
	prTitle, err := ghutils.GetPRTitleByCommit(ghToken, owner, repo, commitSHA)
	if err != nil {
		log.Fatalf("Error fetching PR title: %v", err)
	}
	jiraID, err := ExtractJiraID(prTitle)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	updatedPayload := map[string]interface{}{
		"fields": map[string]interface{}{
			"customfield_15918": []string{newTag},
		},
	}

	err = UpdateJiraWithPayload(updatedPayload, jiraID, credentials, defaultUrl)
	if err != nil {
		log.Println("Error:", err)
	}
	return nil
}

func init() {
	newTag = os.Getenv("NEW_TAG")
	ghToken = os.Getenv("GITHUB_TOKEN")
	owner = os.Getenv("PR_USER")
	repo = os.Getenv("REPOSITORY")
	commitSHA = os.Getenv("GITHUB_SHA")
}
