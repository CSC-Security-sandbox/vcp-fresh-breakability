package jira

import (
	"log"
	ghutils "main/cmd/github"
	"os"

	"github.com/spf13/cobra"
)

var (
	NewTag string
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
	log.Println("Updating jira component tag to:", NewTag)

	_, credentials := GetJiraUrlCredentials()
	jiraID, err := ExtractJiraID(ghutils.PrTitle)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	updatedPayload := map[string]interface{}{
		"fields": map[string]interface{}{
			"customfield_15918": []string{NewTag},
		},
	}

	err = UpdateJiraWithPayload(updatedPayload, jiraID, credentials, defaultUrl)
	if err != nil {
		log.Println("Error:", err)
	}
	return nil
}

func init() {
	NewTag = os.Getenv("NEW_TAG")
}
