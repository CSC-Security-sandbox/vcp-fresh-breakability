package jira

import (
	"github.com/spf13/cobra"
	"log"
	ghutils "main/cmd/github"
	"os"
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

	// Retrieve the current value of the custom field
	currentValue, err := GetJiraFieldValue(jiraID, "customfield_15918", credentials, defaultUrl)
	if err != nil {
		log.Println("Error retrieving current value:", err)
		os.Exit(1)
	}

	// Ensure currentValue is a slice of strings
	var updatedValues []string
	// Check if any component version tags already exist
	if currentArray, ok := currentValue.([]interface{}); ok {
		// Convert []interface{} to []string
		var stringArray []string
		tagExists := false

		for _, item := range currentArray {
			if str, ok := item.(string); ok {
				stringArray = append(stringArray, str)
				if str == NewTag {
					tagExists = true
				}
			}
		}
		// Append NewTag only if it does not exist
		if !tagExists {
			updatedValues = append(stringArray, NewTag)
		} else {
			updatedValues = stringArray
		}
	} else {
		updatedValues = []string{NewTag}
	}
	// Prepare the payload with the updated values
	updatedPayload := map[string]interface{}{
		"fields": map[string]interface{}{
			"customfield_15918": updatedValues,
		},
	}

	err = UpdateJiraWithPayload(updatedPayload, jiraID, credentials, defaultUrl)
	if err != nil {
		log.Println("Error updating Jira:", err)
		os.Exit(1)
	}
	return nil
}

func init() {
	NewTag = os.Getenv("NEW_TAG")
}
