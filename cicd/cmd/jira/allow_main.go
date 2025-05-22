package jira

import (
	"github.com/andygrunwald/go-jira"
	"log"
	"os"
)

func allowMain(issue *jira.Issue) error {
	issueType := issue.Fields.Type.Name
	if issueType != "Story" && issueType != "Bug" {
		log.Println("Error: Issue type can be only 'Story' or 'Bug'.")
		os.Exit(1)
	}
	log.Printf("Issue %s is in the expected type: %s.\n", issue.Key, issueType)
	return nil
}
