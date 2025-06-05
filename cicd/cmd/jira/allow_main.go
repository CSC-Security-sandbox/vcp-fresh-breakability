package jira

import (
	"log"
	"os"

	"github.com/andygrunwald/go-jira"
)

func allowMain(issue *jira.Issue) error {
	issueType := issue.Fields.Type.Name
	if issueType != "Story" && issueType != "Bug" && issueType != "Documentation" {
		log.Println("Error: Issue type can be only 'Story' or 'Bug' or 'Documentation'.")
		os.Exit(1)
	}
	return nil
}
