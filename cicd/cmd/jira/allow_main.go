package jira

import (
	"log"
	ghutils "main/cmd/github"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var allowMainCmd = &cobra.Command{
	Use:   "main",
	Short: "Command to check if a PR is allowed to be merged with basic jira validations",
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		err := allowMain()
		if err != nil {
			return err
		}
		return nil
	},
}

func allowMain() error {
	_, credentials := GetJiraUrlCredentials()

	jiraID, err := ExtractJiraID(ghutils.PrTitle)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	issue, err := GetJiraIssue(jiraID, credentials, defaultUrl)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	issueType := issue.Fields.Type.Name
	status := issue.Fields.Status.Name
	emailAddress := issue.Fields.Assignee.EmailAddress
	emailAddress = NormalizeEmail(emailAddress)

	if issueType != "Story" && issueType != "Bug" {
		log.Println("Error: Issue type can be only 'Story' or 'Bug'.")
		os.Exit(1)
	}

	if status != "In Development" {
		log.Println("Error:Issue Status is not 'IN Development'.")
		os.Exit(1)
	}

	user, err := ghutils.GetGithubUser(ghutils.GhToken, ghutils.PrUser)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	if user.Email == nil {
		log.Println("Error: Email not available for user:", ghutils.PrUser)
		os.Exit(1)
	}

	ghEmail := *user.Email
	ghEmail = NormalizeEmail(ghEmail)

	if emailAddress != ghEmail {
		log.Println("Email mismatch. Authors do not match.")
		os.Exit(1)
	} else {
		log.Println("Allowed to merge.")
	}
	return nil
}

// NormalizeEmail converts the email to lower case
func NormalizeEmail(email string) string {
	email = strings.ToLower(email)
	return email
}
