package jira

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/andygrunwald/go-jira"
	"github.com/spf13/cobra"
)

var (
	username      string
	password      string
	jiraServerUrl string
)

const defaultUrl = "https://jira.ngage.netapp.com"
const jiraApiUser = "JIRA_API_USER"
const jiraApiToken = "JIRA_API_TOKEN"
const jiraServer = "JIRA_SERVER"

var url string

var JiraCmd = &cobra.Command{
	Use:   "jira",
	Short: "A command to handle all jira functionalities",
}

type BaseURL string

type ClientCredentials struct {
	Username string
	Password string
}

func GetJiraUrlCredentials() (BaseURL, ClientCredentials) {
	if jiraServerUrl != "" {
		url = jiraServerUrl
	} else {
		url = defaultUrl
	}

	jiraUrl := BaseURL(url)
	credentials := ClientCredentials{
		Username: username,
		Password: password,
	}

	return jiraUrl, credentials
}

func ExtractJiraID(prTitle string) (string, error) {
	re := regexp.MustCompile(`^(VSCP-[0-9]+):`) // Assuming PR is of type VSCP-1234: <title>
	matches := re.FindStringSubmatch(prTitle)
	if len(matches) == 0 {
		return "", fmt.Errorf("PR title is not in the correct format. Required format: VSCP-<IssueNumber>: <title>\nCurrent PR title: %s", prTitle)
	}
	return matches[1], nil
}

func GetJiraIssue(jiraID string, credentials ClientCredentials, baseURL string) (*jira.Issue, error) {
	tp := jira.BearerAuthTransport{
		Token: credentials.Password,
	}

	client, err := jira.NewClient(tp.Client(), baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create Jira client: %w", err)
	}

	issue, _, err := client.Issue.Get(jiraID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Jira issue: %w", err)
	}

	return issue, nil
}

func UpdateJiraWithPayload(updatedPayload map[string]interface{}, issueID string, credentials ClientCredentials, baseURL string) error {
	tp := jira.BearerAuthTransport{
		Token: credentials.Password,
	}

	client, err := jira.NewClient(tp.Client(), baseURL)
	if err != nil {
		return fmt.Errorf("failed to create Jira client: %w", err)
	}

	payload, _ := json.Marshal(updatedPayload)
	log.Printf("Request Payload: %s\n", string(payload))

	// Perform the update
	_, err = client.Issue.UpdateIssue(issueID, updatedPayload)
	if err != nil {
		return fmt.Errorf("failed to update payload: %w", err)
	}

	log.Println("Successfully updated JIRA issue")
	return nil
}

func init() {
	username = os.Getenv(jiraApiUser)
	password = os.Getenv(jiraApiToken)
	jiraServerUrl = os.Getenv(jiraServer)

	JiraCmd.AddCommand(allowMergeCmd)
	JiraCmd.AddCommand(tagJiraComponentVersionCmd)
}
