package jira

import (
	"fmt"
	"github.com/andygrunwald/go-jira"
	"github.com/spf13/cobra"
	"os"
	"regexp"
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
		return "", fmt.Errorf("PR title is not in the correct format")
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

func init() {
	username = os.Getenv(jiraApiUser)
	password = os.Getenv(jiraApiToken)
	jiraServerUrl = os.Getenv(jiraServer)

	JiraCmd.AddCommand(allowMainCmd)
}
