package jira

import (
	"encoding/json"
	"fmt"
	"log"
	ghutils "main/cmd/github"
	"os"
	"regexp"
	"strings"

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

func GetJiraClient(credentials ClientCredentials, baseURL string) (*jira.Client, error) {
	tp := jira.BearerAuthTransport{
		Token: credentials.Password,
	}

	client, err := jira.NewClient(tp.Client(), baseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create Jira client: %w", err)
	}
	return client, nil
}

func GetJiraIssue(jiraID string, client *jira.Client) (*jira.Issue, error) {
	issue, _, err := client.Issue.Get(jiraID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve Jira issue: %w", err)
	}

	return issue, nil
}

func CheckIssueStatus(issue *jira.Issue, expectedStatuses []string, errorMessage string) error {
	for _, expectedStatus := range expectedStatuses {
		if issue.Fields.Status.Name == expectedStatus {
			log.Printf("Issue %s is in the expected status: %s.\n", issue.Key, expectedStatus)
			return nil
		}
	}
	log.Printf("Issue %s is not in the expected statuses (Status: %s). %s\n", issue.Key, issue.Fields.Status.Name, errorMessage)
	return fmt.Errorf("Issue %s is not in the expected statuses: %v", issue.Key, expectedStatuses)
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

func ValidateAssigneeEmail(issue *jira.Issue) error {
	if issue.Fields.Assignee == nil || issue.Fields.Assignee.EmailAddress == "" {
		return fmt.Errorf("assignee or email address is missing")
	}

	emailAddress := NormalizeEmail(issue.Fields.Assignee.EmailAddress)
	log.Println("Email Address:", emailAddress)

	ghUser, err := ghutils.GetGithubUser(ghutils.GhToken, ghutils.PrUser)
	if err != nil {
		log.Println("Error:", err)
		os.Exit(1)
	}

	if ghUser.Email == nil {
		return fmt.Errorf("email not available for GitHub user: %s", ghutils.PrUser)
	}

	ghEmail := NormalizeEmail(*ghUser.Email)

	if emailAddress != ghEmail {
		return fmt.Errorf("email mismatch: authors do not match")
	}

	log.Println("Emails match. Validation successful.")
	return nil
}

func ValidateIssueType(issue *jira.Issue, expectedType string, errorMessage string) error {
	issueType := issue.Fields.Type.Name
	if issueType != expectedType {
		log.Printf("Error: %s (Expected: %s, Found: %s)\n", errorMessage, expectedType, issueType)
		return fmt.Errorf("issue type mismatch: expected %s, found %s", expectedType, issueType)
	}
	log.Printf("Issue %s is in the expected type: %s.\n", issue.Key, issueType)
	return nil
}

func ValidateCustomField(fieldsMap map[string]interface{}, fieldKey, expectedValue string) error {
	if approvalField, ok := fieldsMap[fieldKey].(map[string]interface{}); ok {
		if value, ok := approvalField["value"].(string); ok {
			log.Printf("Value of %s: %s\n", expectedValue, value)
			if value != "Yes" {
				return fmt.Errorf("%s should be marked Yes. Currently in state: %s", expectedValue, value)
			}
		} else {
			return fmt.Errorf("Value field in %s is not a string or is missing", expectedValue)
		}
	} else {
		return fmt.Errorf("%s is not a map or is missing", expectedValue)
	}
	return nil
}

// NormalizeEmail converts the email to lower case
func NormalizeEmail(email string) string {
	email = strings.ToLower(email)
	return email
}

func extractMergedCount(customField string) (int, error) {
	// Define a regex pattern to extract mergedCount
	pattern := `mergedCount=(\d+)`
	re := regexp.MustCompile(pattern)

	// Find the first match
	matches := re.FindStringSubmatch(customField)
	if len(matches) < 2 {
		return 0, fmt.Errorf("mergedCount not found in customField")
	}

	// Convert the matched value to an integer
	var mergedCount int
	_, err := fmt.Sscanf(matches[1], "%d", &mergedCount)
	if err != nil {
		return 0, fmt.Errorf("failed to parse mergedCount: %w", err)
	}

	return mergedCount, nil
}

func extractFieldsAsMap(issue *jira.Issue) (map[string]interface{}, error) {
	// Marshal the Fields struct into JSON
	fieldsJSON, err := json.Marshal(issue.Fields)
	if err != nil {
		return nil, err
	}

	// Unmarshal the JSON into a map
	var fieldsMap map[string]interface{}
	err = json.Unmarshal(fieldsJSON, &fieldsMap)
	if err != nil {
		return nil, err
	}

	return fieldsMap, nil
}

func init() {
	username = os.Getenv(jiraApiUser)
	password = os.Getenv(jiraApiToken)
	jiraServerUrl = os.Getenv(jiraServer)

	JiraCmd.AddCommand(allowMergeCmd)
	JiraCmd.AddCommand(tagJiraComponentVersionCmd)
}
