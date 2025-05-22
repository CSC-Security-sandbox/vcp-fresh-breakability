package jira

import (
	"fmt"
	"github.com/andygrunwald/go-jira"
	"log"
	"os"
)

func allowRelease(issue *jira.Issue) error {
	// Check if type is RC/HF only
	err := ValidateIssueType(issue, "RC/HF Approval", "Issue link type can be only 'RC/HF Approval'")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	fieldsMap, err := extractFieldsAsMap(issue)
	if err != nil {
		log.Println("Error converting Fields to map:", err)
		os.Exit(1)
	}

	// Dev Approval
	err = ValidateCustomField(fieldsMap, "customfield_19030", "Dev Approval")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	// QA Approval
	err = ValidateCustomField(fieldsMap, "customfield_19031", "QA Approval")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	// Release Approval
	err = ValidateCustomField(fieldsMap, "customfield_19033", "Release Approval")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	err = checkLinkedIssuesStatus(issue)
	log.Println("Linked Issues Status Check Completed")
	if err != nil {
		log.Println("Error checking linked issues status:", err)
		os.Exit(1)
	}

	return nil
}

func checkLinkedIssuesStatus(issue *jira.Issue) error {
	log.Println("Checking linked issues status...")
	if issue.Fields.IssueLinks == nil || len(issue.Fields.IssueLinks) == 0 {
		log.Println("No issue links/parent links found.")
		os.Exit(1)
	}

	_, credentials := GetJiraUrlCredentials()
	client, err := GetJiraClient(credentials, defaultUrl)

	if err != nil {
		return fmt.Errorf("failed to create Jira client: %w", err)
	}

	// For every issue link, check if the PR is merged
	for _, link := range issue.Fields.IssueLinks {
		if link.OutwardIssue == nil {
			log.Println("Skipping link with no outward issue.")
			continue
		}

		linkedIssueKey := link.OutwardIssue.Key
		linkedIssue, _, err := client.Issue.Get(linkedIssueKey, nil)
		if err != nil {
			log.Printf("Failed to fetch linked issue %s: %v\n", linkedIssueKey, err)
			os.Exit(1)
		}

		log.Printf("Linked Issue: %s\n", linkedIssueKey)

		err = ValidateIssueType(linkedIssue, "Bug", "Issue link type can be only 'Bug'")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		err = CheckIssueStatus(linkedIssue, "Done", "Expected in state Done.")
		if err != nil {
			log.Println(err)
			os.Exit(1)
		}

		// Check if linked issue is for same assignee
		err = ValidateAssigneeEmail(linkedIssue)
		if err != nil {
			log.Println("Error in Validating email: ", err)
			os.Exit(1)
		}

		// Extract Merged PR count
		err = checkMergedPrCount(linkedIssue, "customfield_17900")
		if err != nil {
			log.Printf("Error processing custom field for linked issue %s: %v\n", linkedIssueKey, err)
			os.Exit(1)
		}
	}

	return nil
}

func checkMergedPrCount(issue *jira.Issue, fieldKey string) error {
	fieldsMap, err := extractFieldsAsMap(issue)
	if err != nil {
		return fmt.Errorf("error converting Fields to map: %w", err)
	}

	if customField, exists := fieldsMap["customfield_17900"]; exists {
		customFieldStr, ok := customField.(string) // Assert the type as string
		if !ok {
			return fmt.Errorf("customfield_17900 is not a string")
		}

		mergedCount, err := extractMergedCount(customFieldStr)
		if err != nil {
			log.Fatalf("Error extracting mergedCount: %v", err)
		}
		log.Printf("Merged Count: %d\n", mergedCount)
		if mergedCount < 1 {
			log.Println("No merged PR's on issue link. Not allowed to merge.")
			os.Exit(1)
		} else {
			log.Println("Merged count is greater than or equal to 1. Allowed to merge.")
		}
	} else {
		log.Println("No PR's on the linked issue")
		os.Exit(1)
	}

	return nil
}
