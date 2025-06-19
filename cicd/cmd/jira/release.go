package jira

import (
	"fmt"
	"log"
	ghutils "main/cmd/github"
	"os"
	"os/exec"
	"strings"

	"github.com/andygrunwald/go-jira"
)

func allowRelease(issue *jira.Issue) error {
	// Check if type is RC/HF only
	err := ValidateIssueType(issue, "RC/HF Approval", "Issue link type can be only 'RC/HF Approval'")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	fieldsMap, err := ExtractFieldsAsMap(issue)
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
	// Get target release version
	targetVersionName, err := GetTargetVersionName(fieldsMap)
	if err != nil {
		log.Println("Error obtaining RC targetVersion: ", err)
		os.Exit(1)
	}
	// RC/HF main/Hotfix backport
	err = validateTargetVersionBranchMatch(targetVersionName)
	if err != nil {
		log.Println("Error in validating RC/HF target branch with base branch:", err)
		os.Exit(1)
	}
	log.Println("RC/HF target branch validation completed successfully")
	return nil
}

func GetTargetVersionName(fieldsMap map[string]interface{}) (string, error) {
	if targetVersion, ok := fieldsMap["customfield_19600"].(map[string]interface{}); ok {
		if targetVersionName, ok := targetVersion["name"].(string); ok {
			log.Printf("RC/HF Target Version: %s\n", targetVersionName)
			return targetVersionName, nil
		} else {
			return "", fmt.Errorf("RC/HF Target Version is not a string or is missing")
		}
	} else {
		return "", fmt.Errorf("RC/HF Target Version value is not set in the given RC/HF issue")
	}
}

// RCHF main backport
func validateTargetVersionBranchMatch(targetVersion string) error {
	err := ghutils.FetchTags()
	if err != nil {
		log.Printf("Error fetching tags: %s", err)
		os.Exit(1)
	}
	// Ensure Target version is matching target branch release "version"
	baseRefTag, err := checkAndExtractBaseRef(githubBaseRef)
	if err != nil {
		return err
	} else {
		log.Println("Extracted branch tag:", baseRefTag)
	}
	extractedTagVersion, err := extractTargetVersion(targetVersion)
	if err != nil {
		return err
	} else {
		log.Println("Extracted jira target version:", extractedTagVersion)
	}
	if extractedTagVersion != baseRefTag {
		return fmt.Errorf("Rc/HF Target version %s does not match the expected base branch release version %s.\n", extractedTagVersion, baseRefTag)
	} else {
		log.Printf("Target version %s matches the expected base branch release %s.\n", extractedTagVersion, baseRefTag)
	}
	// Ensure release branch is not final tagged
	tagExists, err := checkGitTagExists(targetVersion)
	if err != nil {
		return fmt.Errorf("error checking git tag for release: %w", err)
	} else if tagExists {
		return fmt.Errorf("Git tag '%s' exists. Please choose the next hotfix/backport branch version\n", targetVersion)
	} else {
		log.Printf("Git tag '%s' does not exist.\n", targetVersion)
	}
	// Check if the branch is RC tagged
	err = isRCTagged(targetVersion)
	if err != nil {
		return err
	}
	return nil
}

func isRCTagged(targetVersion string) error {
	log.Printf("Finding latest RC for target version:%s-RC.*", targetVersion)
	tagPattern := fmt.Sprintf("%s-RC.*", targetVersion)
	cmd := exec.Command("git", "tag", "-l", "--sort=-v:refname", tagPattern)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("error executing git command: %w", err)
	}
	// Parse the tags and extract RC numbers
	tags := strings.Split(strings.TrimSpace(string(output)), "\n")
	if len(tags) == 0 || tags[0] == "" {
		return fmt.Errorf("rc tag for %s does not exist. Please check if the first build of the release is created by build team", tagPattern)
	}
	log.Println("RC tag exists")
	return nil
}

func checkGitTagExists(tagName string) (bool, error) {
	// Execute the git command to list tags matching the given tag name
	cmd := exec.Command("git", "tag", "-l", tagName)
	output, err := cmd.Output()
	if err != nil {
		return false, fmt.Errorf("error executing git command: %w", err)
	}
	// Check if the output contains the tag name
	return strings.TrimSpace(string(output)) == tagName, nil
}

func extractTargetVersion(version string) (string, error) {
	parts := strings.Split(version, ".")
	if len(parts) > 0 {
		return parts[0], nil
	}
	return "", fmt.Errorf("invalid version format: %s", version)
}

func checkAndExtractBaseRef(githubBaseRef string) (string, error) {
	// Check if githubBaseRef starts with "release/2"
	if strings.HasPrefix(githubBaseRef, "release/2") {
		// Extract the value after "release/"
		parts := strings.SplitN(githubBaseRef, "/", 2)
		if len(parts) == 2 {
			return parts[1], nil
		}
		return "", fmt.Errorf("invalid format: %s", githubBaseRef)
	}
	return "", fmt.Errorf("githubBaseRef does not match the expected format")
}

func checkLinkedIssuesStatus(issue *jira.Issue) error {
	log.Println("Checking linked issues status...")
	if issue.Fields.IssueLinks == nil || len(issue.Fields.IssueLinks) == 0 {
		log.Println("No issue links/parent links found.")
		os.Exit(1)
	}
	_, credentials := GetJiraUrlCredentials()
	client, err := GetJiraClient(credentials, DefaultUrl)
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
		if linkedIssue.Fields.Status.Name != "Done" {
			log.Printf("Error: Linked issue %s is not in the expected status 'Done'. Current status: '%s'.\n", linkedIssue.Key, linkedIssue.Fields.Status.Name)
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
	fieldsMap, err := ExtractFieldsAsMap(issue)
	if err != nil {
		return fmt.Errorf("error converting Fields to map: %w", err)
	}
	if customField, exists := fieldsMap[fieldKey]; exists {
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
