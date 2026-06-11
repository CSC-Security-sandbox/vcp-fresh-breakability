package tag

import (
	"fmt"
	"log"
	ghutils "main/cmd/github"
	"main/cmd/jira"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
)

var githubBase string
var FetchLatestBuildCmd = &cobra.Command{
	Use:   "fetch_latest_build",
	Short: "Command tag component version",
	RunE: func(cmd *cobra.Command, args []string) error {
		log.Println("Fetching latest build...")
		cmd.SilenceUsage = true
		// Define a variable to hold the newTag
		var newTag string

		// Call fetchLatestBuild function and capture the newTag
		err := func() error {
			err := fetchLatestBuild(&newTag)
			if err != nil {
				return err
			}
			return nil
		}()

		if err != nil {
			return err
		}

		// Set the newTag as a GitHub Action output variable
		os.Stdout.Write([]byte(newTag + "\n"))

		return nil
	},
}

func fetchLatestBuild(newTag *string) error {
	checkCmd := exec.Command("git", "describe", "--exact-match", "--tags", "HEAD")
	if output, err := checkCmd.Output(); err == nil {
		existingTag := strings.TrimSpace(string(output))
		log.Printf("HEAD already has tag: %s, skipping tag creation", existingTag)
		*newTag = existingTag
		return nil
	}

	// Fetch the latest tags from the remote repository
	err := ghutils.FetchTags()
	if err != nil {
		return fmt.Errorf("error fetching tags: %w", err)
	}

	// Define the tag pattern to match
	var tagPattern string
	if strings.TrimSpace(githubBase) == "main" {
		tagPattern = "2*-DEV.*"
	} else {
		targetVersionName, err := fetchTargetVersion(ghutils.PrTitle)
		if err != nil {
			log.Println("Error fetching target version:", err)
			os.Exit(1)
		}
		if isOCIBranch() {
			tagPattern = fmt.Sprintf("%s-OCI-RC.*", targetVersionName)
		} else {
			tagPattern = fmt.Sprintf("%s-RC.*", targetVersionName)
		}
	}

	// Execute the git command to list tags sorted by version (descending)
	cmd := exec.Command("git", "tag", "-l", "--sort=-v:refname", tagPattern)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("error executing git command: %w", err)
	}

	// Split the output into individual tags
	tags := strings.Split(strings.TrimSpace(string(output)), "\n")

	// Check if any tags were found
	if len(tags) == 0 || tags[0] == "" {
		log.Printf("No tags found matching the pattern: %s", tagPattern)
		return nil
	}

	// The first tag in the sorted output is the latest
	latestTag := tags[0]

	// Increment the latest tag
	newTagVal, err := incrementTag(latestTag)
	if err != nil {
		return fmt.Errorf("error incrementing tag: %w", err)
	}
	*newTag = newTagVal
	log.Printf("Calculated new tag: %s", *newTag)

	// Create the tag locally
	createTagCmd := exec.Command("git", "tag", "-a", *newTag, "-m", fmt.Sprintf("Auto-generated tag %s", *newTag))
	if output, err := createTagCmd.CombinedOutput(); err != nil {
		if strings.Contains(string(output), "already exists") {
			log.Printf("Tag %s already exists locally", *newTag)
		} else {
			return fmt.Errorf("failed to create tag: %w, output: %s", err, string(output))
		}
	}

	// Push the tag to remote
	pushTagCmd := exec.Command("git", "push", "origin", *newTag)
	if output, err := pushTagCmd.CombinedOutput(); err != nil {
		if strings.Contains(string(output), "already exists") {
			log.Printf("Tag %s already exists on remote, continuing", *newTag)
		} else {
			return fmt.Errorf("failed to push tag: %w, output: %s", err, string(output))
		}
	}

	log.Printf("Successfully created and pushed tag: %s", *newTag)

	return nil
}

func isOCIBranch() bool {
	return strings.HasPrefix(strings.TrimSpace(githubBase), "release/oci/")
}

func fetchTargetVersion(prTitle string) (string, error) {
	// Fetch Jira credentials
	log.Println("Fetching Jira credentials...")
	_, credentials := jira.GetJiraUrlCredentials()

	// Extract Jira ID from PR title
	jiraID, err := jira.ExtractJiraID(prTitle)
	if err != nil {
		return "", fmt.Errorf("error extracting Jira ID: %w", err)
	}
	log.Println("PR Title:", prTitle, "Jira ID:", jiraID)

	// Create Jira client
	client, err := jira.GetJiraClient(credentials, jira.DefaultUrl)
	if err != nil {
		return "", fmt.Errorf("error creating Jira client: %w", err)
	}

	// Fetch Jira issue
	issue, err := jira.GetJiraIssue(jiraID, client)
	if err != nil {
		return "", fmt.Errorf("error fetching Jira issue: %w", err)
	}

	// Extract fields as a map
	fieldsMap, err := jira.ExtractFieldsAsMap(issue)
	if err != nil {
		return "", fmt.Errorf("error converting fields to map: %w", err)
	}

	// Get target version name
	targetVersionName, err := jira.GetTargetVersionName(fieldsMap)
	if err != nil {
		return "", fmt.Errorf("error fetching target version name: %w", err)
	}

	return targetVersionName, nil
}

func incrementTag(tag string) (string, error) {
	// Split the tag into parts by "."
	parts := strings.Split(tag, ".")
	if len(parts) < 3 {
		return "", fmt.Errorf("invalid tag format: %s", tag)
	}

	// Parse the last part as an integer
	lastPart := parts[len(parts)-1]
	lastNum, err := strconv.Atoi(lastPart)
	if err != nil {
		return "", fmt.Errorf("failed to parse last part of tag as number: %s", lastPart)
	}

	// Increment the last number
	lastNum++

	// Reconstruct the tag with the incremented number
	parts[len(parts)-1] = strconv.Itoa(lastNum)
	return strings.Join(parts, "."), nil
}

func init() {
	githubBase = os.Getenv("GITHUB_BASE")
}
