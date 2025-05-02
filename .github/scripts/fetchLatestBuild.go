package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

func main() {
	// Fetch the latest tags from the remote repository
	err := fetchTags()
	if err != nil {
		log.Printf("Error fetching tags:", err)
		os.Exit(1)
	}

	// Define the tag pattern to match
	tagPattern := "*-DEV.*"

	// Execute the git command to list tags sorted by version (descending)
	cmd := exec.Command("git", "tag", "-l", "--sort=-v:refname", tagPattern)
	output, err := cmd.Output()
	if err != nil {
		log.Printf("Error executing git command:", err)
		os.Exit(1)
	}

	// Split the output into individual tags
	tags := strings.Split(strings.TrimSpace(string(output)), "\n")

	// Check if any tags were found
	if len(tags) == 0 || tags[0] == "" {
		log.Printf("No tags found matching the pattern:", tagPattern)
		os.Exit(0)
	}

	// The first tag in the sorted output is the latest
	latestTag := tags[0]

	// Increment the latest tag
	newTag, err := incrementTag(latestTag)
	if err != nil {
		log.Printf("Error incrementing tag:", err)
		os.Exit(1)
	}
        log.SetFlags(0)
	log.Printf(newTag)
        log.SetFlags(log.LstdFlags)
}

// fetchTags fetches the latest tags from the remote repository 
func fetchTags() error {
	cmd := exec.Command("git", "fetch", "--tags")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to fetch tags: %s, output: %s", err, string(output))
	}
	return nil
}

// incrementTag increments the last numeric part of the tag by 1
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
