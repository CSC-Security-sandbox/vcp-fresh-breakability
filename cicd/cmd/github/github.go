package ghutils

import (
	"context"
	"fmt"
	"os"
	"os/exec"

	gh "github.com/google/go-github/v79/github"
	"golang.org/x/oauth2"
)

var (
	PrTitle string
	GhToken string
	PrUser  string
)

func GetGithubUser(ghToken, prUser string) (*gh.User, error) {
	if ghToken == "" {
		return nil, fmt.Errorf("GITHUB_TOKEN is not set")
	}
	if prUser == "" {
		return nil, fmt.Errorf("PR_USER is not set")
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: ghToken},
	)
	tc := oauth2.NewClient(ctx, ts)
	ghClient := gh.NewClient(tc)

	// Fetch user details
	user, _, err := ghClient.Users.Get(ctx, prUser)
	if err != nil {
		return nil, fmt.Errorf("error fetching GitHub user: %w", err)
	}

	return user, nil
}

// fetchTags fetches the latest tags from the remote repository
func FetchTags() error {
	cmd := exec.Command("git", "fetch", "--tags")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to fetch tags: %s, output: %s", err, string(output))
	}
	return nil
}

func init() {
	PrTitle = os.Getenv("PR_TITLE")
	GhToken = os.Getenv("GITHUB_TOKEN")
	PrUser = os.Getenv("PR_USER")
}
