package ghutils

import (
	"context"
	"fmt"
	gh "github.com/google/go-github/v50/github"
	"golang.org/x/oauth2"
	"log"
	"os"
)

var (
	PrTitle string
	GhToken string
	PrUser  string
)

func GetPRTitleByCommit(ghToken, owner, repo, commitSHA string) (string, error) {
	client, err := NewGithubClient(ghToken)
	if err != nil {
		return "", err
	}
	log.Printf("Owner: %s, Repo: %s, CommitSHA: %s\n", owner, repo, commitSHA)
	if owner == "" || repo == "" || commitSHA == "" {
		return "", fmt.Errorf("repository owner, name, or commit SHA is not set")
	}

	ctx := context.Background()
	prs, _, err := client.PullRequests.ListPullRequestsWithCommit(ctx, owner, repo, commitSHA, nil)
	if err != nil {
		return "", fmt.Errorf("error fetching pull requests: %w", err)
	}

	for _, pr := range prs {
		if pr.GetMerged() {
			return pr.GetTitle(), nil
		}
	}

	return "", fmt.Errorf("no merged pull request found for the given commit")
}

func NewGithubClient(ghToken string) (*gh.Client, error) {
	if ghToken == "" {
		return nil, fmt.Errorf("GITHUB_TOKEN is not set")
	}

	ctx := context.Background()
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: ghToken})
	tc := oauth2.NewClient(ctx, ts)
	client := gh.NewClient(tc)

	return client, nil
}

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

func init() {
	PrTitle = os.Getenv("PR_TITLE")
	GhToken = os.Getenv("GITHUB_TOKEN")
	PrUser = os.Getenv("PR_USER")
}
