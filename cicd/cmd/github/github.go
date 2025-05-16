package ghutils

import (
	"context"
	"fmt"
	gh "github.com/google/go-github/v50/github"
	"golang.org/x/oauth2"
	"os"
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

func init() {
	PrTitle = os.Getenv("PR_TITLE")
	GhToken = os.Getenv("GITHUB_TOKEN")
	PrUser = os.Getenv("PR_USER")
}
