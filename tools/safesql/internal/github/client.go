// Package github provides GitHub integration for SafeSQL.
package github

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// Client handles GitHub API interactions.
type Client struct {
	token      string
	httpClient *http.Client
	baseURL    string
}

// FileSource represents a GitHub file reference.
type FileSource struct {
	Owner    string
	Repo     string
	Branch   string
	FilePath string
}

// FileContent contains the fetched file and its metadata.
type FileContent struct {
	Content   string
	SHA       string
	CommitSHA string
	Path      string
	URL       string
}

// PRMetadata contains Pull Request information.
type PRMetadata struct {
	Number    int
	Title     string
	URL       string
	Author    string
	Approvers []string
	MergedAt  *time.Time
	MergeSHA  string
}

// New creates a new GitHub client.
func New(token string) *Client {
	return &Client{
		token:      token,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		baseURL:    "https://api.github.com",
	}
}

// ParseGitHubRef parses a GitHub reference string.
// Format: "owner/repo@branch:path/to/file.sql"
// Or short: "branch:path/to/file.sql" (uses default repo)
func ParseGitHubRef(ref string, defaultOwner, defaultRepo string) (*FileSource, error) {
	// Full format: owner/repo@branch:path
	fullRe := regexp.MustCompile(`^([^/]+)/([^@]+)@([^:]+):(.+)$`)
	if matches := fullRe.FindStringSubmatch(ref); len(matches) == 5 {
		return &FileSource{
			Owner:    matches[1],
			Repo:     matches[2],
			Branch:   matches[3],
			FilePath: matches[4],
		}, nil
	}

	// Short format: branch:path (uses defaults)
	shortRe := regexp.MustCompile(`^([^:]+):(.+)$`)
	if matches := shortRe.FindStringSubmatch(ref); len(matches) == 3 {
		if defaultOwner == "" || defaultRepo == "" {
			return nil, fmt.Errorf("short format requires default repository configuration")
		}
		return &FileSource{
			Owner:    defaultOwner,
			Repo:     defaultRepo,
			Branch:   matches[1],
			FilePath: matches[2],
		}, nil
	}

	return nil, fmt.Errorf("invalid GitHub reference format: %s (expected 'owner/repo@branch:path' or 'branch:path')", ref)
}

// GetFile fetches a file from GitHub.
func (c *Client) GetFile(ctx context.Context, source *FileSource) (*FileContent, error) {
	// Get file content from the contents API
	url := fmt.Sprintf("%s/repos/%s/%s/contents/%s?ref=%s",
		c.baseURL, source.Owner, source.Repo, source.FilePath, source.Branch)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GitHub API error (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
		SHA      string `json:"sha"`
		Path     string `json:"path"`
		HTMLURL  string `json:"html_url"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Decode base64 content
	if result.Encoding != "base64" {
		return nil, fmt.Errorf("unexpected encoding: %s", result.Encoding)
	}

	content, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(result.Content, "\n", ""))
	if err != nil {
		return nil, fmt.Errorf("failed to decode content: %w", err)
	}

	// Get the latest commit SHA for this file on the branch
	commitSHA, err := c.getLatestCommitSHA(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("failed to get commit SHA: %w", err)
	}

	return &FileContent{
		Content:   string(content),
		SHA:       result.SHA,
		CommitSHA: commitSHA,
		Path:      result.Path,
		URL:       result.HTMLURL,
	}, nil
}

func (c *Client) getLatestCommitSHA(ctx context.Context, source *FileSource) (string, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/commits?sha=%s&path=%s&per_page=1",
		c.baseURL, source.Owner, source.Repo, source.Branch, source.FilePath)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get commits: status %d", resp.StatusCode)
	}

	var commits []struct {
		SHA string `json:"sha"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&commits); err != nil {
		return "", err
	}

	if len(commits) == 0 {
		return "", fmt.Errorf("no commits found for file")
	}

	return commits[0].SHA, nil
}

// GetPRForCommit finds the PR that introduced a commit.
func (c *Client) GetPRForCommit(ctx context.Context, owner, repo, commitSHA string) (*PRMetadata, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/commits/%s/pulls",
		c.baseURL, owner, repo, commitSHA)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get PRs for commit: status %d", resp.StatusCode)
	}

	var prs []struct {
		Number   int    `json:"number"`
		Title    string `json:"title"`
		HTMLURL  string `json:"html_url"`
		MergedAt string `json:"merged_at"`
		MergeSHA string `json:"merge_commit_sha"`
		User     struct {
			Login string `json:"login"`
		} `json:"user"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&prs); err != nil {
		return nil, err
	}

	if len(prs) == 0 {
		return nil, nil // No PR found (direct push)
	}

	pr := prs[0]
	metadata := &PRMetadata{
		Number:   pr.Number,
		Title:    pr.Title,
		URL:      pr.HTMLURL,
		Author:   pr.User.Login,
		MergeSHA: pr.MergeSHA,
	}

	if pr.MergedAt != "" {
		t, _ := time.Parse(time.RFC3339, pr.MergedAt)
		metadata.MergedAt = &t
	}

	// Get PR reviews to find approvers
	approvers, err := c.getPRApprovers(ctx, owner, repo, pr.Number)
	if err == nil {
		metadata.Approvers = approvers
	}

	return metadata, nil
}

func (c *Client) getPRApprovers(ctx context.Context, owner, repo string, prNumber int) ([]string, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews",
		c.baseURL, owner, repo, prNumber)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get reviews: status %d", resp.StatusCode)
	}

	var reviews []struct {
		State string `json:"state"`
		User  struct {
			Login string `json:"login"`
		} `json:"user"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&reviews); err != nil {
		return nil, err
	}

	approverSet := make(map[string]bool)
	for _, review := range reviews {
		if review.State == "APPROVED" {
			approverSet[review.User.Login] = true
		}
	}

	var approvers []string
	for approver := range approverSet {
		approvers = append(approvers, approver)
	}

	return approvers, nil
}

// VerifyCommitUnchanged checks if the commit SHA still matches.
func (c *Client) VerifyCommitUnchanged(ctx context.Context, source *FileSource, expectedSHA string) (bool, string, error) {
	currentSHA, err := c.getLatestCommitSHA(ctx, source)
	if err != nil {
		return false, "", err
	}
	return currentSHA == expectedSHA, currentSHA, nil
}

// GetBranchHead gets the HEAD commit of a branch.
func (c *Client) GetBranchHead(ctx context.Context, owner, repo, branch string) (string, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/git/ref/heads/%s",
		c.baseURL, owner, repo, branch)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to get branch: status %d", resp.StatusCode)
	}

	var result struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	return result.Object.SHA, nil
}

