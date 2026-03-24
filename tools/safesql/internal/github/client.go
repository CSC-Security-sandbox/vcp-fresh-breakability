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
	Number     int
	Title      string
	URL        string
	Author     string
	Approvers  []string
	MergedAt   *time.Time
	MergeSHA   string
	State      string // "open", "closed", "merged"
	HeadBranch string
	HeadSHA    string
	BaseBranch string
}

// PRFile represents a file in a pull request.
type PRFile struct {
	Filename string
	Status   string // "added", "modified", "removed"
	SHA      string
	Content  string
}

// New creates a new GitHub client.
func New(token string) *Client {
	return &Client{
		token:      token,
		httpClient: &http.Client{Timeout: 120 * time.Second},
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
	approvers, err := c.GetPRApprovers(ctx, owner, repo, pr.Number)
	if err == nil {
		metadata.Approvers = approvers
	}

	return metadata, nil
}

func (c *Client) GetPRApprovers(ctx context.Context, owner, repo string, prNumber int) ([]string, error) {
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

// GetPR fetches pull request details.
func (c *Client) GetPR(ctx context.Context, owner, repo string, prNumber int) (*PRMetadata, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d",
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
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get PR (status %d): %s", resp.StatusCode, string(body))
	}

	var pr struct {
		Number   int    `json:"number"`
		Title    string `json:"title"`
		HTMLURL  string `json:"html_url"`
		State    string `json:"state"`
		MergedAt string `json:"merged_at"`
		MergeSHA string `json:"merge_commit_sha"`
		User     struct {
			Login string `json:"login"`
		} `json:"user"`
		Head struct {
			Ref string `json:"ref"`
			SHA string `json:"sha"`
		} `json:"head"`
		Base struct {
			Ref string `json:"ref"`
		} `json:"base"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("failed to decode PR: %w", err)
	}

	metadata := &PRMetadata{
		Number:     pr.Number,
		Title:      pr.Title,
		URL:        pr.HTMLURL,
		Author:     pr.User.Login,
		MergeSHA:   pr.MergeSHA,
		State:      pr.State,
		HeadBranch: pr.Head.Ref,
		HeadSHA:    pr.Head.SHA,
		BaseBranch: pr.Base.Ref,
	}

	// Determine if merged
	if pr.MergedAt != "" {
		t, _ := time.Parse(time.RFC3339, pr.MergedAt)
		metadata.MergedAt = &t
		metadata.State = "merged"
	}

	// Get PR reviews to find approvers
	approvers, err := c.GetPRApprovers(ctx, owner, repo, pr.Number)
	if err == nil {
		metadata.Approvers = approvers
	}

	return metadata, nil
}

// ListPRFiles lists all files in a pull request.
func (c *Client) ListPRFiles(ctx context.Context, owner, repo string, prNumber int) ([]*PRFile, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/files",
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
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to list PR files (status %d): %s", resp.StatusCode, string(body))
	}

	var files []struct {
		Filename string `json:"filename"`
		Status   string `json:"status"`
		SHA      string `json:"sha"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, fmt.Errorf("failed to decode PR files: %w", err)
	}

	var prFiles []*PRFile
	for _, f := range files {
		prFiles = append(prFiles, &PRFile{
			Filename: f.Filename,
			Status:   f.Status,
			SHA:      f.SHA,
		})
	}

	return prFiles, nil
}

// GetPRFile fetches a specific file from a PR branch.
func (c *Client) GetPRFile(ctx context.Context, owner, repo, branch, filePath string) (*FileContent, error) {
	source := &FileSource{
		Owner:    owner,
		Repo:     repo,
		Branch:   branch,
		FilePath: filePath,
	}
	return c.GetFile(ctx, source)
}

// CommitFileToPR commits a file to the PR branch.
// Note: This uses GitHub Contents API which signs commits with GitHub's web-flow key.
// For user GPG-signed commits, the repository must allow web-flow signatures,
// or you need to use git operations directly (see CommitFileToPRWithGit).
func (c *Client) CommitFileToPR(ctx context.Context, owner, repo, branch, filePath, content, message string) error {
	// First, try to get the existing file to get its SHA (for updates)
	var existingSHA string
	existingFile, err := c.GetPRFile(ctx, owner, repo, branch, filePath)
	if err == nil {
		existingSHA = existingFile.SHA
	}

	// Create or update file
	url := fmt.Sprintf("%s/repos/%s/%s/contents/%s",
		c.baseURL, owner, repo, filePath)

	encodedContent := base64.StdEncoding.EncodeToString([]byte(content))

	payload := map[string]interface{}{
		"message": message,
		"content": encodedContent,
		"branch":  branch,
	}

	if existingSHA != "" {
		payload["sha"] = existingSHA
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to commit file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to commit file (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// CreatePRReviewWithSuggestion creates a PR review with a commit suggestion
// This allows users to click "Commit suggestion" button to commit with their GPG key
func (c *Client) CreatePRReviewWithSuggestion(ctx context.Context, owner, repo string, prNumber int, filePath, newContent, message string, isUpdate bool) error {
	// Get PR details to get head SHA
	pr, err := c.GetPR(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to get PR: %w", err)
	}

	if isUpdate {
		// For updates to existing files, use single-file review comment with suggestion
		return c.createSingleFileReviewComment(ctx, owner, repo, prNumber, pr.HeadSHA, filePath, newContent, message)
	}

	// For new files, we can't use suggestion syntax (it only works for existing files)
	// Post as a general review comment with instructions
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews",
		c.baseURL, owner, repo, prNumber)

	commentBody := fmt.Sprintf("%s\n\n**Please commit the JSON below to this branch as:** `%s`\n\n"+
		"<details>\n<summary>📄 Click to view plan JSON</summary>\n\n```json\n%s\n```\n</details>",
		message, filePath, newContent)

	payload := map[string]interface{}{
		"commit_id": pr.HeadSHA,
		"body":      commentBody,
		"event":     "COMMENT",
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create review: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create review (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// createSingleFileReviewComment creates a review comment on a single file with suggestion
func (c *Client) createSingleFileReviewComment(ctx context.Context, owner, repo string, prNumber int, commitSHA, filePath, newContent, message string) error {
	// Use the review API with file-level comment
	// GitHub's suggestion feature replaces the entire file content
	url := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews",
		c.baseURL, owner, repo, prNumber)

	commentBody := fmt.Sprintf("%s\n\n**Click 'Commit suggestion' below to update the plan (signed with your GPG key):**\n\n```suggestion\n%s\n```",
		message, newContent)

	// Create a review with a single comment containing the suggestion
	payload := map[string]interface{}{
		"commit_id": commitSHA,
		"body":      message,
		"event":     "COMMENT",
		"comments": []map[string]interface{}{
			{
				"path": filePath,
				"body": commentBody,
				"line": 1, // Suggestions work on any line
			},
		},
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create review: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create review (status %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// CreatePR creates a new pull request.
func (c *Client) CreatePR(ctx context.Context, owner, repo, title, body, head, base string) (int, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/pulls", c.baseURL, owner, repo)

	payload := map[string]interface{}{
		"title": title,
		"body":  body,
		"head":  head,
		"base":  base,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(payloadBytes)))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to create PR: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to create PR (status %d): %s", resp.StatusCode, string(body))
	}

	var result struct {
		Number int `json:"number"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Number, nil
}
