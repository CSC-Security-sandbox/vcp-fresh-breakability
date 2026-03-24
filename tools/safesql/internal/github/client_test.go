package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	client := New("test-token")

	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.token != "test-token" {
		t.Errorf("expected token 'test-token', got %q", client.token)
	}
	if client.baseURL != "https://api.github.com" {
		t.Errorf("expected baseURL 'https://api.github.com', got %q", client.baseURL)
	}
	if client.httpClient == nil {
		t.Error("expected non-nil httpClient")
	}
}

func TestParseGitHubRefFull(t *testing.T) {
	ref := "owner/repo@branch:path/to/file.sql"
	source, err := ParseGitHubRef(ref, "", "")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if source.Owner != "owner" {
		t.Errorf("expected Owner 'owner', got %q", source.Owner)
	}
	if source.Repo != "repo" {
		t.Errorf("expected Repo 'repo', got %q", source.Repo)
	}
	if source.Branch != "branch" {
		t.Errorf("expected Branch 'branch', got %q", source.Branch)
	}
	if source.FilePath != "path/to/file.sql" {
		t.Errorf("expected FilePath 'path/to/file.sql', got %q", source.FilePath)
	}
}

func TestParseGitHubRefShort(t *testing.T) {
	ref := "main:sql-queries/test.sql"
	source, err := ParseGitHubRef(ref, "default-owner", "default-repo")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if source.Owner != "default-owner" {
		t.Errorf("expected Owner 'default-owner', got %q", source.Owner)
	}
	if source.Repo != "default-repo" {
		t.Errorf("expected Repo 'default-repo', got %q", source.Repo)
	}
	if source.Branch != "main" {
		t.Errorf("expected Branch 'main', got %q", source.Branch)
	}
	if source.FilePath != "sql-queries/test.sql" {
		t.Errorf("expected FilePath 'sql-queries/test.sql', got %q", source.FilePath)
	}
}

func TestParseGitHubRefShortNoDefaults(t *testing.T) {
	ref := "main:test.sql"
	_, err := ParseGitHubRef(ref, "", "")

	if err == nil {
		t.Error("expected error for short format without defaults")
	}
	if !strings.Contains(err.Error(), "default repository configuration") {
		t.Errorf("expected error about default repository, got: %v", err)
	}
}

func TestParseGitHubRefInvalid(t *testing.T) {
	invalidRefs := []string{
		"invalid",
		"no-colon",
		"",
	}

	for _, ref := range invalidRefs {
		t.Run(ref, func(t *testing.T) {
			_, err := ParseGitHubRef(ref, "", "")
			if err == nil {
				t.Errorf("expected error for invalid ref %q", ref)
			}
		})
	}
}

func TestGetFile(t *testing.T) {
	// Create a mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/repos/owner/repo/contents/") {
			// File contents endpoint
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"content": "U0VMRUNUICogRlJPTSB1c2Vycw==",
				"encoding": "base64",
				"sha": "abc123",
				"path": "test.sql",
				"html_url": "https://github.com/owner/repo/blob/main/test.sql"
			}`))
		} else if strings.HasPrefix(r.URL.Path, "/repos/owner/repo/commits") {
			// Commits endpoint
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"sha": "commit123"}]`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	source := &FileSource{
		Owner:    "owner",
		Repo:     "repo",
		Branch:   "main",
		FilePath: "test.sql",
	}

	ctx := context.Background()
	content, err := client.GetFile(ctx, source)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content.Content != "SELECT * FROM users" {
		t.Errorf("expected content 'SELECT * FROM users', got %q", content.Content)
	}
	if content.SHA != "abc123" {
		t.Errorf("expected SHA 'abc123', got %q", content.SHA)
	}
	if content.CommitSHA != "commit123" {
		t.Errorf("expected CommitSHA 'commit123', got %q", content.CommitSHA)
	}
}

func TestGetFileNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message": "Not Found"}`))
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	source := &FileSource{
		Owner:    "owner",
		Repo:     "repo",
		Branch:   "main",
		FilePath: "nonexistent.sql",
	}

	ctx := context.Background()
	_, err := client.GetFile(ctx, source)

	if err == nil {
		t.Error("expected error for not found file")
	}
}

func TestGetPR(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/pulls/42") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{
				"number": 42,
				"title": "Test PR",
				"html_url": "https://github.com/owner/repo/pull/42",
				"state": "open",
				"user": {"login": "testuser"},
				"head": {"ref": "feature-branch", "sha": "head123"},
				"base": {"ref": "main"}
			}`))
		} else if strings.HasSuffix(r.URL.Path, "/reviews") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"state": "APPROVED", "user": {"login": "reviewer1"}}]`))
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	ctx := context.Background()
	pr, err := client.GetPR(ctx, "owner", "repo", 42)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pr.Number != 42 {
		t.Errorf("expected Number 42, got %d", pr.Number)
	}
	if pr.Title != "Test PR" {
		t.Errorf("expected Title 'Test PR', got %q", pr.Title)
	}
	if pr.State != "open" {
		t.Errorf("expected State 'open', got %q", pr.State)
	}
	if pr.Author != "testuser" {
		t.Errorf("expected Author 'testuser', got %q", pr.Author)
	}
	if pr.HeadBranch != "feature-branch" {
		t.Errorf("expected HeadBranch 'feature-branch', got %q", pr.HeadBranch)
	}
}

func TestGetPRApprovers(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"state": "APPROVED", "user": {"login": "approver1"}},
			{"state": "COMMENTED", "user": {"login": "commenter"}},
			{"state": "APPROVED", "user": {"login": "approver2"}},
			{"state": "CHANGES_REQUESTED", "user": {"login": "requester"}}
		]`))
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	ctx := context.Background()
	approvers, err := client.GetPRApprovers(ctx, "owner", "repo", 42)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(approvers) != 2 {
		t.Errorf("expected 2 approvers, got %d: %v", len(approvers), approvers)
	}
}

func TestListPRFiles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"filename": "test.sql", "status": "added", "sha": "sha1"},
			{"filename": "test-plan.json", "status": "added", "sha": "sha2"}
		]`))
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	ctx := context.Background()
	files, err := client.ListPRFiles(ctx, "owner", "repo", 42)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d", len(files))
	}
	if files[0].Filename != "test.sql" {
		t.Errorf("expected first file 'test.sql', got %q", files[0].Filename)
	}
}

func TestVerifyCommitUnchanged(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"sha": "expected-sha"}]`))
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	source := &FileSource{
		Owner:    "owner",
		Repo:     "repo",
		Branch:   "main",
		FilePath: "test.sql",
	}

	ctx := context.Background()

	// Test matching SHA
	unchanged, currentSHA, err := client.VerifyCommitUnchanged(ctx, source, "expected-sha")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !unchanged {
		t.Error("expected unchanged to be true for matching SHA")
	}
	if currentSHA != "expected-sha" {
		t.Errorf("expected currentSHA 'expected-sha', got %q", currentSHA)
	}

	// Test non-matching SHA
	unchanged, _, err = client.VerifyCommitUnchanged(ctx, source, "different-sha")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unchanged {
		t.Error("expected unchanged to be false for different SHA")
	}
}

func TestGetBranchHead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"object": {"sha": "branch-head-sha"}}`))
	}))
	defer server.Close()

	client := New("test-token")
	client.baseURL = server.URL

	ctx := context.Background()
	sha, err := client.GetBranchHead(ctx, "owner", "repo", "main")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sha != "branch-head-sha" {
		t.Errorf("expected SHA 'branch-head-sha', got %q", sha)
	}
}

func TestPRMetadataMerged(t *testing.T) {
	mergedAt := time.Now().UTC()
	metadata := &PRMetadata{
		Number:     1,
		Title:      "Test PR",
		State:      "merged",
		MergedAt:   &mergedAt,
		HeadBranch: "feature",
		BaseBranch: "main",
	}

	if metadata.MergedAt == nil {
		t.Error("expected MergedAt to be set")
	}
	if metadata.State != "merged" {
		t.Errorf("expected State 'merged', got %q", metadata.State)
	}
}

func TestFileSource(t *testing.T) {
	source := &FileSource{
		Owner:    "owner",
		Repo:     "repo",
		Branch:   "main",
		FilePath: "path/to/file.sql",
	}

	if source.Owner != "owner" {
		t.Errorf("expected Owner 'owner', got %q", source.Owner)
	}
	if source.FilePath != "path/to/file.sql" {
		t.Errorf("expected FilePath 'path/to/file.sql', got %q", source.FilePath)
	}
}

func TestFileContent(t *testing.T) {
	content := &FileContent{
		Content:   "SELECT * FROM users",
		SHA:       "file-sha",
		CommitSHA: "commit-sha",
		Path:      "test.sql",
		URL:       "https://github.com/owner/repo/blob/main/test.sql",
	}

	if content.Content != "SELECT * FROM users" {
		t.Errorf("unexpected Content: %q", content.Content)
	}
	if content.SHA != "file-sha" {
		t.Errorf("unexpected SHA: %q", content.SHA)
	}
}

func TestPRFile(t *testing.T) {
	file := &PRFile{
		Filename: "test.sql",
		Status:   "added",
		SHA:      "file-sha",
	}

	if file.Filename != "test.sql" {
		t.Errorf("unexpected Filename: %q", file.Filename)
	}
	if file.Status != "added" {
		t.Errorf("unexpected Status: %q", file.Status)
	}
}
