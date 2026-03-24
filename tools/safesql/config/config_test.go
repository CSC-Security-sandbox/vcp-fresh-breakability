package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test GitHub defaults
	if cfg.GitHub.Branch != "sql-queries" {
		t.Errorf("expected Branch to be 'sql-queries', got %q", cfg.GitHub.Branch)
	}
	if !cfg.GitHub.RequireGitHubSource {
		t.Error("expected RequireGitHubSource to be true")
	}
	if !cfg.GitHub.RequireMergedPR {
		t.Error("expected RequireMergedPR to be true")
	}
	if cfg.GitHub.MinApprovers != 1 {
		t.Errorf("expected MinApprovers to be 1, got %d", cfg.GitHub.MinApprovers)
	}

	// Test Database defaults
	if !cfg.Database.UseVCPConfig {
		t.Error("expected UseVCPConfig to be true")
	}
	if cfg.Database.Host != "127.0.0.1" {
		t.Errorf("expected Host to be '127.0.0.1', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != "5432" {
		t.Errorf("expected Port to be '5432', got %q", cfg.Database.Port)
	}
	if cfg.Database.User != "postgres" {
		t.Errorf("expected User to be 'postgres', got %q", cfg.Database.User)
	}
	if cfg.Database.DBName != "vcp" {
		t.Errorf("expected DBName to be 'vcp', got %q", cfg.Database.DBName)
	}
	if cfg.Database.SSLMode != "disable" {
		t.Errorf("expected SSLMode to be 'disable', got %q", cfg.Database.SSLMode)
	}

	// Test Thresholds defaults
	if cfg.Thresholds.MaxRowsDefault != 100 {
		t.Errorf("expected MaxRowsDefault to be 100, got %d", cfg.Thresholds.MaxRowsDefault)
	}
	if cfg.Thresholds.WarningThreshold != 10 {
		t.Errorf("expected WarningThreshold to be 10, got %d", cfg.Thresholds.WarningThreshold)
	}
	if cfg.Thresholds.BlockThreshold != 1000 {
		t.Errorf("expected BlockThreshold to be 1000, got %d", cfg.Thresholds.BlockThreshold)
	}
	if cfg.Thresholds.PlanExpiry != 1*time.Hour {
		t.Errorf("expected PlanExpiry to be 1 hour, got %v", cfg.Thresholds.PlanExpiry)
	}
	if cfg.Thresholds.QueryTimeout != 60*time.Second {
		t.Errorf("expected QueryTimeout to be 60 seconds, got %v", cfg.Thresholds.QueryTimeout)
	}

	// Test Audit defaults
	if !cfg.Audit.Enabled {
		t.Error("expected Audit.Enabled to be true")
	}
	if cfg.Audit.FilePath != ".safesql/audit/" {
		t.Errorf("expected Audit.FilePath to be '.safesql/audit/', got %q", cfg.Audit.FilePath)
	}

	// Test Storage defaults
	if cfg.Storage.Backend != "gcs" {
		t.Errorf("expected Storage.Backend to be 'gcs', got %q", cfg.Storage.Backend)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Save and clear environment
	origVars := map[string]string{
		"SAFESQL_GITHUB_TOKEN":  os.Getenv("SAFESQL_GITHUB_TOKEN"),
		"GITHUB_TOKEN":          os.Getenv("GITHUB_TOKEN"),
		"SAFESQL_GITHUB_REPO":   os.Getenv("SAFESQL_GITHUB_REPO"),
		"SAFESQL_GITHUB_BRANCH": os.Getenv("SAFESQL_GITHUB_BRANCH"),
		"DB_HOST":               os.Getenv("DB_HOST"),
		"DB_PORT":               os.Getenv("DB_PORT"),
		"DB_USER":               os.Getenv("DB_USER"),
		"DB_PASSWORD":           os.Getenv("DB_PASSWORD"),
		"DB_NAME":               os.Getenv("DB_NAME"),
		"DB_SSLMODE":            os.Getenv("DB_SSLMODE"),
		"SAFESQL_GCS_BUCKET":    os.Getenv("SAFESQL_GCS_BUCKET"),
	}
	defer func() {
		for k, v := range origVars {
			if v != "" {
				os.Setenv(k, v)
			} else {
				os.Unsetenv(k)
			}
		}
	}()

	// Clear env vars
	for k := range origVars {
		os.Unsetenv(k)
	}

	// Set test values
	os.Setenv("SAFESQL_GITHUB_TOKEN", "test-token")
	os.Setenv("SAFESQL_GITHUB_REPO", "test/repo")
	os.Setenv("SAFESQL_GITHUB_BRANCH", "test-branch")
	os.Setenv("DB_HOST", "test-host")
	os.Setenv("DB_PORT", "5433")
	os.Setenv("DB_USER", "test-user")
	os.Setenv("DB_PASSWORD", "test-password")
	os.Setenv("DB_NAME", "test-db")
	os.Setenv("DB_SSLMODE", "require")
	os.Setenv("SAFESQL_GCS_BUCKET", "test-bucket")

	cfg := DefaultConfig()
	cfg.loadFromEnv()

	if cfg.GitHub.Token != "test-token" {
		t.Errorf("expected Token to be 'test-token', got %q", cfg.GitHub.Token)
	}
	if cfg.GitHub.Repository != "test/repo" {
		t.Errorf("expected Repository to be 'test/repo', got %q", cfg.GitHub.Repository)
	}
	if cfg.GitHub.Branch != "test-branch" {
		t.Errorf("expected Branch to be 'test-branch', got %q", cfg.GitHub.Branch)
	}
	if cfg.Database.Host != "test-host" {
		t.Errorf("expected Host to be 'test-host', got %q", cfg.Database.Host)
	}
	if cfg.Database.Port != "5433" {
		t.Errorf("expected Port to be '5433', got %q", cfg.Database.Port)
	}
	if cfg.Database.User != "test-user" {
		t.Errorf("expected User to be 'test-user', got %q", cfg.Database.User)
	}
	if cfg.Database.Password != "test-password" {
		t.Errorf("expected Password to be 'test-password', got %q", cfg.Database.Password)
	}
	if cfg.Database.DBName != "test-db" {
		t.Errorf("expected DBName to be 'test-db', got %q", cfg.Database.DBName)
	}
	if cfg.Database.SSLMode != "require" {
		t.Errorf("expected SSLMode to be 'require', got %q", cfg.Database.SSLMode)
	}
	if cfg.Storage.GCSBucket != "test-bucket" {
		t.Errorf("expected GCSBucket to be 'test-bucket', got %q", cfg.Storage.GCSBucket)
	}
}

func TestLoadFromEnvGitHubTokenFallback(t *testing.T) {
	// Save and clear environment
	origSafesqlToken := os.Getenv("SAFESQL_GITHUB_TOKEN")
	origGithubToken := os.Getenv("GITHUB_TOKEN")
	defer func() {
		if origSafesqlToken != "" {
			os.Setenv("SAFESQL_GITHUB_TOKEN", origSafesqlToken)
		} else {
			os.Unsetenv("SAFESQL_GITHUB_TOKEN")
		}
		if origGithubToken != "" {
			os.Setenv("GITHUB_TOKEN", origGithubToken)
		} else {
			os.Unsetenv("GITHUB_TOKEN")
		}
	}()

	// Clear SAFESQL_GITHUB_TOKEN, set GITHUB_TOKEN
	os.Unsetenv("SAFESQL_GITHUB_TOKEN")
	os.Setenv("GITHUB_TOKEN", "fallback-token")

	cfg := DefaultConfig()
	cfg.loadFromEnv()

	if cfg.GitHub.Token != "fallback-token" {
		t.Errorf("expected Token to be 'fallback-token', got %q", cfg.GitHub.Token)
	}
}

func TestLoadFromFile(t *testing.T) {
	// Create a temp config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	content := `
github:
  repository: "file/repo"
  branch: "file-branch"
  token: "file-token"
  require_github_source: false
  require_merged_pr: false
  min_approvers: 2
database:
  host: "file-host"
  port: "5434"
  user: "file-user"
  password: "file-password"
  dbname: "file-db"
  sslmode: "verify-full"
thresholds:
  max_rows_default: 200
  warning_threshold: 20
  block_threshold: 2000
`

	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg := DefaultConfig()
	if err := cfg.loadFromFile(configPath); err != nil {
		t.Fatalf("failed to load config from file: %v", err)
	}

	if cfg.GitHub.Repository != "file/repo" {
		t.Errorf("expected Repository to be 'file/repo', got %q", cfg.GitHub.Repository)
	}
	if cfg.GitHub.Branch != "file-branch" {
		t.Errorf("expected Branch to be 'file-branch', got %q", cfg.GitHub.Branch)
	}
	if cfg.GitHub.RequireGitHubSource {
		t.Error("expected RequireGitHubSource to be false")
	}
	if cfg.GitHub.MinApprovers != 2 {
		t.Errorf("expected MinApprovers to be 2, got %d", cfg.GitHub.MinApprovers)
	}
	if cfg.Database.Host != "file-host" {
		t.Errorf("expected Host to be 'file-host', got %q", cfg.Database.Host)
	}
	if cfg.Thresholds.MaxRowsDefault != 200 {
		t.Errorf("expected MaxRowsDefault to be 200, got %d", cfg.Thresholds.MaxRowsDefault)
	}
}

func TestLoadFromFileNotFound(t *testing.T) {
	cfg := DefaultConfig()
	err := cfg.loadFromFile("/nonexistent/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name        string
		setupConfig func(*Config)
		wantErr     bool
		errContains string
	}{
		{
			name: "valid config with github disabled",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = false
				c.Database.Host = "localhost"
				c.Storage.Backend = "gcs"
				c.Storage.GCSBucket = "test-bucket"
			},
			wantErr: false,
		},
		{
			name: "missing repository when github required",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = true
				c.GitHub.Repository = ""
				c.GitHub.Token = "token"
				c.Database.Host = "localhost"
			},
			wantErr:     true,
			errContains: "github.repository is required",
		},
		{
			name: "missing token when github required",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = true
				c.GitHub.Repository = "test/repo"
				c.GitHub.Token = ""
				c.Database.Host = "localhost"
			},
			wantErr:     true,
			errContains: "github.token is required",
		},
		{
			name: "missing database host",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = false
				c.Database.Host = ""
			},
			wantErr:     true,
			errContains: "database host not configured",
		},
		{
			name: "invalid max rows threshold",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = false
				c.Database.Host = "localhost"
				c.Thresholds.MaxRowsDefault = 0
			},
			wantErr:     true,
			errContains: "max_rows_default must be positive",
		},
		{
			name: "missing GCS bucket when backend is gcs",
			setupConfig: func(c *Config) {
				c.GitHub.RequireGitHubSource = false
				c.Database.Host = "localhost"
				c.Storage.Backend = "gcs"
				c.Storage.GCSBucket = ""
			},
			wantErr:     true,
			errContains: "GCS bucket is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.setupConfig(cfg)

			err := cfg.Validate()

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				} else if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("expected error containing %q, got %q", tt.errContains, err.Error())
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	// Save original env vars
	origVars := map[string]string{
		"DB_HOST":            os.Getenv("DB_HOST"),
		"SAFESQL_GCS_BUCKET": os.Getenv("SAFESQL_GCS_BUCKET"),
	}
	defer func() {
		for k, v := range origVars {
			if v != "" {
				os.Setenv(k, v)
			} else {
				os.Unsetenv(k)
			}
		}
	}()

	// Set minimal env vars
	os.Setenv("DB_HOST", "test-host")

	// Test loading with empty config path
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.Database.Host != "test-host" {
		t.Errorf("expected Host to be 'test-host', got %q", cfg.Database.Host)
	}
}

func TestLoadWithConfigFile(t *testing.T) {
	// Save original env vars
	origHost := os.Getenv("DB_HOST")
	defer func() {
		if origHost != "" {
			os.Setenv("DB_HOST", origHost)
		} else {
			os.Unsetenv("DB_HOST")
		}
	}()

	// Clear env var so file values are used
	os.Unsetenv("DB_HOST")

	// Create a temp config file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")

	content := `
database:
  host: "config-file-host"
  port: "5432"
`

	if err := os.WriteFile(configPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Database.Host != "config-file-host" {
		t.Errorf("expected Host to be 'config-file-host', got %q", cfg.Database.Host)
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
