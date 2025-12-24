// Package config provides configuration management for SafeSQL.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the SafeSQL configuration.
type Config struct {
	GitHub     GitHubConfig     `yaml:"github"`
	Database   DatabaseConfig   `yaml:"database"`
	Thresholds ThresholdsConfig `yaml:"thresholds"`
	Audit      AuditConfig      `yaml:"audit"`
}

// GitHubConfig holds GitHub integration settings.
type GitHubConfig struct {
	Repository          string `yaml:"repository"`
	Branch              string `yaml:"branch"`
	Token               string `yaml:"token"`
	RequireGitHubSource bool   `yaml:"require_github_source"`
	RequireMergedPR     bool   `yaml:"require_merged_pr"`
	MinApprovers        int    `yaml:"min_approvers"`
}

// DatabaseConfig holds database connection settings.
type DatabaseConfig struct {
	UseVCPConfig bool   `yaml:"use_vcp_config"`
	Host         string `yaml:"host"`
	Port         string `yaml:"port"`
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	DBName       string `yaml:"dbname"`
	SSLMode      string `yaml:"sslmode"`
}

// ThresholdsConfig holds safety threshold settings.
type ThresholdsConfig struct {
	MaxRowsDefault   int           `yaml:"max_rows_default"`
	WarningThreshold int           `yaml:"warning_threshold"`
	BlockThreshold   int           `yaml:"block_threshold"`
	PlanExpiry       time.Duration `yaml:"plan_expiry"`
	QueryTimeout     time.Duration `yaml:"query_timeout"`
}

// AuditConfig holds audit settings.
type AuditConfig struct {
	Enabled     bool   `yaml:"enabled"`
	FilePath    string `yaml:"file_path"`
	GitHubAudit bool   `yaml:"github_audit"`
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		GitHub: GitHubConfig{
			Branch:              "sql-queries",
			RequireGitHubSource: true,
			RequireMergedPR:     true,
			MinApprovers:        1,
		},
		Database: DatabaseConfig{
			UseVCPConfig: true,
			Port:         "5432",
			SSLMode:      "require",
		},
		Thresholds: ThresholdsConfig{
			MaxRowsDefault:   100,
			WarningThreshold: 10,
			BlockThreshold:   1000,
			PlanExpiry:       1 * time.Hour,
			QueryTimeout:     60 * time.Second,
		},
		Audit: AuditConfig{
			Enabled:  true,
			FilePath: ".safesql/audit/",
		},
	}
}

// Load reads configuration from file and environment variables.
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	// Try to load from file if it exists
	if configPath != "" {
		if err := cfg.loadFromFile(configPath); err != nil {
			return nil, fmt.Errorf("failed to load config from file: %w", err)
		}
	} else {
		// Try default locations
		defaultPaths := []string{
			".safesql/config.yaml",
			filepath.Join(os.Getenv("HOME"), ".safesql/config.yaml"),
		}
		for _, p := range defaultPaths {
			if _, err := os.Stat(p); err == nil {
				if err := cfg.loadFromFile(p); err != nil {
					return nil, fmt.Errorf("failed to load config from %s: %w", p, err)
				}
				break
			}
		}
	}

	// Override with environment variables
	cfg.loadFromEnv()

	return cfg, nil
}

func (c *Config) loadFromFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, c)
}

func (c *Config) loadFromEnv() {
	if token := os.Getenv("SAFESQL_GITHUB_TOKEN"); token != "" {
		c.GitHub.Token = token
	}
	if token := os.Getenv("GITHUB_TOKEN"); token != "" && c.GitHub.Token == "" {
		c.GitHub.Token = token
	}
	if repo := os.Getenv("SAFESQL_GITHUB_REPO"); repo != "" {
		c.GitHub.Repository = repo
	}
	if branch := os.Getenv("SAFESQL_GITHUB_BRANCH"); branch != "" {
		c.GitHub.Branch = branch
	}

	// Database overrides
	if host := os.Getenv("DB_HOST"); host != "" {
		c.Database.Host = host
	}
	if port := os.Getenv("DB_PORT"); port != "" {
		c.Database.Port = port
	}
	if user := os.Getenv("DB_USER"); user != "" {
		c.Database.User = user
	}
	if pass := os.Getenv("DB_PASSWORD"); pass != "" {
		c.Database.Password = pass
	}
	if name := os.Getenv("DB_NAME"); name != "" {
		c.Database.DBName = name
	}
	if sslmode := os.Getenv("DB_SSLMODE"); sslmode != "" {
		c.Database.SSLMode = sslmode
	}
}

// Validate checks if the configuration is valid for operation.
func (c *Config) Validate() error {
	if c.GitHub.RequireGitHubSource {
		if c.GitHub.Repository == "" {
			return fmt.Errorf("github.repository is required when require_github_source is true")
		}
		if c.GitHub.Token == "" {
			return fmt.Errorf("github.token is required (set via SAFESQL_GITHUB_TOKEN or GITHUB_TOKEN env var)")
		}
	}

	if !c.Database.UseVCPConfig {
		if c.Database.Host == "" {
			return fmt.Errorf("database.host is required when use_vcp_config is false")
		}
	}

	if c.Thresholds.MaxRowsDefault <= 0 {
		return fmt.Errorf("thresholds.max_rows_default must be positive")
	}

	return nil
}

// GetPlanStorePath returns the path where plans are stored.
func (c *Config) GetPlanStorePath() string {
	return ".safesql/plans/"
}

// GetAuditPath returns the path where audit logs are stored.
func (c *Config) GetAuditPath() string {
	if c.Audit.FilePath != "" {
		return c.Audit.FilePath
	}
	return ".safesql/audit/"
}
