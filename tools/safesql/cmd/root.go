// Package cmd implements the SafeSQL CLI commands.
package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/config"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/logging"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/setup"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/storage"
)

var (
	cfgFile    string
	cfg        *config.Config
	verbose    bool
	logger     *logging.IntegratedLogger
	gcsStorage *storage.Storage // GCS storage client
)

func init() {
	// Initialize integrated logger with journald support
	// Journald is always enabled (will gracefully fallback if unavailable)
	logger = logging.NewIntegratedLogger(true, "safesql-cli")
}

// Execute runs the root command.
func Execute() error {
	if len(os.Args) < 2 {
		printUsage()
		return nil
	}

	command := os.Args[1]
	args := os.Args[2:]

	// Handle utility commands that don't need full setup
	switch command {
	case "help", "-h", "--help":
		printUsage()
		return nil
	case "version", "-v", "--version":
		logger.Info("SafeSQL v1.0.0")
		return nil
	case "env":
		setup.ShowEnvironment()
		return nil
	case "check-db":
		// Run auto-setup first
		if err := setup.AutoSetup(); err != nil {
			return fmt.Errorf("auto-setup failed: %w", err)
		}
		return setup.CheckDatabaseConnectivity()
	}

	// Run auto-setup for all operational commands
	if err := setup.AutoSetup(); err != nil {
		return fmt.Errorf("auto-setup failed: %w", err)
	}

	// Load configuration
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Initialize GCS storage
	ctx := context.Background()
	gcsStorage, err = storage.New(ctx, cfg.Storage.GCSBucket)
	if err != nil {
		return fmt.Errorf("failed to initialize GCS storage: %w", err)
	}
	defer gcsStorage.Close()

	// Execute the command
	switch command {
	case "plan":
		return runPlan(args)
	case "apply":
		return runApply(args)
	case "show":
		return runShow(args)
	case "audit":
		return runAudit(args)
	case "rollback":
		return runRollback(args)
	case "verify-github":
		return runVerifyGitHub()
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func printUsage() {
	logger.Info(`SafeSQL - Safe SQL Execution Framework

Usage:
  safesql <command> [flags]

Commands:
  plan           Generate an execution plan from a SQL file or PR
  apply          Execute a plan after verification
  show           Display plan details
  audit          View execution history
  rollback       Undo a previous execution
  env            Show environment configuration
  check-db       Test database connectivity
  verify-github  Verify GitHub token and GPG key configuration

Flags:
  -c, --config string   Config file (default: .safesql/config.yaml)
  -v, --verbose         Enable verbose output
  -h, --help            Show help

PR Workflow (Recommended for Production):
  # Generate plan from PR (creates commit suggestion)
  # Ticket is extracted from PR title if not provided
  # If plan exists and is valid, it will be reused
  safesql plan --pr 42
  safesql plan --pr 42 --ticket JIRA-123
  
  # Then: Go to PR, click "Commit suggestion" (signed with YOUR GPG key)

  # Show plan from PR
  safesql show --pr 42

  # Apply plan from merged PR (plan must be < 1 hour old)
  safesql apply --pr 42

Direct Execution (Development/Testing):
  # Generate plan from GitHub
  safesql plan --github "main:sql-queries/delete-stale-jobs.sql" --ticket TICKET-1234

  # Generate plan from local file
  safesql plan --file query.sql --ticket TICKET-1234 --force

  # Apply a plan by ID
  safesql apply --plan plan-john_doe-TICKET-1234-20240115-143022

  # Show plan details by ID
  safesql show --plan plan-john_doe-TICKET-1234-20240115-143022

Other Commands:
  # View audit history
  safesql audit --last 10

  # Rollback an execution
  safesql rollback --pr 42

  # Check environment and database
  safesql env
  safesql check-db

Environment Variables:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
  GITHUB_TOKEN, SAFESQL_GITHUB_REPO, SAFESQL_GITHUB_BRANCH
  SAFESQL_GCS_BUCKET (required for GCS storage backend)
  SAFESQL_CONFIG_DIR, SAFESQL_OPERATOR
  SAFESQL_NO_AUTO_SETUP, SAFESQL_AUTO_FETCH_PASSWORD, SAFESQL_AUTO_PORT_FORWARD

Logging:
  - Concurrent output to stdout, journald, and Google Cloud Logging
  - Journald integration always enabled (gracefully falls back if unavailable)
  - Complete audit trail with structured metadata

Auto-Setup Features:
  - Automatically fetches DB password from Kubernetes secrets
  - Automatically sets up port-forward to database
  - Automatically creates necessary directories
  - Automatically detects operator username

For more information, see: doc/safesql/usage-guide.md`)
}

// parseFirstJSON parses only the first JSON object from data, ignoring any trailing content
// This handles cases where commit suggestions appended multiple JSON objects
func parseFirstJSON(data []byte, v interface{}) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	return dec.Decode(v)
}

// getPlanStorage returns the storage backend for plans.
func getPlanStorage() planner.StorageBackend {
	return gcsStorage
}

// getAuditStorage returns the storage backend for audits.
func getAuditStorage() audit.StorageBackend {
	return storage.NewAuditStorageAdapter(gcsStorage)
}
