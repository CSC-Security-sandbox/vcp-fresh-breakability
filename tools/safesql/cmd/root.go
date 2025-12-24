// Package cmd implements the SafeSQL CLI commands.
package cmd

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/config"
)

var (
	cfgFile string
	cfg     *config.Config
	verbose bool
	logger  *slog.Logger
)

func init() {
	// Initialize logger with text handler for CLI output
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	logger = slog.New(handler)
}

// Execute runs the root command.
func Execute() error {
	if len(os.Args) < 2 {
		printUsage()
		return nil
	}

	// Load configuration
	var err error
	cfg, err = config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	command := os.Args[1]
	args := os.Args[2:]

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
	case "help", "-h", "--help":
		printUsage()
		return nil
	case "version", "-v", "--version":
		logger.Info("SafeSQL v1.0.0")
		return nil
	default:
		return fmt.Errorf("unknown command: %s", command)
	}
}

func printUsage() {
	logger.Info(`SafeSQL - Safe SQL Execution Framework

Usage:
  safesql <command> [flags]

Commands:
  plan      Generate an execution plan from a SQL file
  apply     Execute a plan after verification
  show      Display plan details
  audit     View execution history
  rollback  Undo a previous execution

Flags:
  -c, --config string   Config file (default: .safesql/config.yaml)
  -v, --verbose         Enable verbose output
  -h, --help            Show help

Examples:
  # Generate plan from GitHub
  safesql plan --github "owner/repo@branch:path/to/query.sql" --operator john.doe --ticket JIRA-1234

  # Generate plan from local file (if allowed)
  safesql plan --file query.sql --operator john.doe --ticket JIRA-1234

  # Apply a plan
  safesql apply --plan plan-20240115-143022-abc123

  # Show plan details
  safesql show --plan plan-20240115-143022-abc123

  # View audit history
  safesql audit --last 10

  # Rollback an execution
  safesql rollback --audit exec-20240115-143522-xyz789

For more information, see: doc/safesql/`)
}
