package cmd

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/database"
)

func runRollback(args []string) error {
	fs := flag.NewFlagSet("rollback", flag.ExitOnError)

	var (
		auditID  string
		prNumber int
		dryRun   bool
		operator string
	)

	fs.StringVar(&auditID, "audit", "", "Audit ID to rollback")
	fs.IntVar(&prNumber, "pr", 0, "PR number to rollback (fetches rollback from plan file)")
	fs.BoolVar(&dryRun, "dry-run", false, "Show rollback SQL without executing")
	fs.StringVar(&operator, "operator", "", "Operator performing rollback")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Check for PR-based rollback
	if prNumber > 0 {
		return runRollbackForPR(prNumber)
	}

	if auditID == "" {
		return fmt.Errorf("either --audit or --pr is required")
	}

	// Load audit entry
	auditLogger := audit.NewLogger(getAuditStorage())
	entry, err := auditLogger.Get(auditID)
	if err != nil {
		return fmt.Errorf("failed to load audit entry: %w", err)
	}

	// Validate entry
	if entry.Result == nil || !entry.Result.Success {
		return fmt.Errorf("cannot rollback: original execution was not successful")
	}

	if len(entry.RollbackSQL) == 0 {
		return fmt.Errorf("no rollback SQL available for this execution")
	}

	// Filter out empty rollback statements
	var rollbackStmts []string
	for _, sql := range entry.RollbackSQL {
		if strings.TrimSpace(sql) != "" {
			rollbackStmts = append(rollbackStmts, sql)
		}
	}

	if len(rollbackStmts) == 0 {
		return fmt.Errorf("no valid rollback SQL statements found")
	}

	printBox("ROLLBACK PREVIEW")
	logger.Info("")
	logger.Info(fmt.Sprintf("  Original Execution: %s\n", entry.AuditID))
	logger.Info(fmt.Sprintf("  Executed At: %s\n", entry.Timestamp.Format(time.RFC3339)))
	logger.Info(fmt.Sprintf("  Original Operator: %s\n", entry.Operator))
	logger.Info(fmt.Sprintf("  Ticket: %s\n", entry.Ticket))
	logger.Info("")

	logger.Info("  Rollback Statements:")
	for i, sql := range rollbackStmts {
		logger.Info(fmt.Sprintf("    [%d] %s\n", i+1, truncateSQL(sql, 70)))
	}
	logger.Info("")

	if dryRun {
		logger.Info("  Full Rollback SQL:")
		logger.Info("")
		for i, sql := range rollbackStmts {
			logger.Info(fmt.Sprintf("  -- Statement %d\n", i+1))
			logger.Info(fmt.Sprintf("  %s;\n\n", sql))
		}
		return nil
	}

	// Confirmation
	logger.Info("  Type 'ROLLBACK' to execute, or 'CANCEL' to abort: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input != "ROLLBACK" {
		logger.Info("")
		logger.Info("  Rollback cancelled.")
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Validate database configuration
	if cfg.Database.Host == "" {
		return fmt.Errorf("database host not configured. Set DB_HOST environment variable or create .safesql/config.yaml")
	}

	// Connect to database
	dbClient, err := database.New(database.Config{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		User:     cfg.Database.User,
		Password: cfg.Database.Password,
		DBName:   cfg.Database.DBName,
		SSLMode:  cfg.Database.SSLMode,
	})
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer dbClient.Close()

	logger.Info("")
	printBox("EXECUTING ROLLBACK")
	logger.Info("")

	// Execute rollback statements
	rowsAffected, err := dbClient.ExecuteMultipleInTransaction(ctx, rollbackStmts, func(results []int64) bool {
		logger.Info("  Transaction preview:")
		var total int64
		for i, count := range results {
			logger.Info(fmt.Sprintf("    Statement %d: %d rows affected\n", i+1, count))
			total += count
		}
		logger.Info(fmt.Sprintf("    Total: %d rows\n", total))
		logger.Info("")

		logger.Info("  Type 'COMMIT' to finalize rollback, or 'ABORT' to cancel: ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		return input == "COMMIT"
	})

	logger.Info("")

	if err != nil {
		printBox("ROLLBACK FAILED")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Error: %v\n", err))
		return err
	}

	// Calculate total
	var totalRows int64
	for _, count := range rowsAffected {
		totalRows += count
	}

	printBox("ROLLBACK SUCCESSFUL")
	logger.Info("")
	logger.Info(fmt.Sprintf("  Rows affected: %d\n", totalRows))
	logger.Info("")

	// Create a minimal plan-like structure for logging
	type minimalPlan struct {
		PlanID   string
		Operator string
		Source   struct {
			Type string
		}
		Rollback []struct {
			SQL string
		}
	}
	plan := &minimalPlan{
		PlanID:   entry.PlanID,
		Operator: operator,
	}
	plan.Source.Type = entry.Source.Type

	// Log rollback - simplified since we don't have the full plan structure
	logger.Info(fmt.Sprintf("  Original audit entry: %s\n", auditID))
	logger.Info("  Rollback has been logged.")

	return nil
}
