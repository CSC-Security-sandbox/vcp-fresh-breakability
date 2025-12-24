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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/executor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

func runApply(args []string) error {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)

	var (
		planID string
		force  bool
	)

	fs.StringVar(&planID, "plan", "", "Plan ID to apply (required)")
	fs.BoolVar(&force, "force", false, "Force execution even with warnings")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if planID == "" {
		return fmt.Errorf("--plan is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Load plan
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, cfg.GetPlanStorePath())
	plan, err := pb.Load(planID)
	if err != nil {
		return fmt.Errorf("failed to load plan: %w", err)
	}

	printBox("VERIFYING PLAN", "yellow")
	logger.Info("")

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

	// Create GitHub client if needed
	var ghClient *github.Client
	if plan.Source.Type == "github" {
		ghClient = github.New(cfg.GitHub.Token)
	}

	// Create executor
	exec := executor.New(dbClient, ghClient, pb)

	// Verify plan
	verifyResult, err := exec.VerifyPlan(ctx, plan)
	if err != nil {
		return fmt.Errorf("verification error: %w", err)
	}

	// Print verification results
	printVerificationResult(verifyResult, plan)

	if !verifyResult.Valid {
		printBox("EXECUTION BLOCKED", "red")
		logger.Info("")
		logger.Info("  The plan cannot be applied due to the above errors.")
		logger.Info("  Create a new plan to capture the current state:")
		logger.Info(fmt.Sprintf("    safesql plan --github \"%s@%s:%s\"\n",
			plan.Source.Repository, plan.Source.Branch, plan.Source.FilePath))

		// Log abort
		auditLogger := audit.NewLogger(cfg.GetAuditPath())
		auditLogger.LogAbort(plan, "Verification failed: "+strings.Join(verifyResult.Errors, "; "))

		return fmt.Errorf("plan verification failed")
	}

	printBox("READY TO EXECUTE", "green")
	logger.Info("")
	logger.Info("  All verifications passed. The following queries will be executed:")
	logger.Info("")

	for i, stmt := range plan.Query.Statements {
		logger.Info(fmt.Sprintf("  [%d] %s\n", i+1, truncateSQL(stmt.SQL, 80)))
	}
	logger.Info("")

	// Warning for high row counts
	if plan.Impact.TotalRows > int64(cfg.Thresholds.WarningThreshold) {
		logger.Info(fmt.Sprintf("  ⚠️  WARNING: This will affect %d rows\n", plan.Impact.TotalRows))
		logger.Info("")
	}

	// Confirmation
	logger.Info("  Type 'APPLY' to execute, or 'ABORT' to cancel: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input != "APPLY" {
		logger.Info("")
		logger.Info("  Execution aborted by user.")

		// Log abort
		auditLogger := audit.NewLogger(cfg.GetAuditPath())
		auditLogger.LogAbort(plan, "Aborted by user")

		return nil
	}

	logger.Info("")
	printBox("EXECUTING", "yellow")
	logger.Info("")

	// Execute with confirmation callback
	execResult, err := exec.Execute(ctx, plan, func(rowsAffected []int64) bool {
		logger.Info("  Transaction preview:")
		var total int64
		for i, count := range rowsAffected {
			logger.Info(fmt.Sprintf("    Statement %d: %d rows affected\n", i+1, count))
			total += count
		}
		logger.Info(fmt.Sprintf("    Total: %d rows\n", total))
		logger.Info("")

		// Check if row count matches expected
		if total != plan.Impact.TotalRows {
			logger.Info(fmt.Sprintf("  ⚠️  WARNING: Row count (%d) differs from plan (%d)\n", total, plan.Impact.TotalRows))
			logger.Info("")
		}

		logger.Info("  Type 'COMMIT' to finalize, or 'ROLLBACK' to cancel: ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		return input == "COMMIT"
	})

	// Log execution
	auditLogger := audit.NewLogger(cfg.GetAuditPath())
	auditEntry, _ := auditLogger.LogApply(plan, verifyResult, execResult)

	logger.Info("")
	if execResult != nil && execResult.Success {
		printBox("EXECUTION SUCCESSFUL", "green")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Rows affected: %d\n", execResult.TotalRows))
		logger.Info(fmt.Sprintf("  Duration: %v\n", execResult.Duration))
		if auditEntry != nil {
			logger.Info(fmt.Sprintf("  Audit ID: %s\n", auditEntry.AuditID))
		}
		logger.Info("")
		logger.Info("  Rollback available:")
		if auditEntry != nil {
			logger.Info(fmt.Sprintf("    safesql rollback --audit %s\n", auditEntry.AuditID))
		}
	} else if execResult != nil && execResult.RolledBack {
		printBox("EXECUTION ROLLED BACK", "yellow")
		logger.Info("")
		logger.Info("  Transaction was rolled back (no changes made).")
		if execResult.Error != nil {
			logger.Info(fmt.Sprintf("  Reason: %v\n", execResult.Error))
		}
	} else {
		printBox("EXECUTION FAILED", "red")
		logger.Info("")
		if err != nil {
			logger.Info(fmt.Sprintf("  Error: %v\n", err))
		}
	}

	return err
}

func printVerificationResult(result *executor.VerificationResult, plan *planner.Plan) {
	// Plan expiry check
	if result.PlanExpired {
		logger.Info(fmt.Sprintf("  ❌ Plan expired at %s\n", plan.ExpiresAt.Format(time.RFC3339)))
	} else {
		remaining := time.Until(plan.ExpiresAt)
		logger.Info(fmt.Sprintf("  ✓ Plan not expired (%v remaining)\n", remaining.Round(time.Minute)))
	}

	// Signature check
	if result.SignatureInvalid {
		logger.Info("  ❌ Plan signature invalid")
	} else {
		logger.Info("  ✓ Plan signature valid")
	}

	// Commit check (GitHub source)
	if plan.Source.Type == "github" {
		if result.CommitMismatch {
			logger.Info("  ❌ Commit SHA mismatch (file changed since plan)")
		} else {
			logger.Info(fmt.Sprintf("  ✓ Commit SHA matches (%s)\n", plan.Source.CommitSHA[:12]))
		}
	}

	// State drift check
	if result.StateDrift {
		logger.Info("  ❌ State drift detected (data changed since plan)")
	} else {
		logger.Info("  ✓ State hash matches (no drift)")
	}

	// Row count check
	if result.RowCountMismatch {
		logger.Info("  ❌ Row count mismatch")
	} else {
		logger.Info(fmt.Sprintf("  ✓ Row count unchanged (%d rows)\n", plan.Impact.TotalRows))
	}

	logger.Info("")

	// Print detailed errors if any
	if len(result.Errors) > 0 {
		logger.Info("  Errors:")
		for _, e := range result.Errors {
			logger.Info(fmt.Sprintf("    • %s\n", e))
		}
		logger.Info("")
	}
}

func truncateSQL(sql string, maxLen int) string {
	// Remove newlines and extra spaces
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) > maxLen {
		return sql[:maxLen-3] + "..."
	}
	return sql
}
