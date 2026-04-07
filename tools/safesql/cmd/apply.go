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
		planID   string
		prNumber int
		force    bool
	)

	fs.StringVar(&planID, "plan", "", "Plan ID to apply")
	fs.IntVar(&prNumber, "pr", 0, "Pull request number (applies plan from merged PR)")
	fs.BoolVar(&force, "force", false, "Force execution even with warnings")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Handle PR-based apply
	if prNumber > 0 {
		if planID != "" {
			return fmt.Errorf("cannot specify both --plan and --pr")
		}
		return runApplyForPR(prNumber)
	}

	if planID == "" {
		return fmt.Errorf("either --plan or --pr is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Load plan
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, getPlanStorage())
	plan, err := pb.Load(ctx, planID)
	if err != nil {
		return fmt.Errorf("failed to load plan: %w", err)
	}

	printBox("VERIFYING PLAN")
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
		UseIAM:   cfg.Database.UseIAM,
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
		printBox("EXECUTION BLOCKED")
		logger.Info("")
		logger.Info("  The plan cannot be applied due to the above errors.")
		logger.Info("  Create a new plan to capture the current state:")
		logger.Info(fmt.Sprintf("    safesql plan --github \"%s@%s:%s\"\n",
			plan.Source.Repository, plan.Source.Branch, plan.Source.FilePath))

		// Log abort
		auditLogger := audit.NewLogger(getAuditStorage())
		auditLogger.LogAbort(plan, "Verification failed: "+strings.Join(verifyResult.Errors, "; "))

		return fmt.Errorf("plan verification failed")
	}

	printBox("READY TO EXECUTE")
	logger.Info("")
	logger.Info("  All verifications passed. The following queries will be executed:")
	logger.Info("")

	for i, stmt := range plan.Query.Statements {
		logger.Info(fmt.Sprintf("  [%d] %s\n", i+1, truncateSQL(stmt.SQL, 80)))
	}
	logger.Info("")

	// Warning for high row counts
	if plan.Impact.TotalRows > int64(cfg.Thresholds.WarningThreshold) {
		logger.Info(fmt.Sprintf("  [WARNING] This will affect %d rows\n", plan.Impact.TotalRows))
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
		auditLogger := audit.NewLogger(getAuditStorage())
		auditLogger.LogAbort(plan, "Aborted by user")

		return nil
	}

	logger.Info("")
	printBox("EXECUTING")
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
			logger.Info(fmt.Sprintf("  [WARNING] Row count (%d) differs from plan (%d)\n", total, plan.Impact.TotalRows))
			logger.Info("")
		}

		logger.Info("  Type 'COMMIT' to finalize, or 'ROLLBACK' to cancel: ")
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		return input == "COMMIT"
	})

	// Log execution
	auditLogger := audit.NewLogger(getAuditStorage())
	auditEntry, _ := auditLogger.LogApply(plan, verifyResult, execResult)

	logger.Info("")
	if execResult != nil && execResult.Success {
		printBox("EXECUTION SUCCESSFUL")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Rows affected: %d\n", execResult.TotalRows))
		logger.Info(fmt.Sprintf("  Duration: %v\n", execResult.Duration))
		if auditEntry != nil {
			logger.Info(fmt.Sprintf("  Audit ID: %s\n", auditEntry.AuditID))
		}
		logger.Info("")
		logger.Info("  Note: For PR-based workflows, use 'safesql rollback --pr <number>'\n")
	} else if execResult != nil && execResult.RolledBack {
		printBox("EXECUTION ROLLED BACK")
		logger.Info("")
		logger.Info("  Transaction was rolled back (no changes made).")
		if execResult.Error != nil {
			logger.Info(fmt.Sprintf("  Reason: %v\n", execResult.Error))
		}
	} else {
		printBox("EXECUTION FAILED")
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
		logger.Info(fmt.Sprintf("  [FAIL] Plan expired at %s\n", plan.ExpiresAt.Format(time.RFC3339)))
	} else {
		remaining := time.Until(plan.ExpiresAt)
		logger.Info(fmt.Sprintf("  [PASS] Plan not expired (%v remaining)\n", remaining.Round(time.Minute)))
	}

	// Signature check
	if result.SignatureInvalid {
		logger.Info("  [FAIL] Plan signature invalid")
	} else {
		logger.Info("  [PASS] Plan signature valid")
	}

	// Commit check (GitHub source)
	if plan.Source.Type == "github" {
		if result.CommitMismatch {
			logger.Info("  [FAIL] Commit SHA mismatch (file changed since plan)")
		} else {
			logger.Info(fmt.Sprintf("  [PASS] Commit SHA matches (%s)\n", plan.Source.CommitSHA[:12]))
		}
	}

	// State drift check
	if result.StateDrift {
		logger.Info("  [FAIL] State drift detected (data changed since plan)")
	} else {
		logger.Info("  [PASS] State hash matches (no drift)")
	}

	// Row count check
	if result.RowCountMismatch {
		logger.Info("  [FAIL] Row count mismatch")
	} else {
		logger.Info(fmt.Sprintf("  [PASS] Row count unchanged (%d rows)\n", plan.Impact.TotalRows))
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
