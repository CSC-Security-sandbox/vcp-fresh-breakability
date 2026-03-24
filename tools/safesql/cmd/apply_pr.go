package cmd

import (
	"bufio"
	"context"
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

func runApplyForPR(prNumber int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// Parse repository from config
	parts := strings.SplitN(cfg.GitHub.Repository, "/", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid repository format in config: %s", cfg.GitHub.Repository)
	}
	owner, repo := parts[0], parts[1]

	// Create GitHub client
	ghClient := github.New(cfg.GitHub.Token)

	// Get PR details
	logger.Info(fmt.Sprintf("Fetching PR #%d...\n", prNumber))
	pr, err := ghClient.GetPR(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to get PR: %w", err)
	}

	logger.Info(fmt.Sprintf("PR #%d: %s\n", pr.Number, pr.Title))
	logger.Info(fmt.Sprintf("State: %s\n", pr.State))
	logger.Info(fmt.Sprintf("Author: %s\n", pr.Author))
	logger.Info("")

	// Check PR state - must be open (not merged)
	if pr.State != "open" {
		return fmt.Errorf("PR must be in OPEN state to apply. Current state: %s", pr.State)
	}

	// Check PR approvals - must have at least 2 approvals
	logger.Info("Checking PR approvals...\n")
	approvers, err := ghClient.GetPRApprovers(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to get PR approvals: %w", err)
	}

	if len(approvers) < 2 {
		logger.Info("")
		printBox("INSUFFICIENT APPROVALS")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Current approvals: %d\n", len(approvers)))
		if len(approvers) > 0 {
			logger.Info("  Approved by:\n")
			for _, approver := range approvers {
				logger.Info(fmt.Sprintf("    - %s\n", approver))
			}
		}
		logger.Info("  Required approvals: 2\n")
		logger.Info("")
		logger.Info("  Please get at least 2 approvals before applying the plan.\n")
		logger.Info("")
		return fmt.Errorf("PR requires at least 2 approvals (currently has %d)", len(approvers))
	}

	logger.Info(fmt.Sprintf("PR has %d approvals ✓\n", len(approvers)))
	logger.Info("Approved by:\n")
	for _, approver := range approvers {
		logger.Info(fmt.Sprintf("  - %s\n", approver))
	}
	logger.Info("")

	// List files in PR
	logger.Info("Checking PR files...\n")
	files, err := ghClient.ListPRFiles(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to list PR files: %w", err)
	}

	// Find SQL file and plan file
	var sqlFile, planFile *github.PRFile
	for _, f := range files {
		if strings.HasSuffix(f.Filename, ".sql") && f.Status != "removed" {
			if sqlFile != nil {
				return fmt.Errorf("validation error: multiple SQL files found in PR")
			}
			sqlFile = f
		}
		if strings.HasSuffix(f.Filename, "-plan.json") && f.Status != "removed" {
			planFile = f
		}
	}

	if sqlFile == nil {
		return fmt.Errorf("validation error: no SQL file found in merged PR")
	}

	if planFile == nil {
		return fmt.Errorf("validation error: no plan file found in merged PR. The PR must contain both SQL and plan files.\n" +
			"This should not happen if you used 'safesql plan --pr %d' before merging", prNumber)
	}

	// Verify plan filename matches SQL filename
	expectedPlanName := strings.TrimSuffix(sqlFile.Filename, ".sql") + "-plan.json"
	if planFile.Filename != expectedPlanName {
		return fmt.Errorf("validation error: plan filename '%s' does not match SQL filename '%s'",
			planFile.Filename, sqlFile.Filename)
	}

	logger.Info(fmt.Sprintf("Found SQL file: %s\n", sqlFile.Filename))
	logger.Info(fmt.Sprintf("Found plan file: %s\n", planFile.Filename))
	logger.Info("")

	// Fetch plan file from PR branch
	logger.Info("Fetching plan from PR branch...\n")
	planContent, err := ghClient.GetPRFile(ctx, owner, repo, pr.HeadBranch, planFile.Filename)
	if err != nil {
		return fmt.Errorf("failed to fetch plan file from PR: %w", err)
	}

	// Parse plan (only first JSON object if file contains multiple)
	var plan planner.Plan
	if err := parseFirstJSON([]byte(planContent.Content), &plan); err != nil {
		return fmt.Errorf("failed to parse plan: %w", err)
	}

	logger.Info(fmt.Sprintf("Plan ID: %s\n", plan.PlanID))
	logger.Info(fmt.Sprintf("Created: %s\n", plan.CreatedAt.Format(time.RFC3339)))
	logger.Info("")

	// Check plan age (must be < 1 hour old)
	planAge := time.Since(plan.CreatedAt)
	if planAge > 1*time.Hour {
		logger.Info("")
		printBox("PLAN EXPIRED")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Plan age: %v (created at %s)\n", planAge.Round(time.Minute), plan.CreatedAt.Format(time.RFC3339)))
		logger.Info("  Maximum age: 1 hour\n")
		logger.Info("")
		logger.Info("The plan is too old and must be regenerated with current database state.\n")
		logger.Info("")
		logger.Info("Next steps:\n")
		logger.Info(fmt.Sprintf("  1. Create a new branch from %s\n", pr.BaseBranch))
		logger.Info("  2. Copy the SQL file to the new branch\n")
		logger.Info("  3. Create a new PR\n")
		logger.Info("  4. Run: safesql plan --pr <new-pr-number> --ticket <ticket>\n")
		logger.Info("  5. Get the new PR reviewed and merged\n")
		logger.Info(fmt.Sprintf("  6. Run: safesql apply --pr <new-pr-number>\n"))
		logger.Info("")
		
		return fmt.Errorf("plan expired: created %v ago, must be < 1 hour old", planAge.Round(time.Minute))
	}

	logger.Info(fmt.Sprintf("Plan age: %v (within 1 hour limit)\n", planAge.Round(time.Minute)))
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

	// Create executor
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, getPlanStorage())
	exec := executor.New(dbClient, ghClient, pb)

	// Verify plan
	printBox("VERIFYING PLAN")
	logger.Info("")

	verifyResult, err := exec.VerifyPlan(ctx, &plan)
	if err != nil {
		return fmt.Errorf("verification error: %w", err)
	}

	// Print verification results
	printVerificationResult(verifyResult, &plan)

	if !verifyResult.Valid {
		printBox("EXECUTION BLOCKED")
		logger.Info("")
		logger.Info("  The plan cannot be applied due to the above errors.")
		logger.Info("  The database state has changed since the plan was created.")
		logger.Info("")
		logger.Info("  Create a new PR with updated plan:")
		logger.Info(fmt.Sprintf("    1. Create new branch from %s\n", pr.BaseBranch))
		logger.Info("    2. Copy SQL file to new branch\n")
		logger.Info("    3. Create new PR and run: safesql plan --pr <new-pr> --ticket <ticket>\n")

		// Log abort
		auditLogger := audit.NewLogger(getAuditStorage())
		auditLogger.LogAbort(&plan, "Verification failed: "+strings.Join(verifyResult.Errors, "; "))

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
		auditLogger.LogAbort(&plan, "Aborted by user")

		return nil
	}

	logger.Info("")
	printBox("EXECUTING")
	logger.Info("")

	// Execute with confirmation callback
	execResult, err := exec.Execute(ctx, &plan, func(rowsAffected []int64) bool {
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
	auditEntry, _ := auditLogger.LogApply(&plan, verifyResult, execResult)

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

		// Check if rollback is available (already in plan from plan phase)
		rollbackAvailable := len(plan.Rollback) > 0
		if rollbackAvailable {
			logger.Info("  Rollback available:\n")
			logger.Info(fmt.Sprintf("    safesql rollback --pr %d\n", prNumber))
		} else {
			logger.Info("  No rollback needed (SELECT query)\n")
		}
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
