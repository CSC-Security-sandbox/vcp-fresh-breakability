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

func runRollbackForPR(prNumber int) error {
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
	logger.Info("")

	// List files in PR
	logger.Info("Checking PR files...\n")
	files, err := ghClient.ListPRFiles(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to list PR files: %w", err)
	}

	// Find plan file
	var planFile *github.PRFile
	for _, f := range files {
		if strings.HasSuffix(f.Filename, "-plan.json") && f.Status != "removed" {
			planFile = f
			break
		}
	}

	if planFile == nil {
		return fmt.Errorf("no plan file found in PR")
	}

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
	logger.Info(fmt.Sprintf("Created at: %s\n", plan.CreatedAt.Format(time.RFC3339)))
	logger.Info("")

	// Check if rollback is available
	if len(plan.Rollback) == 0 {
		return fmt.Errorf("no rollback data available in plan. This may be a SELECT query or the plan was created before rollback support")
	}

	logger.Info(fmt.Sprintf("Rollback statements available: %d\n", len(plan.Rollback)))
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

	printBox("ROLLBACK PLAN")
	logger.Info("")
	logger.Info("  The following rollback statements will be executed:")
	logger.Info("")

	for i, rb := range plan.Rollback {
		logger.Info(fmt.Sprintf("  [%d] %s\n", i+1, truncateSQL(rb.SQL, 80)))
	}
	logger.Info("")

	// Confirmation
	logger.Info("  Type 'ROLLBACK' to execute, or 'ABORT' to cancel: ")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input != "ROLLBACK" {
		logger.Info("")
		logger.Info("  Rollback aborted by user.")
		return nil
	}

	logger.Info("")
	printBox("EXECUTING ROLLBACK")
	logger.Info("")

	// Execute rollback
	execResult, err := exec.ExecuteRollback(ctx, &plan)

	// Log rollback (no original audit ID since we don't track it in plan anymore)
	auditLogger := audit.NewLogger(getAuditStorage())
	auditEntry, _ := auditLogger.LogRollback("", &plan, execResult)

	logger.Info("")
	if execResult != nil && execResult.Success {
		printBox("ROLLBACK SUCCESSFUL")
		logger.Info("")
		logger.Info(fmt.Sprintf("  Rows affected: %d\n", execResult.TotalRows))
		logger.Info(fmt.Sprintf("  Duration: %v\n", execResult.Duration))
		if auditEntry != nil {
			logger.Info(fmt.Sprintf("  Audit ID: %s\n", auditEntry.AuditID))
		}
		logger.Info("")
		logger.Info("  The changes have been rolled back successfully.")
	} else if execResult != nil && execResult.RolledBack {
		printBox("ROLLBACK FAILED")
		logger.Info("")
		logger.Info("  Rollback transaction was rolled back (no changes made).")
		if execResult.Error != nil {
			logger.Info(fmt.Sprintf("  Reason: %v\n", execResult.Error))
		}
	} else {
		printBox("ROLLBACK FAILED")
		logger.Info("")
		if err != nil {
			logger.Info(fmt.Sprintf("  Error: %v\n", err))
		}
	}

	return err
}
