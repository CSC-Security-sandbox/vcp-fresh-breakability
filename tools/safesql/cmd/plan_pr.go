package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/analyzer"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/rollback"
)

func runPlanForPR(prNumber int, ticket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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

	// Extract ticket from PR title if not provided
	if ticket == "" {
		ticket = extractTicketFromPRTitle(pr.Title)
		if ticket == "" {
			return fmt.Errorf("ticket is required. Either provide --ticket flag or include ticket in PR title (e.g., 'NFSAAS-12345: Description' or 'VSCP-12345: Description')")
		}
		logger.Info(fmt.Sprintf("Extracted ticket from PR title: %s\n", ticket))
		logger.Info("")
	}

	// Check PR state
	if pr.State == "merged" {
		return fmt.Errorf("PR is already merged. Use 'safesql apply --pr %d' to apply the plan", prNumber)
	}

	if pr.State != "open" {
		return fmt.Errorf("PR is closed. Cannot generate plan for closed PR")
	}

	// List files in PR
	logger.Info("Checking PR files...\n")
	files, err := ghClient.ListPRFiles(ctx, owner, repo, prNumber)
	if err != nil {
		return fmt.Errorf("failed to list PR files: %w", err)
	}

	// Find SQL files (must be exactly one)
	var sqlFiles []*github.PRFile
	for _, f := range files {
		if strings.HasSuffix(f.Filename, ".sql") && f.Status != "removed" {
			sqlFiles = append(sqlFiles, f)
		}
	}

	if len(sqlFiles) == 0 {
		return fmt.Errorf("validation error: no SQL file found in PR")
	}

	if len(sqlFiles) > 1 {
		return fmt.Errorf("validation error: multiple SQL files found in PR. Only one SQL file per PR is allowed")
	}

	sqlFile := sqlFiles[0]
	logger.Info(fmt.Sprintf("Found SQL file: %s\n", sqlFile.Filename))
	logger.Info("")

	// Check if plan already exists
	planFilename := strings.TrimSuffix(sqlFile.Filename, ".sql") + "-plan.json"
	var existingPlanFile *github.PRFile
	for _, f := range files {
		if f.Filename == planFilename {
			existingPlanFile = f
			break
		}
	}

	// Fetch SQL file content
	logger.Info("Fetching SQL file content...\n")
	fileContent, err := ghClient.GetPRFile(ctx, owner, repo, pr.HeadBranch, sqlFile.Filename)
	if err != nil {
		return fmt.Errorf("failed to fetch SQL file: %w", err)
	}

	content := fileContent.Content

	// Parse and validate SQL
	logger.Info("Parsing and validating SQL...\n")
	p := parser.New(nil)
	parseResult, err := p.Parse(content)
	if err != nil {
		return fmt.Errorf("failed to parse SQL: %w", err)
	}

	// Validate
	v := parser.NewValidator(
		parser.WithRequireWhere(true),
		parser.WithBlockDangerous(true),
	)
	validationResult := v.Validate(parseResult)

	if !validationResult.Valid {
		printBox("VALIDATION FAILED")
		for _, e := range validationResult.Errors {
			logger.Info(fmt.Sprintf("  [ERROR] %s: %s\n", e.Rule, e.Description))
		}
		return fmt.Errorf("validation error: query validation failed. Fix the SQL and push to PR")
	}

	// Print warnings
	if len(validationResult.Warnings) > 0 {
		printBox("WARNINGS")
		for _, w := range validationResult.Warnings {
			logger.Info(fmt.Sprintf("  [WARNING] %s: %s\n", w.Rule, w.Description))
		}
		logger.Info("")
	}

	// Validate database configuration
	if cfg.Database.Host == "" {
		return fmt.Errorf("database host not configured. Set DB_HOST environment variable or create .safesql/config.yaml")
	}
	if cfg.Database.User == "" {
		return fmt.Errorf("database user not configured. Set DB_USER environment variable or create .safesql/config.yaml")
	}
	if cfg.Database.Password == "" && !cfg.Database.UseIAM {
		return fmt.Errorf("database password not configured. Set DB_PASSWORD environment variable or create .safesql/config.yaml")
	}
	if cfg.Database.DBName == "" {
		return fmt.Errorf("database name not configured. Set DB_NAME environment variable or create .safesql/config.yaml")
	}

	// Connect to database
	logger.Info("Connecting to database...\n")
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

	// Analyze impact
	logger.Info("Analyzing impact...\n")
	a := analyzer.New(dbClient)
	analysisResult, err := a.Analyze(ctx, parseResult)
	if err != nil {
		return fmt.Errorf("failed to analyze impact: %w", err)
	}

	// Check thresholds
	if analysisResult.TotalRows > int64(cfg.Thresholds.BlockThreshold) {
		return fmt.Errorf("validation error: query would affect %d rows, exceeds block threshold of %d",
			analysisResult.TotalRows, cfg.Thresholds.BlockThreshold)
	}

	// Generate rollback SQL
	logger.Info("Generating rollback SQL...\n")
	rbGen := rollback.New()
	var rollbackSQL []string
	for i, stmt := range parseResult.Statements {
		var impact *analyzer.StatementImpact
		for j := range analysisResult.Statements {
			if analysisResult.Statements[j].StatementIndex == i {
				impact = &analysisResult.Statements[j]
				break
			}
		}

		if impact != nil && impact.RowsPreview != nil {
			rb, _ := rbGen.GenerateConsolidated(&stmt, impact.RowsPreview, "uuid")
			rollbackSQL = append(rollbackSQL, rb)
		} else {
			rollbackSQL = append(rollbackSQL, "")
		}
	}

	// Auto-fetch username
	operator, err := getCurrentUsername()
	if err != nil {
		return fmt.Errorf("failed to get current username: %w", err)
	}
	if envOperator := os.Getenv("SAFESQL_OPERATOR"); envOperator != "" {
		operator = envOperator
	}

	// Build source info
	sourceInfo := planner.SourceInfo{
		Type:       "github",
		Repository: fmt.Sprintf("%s/%s", owner, repo),
		Branch:     pr.HeadBranch,
		CommitSHA:  fileContent.CommitSHA,
		FilePath:   sqlFile.Filename,
		FileHash:   computeFileHash(content),
		PRMetadata: pr,
	}

	// Build plan
	logger.Info("Building execution plan...\n")
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, getPlanStorage())
	plan, err := pb.Build(sourceInfo, parseResult, analysisResult, rollbackSQL, operator, ticket)
	if err != nil {
		return fmt.Errorf("failed to build plan: %w", err)
	}

	// Save plan to GCS
	if err := pb.Save(ctx, plan); err != nil {
		return fmt.Errorf("failed to save plan: %w", err)
	}

	// Log to audit
	auditLogger := audit.NewLogger(getAuditStorage())
	if _, err := auditLogger.LogPlan(plan); err != nil {
		logger.Info(fmt.Sprintf("Warning: failed to log audit: %v\n", err))
	}

	// Check if plan file already exists in PR and if it's still valid
	logger.Info("")
	var existingPlan *planner.Plan
	var planExists bool
	var planExpired bool
	
	if existingPlanFile != nil {
		logger.Info("Checking existing plan validity...\n")
		
		// Fetch the actual file content
		fileContent, err := ghClient.GetPRFile(ctx, owner, repo, pr.HeadBranch, planFilename)
		if err == nil && fileContent != nil {
			// Plan file exists, try to parse it (only first JSON object)
			planExists = true
			var existingPlanData planner.Plan
			if err := parseFirstJSON([]byte(fileContent.Content), &existingPlanData); err == nil {
				existingPlan = &existingPlanData
				
				// Check if SQL has changed by comparing file hash
				currentFileHash := computeFileHash(content)
				if existingPlan.Source.FileHash != currentFileHash {
					logger.Info("SQL file has changed since plan was created\n")
					logger.Info(fmt.Sprintf("  Previous hash: %s\n", existingPlan.Source.FileHash[:12]))
					logger.Info(fmt.Sprintf("  Current hash:  %s\n", currentFileHash[:12]))
					logger.Info("  Regenerating plan with new SQL...\n")
					logger.Info("")
					// Continue to generate new plan
				} else if time.Now().After(existingPlan.ExpiresAt) {
					// Plan expired
					planExpired = true
					logger.Info(fmt.Sprintf("Found existing plan (EXPIRED at %s)\n", existingPlan.ExpiresAt.Format(time.RFC3339)))
				} else {
					// Plan is valid and SQL hasn't changed
					logger.Info(fmt.Sprintf("Found valid existing plan (expires at %s)\n", existingPlan.ExpiresAt.Format(time.RFC3339)))
					logger.Info(fmt.Sprintf("SQL unchanged (hash: %s)\n", currentFileHash[:12]))
					logger.Info("")
					
					// Use existing plan
					printBox("USING EXISTING PLAN FROM PR")
					logger.Info("")
					logger.Info(fmt.Sprintf("  Plan ID: %s\n", existingPlan.PlanID))
					logger.Info(fmt.Sprintf("  Expires: %s\n", existingPlan.ExpiresAt.Format(time.RFC3339)))
					logger.Info(fmt.Sprintf("  SQL Hash: %s\n", currentFileHash[:12]))
					logger.Info(fmt.Sprintf("  Plan file: %s\n", planFilename))
					logger.Info(fmt.Sprintf("  PR: %s\n", pr.URL))
					logger.Info("")
					logger.Info("Next steps:\n")
					logger.Info("  1. Get the PR reviewed, approved, and merged\n")
					logger.Info(fmt.Sprintf("  2. Run: safesql apply --pr %d\n", prNumber))
					
					return nil
				}
			}
		}
	} else {
		logger.Info("No existing plan found in PR\n")
	}

	// Print plan summary (PR-specific, without local file paths)
	printPlanSummaryForPR(plan)

	// Create commit suggestion for the plan
	logger.Info("")
	
	planJSON, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	var reviewMessage string
	var actionMessage string
	
	// Check if SQL changed
	sqlChanged := false
	if existingPlan != nil && existingPlan.Source.FileHash != plan.Source.FileHash {
		sqlChanged = true
	}
	
	if planExists && sqlChanged {
		// SQL changed - need new plan
		logger.Info("Creating commit suggestion to UPDATE plan (SQL changed)...\n")
		reviewMessage = fmt.Sprintf("## 🔄 Updated Execution Plan (SQL Changed)\n\n"+
			"**Plan File:** `%s`\n"+
			"**New Plan ID:** `%s`\n"+
			"**Expires:** %s\n"+
			"**Previous SQL Hash:** `%s`\n"+
			"**Current SQL Hash:** `%s`\n\n"+
			"The SQL query has changed. Click 'Commit suggestion' below to update the plan.",
			planFilename, plan.PlanID, plan.ExpiresAt.Format(time.RFC3339),
			existingPlan.Source.FileHash[:12], plan.Source.FileHash[:12])
		actionMessage = "COMMIT SUGGESTION POSTED (SQL CHANGED)"
	} else if planExists && planExpired {
		// Update existing expired plan
		logger.Info("Creating commit suggestion to UPDATE expired plan...\n")
		reviewMessage = fmt.Sprintf("## 🔄 Updated Execution Plan (Previous Expired)\n\n"+
			"**Plan File:** `%s`\n"+
			"**New Plan ID:** `%s`\n"+
			"**Expires:** %s\n\n"+
			"The previous plan expired. Click 'Commit suggestion' below to update it.",
			planFilename, plan.PlanID, plan.ExpiresAt.Format(time.RFC3339))
		actionMessage = "COMMIT SUGGESTION POSTED (EXPIRED)"
	} else if planExists {
		// Update existing plan (shouldn't happen, but handle it)
		logger.Info("Creating commit suggestion to UPDATE plan...\n")
		reviewMessage = fmt.Sprintf("## 🔄 Updated Execution Plan\n\n"+
			"**Plan File:** `%s`\n"+
			"**New Plan ID:** `%s`\n"+
			"**Expires:** %s\n\n"+
			"Click 'Commit suggestion' below to update the plan.",
			planFilename, plan.PlanID, plan.ExpiresAt.Format(time.RFC3339))
		actionMessage = "COMMIT SUGGESTION POSTED (UPDATE)"
	} else {
		// Create new plan
		logger.Info("Creating commit suggestion to ADD plan...\n")
		reviewMessage = fmt.Sprintf("## ✅ Execution Plan Generated\n\n"+
			"**Plan File:** `%s`\n"+
			"**Plan ID:** `%s`\n"+
			"**Expires:** %s\n\n"+
			"Click 'Commit suggestion' below to add the plan file.",
			planFilename, plan.PlanID, plan.ExpiresAt.Format(time.RFC3339))
		actionMessage = "COMMIT SUGGESTION POSTED (NEW)"
	}

	err = ghClient.CreatePRReviewWithSuggestion(ctx, owner, repo, prNumber, planFilename, string(planJSON), reviewMessage, planExists)
	if err != nil {
		return fmt.Errorf("failed to create commit suggestion: %w", err)
	}

	logger.Info("")
	printBox(actionMessage)
	logger.Info("")
	logger.Info(fmt.Sprintf("  Plan file: %s\n", planFilename))
	logger.Info(fmt.Sprintf("  PR: %s\n", pr.URL))
	logger.Info("")
	logger.Info("Next steps:\n")
	logger.Info("  1. Go to the PR and find the review comment\n")
	logger.Info("  2. Click 'Commit suggestion' button\n")
	logger.Info("     (Will be signed with YOUR GPG key)\n")
	logger.Info("  3. Get the PR reviewed, approved, and merged\n")
	logger.Info(fmt.Sprintf("  4. Run: safesql apply --pr %d\n", prNumber))

	return nil
}

// printPlanSummaryForPR prints plan summary without local file paths (PR workflow only)
func printPlanSummaryForPR(plan *planner.Plan) {
	printBox("EXECUTION PLAN GENERATED")
	logger.Info("")
	logger.Info(fmt.Sprintf("  Plan ID: %s\n", plan.PlanID))
	logger.Info(fmt.Sprintf("  Expires: %s\n", plan.ExpiresAt.Format(time.RFC3339)))
	logger.Info("")

	logger.Info("  Source:")
	logger.Info(fmt.Sprintf("    Type: %s\n", plan.Source.Type))
	if plan.Source.Type == "github" {
		logger.Info(fmt.Sprintf("    Repository: %s\n", plan.Source.Repository))
		logger.Info(fmt.Sprintf("    Branch: %s\n", plan.Source.Branch))
		logger.Info(fmt.Sprintf("    Commit: %s\n", plan.Source.CommitSHA[:12]))
		if plan.Source.PRMetadata != nil {
			logger.Info(fmt.Sprintf("    PR #%d: %s\n", plan.Source.PRMetadata.Number, plan.Source.PRMetadata.Title))
			logger.Info(fmt.Sprintf("    Approvers: %v\n", plan.Source.PRMetadata.Approvers))
		}
	}
	logger.Info(fmt.Sprintf("    File: %s\n", plan.Source.FilePath))
	logger.Info("")

	logger.Info("  Impact Analysis:")
	logger.Info(fmt.Sprintf("    Total Statements: %d\n", len(plan.Query.Statements)))
	logger.Info(fmt.Sprintf("    Total Rows Affected: %d\n", plan.Impact.TotalRows))
	logger.Info(fmt.Sprintf("    Tables: %v\n", plan.Impact.UniqueTables))
	logger.Info("")

	// Display all statements
	for i, stmtInfo := range plan.Query.Statements {
		logger.Info(fmt.Sprintf("  Statement %d (%s):\n", i+1, stmtInfo.Type))
		logger.Info(fmt.Sprintf("    SQL: %s\n", truncateSQL(stmtInfo.SQL, 70)))

		// Find impact analysis for this statement if it exists
		var impact *analyzer.StatementImpact
		for j := range plan.Impact.Statements {
			if plan.Impact.Statements[j].StatementIndex == i {
				impact = &plan.Impact.Statements[j]
				break
			}
		}

		if impact != nil {
			logger.Info(fmt.Sprintf("    Table: %s\n", impact.Table))
			logger.Info(fmt.Sprintf("    Rows Affected: %d\n", impact.AffectedRows))
		} else {
			// SELECT statements or other non-mutating statements
			if stmtInfo.Table != "" {
				logger.Info(fmt.Sprintf("    Table: %s\n", stmtInfo.Table))
			}
			logger.Info(fmt.Sprintf("    Rows Affected: %d\n", 0))
		}
		logger.Info("")
	}
	// Note: No local file path or "apply --plan" command for PR workflow
}

// extractTicketFromPRTitle extracts ticket ID from PR title
// Supports formats like "NFSAAS-12345: Description" or "VSCP-12345: Description"
func extractTicketFromPRTitle(title string) string {
	// Pattern matches: PROJECT-NUMBER at the start of the title
	// Examples: NFSAAS-153765, VSCP-1537, JIRA-123, etc.
	re := regexp.MustCompile(`^([A-Z]+[-_][0-9]+)`)
	matches := re.FindStringSubmatch(title)
	if len(matches) > 1 {
		return matches[1]
	}
	return ""
}
