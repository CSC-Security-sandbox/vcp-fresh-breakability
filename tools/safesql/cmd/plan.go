package cmd

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"os"
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

func runPlan(args []string) error {
	fs := flag.NewFlagSet("plan", flag.ExitOnError)

	var (
		githubRef string
		localFile string
		operator  string
		ticket    string
		force     bool
	)

	fs.StringVar(&githubRef, "github", "", "GitHub reference (owner/repo@branch:path or branch:path)")
	fs.StringVar(&localFile, "file", "", "Local file path (requires --force if GitHub source required)")
	fs.StringVar(&operator, "operator", "", "Operator name/email (required)")
	fs.StringVar(&ticket, "ticket", "", "Ticket reference (required)")
	fs.BoolVar(&force, "force", false, "Force local file execution (bypasses GitHub requirement)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validate inputs
	if operator == "" {
		return fmt.Errorf("--operator is required")
	}
	if ticket == "" {
		return fmt.Errorf("--ticket is required")
	}
	if githubRef == "" && localFile == "" {
		return fmt.Errorf("either --github or --file is required")
	}

	// Check GitHub requirement
	if cfg.GitHub.RequireGitHubSource && localFile != "" && !force {
		return fmt.Errorf("local files not allowed - use --github or --force to override")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Fetch query content
	var content string
	var sourceInfo planner.SourceInfo

	if githubRef != "" {
		// Parse and fetch from GitHub
		parts := strings.SplitN(cfg.GitHub.Repository, "/", 2)
		var defaultOwner, defaultRepo string
		if len(parts) == 2 {
			defaultOwner, defaultRepo = parts[0], parts[1]
		}

		source, err := github.ParseGitHubRef(githubRef, defaultOwner, defaultRepo)
		if err != nil {
			return fmt.Errorf("invalid GitHub reference: %w", err)
		}

		ghClient := github.New(cfg.GitHub.Token)
		fileContent, err := ghClient.GetFile(ctx, source)
		if err != nil {
			return fmt.Errorf("failed to fetch from GitHub: %w", err)
		}

		content = fileContent.Content
		sourceInfo = planner.SourceInfo{
			Type:       "github",
			Repository: fmt.Sprintf("%s/%s", source.Owner, source.Repo),
			Branch:     source.Branch,
			CommitSHA:  fileContent.CommitSHA,
			FilePath:   fileContent.Path,
			FileHash:   computeFileHash(content),
		}

		// Get PR metadata
		prMeta, err := ghClient.GetPRForCommit(ctx, source.Owner, source.Repo, fileContent.CommitSHA)
		if err == nil && prMeta != nil {
			sourceInfo.PRMetadata = prMeta
		}

		// Check PR requirements
		if cfg.GitHub.RequireMergedPR && (prMeta == nil || prMeta.MergedAt == nil) {
			return fmt.Errorf("query file must be from a merged PR")
		}
		if cfg.GitHub.MinApprovers > 0 && (prMeta == nil || len(prMeta.Approvers) < cfg.GitHub.MinApprovers) {
			return fmt.Errorf("PR must have at least %d approvers", cfg.GitHub.MinApprovers)
		}
	} else {
		// Read local file
		data, err := os.ReadFile(localFile)
		if err != nil {
			return fmt.Errorf("failed to read file: %w", err)
		}
		content = string(data)
		sourceInfo = planner.SourceInfo{
			Type:     "local",
			FilePath: localFile,
			FileHash: computeFileHash(content),
		}
	}

	// Parse and validate SQL
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
		printBox("VALIDATION FAILED", "red")
		for _, e := range validationResult.Errors {
			logger.Info(fmt.Sprintf("  ❌ %s: %s\n", e.Rule, e.Description))
		}
		return fmt.Errorf("query validation failed")
	}

	// Print warnings
	if len(validationResult.Warnings) > 0 {
		printBox("WARNINGS", "yellow")
		for _, w := range validationResult.Warnings {
			logger.Info(fmt.Sprintf("  ⚠️  %s: %s\n", w.Rule, w.Description))
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
	if cfg.Database.Password == "" {
		return fmt.Errorf("database password not configured. Set DB_PASSWORD environment variable or create .safesql/config.yaml")
	}
	if cfg.Database.DBName == "" {
		return fmt.Errorf("database name not configured. Set DB_NAME environment variable or create .safesql/config.yaml")
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

	// Analyze impact
	a := analyzer.New(dbClient)
	analysisResult, err := a.Analyze(ctx, parseResult)
	if err != nil {
		return fmt.Errorf("failed to analyze impact: %w", err)
	}

	// Check thresholds
	if analysisResult.TotalRows > int64(cfg.Thresholds.BlockThreshold) {
		return fmt.Errorf("query would affect %d rows, exceeds block threshold of %d",
			analysisResult.TotalRows, cfg.Thresholds.BlockThreshold)
	}

	// Generate rollback SQL
	rbGen := rollback.New()
	var rollbackSQL []string
	for i, stmt := range parseResult.Statements {
		var impact *analyzer.StatementImpact
		// Find the impact analysis for this statement by matching StatementIndex
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

	// Build plan
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, cfg.GetPlanStorePath())
	plan, err := pb.Build(sourceInfo, parseResult, analysisResult, rollbackSQL, operator, ticket)
	if err != nil {
		return fmt.Errorf("failed to build plan: %w", err)
	}

	// Save plan
	if err := pb.Save(plan); err != nil {
		return fmt.Errorf("failed to save plan: %w", err)
	}

	// Log to audit
	auditLogger := audit.NewLogger(cfg.GetAuditPath())
	if _, err := auditLogger.LogPlan(plan); err != nil {
		logger.Info(fmt.Sprintf("Warning: failed to log audit: %v\n", err))
	}

	// Print plan summary
	printPlanSummary(plan)

	return nil
}

func printPlanSummary(plan *planner.Plan) {
	printBox("EXECUTION PLAN GENERATED", "green")
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

	// Display all statements, including SELECT statements that don't have impact analysis
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
			if len(impact.RowsPreview) > 0 {
				logger.Info(fmt.Sprintf("    Preview (first %d rows):\n", len(impact.RowsPreview)))
				for j, row := range impact.RowsPreview {
					if j >= 3 {
						logger.Info(fmt.Sprintf("      ... and %d more\n", len(impact.RowsPreview)-3))
						break
					}
					logger.Info(fmt.Sprintf("      %v\n", formatRowPreview(row)))
				}
			}
		} else {
			// SELECT statements or other non-mutating statements
			if stmtInfo.Table != "" {
				logger.Info(fmt.Sprintf("    Table: %s\n", stmtInfo.Table))
			}
			logger.Info("    (No impact analysis - non-mutating statement)\n")
		}
		logger.Info("")
	}

	logger.Info(fmt.Sprintf("  Plan saved to: %s%s.json\n", cfg.GetPlanStorePath(), plan.PlanID))
	logger.Info("")
	logger.Info("  Next step:")
	logger.Info(fmt.Sprintf("    safesql apply --plan %s\n", plan.PlanID))
}

func computeFileHash(content string) string {
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", hash)
}

func formatRowPreview(row map[string]interface{}) string {
	var parts []string
	for k, v := range row {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	if len(parts) > 5 {
		return strings.Join(parts[:5], ", ") + "..."
	}
	return strings.Join(parts, ", ")
}

func printBox(title, color string) {
	var colorCode string
	switch color {
	case "red":
		colorCode = "\033[31m"
	case "green":
		colorCode = "\033[32m"
	case "yellow":
		colorCode = "\033[33m"
	default:
		colorCode = ""
	}
	reset := "\033[0m"

	border := strings.Repeat("═", len(title)+4)
	logger.Info(fmt.Sprintf("%s╔%s╗%s\n", colorCode, border, reset))
	logger.Info(fmt.Sprintf("%s║  %s  ║%s\n", colorCode, title, reset))
	logger.Info(fmt.Sprintf("%s╚%s╝%s\n", colorCode, border, reset))
}
