package cmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

func runShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)

	var (
		planID   string
		prNumber int
		asJSON   bool
	)

	fs.StringVar(&planID, "plan", "", "Plan ID to show")
	fs.IntVar(&prNumber, "pr", 0, "Pull request number (shows plan from PR)")
	fs.BoolVar(&asJSON, "json", false, "Output as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Handle PR-based show
	if prNumber > 0 {
		if planID != "" {
			return fmt.Errorf("cannot specify both --plan and --pr")
		}
		return runShowForPR(prNumber, asJSON)
	}

	if planID == "" {
		return fmt.Errorf("either --plan or --pr is required")
	}

	// Load plan
	ctx := context.Background()
	pb := planner.NewPlanBuilder(cfg.Thresholds.PlanExpiry, getPlanStorage())
	plan, err := pb.Load(ctx, planID)
	if err != nil {
		return fmt.Errorf("failed to load plan: %w", err)
	}

	if asJSON {
		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal plan: %w", err)
		}
		logger.Info(string(data))
		return nil
	}

	// Print formatted plan details
	printDetailedPlan(plan)
	return nil
}

func printDetailedPlan(plan *planner.Plan) {
	isExpired := time.Now().UTC().After(plan.ExpiresAt)

	if isExpired {
		printBox("PLAN (EXPIRED)")
	} else {
		printBox("PLAN DETAILS")
	}
	logger.Info("")

	// Basic info
	logger.Info(fmt.Sprintf("  Plan ID:    %s", plan.PlanID))
	logger.Info(fmt.Sprintf("  Created:    %s", plan.CreatedAt.Format(time.RFC3339)))
	if isExpired {
		logger.Info(fmt.Sprintf("  Expires:    %s (EXPIRED)", plan.ExpiresAt.Format(time.RFC3339)))
	} else {
		logger.Info(fmt.Sprintf("  Expires:    %s (%v remaining)", plan.ExpiresAt.Format(time.RFC3339), time.Until(plan.ExpiresAt).Round(time.Minute)))
	}
	logger.Info(fmt.Sprintf("  Operator:   %s", plan.Operator))
	logger.Info(fmt.Sprintf("  Ticket:     %s", plan.Ticket))
	logger.Info("")

	// Source info
	logger.Info("  Source:")
	logger.Info(fmt.Sprintf("    Type:       %s", plan.Source.Type))
	if plan.Source.Type == "github" {
		logger.Info(fmt.Sprintf("    Repository: %s", plan.Source.Repository))
		logger.Info(fmt.Sprintf("    Branch:     %s", plan.Source.Branch))
		logger.Info(fmt.Sprintf("    Commit:     %s", plan.Source.CommitSHA))
		if plan.Source.PRMetadata != nil {
			logger.Info(fmt.Sprintf("    PR:         #%d - %s", plan.Source.PRMetadata.Number, plan.Source.PRMetadata.Title))
			logger.Info(fmt.Sprintf("    Author:     %s", plan.Source.PRMetadata.Author))
			logger.Info(fmt.Sprintf("    Approvers:  %s", strings.Join(plan.Source.PRMetadata.Approvers, ", ")))
			if plan.Source.PRMetadata.MergedAt != nil {
				logger.Info(fmt.Sprintf("    Merged:     %s", plan.Source.PRMetadata.MergedAt.Format(time.RFC3339)))
			}
		}
	}
	logger.Info(fmt.Sprintf("    File:       %s", plan.Source.FilePath))
	logger.Info(fmt.Sprintf("    Hash:       %s", plan.Source.FileHash))
	logger.Info("")

	// Query info
	logger.Info("  Query Metadata:")
	logger.Info(fmt.Sprintf("    Ticket:      %s", plan.Query.Metadata.Ticket))
	logger.Info(fmt.Sprintf("    Author:      %s", plan.Query.Metadata.Author))
	logger.Info(fmt.Sprintf("    Description: %s", plan.Query.Metadata.Description))
	logger.Info("")

	// Statements
	logger.Info("  Statements:")
	for i, stmt := range plan.Query.Statements {
		logger.Info("")
		logger.Info(fmt.Sprintf("    [%d] %s on '%s'", i+1, stmt.Type, stmt.Table))
		logger.Info(fmt.Sprintf("        SQL: %s", truncateSQL(stmt.SQL, 60)))
		logger.Info(fmt.Sprintf("        Hash: %s", stmt.Hash[:20]+"..."))

		// Find impact for this statement
		for _, impact := range plan.Impact.Statements {
			if impact.StatementIndex == stmt.Index {
				logger.Info(fmt.Sprintf("        Affected Rows: %d", impact.AffectedRows))
				if impact.WhereClause != "" {
					logger.Info(fmt.Sprintf("        WHERE: %s", truncateSQL(impact.WhereClause, 50)))
				}
				break
			}
		}
	}
	logger.Info("")

	// State snapshots
	if len(plan.Snapshots) > 0 {
		logger.Info("  State Snapshots:")
		for _, snap := range plan.Snapshots {
			logger.Info(fmt.Sprintf("    Statement %d: %d rows captured at %s",
				snap.StatementIndex+1, snap.RowCount, snap.CapturedAt.Format("15:04:05")))
			if snap.RowsHash != "" && len(snap.RowsHash) >= 20 {
				logger.Info(fmt.Sprintf("      Hash: %s", snap.RowsHash[:20]+"..."))
			} else if snap.RowsHash != "" {
				logger.Info(fmt.Sprintf("      Hash: %s", snap.RowsHash))
			} else {
				logger.Info("      Hash: (no rows)")
			}
			if len(snap.RowsPreview) > 0 {
				logger.Info("      Preview:")
				for j, row := range snap.RowsPreview {
					if j >= 3 {
						logger.Info(fmt.Sprintf("        ... and %d more rows", len(snap.RowsPreview)-3))
						break
					}
					logger.Info(fmt.Sprintf("        %v", formatRowPreview(row)))
				}
			}
		}
		logger.Info("")
	}

	// Rollback info
	if len(plan.Rollback) > 0 {
		logger.Info("  Rollback SQL:")
		for _, rb := range plan.Rollback {
			logger.Info(fmt.Sprintf("    [%d] %s", rb.StatementIndex+1, truncateSQL(rb.SQL, 60)))
		}
		logger.Info("")
	}

	// Verification queries
	if len(plan.VerificationQueries) > 0 {
		logger.Info("  Verification Queries:")
		for _, vq := range plan.VerificationQueries {
			logger.Info(fmt.Sprintf("    Statement %d (%s-execution):", vq.StatementIndex+1, vq.Type))
			logger.Info(fmt.Sprintf("      SQL: %s", truncateSQL(vq.SQL, 60)))
			logger.Info(fmt.Sprintf("      Expected Count: %d", vq.ExpectedCount))
			logger.Info(fmt.Sprintf("      Description: %s", vq.Description))
		}
		logger.Info("")
	}

	// Signature
	logger.Info(fmt.Sprintf("  Signature: %s", plan.Signature[:30]+"..."))
	logger.Info("")

	// Next steps
	if !isExpired {
		logger.Info("  To apply this plan:")
		logger.Info(fmt.Sprintf("    safesql apply --plan %s", plan.PlanID))
	} else {
		logger.Info("  This plan has expired. Create a new plan to execute the query.")
	}
}
