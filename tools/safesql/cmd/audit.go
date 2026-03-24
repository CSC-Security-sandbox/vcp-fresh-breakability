package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
)

func runAudit(args []string) error {
	fs := flag.NewFlagSet("audit", flag.ExitOnError)

	var (
		auditID string
		last    int
		date    string
		asJSON  bool
	)

	fs.StringVar(&auditID, "id", "", "Show specific audit entry")
	fs.IntVar(&last, "last", 0, "Show last N entries")
	fs.StringVar(&date, "date", "", "Show entries for date (YYYY-MM-DD)")
	fs.BoolVar(&asJSON, "json", false, "Output as JSON")

	if err := fs.Parse(args); err != nil {
		return err
	}

	auditLogger := audit.NewLogger(getAuditStorage())

	if auditID != "" {
		// Show specific entry
		entry, err := auditLogger.Get(auditID)
		if err != nil {
			return fmt.Errorf("failed to get audit entry: %w", err)
		}

		if asJSON {
			data, _ := json.MarshalIndent(entry, "", "  ")
			logger.Info(string(data))
		} else {
			printAuditEntry(entry)
		}
		return nil
	}

	// List entries
	var entries []*audit.Entry
	var err error

	if date != "" {
		t, err := time.Parse("2006-01-02", date)
		if err != nil {
			return fmt.Errorf("invalid date format (use YYYY-MM-DD): %w", err)
		}
		entries, err = auditLogger.ListByDate(t)
		if err != nil {
			return fmt.Errorf("failed to list entries: %w", err)
		}
	} else {
		if last == 0 {
			last = 10 // Default
		}
		entries, err = auditLogger.List(last)
		if err != nil {
			return fmt.Errorf("failed to list entries: %w", err)
		}
	}

	if len(entries) == 0 {
		logger.Info("No audit entries found.")
		return nil
	}

	if asJSON {
		data, _ := json.MarshalIndent(entries, "", "  ")
		logger.Info(string(data))
		return nil
	}

	// Print table
	printAuditList(entries)
	return nil
}

func printAuditList(entries []*audit.Entry) {
	printBox("AUDIT HISTORY")
	logger.Info("")

	// Header
	logger.Info(fmt.Sprintf("  %-6s  %-8s  %-15s  %-25s  %-12s  %s\n",
		"PR", "TYPE", "OPERATOR", "TICKET", "RESULT", "TIMESTAMP"))
	logger.Info(fmt.Sprintf("  %s\n", repeatChar("-", 95)))

	for _, entry := range entries {
		result := "-"
		if entry.Result != nil {
			if entry.Result.Success {
				result = "OK"
			} else if entry.Result.RolledBack {
				result = "ROLLBACK"
			} else {
				result = "FAIL"
			}
		}

		// Get PR number from source metadata
		prNumber := "-"
		if entry.Source.PRMetadata != nil && entry.Source.PRMetadata.Number > 0 {
			prNumber = fmt.Sprintf("#%d", entry.Source.PRMetadata.Number)
		}

		logger.Info(fmt.Sprintf("  %-6s  %-8s  %-15s  %-25s  %-12s  %s\n",
			prNumber,
			entry.Type,
			truncate(entry.Operator, 15),
			entry.Ticket, // Show full ticket name
			result,
			entry.Timestamp.Format("2006-01-02 15:04"),
		))
	}

	logger.Info("")
	logger.Info("  Use 'safesql audit --id <audit-id>' to see details")
}

func printAuditEntry(entry *audit.Entry) {
	var title string
	switch entry.Type {
	case audit.EntryTypePlan:
		title = "PLAN CREATED"
	case audit.EntryTypeApply:
		if entry.Result != nil && entry.Result.Success {
			title = "EXECUTION SUCCESS"
		} else if entry.Result != nil && entry.Result.RolledBack {
			title = "EXECUTION ROLLED BACK"
		} else {
			title = "EXECUTION FAILED"
		}
	case audit.EntryTypeRollback:
		title = "ROLLBACK EXECUTED"
	case audit.EntryTypeAbort:
		title = "EXECUTION ABORTED"
	}

	printBox(title)
	logger.Info("")

	logger.Info(fmt.Sprintf("  Audit ID:  %s\n", entry.AuditID))
	logger.Info(fmt.Sprintf("  Type:      %s\n", entry.Type))
	logger.Info(fmt.Sprintf("  Timestamp: %s\n", entry.Timestamp.Format(time.RFC3339)))
	logger.Info(fmt.Sprintf("  Operator:  %s\n", entry.Operator))
	logger.Info(fmt.Sprintf("  Ticket:    %s\n", entry.Ticket))
	logger.Info(fmt.Sprintf("  Plan ID:   %s\n", entry.PlanID))
	logger.Info("")

	// Source
	logger.Info("  Source:")
	logger.Info(fmt.Sprintf("    Type: %s\n", entry.Source.Type))
	if entry.Source.Type == "github" {
		logger.Info(fmt.Sprintf("    Repository: %s\n", entry.Source.Repository))
		logger.Info(fmt.Sprintf("    Branch: %s\n", entry.Source.Branch))
		logger.Info(fmt.Sprintf("    Commit: %s\n", entry.Source.CommitSHA))
	}
	logger.Info(fmt.Sprintf("    File: %s\n", entry.Source.FilePath))
	logger.Info("")

	// Verification
	if entry.Verification != nil {
		logger.Info("  Verification:")
		if entry.Verification.Valid {
			logger.Info("    Status: [PASS] All checks passed")
		} else {
			logger.Info("    Status: [FAIL] Failed")
			for _, e := range entry.Verification.Errors {
				logger.Info(fmt.Sprintf("      - %s\n", e))
			}
		}
		logger.Info("")
	}

	// Statements
	logger.Info("  Statements:")
	for _, stmt := range entry.Statements {
		logger.Info(fmt.Sprintf("    [%d] %s\n", stmt.Index+1, truncateSQL(stmt.SQL, 60)))
		if stmt.Table != "" {
			logger.Info(fmt.Sprintf("        Table: %s\n", stmt.Table))
		}
		if stmt.RowsAffected > 0 {
			logger.Info(fmt.Sprintf("        Rows Affected: %d\n", stmt.RowsAffected))
		}
		if len(stmt.PreState) > 0 {
			logger.Info(fmt.Sprintf("        Pre-state: %d rows captured\n", len(stmt.PreState)))
		}
	}
	logger.Info("")

	// Result
	if entry.Result != nil {
		logger.Info("  Result:")
		if entry.Result.Success {
			logger.Info("    Status: [SUCCESS]\n")
			logger.Info(fmt.Sprintf("    Total Rows: %d\n", entry.Result.TotalRows))
			logger.Info(fmt.Sprintf("    Duration: %v\n", entry.Result.Duration))
		} else if entry.Result.RolledBack {
			logger.Info("    Status: [ROLLED BACK]\n")
			if entry.Result.ErrorMessage != "" {
				logger.Info(fmt.Sprintf("    Reason: %s\n", entry.Result.ErrorMessage))
			}
		} else {
			logger.Info("    Status: [FAILED]\n")
			if entry.Result.ErrorMessage != "" {
				logger.Info(fmt.Sprintf("    Error: %s\n", entry.Result.ErrorMessage))
			}
		}
		logger.Info("")
	}

	// Rollback SQL
	if len(entry.RollbackSQL) > 0 && entry.Result != nil && entry.Result.Success {
		logger.Info("  Rollback Available:")
		// Show PR-based rollback if PR metadata exists
		if entry.Source.PRMetadata != nil && entry.Source.PRMetadata.Number > 0 {
			logger.Info(fmt.Sprintf("    safesql rollback --pr %d\n", entry.Source.PRMetadata.Number))
		}
		logger.Info("")
		logger.Info("  Rollback SQL:")
		for i, sql := range entry.RollbackSQL {
			if sql != "" {
				logger.Info(fmt.Sprintf("    [%d] %s\n", i+1, truncateSQL(sql, 60)))
			}
		}
	}
}

func truncate(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

func repeatChar(char string, count int) string {
	return strings.Repeat(char, count)
}
