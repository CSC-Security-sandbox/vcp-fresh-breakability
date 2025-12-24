// Package analyzer provides impact analysis for SQL statements.
package analyzer

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
)

// StatementImpact contains the impact analysis for a single statement.
type StatementImpact struct {
	StatementIndex int                      `json:"statement_index"`
	SQL            string                   `json:"sql"`
	SQLHash        string                   `json:"sql_hash"`
	Type           parser.StatementType     `json:"type"`
	Table          string                   `json:"table"`
	WhereClause    string                   `json:"where_clause,omitempty"`
	AffectedRows   int64                    `json:"affected_rows"`
	EstimatedRows  int64                    `json:"estimated_rows"`
	RowsPreview    []map[string]interface{} `json:"rows_preview,omitempty"`
	RowsHash       string                   `json:"rows_hash"`
	ExplainPlan    string                   `json:"explain_plan,omitempty"`
}

// AnalysisResult contains the complete impact analysis.
type AnalysisResult struct {
	Statements   []StatementImpact `json:"statements"`
	TotalRows    int64             `json:"total_rows"`
	TablesCount  int               `json:"tables_count"`
	UniqueTables []string          `json:"unique_tables"`
}

// Analyzer performs impact analysis on SQL statements.
type Analyzer struct {
	db           *database.Client
	previewLimit int
}

// New creates a new Analyzer.
func New(db *database.Client) *Analyzer {
	return &Analyzer{
		db:           db,
		previewLimit: 10, // Default preview limit
	}
}

// WithPreviewLimit sets the number of rows to preview.
func (a *Analyzer) WithPreviewLimit(limit int) *Analyzer {
	a.previewLimit = limit
	return a
}

// Analyze performs impact analysis on parsed statements.
func (a *Analyzer) Analyze(ctx context.Context, parseResult *parser.ParseResult) (*AnalysisResult, error) {
	result := &AnalysisResult{}
	tableSet := make(map[string]bool)

	for i, stmt := range parseResult.Statements {
		impact, err := a.analyzeStatement(ctx, i, &stmt)
		if err != nil {
			return nil, fmt.Errorf("failed to analyze statement %d: %w", i+1, err)
		}

		result.Statements = append(result.Statements, *impact)
		result.TotalRows += impact.AffectedRows

		for _, table := range stmt.Tables {
			tableSet[table] = true
		}
	}

	// Compile unique tables
	for table := range tableSet {
		result.UniqueTables = append(result.UniqueTables, table)
	}
	sort.Strings(result.UniqueTables)
	result.TablesCount = len(result.UniqueTables)

	return result, nil
}

func (a *Analyzer) analyzeStatement(ctx context.Context, index int, stmt *parser.Statement) (*StatementImpact, error) {
	impact := &StatementImpact{
		StatementIndex: index,
		SQL:            stmt.SQL,
		SQLHash:        stmt.Hash,
		Type:           stmt.Type,
		WhereClause:    stmt.WhereClause,
	}

	if len(stmt.Tables) > 0 {
		impact.Table = stmt.Tables[0]
	}

	// Skip analysis for non-mutating statements
	if !stmt.IsMutatingStatement() {
		return impact, nil
	}

	// Check if table exists
	if impact.Table != "" {
		exists, err := a.db.TableExists(ctx, impact.Table)
		if err != nil {
			return nil, fmt.Errorf("failed to check table existence: %w", err)
		}
		if !exists {
			return nil, fmt.Errorf("table '%s' does not exist", impact.Table)
		}
	}

	// Count affected rows
	if impact.Table != "" {
		count, err := a.db.CountAffectedRows(ctx, impact.Table, stmt.WhereClause)
		if err != nil {
			return nil, fmt.Errorf("failed to count affected rows: %w", err)
		}
		impact.AffectedRows = count
	}

	// Get estimated rows from EXPLAIN
	if stmt.Type == parser.StatementUpdate || stmt.Type == parser.StatementDelete {
		// Build a SELECT query to explain
		selectQuery := fmt.Sprintf("SELECT * FROM %s", impact.Table)
		if stmt.WhereClause != "" {
			selectQuery += " WHERE " + stmt.WhereClause
		}

		estimated, err := a.db.GetEstimatedRows(ctx, selectQuery)
		if err == nil {
			impact.EstimatedRows = estimated
		}

		// Get explain plan
		plan, err := a.db.GetExplainPlan(ctx, selectQuery)
		if err == nil {
			impact.ExplainPlan = plan
		}
	}

	// Get rows preview for UPDATE/DELETE
	if (stmt.Type == parser.StatementUpdate || stmt.Type == parser.StatementDelete) && impact.Table != "" {
		preview, err := a.db.GetRowsSnapshot(ctx, impact.Table, stmt.WhereClause, a.previewLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to get rows preview: %w", err)
		}
		impact.RowsPreview = preview
		impact.RowsHash = computeRowsHash(preview)
	}

	return impact, nil
}

// computeRowsHash creates a deterministic hash of row data.
func computeRowsHash(rows []map[string]interface{}) string {
	if len(rows) == 0 {
		return ""
	}

	// Sort and serialize for deterministic hash
	data, _ := json.Marshal(rows)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", hash)
}

// VerifyStateUnchanged checks if the current state matches the expected hash.
func (a *Analyzer) VerifyStateUnchanged(ctx context.Context, table, whereClause, expectedHash string, limit int) (bool, string, error) {
	rows, err := a.db.GetRowsSnapshot(ctx, table, whereClause, limit)
	if err != nil {
		return false, "", err
	}

	currentHash := computeRowsHash(rows)
	return currentHash == expectedHash, currentHash, nil
}

// VerifyRowCount checks if the current row count matches the expected count.
func (a *Analyzer) VerifyRowCount(ctx context.Context, table, whereClause string, expectedCount int64) (bool, int64, error) {
	count, err := a.db.CountAffectedRows(ctx, table, whereClause)
	if err != nil {
		return false, 0, err
	}

	return count == expectedCount, count, nil
}
