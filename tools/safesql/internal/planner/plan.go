// Package planner handles execution plan generation and verification.
package planner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/analyzer"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
)

// Plan represents an execution plan.
type Plan struct {
	PlanID    string    `json:"plan_id"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Operator  string    `json:"operator"`
	Ticket    string    `json:"ticket"`

	Source              SourceInfo               `json:"source"`
	Query               QueryInfo                `json:"query"`
	Impact              *analyzer.AnalysisResult `json:"impact"`
	Snapshots           []StateSnapshot          `json:"snapshots"`
	Rollback            []RollbackInfo           `json:"rollback"`
	VerificationQueries []VerificationQuery      `json:"verification_queries"`
	Signature           string                   `json:"signature"`
}

// SourceInfo contains information about the query source.
type SourceInfo struct {
	Type       string             `json:"type"` // "github" or "local"
	Repository string             `json:"repository,omitempty"`
	Branch     string             `json:"branch,omitempty"`
	CommitSHA  string             `json:"commit_sha,omitempty"`
	FilePath   string             `json:"file_path"`
	FileHash   string             `json:"file_hash"`
	PRMetadata *github.PRMetadata `json:"pr_metadata,omitempty"`
}

// QueryInfo contains the SQL query details.
type QueryInfo struct {
	RawSQL     string               `json:"raw_sql"`
	Hash       string               `json:"hash"`
	Metadata   parser.QueryMetadata `json:"metadata"`
	Statements []StatementInfo      `json:"statements"`
}

// StatementInfo contains details about a single statement.
type StatementInfo struct {
	Index    int                  `json:"index"`
	SQL      string               `json:"sql"`
	Hash     string               `json:"hash"`
	Type     parser.StatementType `json:"type"`
	Table    string               `json:"table"`
	HasWhere bool                 `json:"has_where"`
}

// IsMutating returns true if this statement modifies data (INSERT/UPDATE/DELETE).
// SELECT and OTHER statements are not mutating and should not contribute to
// the affected-rows total used for plan verification.
func (s StatementInfo) IsMutating() bool {
	return s.Type == parser.StatementInsert ||
		s.Type == parser.StatementUpdate ||
		s.Type == parser.StatementDelete
}

// IsTransactionControl returns true if this statement controls transaction boundaries
// (BEGIN, COMMIT, ROLLBACK, etc.). SafeSQL manages its own transaction wrapper, so
// these statements are skipped during execution to prevent premature commit/rollback.
func (s StatementInfo) IsTransactionControl() bool {
	return s.Type == parser.StatementTransaction
}

// StateSnapshot captures the state before execution.
type StateSnapshot struct {
	StatementIndex int                      `json:"statement_index"`
	Table          string                   `json:"table"`
	RowCount       int64                    `json:"row_count"`
	RowsHash       string                   `json:"rows_hash"`
	CapturedAt     time.Time                `json:"captured_at"`
	RowsPreview    []map[string]interface{} `json:"rows_preview,omitempty"`
}

// RollbackInfo contains rollback SQL for a statement.
type RollbackInfo struct {
	StatementIndex int    `json:"statement_index"`
	SQL            string `json:"sql"`
	Hash           string `json:"hash"`
}

// VerificationQuery contains pre/post execution SELECT queries for verification.
type VerificationQuery struct {
	StatementIndex int    `json:"statement_index"` // Index of the statement being verified
	Type           string `json:"type"`            // "pre" or "post"
	SQL            string `json:"sql"`             // The SELECT COUNT(*) query
	ExpectedCount  int64  `json:"expected_count"`  // Expected count for pre-execution, or expected remaining for post
	Description    string `json:"description"`     // Human-readable description
}

// StorageBackend defines the interface for plan storage operations.
type StorageBackend interface {
	SavePlan(ctx context.Context, planID string, data []byte) error
	LoadPlan(ctx context.Context, planID string) ([]byte, error)
	DeletePlan(ctx context.Context, planID string) error
	ListPlans(ctx context.Context) ([]string, error)
}

// PlanBuilder builds execution plans.
type PlanBuilder struct {
	planExpiry time.Duration
	storage    StorageBackend
}

// NewPlanBuilder creates a new PlanBuilder with a storage backend.
func NewPlanBuilder(planExpiry time.Duration, storage StorageBackend) *PlanBuilder {
	return &PlanBuilder{
		planExpiry: planExpiry,
		storage:    storage,
	}
}

// Build creates a new execution plan.
func (b *PlanBuilder) Build(
	source SourceInfo,
	parseResult *parser.ParseResult,
	analysisResult *analyzer.AnalysisResult,
	rollbackSQL []string,
	operator, ticket string,
) (*Plan, error) {
	now := time.Now().UTC()
	planID := b.generatePlanIDWithMetadata(now, operator, ticket)

	plan := &Plan{
		PlanID:    planID,
		CreatedAt: now,
		ExpiresAt: now.Add(b.planExpiry),
		Operator:  operator,
		Ticket:    ticket,
		Source:    source,
		Impact:    analysisResult,
	}

	// Build query info
	plan.Query = QueryInfo{
		RawSQL:   parseResult.RawSQL,
		Hash:     parseResult.FileHash,
		Metadata: parseResult.Metadata,
	}

	for i, stmt := range parseResult.Statements {
		plan.Query.Statements = append(plan.Query.Statements, StatementInfo{
			Index:    i,
			SQL:      stmt.SQL,
			Hash:     stmt.Hash,
			Type:     stmt.Type,
			Table:    firstOrEmpty(stmt.Tables),
			HasWhere: stmt.HasWhereClause,
		})
	}

	// Build state snapshots
	for _, impact := range analysisResult.Statements {
		if impact.Type == parser.StatementUpdate || impact.Type == parser.StatementDelete {
			plan.Snapshots = append(plan.Snapshots, StateSnapshot{
				StatementIndex: impact.StatementIndex,
				Table:          impact.Table,
				RowCount:       impact.AffectedRows,
				RowsHash:       impact.RowsHash,
				CapturedAt:     now,
				RowsPreview:    impact.RowsPreview,
			})
		}
	}

	// Build rollback info
	for i, sql := range rollbackSQL {
		if sql != "" {
			plan.Rollback = append(plan.Rollback, RollbackInfo{
				StatementIndex: i,
				SQL:            sql,
				Hash:           computeHash(sql),
			})
		}
	}

	// Build verification queries for UPDATE/DELETE statements
	plan.VerificationQueries = b.buildVerificationQueries(plan.Query.Statements, analysisResult)

	// Sign the plan
	plan.Signature = b.signPlan(plan)

	return plan, nil
}

func (b *PlanBuilder) generatePlanID(t time.Time) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", t.UnixNano(), time.Now().UnixNano())))
	return fmt.Sprintf("plan-%s-%s", t.Format("20060102-150405"), fmt.Sprintf("%x", hash[:4]))
}

// generatePlanIDWithMetadata creates a plan ID with username and ticket
func (b *PlanBuilder) generatePlanIDWithMetadata(t time.Time, username, ticket string) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%d-%d", t.UnixNano(), time.Now().UnixNano())))
	// Sanitize username and ticket for filename safety
	safeUsername := sanitizeForFilename(username)
	safeTicket := sanitizeForFilename(ticket)
	return fmt.Sprintf("plan-%s-%s-%s-%s", t.Format("20060102-150405"), safeUsername, safeTicket, fmt.Sprintf("%x", hash[:4]))
}

// sanitizeForFilename removes characters that are unsafe for filenames
func sanitizeForFilename(s string) string {
	// Replace spaces, slashes, and other unsafe characters with underscores
	unsafe := []string{" ", "/", "\\", ":", "*", "?", "\"", "<", ">", "|", "@", "#", "$", "%", "^", "&"}
	result := s
	for _, char := range unsafe {
		result = strings.ReplaceAll(result, char, "_")
	}
	// Limit length to avoid overly long filenames
	if len(result) > 20 {
		result = result[:20]
	}
	return result
}

func (b *PlanBuilder) signPlan(plan *Plan) string {
	// Create a deterministic signature from plan contents
	data := fmt.Sprintf("%s:%s:%s:%s:%d",
		plan.PlanID,
		plan.Source.CommitSHA,
		plan.Query.Hash,
		plan.Operator,
		plan.Impact.TotalRows,
	)

	for _, snap := range plan.Snapshots {
		data += fmt.Sprintf(":%s:%d:%s", snap.Table, snap.RowCount, snap.RowsHash)
	}

	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("sha256:%x", hash)
}

// Save persists the plan to storage.
func (b *PlanBuilder) Save(ctx context.Context, plan *Plan) error {
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	if err := b.storage.SavePlan(ctx, plan.PlanID, data); err != nil {
		return fmt.Errorf("failed to save plan: %w", err)
	}

	return nil
}

// Load reads a plan from storage.
// Accepts a plan ID (e.g., "plan-20240115-143022-abc123").
func (b *PlanBuilder) Load(ctx context.Context, planID string) (*Plan, error) {
	// Strip .json suffix if present
	planID = strings.TrimSuffix(planID, ".json")

	data, err := b.storage.LoadPlan(ctx, planID)
	if err != nil {
		return nil, fmt.Errorf("failed to load plan '%s': %w", planID, err)
	}

	var plan Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	return &plan, nil
}

// Verify checks if a plan is valid and hasn't expired.
func (b *PlanBuilder) Verify(plan *Plan) error {
	// Check expiry
	if time.Now().UTC().After(plan.ExpiresAt) {
		return fmt.Errorf("plan expired at %s", plan.ExpiresAt.Format(time.RFC3339))
	}

	// Verify signature
	expectedSig := b.signPlan(plan)
	if plan.Signature != expectedSig {
		return fmt.Errorf("plan signature mismatch - plan may have been tampered with")
	}

	return nil
}

// ListPlans returns all stored plans.
func (b *PlanBuilder) ListPlans(ctx context.Context) ([]*Plan, error) {
	planIDs, err := b.storage.ListPlans(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list plans: %w", err)
	}

	var plans []*Plan
	for _, planID := range planIDs {
		plan, err := b.Load(ctx, planID)
		if err != nil {
			continue // Skip invalid plans
		}
		plans = append(plans, plan)
	}

	return plans, nil
}

// DeletePlan removes a plan from storage.
func (b *PlanBuilder) DeletePlan(ctx context.Context, planID string) error {
	return b.storage.DeletePlan(ctx, planID)
}

func computeHash(content string) string {
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", hash)
}

func firstOrEmpty(slice []string) string {
	if len(slice) > 0 {
		return slice[0]
	}
	return ""
}

// buildVerificationQueries generates pre/post execution SELECT queries for UPDATE/DELETE statements.
func (b *PlanBuilder) buildVerificationQueries(statements []StatementInfo, impact *analyzer.AnalysisResult) []VerificationQuery {
	var verificationQueries []VerificationQuery

	for i, stmt := range statements {
		// Only generate verification queries for UPDATE and DELETE
		if stmt.Type != parser.StatementUpdate && stmt.Type != parser.StatementDelete {
			continue
		}

		if stmt.Table == "" {
			continue
		}

		// Find the WHERE clause and expected count from impact analysis
		var whereClause string
		var expectedCount int64
		for _, imp := range impact.Statements {
			if imp.StatementIndex == i {
				whereClause = imp.WhereClause
				expectedCount = imp.AffectedRows
				break
			}
		}

		// Build SELECT COUNT(*) query
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", stmt.Table)
		if whereClause != "" {
			countQuery += " WHERE " + whereClause
		}

		// Pre-execution verification query
		preQuery := VerificationQuery{
			StatementIndex: i,
			Type:           "pre",
			SQL:            countQuery,
			ExpectedCount:  expectedCount,
			Description:    fmt.Sprintf("Verify row count before %s on %s", stmt.Type, stmt.Table),
		}
		verificationQueries = append(verificationQueries, preQuery)

		// Post-execution verification query
		// For UPDATE: Skip post-verification count check because:
		//   - The WHERE clause may reference columns that were updated, making the count unreliable
		//   - Correctness is already verified via rowsAffected matching the pre-execution count
		// For DELETE: Verify that count is 0 (all matching rows deleted)
		if stmt.Type == parser.StatementDelete {
			postQuery := VerificationQuery{
				StatementIndex: i,
				Type:           "post",
				SQL:            countQuery,
				ExpectedCount:  0, // Should be 0 after DELETE (all matching rows deleted)
				Description:    fmt.Sprintf("Verify rows deleted from %s (should be 0 remaining)", stmt.Table),
			}
			verificationQueries = append(verificationQueries, postQuery)
		}
	}

	return verificationQueries
}
