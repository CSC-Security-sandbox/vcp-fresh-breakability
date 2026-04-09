// Package executor handles safe SQL execution with verification.
package executor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/analyzer"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

// VerificationResult contains the results of pre-execution verification.
type VerificationResult struct {
	Valid            bool
	PlanExpired      bool
	CommitMismatch   bool
	StateDrift       bool
	RowCountMismatch bool
	SignatureInvalid bool
	Errors           []string
	Details          map[string]interface{}
}

// ExecutionResult contains the results of execution.
type ExecutionResult struct {
	Success      bool
	RowsAffected []int64
	TotalRows    int64
	ExecutedAt   time.Time
	Timestamp    time.Time // Alias for ExecutedAt (for consistency)
	Duration     time.Duration
	RolledBack   bool
	Error        error
}

// Executor handles safe query execution.
type Executor struct {
	db           *database.Client
	analyzer     *analyzer.Analyzer
	githubClient *github.Client
	planBuilder  *planner.PlanBuilder
}

// New creates a new Executor.
func New(db *database.Client, gh *github.Client, pb *planner.PlanBuilder) *Executor {
	return &Executor{
		db:           db,
		analyzer:     analyzer.New(db),
		githubClient: gh,
		planBuilder:  pb,
	}
}

// VerifyPlan performs pre-execution verification of a plan.
func (e *Executor) VerifyPlan(ctx context.Context, plan *planner.Plan) (*VerificationResult, error) {
	result := &VerificationResult{
		Valid:   true,
		Details: make(map[string]interface{}),
	}

	// 1. Check plan expiry
	if time.Now().UTC().After(plan.ExpiresAt) {
		result.Valid = false
		result.PlanExpired = true
		result.Errors = append(result.Errors, fmt.Sprintf(
			"Plan expired at %s (current time: %s)",
			plan.ExpiresAt.Format(time.RFC3339),
			time.Now().UTC().Format(time.RFC3339),
		))
	}

	// 2. Verify plan signature
	if err := e.planBuilder.Verify(plan); err != nil {
		result.Valid = false
		result.SignatureInvalid = true
		result.Errors = append(result.Errors, fmt.Sprintf("Signature verification failed: %v", err))
	}

	// 3. Verify GitHub commit hasn't changed (if GitHub source)
	if plan.Source.Type == "github" && e.githubClient != nil {
		source := &github.FileSource{
			Owner:    extractOwner(plan.Source.Repository),
			Repo:     extractRepo(plan.Source.Repository),
			Branch:   plan.Source.Branch,
			FilePath: plan.Source.FilePath,
		}

		unchanged, currentSHA, err := e.githubClient.VerifyCommitUnchanged(ctx, source, plan.Source.CommitSHA)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("Failed to verify commit: %v", err))
		} else if !unchanged {
			result.Valid = false
			result.CommitMismatch = true
			result.Errors = append(result.Errors, fmt.Sprintf(
				"Commit SHA mismatch - Plan: %s, Current: %s",
				plan.Source.CommitSHA[:12],
				currentSHA[:12],
			))
			result.Details["expected_commit"] = plan.Source.CommitSHA
			result.Details["current_commit"] = currentSHA
		}
	}

	// 4. Verify state hasn't drifted
	for _, snapshot := range plan.Snapshots {
		// Check row count
		countMatch, currentCount, err := e.analyzer.VerifyRowCount(
			ctx,
			snapshot.Table,
			getWhereClause(plan, snapshot.StatementIndex),
			snapshot.RowCount,
		)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf(
				"Failed to verify row count for statement %d: %v",
				snapshot.StatementIndex+1, err,
			))
			continue
		}

		if !countMatch {
			result.Valid = false
			result.RowCountMismatch = true
			result.Errors = append(result.Errors, fmt.Sprintf(
				"Row count mismatch for statement %d (table: %s) - Plan: %d, Current: %d",
				snapshot.StatementIndex+1,
				snapshot.Table,
				snapshot.RowCount,
				currentCount,
			))
			result.Details[fmt.Sprintf("stmt_%d_expected_rows", snapshot.StatementIndex)] = snapshot.RowCount
			result.Details[fmt.Sprintf("stmt_%d_current_rows", snapshot.StatementIndex)] = currentCount
		}

		// Check state hash
		if snapshot.RowsHash != "" {
			stateMatch, currentHash, err := e.analyzer.VerifyStateUnchanged(
				ctx,
				snapshot.Table,
				getWhereClause(plan, snapshot.StatementIndex),
				snapshot.RowsHash,
				len(snapshot.RowsPreview),
			)
			if err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf(
					"Failed to verify state for statement %d: %v",
					snapshot.StatementIndex+1, err,
				))
				continue
			}

			if !stateMatch {
				result.Valid = false
				result.StateDrift = true
				result.Errors = append(result.Errors, fmt.Sprintf(
					"State drift detected for statement %d (table: %s) - data has changed since plan creation",
					snapshot.StatementIndex+1,
					snapshot.Table,
				))
				result.Details[fmt.Sprintf("stmt_%d_expected_hash", snapshot.StatementIndex)] = snapshot.RowsHash
				result.Details[fmt.Sprintf("stmt_%d_current_hash", snapshot.StatementIndex)] = currentHash
			}
		}
	}

	return result, nil
}

// Execute runs the queries in the plan after verification.
func (e *Executor) Execute(ctx context.Context, plan *planner.Plan, confirmFn func([]int64) bool) (*ExecutionResult, error) {
	startTime := time.Now()
	result := &ExecutionResult{
		ExecutedAt: startTime,
	}

	// Collect all queries
	var queries []string
	for _, stmt := range plan.Query.Statements {
		queries = append(queries, stmt.SQL)
	}

	// Convert plan verification queries to database client format
	var verificationQueries []database.VerificationQueryInfo
	for _, vq := range plan.VerificationQueries {
		verificationQueries = append(verificationQueries, database.VerificationQueryInfo{
			StatementIndex: vq.StatementIndex,
			Type:           vq.Type,
			SQL:            vq.SQL,
			ExpectedCount:  vq.ExpectedCount,
		})
	}

	// Execute with verification queries from plan
	rowsAffected, err := e.db.ExecuteWithVerification(ctx, queries, verificationQueries, confirmFn)
	result.Duration = time.Since(startTime)

	if err != nil {
		result.Error = err
		result.Success = false
		if rowsAffected != nil {
			result.RowsAffected = rowsAffected
			result.RolledBack = true
		}
		return result, err
	}

	result.Success = true
	result.RowsAffected = rowsAffected
	result.Timestamp = result.ExecutedAt
	for i, count := range rowsAffected {
		if i < len(plan.Query.Statements) && plan.Query.Statements[i].IsMutating() {
			result.TotalRows += count
		}
	}

	return result, nil
}

// ExecuteRollback executes rollback statements from a plan.
func (e *Executor) ExecuteRollback(ctx context.Context, plan *planner.Plan) (*ExecutionResult, error) {
	result := &ExecutionResult{
		ExecutedAt: time.Now().UTC(),
	}
	result.Timestamp = result.ExecutedAt

	startTime := time.Now()
	defer func() {
		result.Duration = time.Since(startTime)
	}()

	// Check if rollback is available
	if len(plan.Rollback) == 0 {
		result.Error = fmt.Errorf("no rollback statements available")
		return result, result.Error
	}

	// Extract and split rollback SQL statements
	// Each plan.Rollback entry may contain multiple statements joined by ";\n"
	var rollbackSQL []string
	for _, rb := range plan.Rollback {
		if rb.SQL == "" {
			continue
		}
		// Split by semicolon to get individual statements
		stmts := strings.Split(rb.SQL, ";")
		for _, stmt := range stmts {
			stmt = strings.TrimSpace(stmt)
			if stmt != "" {
				rollbackSQL = append(rollbackSQL, stmt)
			}
		}
	}

	if len(rollbackSQL) == 0 {
		result.Error = fmt.Errorf("no valid rollback statements found")
		return result, result.Error
	}

	// Execute rollback statements in transaction
	rowsAffected, err := e.db.ExecuteMultipleInTransaction(ctx, rollbackSQL, func(results []int64) bool {
		// Auto-confirm rollback (no user confirmation needed during rollback)
		return true
	})

	if err != nil {
		result.Error = err
		result.Success = false
		if rowsAffected != nil {
			result.RowsAffected = rowsAffected
			result.RolledBack = true
		}
		return result, err
	}

	result.Success = true
	result.RowsAffected = rowsAffected
	for _, count := range rowsAffected {
		result.TotalRows += count
	}

	return result, nil
}

// getWhereClause extracts the WHERE clause for a statement from the plan.
func getWhereClause(plan *planner.Plan, stmtIndex int) string {
	for _, impact := range plan.Impact.Statements {
		if impact.StatementIndex == stmtIndex {
			return impact.WhereClause
		}
	}
	return ""
}

// extractOwner extracts the owner from "owner/repo" format.
func extractOwner(repo string) string {
	for i, c := range repo {
		if c == '/' {
			return repo[:i]
		}
	}
	return repo
}

// extractRepo extracts the repo name from "owner/repo" format.
func extractRepo(repo string) string {
	for i, c := range repo {
		if c == '/' {
			return repo[i+1:]
		}
	}
	return repo
}
