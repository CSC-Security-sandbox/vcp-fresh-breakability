// Package audit provides audit logging for SafeSQL operations.
package audit

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/executor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

// EntryType represents the type of audit entry.
type EntryType string

const (
	EntryTypePlan     EntryType = "PLAN"
	EntryTypeApply    EntryType = "APPLY"
	EntryTypeRollback EntryType = "ROLLBACK"
	EntryTypeAbort    EntryType = "ABORT"
)

// Entry represents a single audit log entry.
type Entry struct {
	AuditID      string             `json:"audit_id"`
	Type         EntryType          `json:"type"`
	Timestamp    time.Time          `json:"timestamp"`
	Operator     string             `json:"operator"`
	Ticket       string             `json:"ticket"`
	PlanID       string             `json:"plan_id"`
	Source       planner.SourceInfo `json:"source"`
	Statements   []StatementAudit   `json:"statements"`
	Result       *ExecutionAudit    `json:"result,omitempty"`
	Verification *VerificationAudit `json:"verification,omitempty"`
	RollbackSQL  []string           `json:"rollback_sql,omitempty"`
}

// StatementAudit contains audit info for a single statement.
type StatementAudit struct {
	Index        int                      `json:"index"`
	SQL          string                   `json:"sql"`
	Table        string                   `json:"table"`
	PreState     []map[string]interface{} `json:"pre_state,omitempty"`
	RowsAffected int64                    `json:"rows_affected,omitempty"`
}

// ExecutionAudit contains execution result info.
type ExecutionAudit struct {
	Success      bool          `json:"success"`
	TotalRows    int64         `json:"total_rows"`
	Duration     time.Duration `json:"duration"`
	RolledBack   bool          `json:"rolled_back"`
	ErrorMessage string        `json:"error_message,omitempty"`
}

// VerificationAudit contains verification result info.
type VerificationAudit struct {
	Valid  bool     `json:"valid"`
	Errors []string `json:"errors,omitempty"`
}

// Logger handles audit log operations.
type Logger struct {
	auditPath string
}

// NewLogger creates a new audit Logger.
func NewLogger(auditPath string) *Logger {
	return &Logger{auditPath: auditPath}
}

// LogPlan logs a plan creation.
func (l *Logger) LogPlan(plan *planner.Plan) (*Entry, error) {
	entry := &Entry{
		AuditID:   l.generateAuditID("plan"),
		Type:      EntryTypePlan,
		Timestamp: time.Now().UTC(),
		Operator:  plan.Operator,
		Ticket:    plan.Ticket,
		PlanID:    plan.PlanID,
		Source:    plan.Source,
	}

	// Add statement details
	for _, stmt := range plan.Query.Statements {
		sa := StatementAudit{
			Index: stmt.Index,
			SQL:   stmt.SQL,
			Table: stmt.Table,
		}

		// Add pre-state from snapshots
		for _, snap := range plan.Snapshots {
			if snap.StatementIndex == stmt.Index {
				sa.PreState = snap.RowsPreview
				break
			}
		}

		entry.Statements = append(entry.Statements, sa)
	}

	// Add rollback SQL
	for _, rb := range plan.Rollback {
		entry.RollbackSQL = append(entry.RollbackSQL, rb.SQL)
	}

	return entry, l.save(entry)
}

// LogApply logs an execution attempt.
func (l *Logger) LogApply(plan *planner.Plan, verification *executor.VerificationResult, execResult *executor.ExecutionResult) (*Entry, error) {
	entry := &Entry{
		AuditID:   l.generateAuditID("exec"),
		Type:      EntryTypeApply,
		Timestamp: time.Now().UTC(),
		Operator:  plan.Operator,
		Ticket:    plan.Ticket,
		PlanID:    plan.PlanID,
		Source:    plan.Source,
	}

	// Add verification result
	if verification != nil {
		entry.Verification = &VerificationAudit{
			Valid:  verification.Valid,
			Errors: verification.Errors,
		}
	}

	// Add statement details with results
	for i, stmt := range plan.Query.Statements {
		sa := StatementAudit{
			Index: stmt.Index,
			SQL:   stmt.SQL,
			Table: stmt.Table,
		}

		// Add pre-state from snapshots
		for _, snap := range plan.Snapshots {
			if snap.StatementIndex == stmt.Index {
				sa.PreState = snap.RowsPreview
				break
			}
		}

		// Add rows affected if available
		if execResult != nil && i < len(execResult.RowsAffected) {
			sa.RowsAffected = execResult.RowsAffected[i]
		}

		entry.Statements = append(entry.Statements, sa)
	}

	// Add execution result
	if execResult != nil {
		entry.Result = &ExecutionAudit{
			Success:    execResult.Success,
			TotalRows:  execResult.TotalRows,
			Duration:   execResult.Duration,
			RolledBack: execResult.RolledBack,
		}
		if execResult.Error != nil {
			entry.Result.ErrorMessage = execResult.Error.Error()
		}
	}

	// Add rollback SQL
	for _, rb := range plan.Rollback {
		entry.RollbackSQL = append(entry.RollbackSQL, rb.SQL)
	}

	return entry, l.save(entry)
}

// LogRollback logs a rollback execution.
func (l *Logger) LogRollback(originalAuditID string, plan *planner.Plan, result *executor.ExecutionResult) (*Entry, error) {
	entry := &Entry{
		AuditID:   l.generateAuditID("rollback"),
		Type:      EntryTypeRollback,
		Timestamp: time.Now().UTC(),
		Operator:  plan.Operator,
		PlanID:    plan.PlanID,
		Source:    plan.Source,
	}

	// Add rollback statements
	for i, rb := range plan.Rollback {
		sa := StatementAudit{
			Index: i,
			SQL:   rb.SQL,
		}
		if result != nil && i < len(result.RowsAffected) {
			sa.RowsAffected = result.RowsAffected[i]
		}
		entry.Statements = append(entry.Statements, sa)
	}

	// Add execution result
	if result != nil {
		entry.Result = &ExecutionAudit{
			Success:    result.Success,
			TotalRows:  result.TotalRows,
			Duration:   result.Duration,
			RolledBack: result.RolledBack,
		}
		if result.Error != nil {
			entry.Result.ErrorMessage = result.Error.Error()
		}
	}

	return entry, l.save(entry)
}

// LogAbort logs an aborted execution.
func (l *Logger) LogAbort(plan *planner.Plan, reason string) (*Entry, error) {
	entry := &Entry{
		AuditID:   l.generateAuditID("abort"),
		Type:      EntryTypeAbort,
		Timestamp: time.Now().UTC(),
		Operator:  plan.Operator,
		Ticket:    plan.Ticket,
		PlanID:    plan.PlanID,
		Source:    plan.Source,
		Result: &ExecutionAudit{
			Success:      false,
			ErrorMessage: reason,
		},
	}

	return entry, l.save(entry)
}

func (l *Logger) generateAuditID(prefix string) string {
	return fmt.Sprintf("%s-%s", prefix, time.Now().UTC().Format("20060102-150405"))
}

func (l *Logger) save(entry *Entry) error {
	// Ensure directory exists
	if err := os.MkdirAll(l.auditPath, 0755); err != nil {
		return fmt.Errorf("failed to create audit directory: %w", err)
	}

	// Write audit file
	filename := filepath.Join(l.auditPath, entry.AuditID+".json")
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal audit entry: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write audit file: %w", err)
	}

	return nil
}

// Get retrieves an audit entry by ID.
func (l *Logger) Get(auditID string) (*Entry, error) {
	filename := filepath.Join(l.auditPath, auditID+".json")
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read audit file: %w", err)
	}

	var entry Entry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshal audit entry: %w", err)
	}

	return &entry, nil
}

// List returns recent audit entries.
func (l *Logger) List(limit int) ([]*Entry, error) {
	files, err := filepath.Glob(filepath.Join(l.auditPath, "*.json"))
	if err != nil {
		return nil, err
	}

	// Sort by modification time (newest first)
	// Use zero time as fallback for files that can't be stat'd (they'll sort to the end)
	sort.Slice(files, func(i, j int) bool {
		fi, err := os.Stat(files[i])
		var ti time.Time
		if err == nil && fi != nil {
			ti = fi.ModTime()
		}

		fj, err := os.Stat(files[j])
		var tj time.Time
		if err == nil && fj != nil {
			tj = fj.ModTime()
		}

		return ti.After(tj)
	})

	if limit > 0 && len(files) > limit {
		files = files[:limit]
	}

	var entries []*Entry
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var entry Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		entries = append(entries, &entry)
	}

	return entries, nil
}

// ListByDate returns audit entries for a specific date.
func (l *Logger) ListByDate(date time.Time) ([]*Entry, error) {
	dateStr := date.Format("20060102")
	pattern := filepath.Join(l.auditPath, fmt.Sprintf("*-%s-*.json", dateStr))

	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var entries []*Entry
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var entry Entry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue
		}
		entries = append(entries, &entry)
	}

	return entries, nil
}
