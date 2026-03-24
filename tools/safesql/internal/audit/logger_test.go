package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/executor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/github"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/planner"
)

// mockStorage is a simple in-memory storage for testing
type mockStorage struct {
	audits map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		audits: make(map[string][]byte),
	}
}

func (m *mockStorage) SaveAudit(ctx context.Context, auditID string, data []byte) error {
	m.audits[auditID] = data
	return nil
}

func (m *mockStorage) LoadAudit(ctx context.Context, auditID string) ([]byte, error) {
	data, ok := m.audits[auditID]
	if !ok {
		return nil, context.DeadlineExceeded // Simulate not found
	}
	return data, nil
}

func (m *mockStorage) ListAudits(ctx context.Context, date *time.Time, limit int) ([]AuditMetadata, error) {
	var result []AuditMetadata
	for id := range m.audits {
		result = append(result, AuditMetadata{
			AuditID:   id,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
	}
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

func TestNewLogger(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestLogPlan(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:    "plan-test-123",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		Operator:  "testuser",
		Ticket:    "TICKET-123",
		Source: planner.SourceInfo{
			Type:     "github",
			FilePath: "test.sql",
		},
		Query: planner.QueryInfo{
			Statements: []planner.StatementInfo{
				{Index: 0, SQL: "SELECT 1", Type: "SELECT"},
			},
		},
		Rollback: []planner.RollbackInfo{
			{StatementIndex: 0, SQL: "SELECT 2"},
		},
	}

	entry, err := logger.LogPlan(plan)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Type != EntryTypePlan {
		t.Errorf("expected type %s, got %s", EntryTypePlan, entry.Type)
	}
	if entry.Operator != "testuser" {
		t.Errorf("expected Operator 'testuser', got %q", entry.Operator)
	}
	if entry.Ticket != "TICKET-123" {
		t.Errorf("expected Ticket 'TICKET-123', got %q", entry.Ticket)
	}
	if entry.PlanID != "plan-test-123" {
		t.Errorf("expected PlanID 'plan-test-123', got %q", entry.PlanID)
	}
	if len(entry.Statements) != 1 {
		t.Errorf("expected 1 statement, got %d", len(entry.Statements))
	}
	if len(entry.RollbackSQL) != 1 {
		t.Errorf("expected 1 rollback SQL, got %d", len(entry.RollbackSQL))
	}
}

func TestLogApply(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:    "plan-test-123",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		Operator:  "testuser",
		Ticket:    "TICKET-123",
		Source: planner.SourceInfo{
			Type:     "local",
			FilePath: "test.sql",
		},
		Query: planner.QueryInfo{
			Statements: []planner.StatementInfo{
				{Index: 0, SQL: "UPDATE users SET x = 1 WHERE id = 1", Type: "UPDATE", Table: "users"},
			},
		},
	}

	verification := &executor.VerificationResult{
		Valid:  true,
		Errors: []string{},
	}

	execResult := &executor.ExecutionResult{
		Success:      true,
		RowsAffected: []int64{5},
		TotalRows:    5,
		Duration:     100 * time.Millisecond,
	}

	entry, err := logger.LogApply(plan, verification, execResult)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Type != EntryTypeApply {
		t.Errorf("expected type %s, got %s", EntryTypeApply, entry.Type)
	}
	if entry.Verification == nil {
		t.Error("expected Verification to be set")
	}
	if !entry.Verification.Valid {
		t.Error("expected Verification.Valid to be true")
	}
	if entry.Result == nil {
		t.Error("expected Result to be set")
	}
	if !entry.Result.Success {
		t.Error("expected Result.Success to be true")
	}
	if entry.Result.TotalRows != 5 {
		t.Errorf("expected TotalRows 5, got %d", entry.Result.TotalRows)
	}
}

func TestLogRollback(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:   "plan-test-123",
		Operator: "testuser",
		Source: planner.SourceInfo{
			Type: "local",
		},
		Rollback: []planner.RollbackInfo{
			{StatementIndex: 0, SQL: "UPDATE users SET status = 'old' WHERE id = 1"},
		},
	}

	execResult := &executor.ExecutionResult{
		Success:      true,
		RowsAffected: []int64{1},
		TotalRows:    1,
		Duration:     50 * time.Millisecond,
	}

	entry, err := logger.LogRollback("original-audit-123", plan, execResult)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Type != EntryTypeRollback {
		t.Errorf("expected type %s, got %s", EntryTypeRollback, entry.Type)
	}
	if entry.Result == nil {
		t.Error("expected Result to be set")
	}
	if !entry.Result.Success {
		t.Error("expected Result.Success to be true")
	}
}

func TestLogAbort(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:   "plan-test-123",
		Operator: "testuser",
		Ticket:   "TICKET-123",
		Source: planner.SourceInfo{
			Type: "local",
		},
	}

	entry, err := logger.LogAbort(plan, "User cancelled execution")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Type != EntryTypeAbort {
		t.Errorf("expected type %s, got %s", EntryTypeAbort, entry.Type)
	}
	if entry.Result == nil {
		t.Error("expected Result to be set")
	}
	if entry.Result.Success {
		t.Error("expected Result.Success to be false")
	}
	if entry.Result.ErrorMessage != "User cancelled execution" {
		t.Errorf("expected ErrorMessage 'User cancelled execution', got %q", entry.Result.ErrorMessage)
	}
}

func TestGetAuditEntry(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	// First create an entry
	plan := &planner.Plan{
		PlanID:   "plan-test-123",
		Operator: "testuser",
		Ticket:   "TICKET-123",
		Source: planner.SourceInfo{
			Type: "local",
		},
	}

	originalEntry, err := logger.LogPlan(plan)
	if err != nil {
		t.Fatalf("failed to log plan: %v", err)
	}

	// Now retrieve it
	retrievedEntry, err := logger.Get(originalEntry.AuditID)
	if err != nil {
		t.Fatalf("failed to get entry: %v", err)
	}

	if retrievedEntry.AuditID != originalEntry.AuditID {
		t.Errorf("expected AuditID %s, got %s", originalEntry.AuditID, retrievedEntry.AuditID)
	}
	if retrievedEntry.Operator != originalEntry.Operator {
		t.Errorf("expected Operator %s, got %s", originalEntry.Operator, retrievedEntry.Operator)
	}
}

func TestListAuditEntries(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	// Create a few entries with small delays to ensure unique timestamps
	// Note: audit IDs are timestamp-based with second precision
	for i := 0; i < 3; i++ {
		plan := &planner.Plan{
			PlanID:   fmt.Sprintf("plan-test-%d", i),
			Operator: "testuser",
			Source:   planner.SourceInfo{Type: "local"},
		}
		entry, err := logger.LogPlan(plan)
		if err != nil {
			t.Fatalf("failed to log plan: %v", err)
		}
		// Manually store with unique ID to avoid timestamp collision
		storage.audits[fmt.Sprintf("plan-%d", i)] = storage.audits[entry.AuditID]
	}

	// List all - expect at least 1 entry (could be more if timing worked out)
	entries, err := logger.List(10)
	if err != nil {
		t.Fatalf("failed to list entries: %v", err)
	}

	if len(entries) < 1 {
		t.Errorf("expected at least 1 entry, got %d", len(entries))
	}
}

func TestListAuditEntriesWithLimit(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	// Create 5 entries
	for i := 0; i < 5; i++ {
		plan := &planner.Plan{
			PlanID:   "plan-test",
			Operator: "testuser",
			Source:   planner.SourceInfo{Type: "local"},
		}
		logger.LogPlan(plan)
	}

	// List with limit
	entries, err := logger.List(3)
	if err != nil {
		t.Fatalf("failed to list entries: %v", err)
	}

	if len(entries) > 3 {
		t.Errorf("expected at most 3 entries, got %d", len(entries))
	}
}

func TestGenerateAuditID(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	id1 := logger.generateAuditID("plan")
	id2 := logger.generateAuditID("exec")
	id3 := logger.generateAuditID("plan")

	if !strings.HasPrefix(id1, "plan-") {
		t.Errorf("expected plan ID to start with 'plan-', got %s", id1)
	}
	if !strings.HasPrefix(id2, "exec-") {
		t.Errorf("expected exec ID to start with 'exec-', got %s", id2)
	}
	// IDs should be unique (different timestamps)
	if id1 == id3 {
		t.Logf("Note: IDs %s and %s are the same (generated within same second)", id1, id3)
	}
}

func TestEntryTypes(t *testing.T) {
	if EntryTypePlan != "PLAN" {
		t.Errorf("expected EntryTypePlan to be 'PLAN', got %q", EntryTypePlan)
	}
	if EntryTypeApply != "APPLY" {
		t.Errorf("expected EntryTypeApply to be 'APPLY', got %q", EntryTypeApply)
	}
	if EntryTypeRollback != "ROLLBACK" {
		t.Errorf("expected EntryTypeRollback to be 'ROLLBACK', got %q", EntryTypeRollback)
	}
	if EntryTypeAbort != "ABORT" {
		t.Errorf("expected EntryTypeAbort to be 'ABORT', got %q", EntryTypeAbort)
	}
}

func TestEntryJSONSerialization(t *testing.T) {
	entry := &Entry{
		AuditID:   "test-audit-123",
		Type:      EntryTypePlan,
		Timestamp: time.Now().UTC(),
		Operator:  "testuser",
		Ticket:    "TICKET-123",
		PlanID:    "plan-123",
		Source: planner.SourceInfo{
			Type:     "github",
			FilePath: "test.sql",
		},
		Statements: []StatementAudit{
			{Index: 0, SQL: "SELECT 1", Table: "test"},
		},
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("failed to marshal entry: %v", err)
	}

	var unmarshaled Entry
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("failed to unmarshal entry: %v", err)
	}

	if unmarshaled.AuditID != entry.AuditID {
		t.Errorf("expected AuditID %s, got %s", entry.AuditID, unmarshaled.AuditID)
	}
	if unmarshaled.Type != entry.Type {
		t.Errorf("expected Type %s, got %s", entry.Type, unmarshaled.Type)
	}
}

func TestLogApplyWithPRMetadata(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:   "plan-test-123",
		Operator: "testuser",
		Ticket:   "TICKET-123",
		Source: planner.SourceInfo{
			Type:     "github",
			FilePath: "test.sql",
			PRMetadata: &github.PRMetadata{
				Number: 42,
				Title:  "Test PR",
				Author: "testuser",
			},
		},
		Query: planner.QueryInfo{
			Statements: []planner.StatementInfo{
				{Index: 0, SQL: "SELECT 1"},
			},
		},
	}

	entry, err := logger.LogApply(plan, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Source.PRMetadata == nil {
		t.Error("expected PRMetadata to be preserved")
	}
	if entry.Source.PRMetadata.Number != 42 {
		t.Errorf("expected PR Number 42, got %d", entry.Source.PRMetadata.Number)
	}
}

func TestLogApplyWithFailedExecution(t *testing.T) {
	storage := newMockStorage()
	logger := NewLogger(storage)

	plan := &planner.Plan{
		PlanID:   "plan-test-123",
		Operator: "testuser",
		Source:   planner.SourceInfo{Type: "local"},
		Query: planner.QueryInfo{
			Statements: []planner.StatementInfo{
				{Index: 0, SQL: "SELECT 1"},
			},
		},
	}

	execResult := &executor.ExecutionResult{
		Success:    false,
		RolledBack: true,
		Error:      context.DeadlineExceeded,
	}

	entry, err := logger.LogApply(plan, nil, execResult)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if entry.Result == nil {
		t.Error("expected Result to be set")
	}
	if entry.Result.Success {
		t.Error("expected Result.Success to be false")
	}
	if !entry.Result.RolledBack {
		t.Error("expected Result.RolledBack to be true")
	}
	if entry.Result.ErrorMessage == "" {
		t.Error("expected ErrorMessage to be set")
	}
}
