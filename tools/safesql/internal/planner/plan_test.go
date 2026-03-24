package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/analyzer"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
)

// mockStorage is a simple in-memory storage for testing
type mockStorage struct {
	plans map[string][]byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		plans: make(map[string][]byte),
	}
}

func (m *mockStorage) SavePlan(ctx context.Context, planID string, data []byte) error {
	m.plans[planID] = data
	return nil
}

func (m *mockStorage) LoadPlan(ctx context.Context, planID string) ([]byte, error) {
	data, ok := m.plans[planID]
	if !ok {
		return nil, fmt.Errorf("plan not found: %s", planID)
	}
	return data, nil
}

func (m *mockStorage) DeletePlan(ctx context.Context, planID string) error {
	delete(m.plans, planID)
	return nil
}

func (m *mockStorage) ListPlans(ctx context.Context) ([]string, error) {
	var plans []string
	for id := range m.plans {
		plans = append(plans, id)
	}
	return plans, nil
}

func TestNewPlanBuilder(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	if pb == nil {
		t.Fatal("expected non-nil PlanBuilder")
	}
	if pb.planExpiry != time.Hour {
		t.Errorf("expected planExpiry to be 1 hour, got %v", pb.planExpiry)
	}
}

func TestPlanBuilderBuild(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	source := SourceInfo{
		Type:      "github",
		FilePath:  "test.sql",
		FileHash:  "sha256:abc123",
		CommitSHA: "def456",
	}

	parseResult := &parser.ParseResult{
		RawSQL:   "UPDATE users SET status = 'active' WHERE id = 1",
		FileHash: "sha256:abc123",
		Statements: []parser.Statement{
			{
				SQL:            "UPDATE users SET status = 'active' WHERE id = 1",
				Type:           parser.StatementUpdate,
				Tables:         []string{"users"},
				WhereClause:    "id = 1",
				HasWhereClause: true,
				Hash:           "sha256:stmt1",
			},
		},
	}

	analysisResult := &analyzer.AnalysisResult{
		TotalRows:    1,
		TablesCount:  1,
		UniqueTables: []string{"users"},
		Statements: []analyzer.StatementImpact{
			{
				StatementIndex: 0,
				Type:           parser.StatementUpdate,
				Table:          "users",
				AffectedRows:   1,
				WhereClause:    "id = 1",
				RowsPreview: []map[string]interface{}{
					{"id": 1, "status": "inactive"},
				},
				RowsHash: "sha256:rows1",
			},
		},
	}

	rollbackSQL := []string{"UPDATE users SET status = 'inactive' WHERE id = 1"}

	plan, err := pb.Build(source, parseResult, analysisResult, rollbackSQL, "testuser", "TICKET-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if plan.PlanID == "" {
		t.Error("expected non-empty PlanID")
	}
	if !strings.Contains(plan.PlanID, "testuser") {
		t.Errorf("expected PlanID to contain username, got %s", plan.PlanID)
	}
	if !strings.Contains(plan.PlanID, "TICKET-123") {
		t.Errorf("expected PlanID to contain ticket, got %s", plan.PlanID)
	}
	if plan.Operator != "testuser" {
		t.Errorf("expected Operator to be 'testuser', got %s", plan.Operator)
	}
	if plan.Ticket != "TICKET-123" {
		t.Errorf("expected Ticket to be 'TICKET-123', got %s", plan.Ticket)
	}
	if plan.CreatedAt.IsZero() {
		t.Error("expected CreatedAt to be set")
	}
	if plan.ExpiresAt.Before(plan.CreatedAt) {
		t.Error("expected ExpiresAt to be after CreatedAt")
	}
	if len(plan.Query.Statements) != 1 {
		t.Errorf("expected 1 statement, got %d", len(plan.Query.Statements))
	}
	if len(plan.Snapshots) != 1 {
		t.Errorf("expected 1 snapshot, got %d", len(plan.Snapshots))
	}
	if len(plan.Rollback) != 1 {
		t.Errorf("expected 1 rollback, got %d", len(plan.Rollback))
	}
	if plan.Signature == "" {
		t.Error("expected non-empty Signature")
	}
}

func TestPlanBuilderSaveAndLoad(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	source := SourceInfo{
		Type:     "local",
		FilePath: "test.sql",
		FileHash: "sha256:abc",
	}

	parseResult := &parser.ParseResult{
		RawSQL:   "SELECT 1",
		FileHash: "sha256:abc",
		Statements: []parser.Statement{
			{
				SQL:  "SELECT 1",
				Type: parser.StatementSelect,
				Hash: "sha256:stmt1",
			},
		},
	}

	analysisResult := &analyzer.AnalysisResult{
		TotalRows:    0,
		UniqueTables: []string{},
	}

	plan, err := pb.Build(source, parseResult, analysisResult, nil, "user", "TICKET-1")
	if err != nil {
		t.Fatalf("failed to build plan: %v", err)
	}

	ctx := context.Background()

	// Save the plan
	if err := pb.Save(ctx, plan); err != nil {
		t.Fatalf("failed to save plan: %v", err)
	}

	// Load the plan
	loadedPlan, err := pb.Load(ctx, plan.PlanID)
	if err != nil {
		t.Fatalf("failed to load plan: %v", err)
	}

	if loadedPlan.PlanID != plan.PlanID {
		t.Errorf("expected PlanID %s, got %s", plan.PlanID, loadedPlan.PlanID)
	}
	if loadedPlan.Operator != plan.Operator {
		t.Errorf("expected Operator %s, got %s", plan.Operator, loadedPlan.Operator)
	}
	if loadedPlan.Signature != plan.Signature {
		t.Errorf("expected Signature %s, got %s", plan.Signature, loadedPlan.Signature)
	}
}

func TestPlanBuilderLoadWithJsonSuffix(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	// Create a plan directly in storage
	plan := &Plan{
		PlanID:    "test-plan-123",
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
		Operator:  "user",
	}
	data, _ := json.Marshal(plan)
	storage.plans["test-plan-123"] = data

	ctx := context.Background()

	// Load with .json suffix (should be stripped)
	loadedPlan, err := pb.Load(ctx, "test-plan-123.json")
	if err != nil {
		t.Fatalf("failed to load plan: %v", err)
	}

	if loadedPlan.PlanID != "test-plan-123" {
		t.Errorf("expected PlanID 'test-plan-123', got %s", loadedPlan.PlanID)
	}
}

func TestPlanBuilderLoadNotFound(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	ctx := context.Background()
	_, err := pb.Load(ctx, "nonexistent-plan")

	if err == nil {
		t.Error("expected error for nonexistent plan")
	}
}

func TestPlanBuilderVerify(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	source := SourceInfo{
		Type:     "local",
		FilePath: "test.sql",
		FileHash: "sha256:abc",
	}

	parseResult := &parser.ParseResult{
		RawSQL:   "SELECT 1",
		FileHash: "sha256:abc",
		Statements: []parser.Statement{
			{
				SQL:  "SELECT 1",
				Type: parser.StatementSelect,
				Hash: "sha256:stmt1",
			},
		},
	}

	analysisResult := &analyzer.AnalysisResult{}

	plan, err := pb.Build(source, parseResult, analysisResult, nil, "user", "TICKET-1")
	if err != nil {
		t.Fatalf("failed to build plan: %v", err)
	}

	// Valid plan should verify successfully
	if err := pb.Verify(plan); err != nil {
		t.Errorf("expected valid plan to verify, got error: %v", err)
	}
}

func TestPlanBuilderVerifyExpired(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	plan := &Plan{
		PlanID:    "expired-plan",
		CreatedAt: time.Now().UTC().Add(-2 * time.Hour),
		ExpiresAt: time.Now().UTC().Add(-1 * time.Hour), // Expired
		Signature: "sha256:test",
	}

	err := pb.Verify(plan)
	if err == nil {
		t.Error("expected error for expired plan")
	}
	if !strings.Contains(err.Error(), "expired") {
		t.Errorf("expected error to mention 'expired', got: %v", err)
	}
}

func TestPlanBuilderVerifyTamperedSignature(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	source := SourceInfo{
		Type:     "local",
		FilePath: "test.sql",
		FileHash: "sha256:abc",
	}

	parseResult := &parser.ParseResult{
		RawSQL:   "SELECT 1",
		FileHash: "sha256:abc",
		Statements: []parser.Statement{
			{
				SQL:  "SELECT 1",
				Type: parser.StatementSelect,
				Hash: "sha256:stmt1",
			},
		},
	}

	analysisResult := &analyzer.AnalysisResult{}

	plan, err := pb.Build(source, parseResult, analysisResult, nil, "user", "TICKET-1")
	if err != nil {
		t.Fatalf("failed to build plan: %v", err)
	}

	// Tamper with signature
	plan.Signature = "sha256:tampered"

	err = pb.Verify(plan)
	if err == nil {
		t.Error("expected error for tampered signature")
	}
	if !strings.Contains(err.Error(), "signature mismatch") {
		t.Errorf("expected error to mention 'signature mismatch', got: %v", err)
	}
}

func TestPlanBuilderListPlans(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	ctx := context.Background()

	// Create and save a few plans
	for i := 0; i < 3; i++ {
		source := SourceInfo{
			Type:     "local",
			FilePath: fmt.Sprintf("test%d.sql", i),
			FileHash: fmt.Sprintf("sha256:abc%d", i),
		}

		parseResult := &parser.ParseResult{
			RawSQL:   "SELECT 1",
			FileHash: fmt.Sprintf("sha256:abc%d", i),
			Statements: []parser.Statement{
				{SQL: "SELECT 1", Type: parser.StatementSelect, Hash: "sha256:stmt1"},
			},
		}

		plan, _ := pb.Build(source, parseResult, &analyzer.AnalysisResult{}, nil, "user", fmt.Sprintf("TICKET-%d", i))
		pb.Save(ctx, plan)
	}

	plans, err := pb.ListPlans(ctx)
	if err != nil {
		t.Fatalf("failed to list plans: %v", err)
	}

	if len(plans) != 3 {
		t.Errorf("expected 3 plans, got %d", len(plans))
	}
}

func TestPlanBuilderDeletePlan(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	ctx := context.Background()

	// Create and save a plan
	source := SourceInfo{
		Type:     "local",
		FilePath: "test.sql",
		FileHash: "sha256:abc",
	}

	parseResult := &parser.ParseResult{
		RawSQL:   "SELECT 1",
		FileHash: "sha256:abc",
		Statements: []parser.Statement{
			{SQL: "SELECT 1", Type: parser.StatementSelect, Hash: "sha256:stmt1"},
		},
	}

	plan, _ := pb.Build(source, parseResult, &analyzer.AnalysisResult{}, nil, "user", "TICKET-1")
	pb.Save(ctx, plan)

	// Delete the plan
	if err := pb.DeletePlan(ctx, plan.PlanID); err != nil {
		t.Fatalf("failed to delete plan: %v", err)
	}

	// Verify it's deleted
	_, err := pb.Load(ctx, plan.PlanID)
	if err == nil {
		t.Error("expected error loading deleted plan")
	}
}

func TestGeneratePlanID(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	now := time.Now()
	planID := pb.generatePlanID(now)

	if planID == "" {
		t.Error("expected non-empty planID")
	}
	if !strings.HasPrefix(planID, "plan-") {
		t.Errorf("expected planID to start with 'plan-', got %s", planID)
	}
}

func TestGeneratePlanIDWithMetadata(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	now := time.Now()
	planID := pb.generatePlanIDWithMetadata(now, "john_doe", "TICKET-123")

	if !strings.Contains(planID, "john_doe") {
		t.Errorf("expected planID to contain username, got %s", planID)
	}
	if !strings.Contains(planID, "TICKET-123") {
		t.Errorf("expected planID to contain ticket, got %s", planID)
	}
}

func TestSanitizeForFilename(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"with spaces", "with_spaces"},
		{"with/slashes", "with_slashes"},
		{"with:colons", "with_colons"},
		{"user@domain.com", "user_domain.com"},
		{"verylongstringthatexceedstwentycharacters", "verylongstringthatex"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeForFilename(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeForFilename(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestComputeHash(t *testing.T) {
	hash1 := computeHash("test content")
	hash2 := computeHash("test content")
	hash3 := computeHash("different content")

	if hash1 != hash2 {
		t.Error("expected same content to produce same hash")
	}
	if hash1 == hash3 {
		t.Error("expected different content to produce different hash")
	}
	if !strings.HasPrefix(hash1, "sha256:") {
		t.Errorf("expected hash to start with 'sha256:', got %s", hash1)
	}
}

func TestFirstOrEmpty(t *testing.T) {
	tests := []struct {
		input    []string
		expected string
	}{
		{nil, ""},
		{[]string{}, ""},
		{[]string{"first"}, "first"},
		{[]string{"first", "second"}, "first"},
	}

	for _, tt := range tests {
		result := firstOrEmpty(tt.input)
		if result != tt.expected {
			t.Errorf("firstOrEmpty(%v) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestBuildVerificationQueries(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	statements := []StatementInfo{
		{Index: 0, Type: parser.StatementUpdate, Table: "users", HasWhere: true},
		{Index: 1, Type: parser.StatementDelete, Table: "logs", HasWhere: true},
		{Index: 2, Type: parser.StatementSelect, Table: "data", HasWhere: true}, // Should be skipped
	}

	impact := &analyzer.AnalysisResult{
		Statements: []analyzer.StatementImpact{
			{StatementIndex: 0, WhereClause: "id = 1", AffectedRows: 1},
			{StatementIndex: 1, WhereClause: "age > 30", AffectedRows: 10},
		},
	}

	verificationQueries := pb.buildVerificationQueries(statements, impact)

	// UPDATE gets pre-query only, DELETE gets pre and post
	// So we expect: 1 (pre for UPDATE) + 2 (pre + post for DELETE) = 3
	expectedCount := 3
	if len(verificationQueries) != expectedCount {
		t.Errorf("expected %d verification queries, got %d", expectedCount, len(verificationQueries))
	}

	// Check UPDATE pre-query
	hasUpdatePre := false
	for _, vq := range verificationQueries {
		if vq.StatementIndex == 0 && vq.Type == "pre" {
			hasUpdatePre = true
			if vq.ExpectedCount != 1 {
				t.Errorf("expected UPDATE pre-query count to be 1, got %d", vq.ExpectedCount)
			}
		}
	}
	if !hasUpdatePre {
		t.Error("expected pre-query for UPDATE statement")
	}

	// Check DELETE pre and post queries
	hasDeletePre := false
	hasDeletePost := false
	for _, vq := range verificationQueries {
		if vq.StatementIndex == 1 {
			if vq.Type == "pre" {
				hasDeletePre = true
				if vq.ExpectedCount != 10 {
					t.Errorf("expected DELETE pre-query count to be 10, got %d", vq.ExpectedCount)
				}
			}
			if vq.Type == "post" {
				hasDeletePost = true
				if vq.ExpectedCount != 0 {
					t.Errorf("expected DELETE post-query count to be 0, got %d", vq.ExpectedCount)
				}
			}
		}
	}
	if !hasDeletePre {
		t.Error("expected pre-query for DELETE statement")
	}
	if !hasDeletePost {
		t.Error("expected post-query for DELETE statement")
	}
}

func TestBuildVerificationQueriesNoTable(t *testing.T) {
	storage := newMockStorage()
	pb := NewPlanBuilder(time.Hour, storage)

	statements := []StatementInfo{
		{Index: 0, Type: parser.StatementUpdate, Table: "", HasWhere: true}, // No table
	}

	impact := &analyzer.AnalysisResult{
		Statements: []analyzer.StatementImpact{
			{StatementIndex: 0, AffectedRows: 1},
		},
	}

	verificationQueries := pb.buildVerificationQueries(statements, impact)

	// Should skip statements without table
	if len(verificationQueries) != 0 {
		t.Errorf("expected 0 verification queries for statement without table, got %d", len(verificationQueries))
	}
}
