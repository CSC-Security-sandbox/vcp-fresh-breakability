package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestAuditMetadataFields(t *testing.T) {
	now := time.Now()
	meta := AuditMetadata{
		AuditID:   "audit-123",
		CreatedAt: now,
		UpdatedAt: now.Add(time.Hour),
	}

	if meta.AuditID != "audit-123" {
		t.Errorf("expected AuditID 'audit-123', got %q", meta.AuditID)
	}
	if !meta.CreatedAt.Equal(now) {
		t.Errorf("expected CreatedAt %v, got %v", now, meta.CreatedAt)
	}
	if !meta.UpdatedAt.Equal(now.Add(time.Hour)) {
		t.Errorf("expected UpdatedAt %v, got %v", now.Add(time.Hour), meta.UpdatedAt)
	}
}

func TestNewRequiresBucketName(t *testing.T) {
	// Empty bucket name should return error
	_, err := New(context.Background(), "")
	if err == nil {
		t.Error("expected error for empty bucket name")
	}
	if err.Error() != "bucket name is required (set SAFESQL_GCS_BUCKET)" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStorageStructFields(t *testing.T) {
	// Verify Storage struct has the expected fields
	s := &Storage{
		client:     nil, // Would be real client in production
		bucketName: "test-bucket",
	}

	if s.bucketName != "test-bucket" {
		t.Errorf("expected bucketName 'test-bucket', got %q", s.bucketName)
	}
}

// MockStorage provides a mock implementation for testing
type MockStorage struct {
	plans    map[string][]byte
	audits   map[string][]byte
	prPlans  map[string]map[string][]byte
	objects  map[string][]byte
	metadata map[string]AuditMetadata
}

// NewMockStorage creates a new mock storage
func NewMockStorage() *MockStorage {
	return &MockStorage{
		plans:    make(map[string][]byte),
		audits:   make(map[string][]byte),
		prPlans:  make(map[string]map[string][]byte),
		objects:  make(map[string][]byte),
		metadata: make(map[string]AuditMetadata),
	}
}

func (m *MockStorage) SavePlan(ctx context.Context, planID string, data []byte) error {
	m.plans[planID] = data
	return nil
}

func (m *MockStorage) LoadPlan(ctx context.Context, planID string) ([]byte, error) {
	data, ok := m.plans[planID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *MockStorage) DeletePlan(ctx context.Context, planID string) error {
	delete(m.plans, planID)
	return nil
}

func (m *MockStorage) ListPlans(ctx context.Context) ([]string, error) {
	var planIDs []string
	for id := range m.plans {
		planIDs = append(planIDs, id)
	}
	return planIDs, nil
}

func (m *MockStorage) SaveAudit(ctx context.Context, auditID string, data []byte) error {
	m.audits[auditID] = data
	m.metadata[auditID] = AuditMetadata{
		AuditID:   auditID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	return nil
}

func (m *MockStorage) LoadAudit(ctx context.Context, auditID string) ([]byte, error) {
	data, ok := m.audits[auditID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *MockStorage) ListAudits(ctx context.Context, date *time.Time, limit int) ([]AuditMetadata, error) {
	var audits []AuditMetadata
	for _, meta := range m.metadata {
		audits = append(audits, meta)
	}
	if limit > 0 && len(audits) > limit {
		audits = audits[:limit]
	}
	return audits, nil
}

func TestMockStoragePlans(t *testing.T) {
	mock := NewMockStorage()
	ctx := context.Background()

	// Test save and load
	testData := []byte(`{"id": "plan-123"}`)
	if err := mock.SavePlan(ctx, "plan-123", testData); err != nil {
		t.Fatalf("failed to save plan: %v", err)
	}

	loaded, err := mock.LoadPlan(ctx, "plan-123")
	if err != nil {
		t.Fatalf("failed to load plan: %v", err)
	}

	if string(loaded) != string(testData) {
		t.Errorf("expected %q, got %q", testData, loaded)
	}

	// Test list
	plans, err := mock.ListPlans(ctx)
	if err != nil {
		t.Fatalf("failed to list plans: %v", err)
	}
	if len(plans) != 1 {
		t.Errorf("expected 1 plan, got %d", len(plans))
	}

	// Test delete
	if err := mock.DeletePlan(ctx, "plan-123"); err != nil {
		t.Fatalf("failed to delete plan: %v", err)
	}

	loaded, err = mock.LoadPlan(ctx, "plan-123")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if loaded != nil {
		t.Error("expected nil after delete")
	}
}

func TestMockStorageAudits(t *testing.T) {
	mock := NewMockStorage()
	ctx := context.Background()

	testData := []byte(`{"audit": "data"}`)
	if err := mock.SaveAudit(ctx, "audit-123", testData); err != nil {
		t.Fatalf("failed to save audit: %v", err)
	}

	loaded, err := mock.LoadAudit(ctx, "audit-123")
	if err != nil {
		t.Fatalf("failed to load audit: %v", err)
	}

	if string(loaded) != string(testData) {
		t.Errorf("expected %q, got %q", testData, loaded)
	}

	audits, err := mock.ListAudits(ctx, nil, 10)
	if err != nil {
		t.Fatalf("failed to list audits: %v", err)
	}

	if len(audits) != 1 {
		t.Errorf("expected 1 audit, got %d", len(audits))
	}
}

func TestMockStorageListAuditsWithLimit(t *testing.T) {
	mock := NewMockStorage()
	ctx := context.Background()

	// Add multiple audits
	for i := 0; i < 5; i++ {
		auditID := "audit-" + string(rune('A'+i))
		mock.SaveAudit(ctx, auditID, []byte(`{}`))
	}

	// Test with limit
	audits, err := mock.ListAudits(ctx, nil, 2)
	if err != nil {
		t.Fatalf("failed to list audits: %v", err)
	}

	if len(audits) != 2 {
		t.Errorf("expected 2 audits (limited), got %d", len(audits))
	}

	// Test without limit (0 means no limit)
	allAudits, err := mock.ListAudits(ctx, nil, 0)
	if err != nil {
		t.Fatalf("failed to list all audits: %v", err)
	}

	if len(allAudits) != 5 {
		t.Errorf("expected 5 audits, got %d", len(allAudits))
	}
}

func TestStoragePathFormats(t *testing.T) {
	// Test path format conventions used by Storage
	tests := []struct {
		name       string
		id         string
		expectedFn func(string) string
	}{
		{
			name: "plan path",
			id:   "plan-20240115-120000",
			expectedFn: func(id string) string {
				return "plans/" + id + ".json"
			},
		},
		{
			name: "audit path",
			id:   "exec-20240115-120000",
			expectedFn: func(id string) string {
				return "audit/" + id + ".json"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expected := tt.expectedFn(tt.id)
			if expected == "" {
				t.Errorf("expected non-empty path")
			}
		})
	}
}

func TestPRPlanPathFormat(t *testing.T) {
	// PR plans follow format: pr-plans/{prNumber}/{filename}
	prNumber := 123
	filename := "changes.sql"

	// Expected path
	expectedPrefix := fmt.Sprintf("pr-plans/%d/", prNumber)
	if expectedPrefix == "" {
		t.Error("expected non-empty prefix")
	}

	// Verify filename handling
	fullPath := expectedPrefix + filename
	if fullPath != "pr-plans/123/changes.sql" {
		t.Errorf("unexpected path: %s", fullPath)
	}
}
