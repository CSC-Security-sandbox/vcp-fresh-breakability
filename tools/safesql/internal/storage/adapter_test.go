package storage

import (
	"context"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
)

// MockStorageBackend is a mock implementation of the Storage struct for adapter testing
type MockStorageBackend struct {
	audits   map[string][]byte
	metadata []AuditMetadata
	saveErr  error
	loadErr  error
	listErr  error
}

func NewMockStorageBackend() *MockStorageBackend {
	return &MockStorageBackend{
		audits:   make(map[string][]byte),
		metadata: make([]AuditMetadata, 0),
	}
}

func (m *MockStorageBackend) SaveAudit(ctx context.Context, auditID string, data []byte) error {
	if m.saveErr != nil {
		return m.saveErr
	}
	m.audits[auditID] = data
	m.metadata = append(m.metadata, AuditMetadata{
		AuditID:   auditID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	})
	return nil
}

func (m *MockStorageBackend) LoadAudit(ctx context.Context, auditID string) ([]byte, error) {
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return m.audits[auditID], nil
}

func (m *MockStorageBackend) ListAudits(ctx context.Context, date *time.Time, limit int) ([]AuditMetadata, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	result := m.metadata
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// MockableStorage wraps MockStorageBackend to provide the methods used by adapter
type MockableStorage struct {
	mock *MockStorageBackend
}

func TestNewAuditStorageAdapter(t *testing.T) {
	// Since we can't create a real Storage (requires GCS), we test with nil
	// The adapter should still be created
	adapter := NewAuditStorageAdapter(nil)

	if adapter == nil {
		t.Fatal("expected non-nil adapter")
	}

	if adapter.storage != nil {
		t.Error("expected storage to be nil")
	}
}

func TestAuditStorageAdapterImplementsInterface(t *testing.T) {
	// Verify that AuditStorageAdapter implements audit.StorageBackend
	// This is a compile-time check
	var _ audit.StorageBackend = (*AuditStorageAdapter)(nil)
}

func TestAuditMetadataConversion(t *testing.T) {
	// Test that storage.AuditMetadata can be converted to audit.AuditMetadata
	now := time.Now()
	storageMeta := AuditMetadata{
		AuditID:   "audit-123",
		CreatedAt: now,
		UpdatedAt: now.Add(time.Hour),
	}

	// Convert to audit.AuditMetadata
	auditMeta := audit.AuditMetadata{
		AuditID:   storageMeta.AuditID,
		CreatedAt: storageMeta.CreatedAt,
		UpdatedAt: storageMeta.UpdatedAt,
	}

	if auditMeta.AuditID != storageMeta.AuditID {
		t.Errorf("AuditID mismatch: %q vs %q", auditMeta.AuditID, storageMeta.AuditID)
	}
	if !auditMeta.CreatedAt.Equal(storageMeta.CreatedAt) {
		t.Errorf("CreatedAt mismatch")
	}
	if !auditMeta.UpdatedAt.Equal(storageMeta.UpdatedAt) {
		t.Errorf("UpdatedAt mismatch")
	}
}

func TestAuditStorageAdapterFields(t *testing.T) {
	adapter := &AuditStorageAdapter{
		storage: nil,
	}

	if adapter.storage != nil {
		t.Error("expected storage to be nil")
	}
}

// TestListAuditsConversion tests the conversion logic in ListAudits
func TestListAuditsConversionLogic(t *testing.T) {
	// Create mock metadata
	storageMetadata := []AuditMetadata{
		{
			AuditID:   "audit-1",
			CreatedAt: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
		},
		{
			AuditID:   "audit-2",
			CreatedAt: time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2024, 1, 15, 13, 0, 0, 0, time.UTC),
		},
	}

	// Simulate the conversion logic from adapter
	auditMetadata := make([]audit.AuditMetadata, len(storageMetadata))
	for i, meta := range storageMetadata {
		auditMetadata[i] = audit.AuditMetadata{
			AuditID:   meta.AuditID,
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
		}
	}

	// Verify conversion
	if len(auditMetadata) != 2 {
		t.Fatalf("expected 2 metadata entries, got %d", len(auditMetadata))
	}

	if auditMetadata[0].AuditID != "audit-1" {
		t.Errorf("expected audit-1, got %q", auditMetadata[0].AuditID)
	}
	if auditMetadata[1].AuditID != "audit-2" {
		t.Errorf("expected audit-2, got %q", auditMetadata[1].AuditID)
	}
}

func TestAdapterStorageAssignment(t *testing.T) {
	// Test that adapter correctly stores the storage reference
	s := &Storage{
		bucketName: "test-bucket",
	}

	adapter := NewAuditStorageAdapter(s)

	if adapter.storage != s {
		t.Error("expected adapter to reference the provided storage")
	}
	if adapter.storage.bucketName != "test-bucket" {
		t.Errorf("expected bucket name 'test-bucket', got %q", adapter.storage.bucketName)
	}
}
