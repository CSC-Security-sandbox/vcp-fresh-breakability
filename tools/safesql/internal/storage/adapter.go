package storage

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/audit"
)

// AuditStorageAdapter adapts Storage to implement audit.StorageBackend.
type AuditStorageAdapter struct {
	storage *Storage
}

// NewAuditStorageAdapter creates a new adapter.
func NewAuditStorageAdapter(storage *Storage) *AuditStorageAdapter {
	return &AuditStorageAdapter{storage: storage}
}

// SaveAudit implements audit.StorageBackend.
func (a *AuditStorageAdapter) SaveAudit(ctx context.Context, auditID string, data []byte) error {
	return a.storage.SaveAudit(ctx, auditID, data)
}

// LoadAudit implements audit.StorageBackend.
func (a *AuditStorageAdapter) LoadAudit(ctx context.Context, auditID string) ([]byte, error) {
	return a.storage.LoadAudit(ctx, auditID)
}

// ListAudits implements audit.StorageBackend by converting storage.AuditMetadata to audit.AuditMetadata.
func (a *AuditStorageAdapter) ListAudits(ctx context.Context, date *time.Time, limit int) ([]audit.AuditMetadata, error) {
	storageMetadata, err := a.storage.ListAudits(ctx, date, limit)
	if err != nil {
		return nil, err
	}

	// Convert storage.AuditMetadata to audit.AuditMetadata
	auditMetadata := make([]audit.AuditMetadata, len(storageMetadata))
	for i, meta := range storageMetadata {
		auditMetadata[i] = audit.AuditMetadata{
			AuditID:   meta.AuditID,
			CreatedAt: meta.CreatedAt,
			UpdatedAt: meta.UpdatedAt,
		}
	}

	return auditMetadata, nil
}
