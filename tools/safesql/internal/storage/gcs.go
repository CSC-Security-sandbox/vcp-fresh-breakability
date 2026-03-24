// Package storage provides cloud storage abstraction for SafeSQL artifacts.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Storage provides cloud storage operations for SafeSQL.
type Storage struct {
	client     *storage.Client
	bucketName string
}

// New creates a new Storage client.
func New(ctx context.Context, bucketName string) (*Storage, error) {
	if bucketName == "" {
		return nil, fmt.Errorf("bucket name is required (set SAFESQL_GCS_BUCKET)")
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	return &Storage{
		client:     client,
		bucketName: bucketName,
	}, nil
}

// Close closes the storage client.
func (s *Storage) Close() error {
	return s.client.Close()
}

// SavePlan stores a plan JSON in GCS.
// Path format: plans/{planID}.json
func (s *Storage) SavePlan(ctx context.Context, planID string, data []byte) error {
	objectPath := path.Join("plans", planID+".json")
	return s.writeObject(ctx, objectPath, data)
}

// LoadPlan retrieves a plan JSON from GCS.
func (s *Storage) LoadPlan(ctx context.Context, planID string) ([]byte, error) {
	objectPath := path.Join("plans", planID+".json")
	return s.readObject(ctx, objectPath)
}

// DeletePlan removes a plan from GCS.
func (s *Storage) DeletePlan(ctx context.Context, planID string) error {
	objectPath := path.Join("plans", planID+".json")
	return s.deleteObject(ctx, objectPath)
}

// ListPlans returns all plan IDs stored in GCS.
func (s *Storage) ListPlans(ctx context.Context) ([]string, error) {
	prefix := "plans/"
	var planIDs []string

	it := s.client.Bucket(s.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list plans: %w", err)
		}

		// Extract plan ID from path (plans/plan-xxx.json -> plan-xxx)
		if strings.HasSuffix(attrs.Name, ".json") {
			planID := strings.TrimPrefix(attrs.Name, prefix)
			planID = strings.TrimSuffix(planID, ".json")
			planIDs = append(planIDs, planID)
		}
	}

	return planIDs, nil
}

// SaveAudit stores an audit entry JSON in GCS.
// Path format: audit/{auditID}.json
func (s *Storage) SaveAudit(ctx context.Context, auditID string, data []byte) error {
	objectPath := path.Join("audit", auditID+".json")
	return s.writeObject(ctx, objectPath, data)
}

// LoadAudit retrieves an audit entry JSON from GCS.
func (s *Storage) LoadAudit(ctx context.Context, auditID string) ([]byte, error) {
	objectPath := path.Join("audit", auditID+".json")
	return s.readObject(ctx, objectPath)
}

// AuditMetadata contains metadata about an audit entry.
// This is defined here to avoid circular imports but matches audit.AuditMetadata.
type AuditMetadata struct {
	AuditID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// ListAudits returns audit entries, optionally filtered by date and limited.
func (s *Storage) ListAudits(ctx context.Context, date *time.Time, limit int) ([]AuditMetadata, error) {
	prefix := "audit/"
	var audits []AuditMetadata

	it := s.client.Bucket(s.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list audits: %w", err)
		}

		if !strings.HasSuffix(attrs.Name, ".json") {
			continue
		}

		auditID := strings.TrimPrefix(attrs.Name, prefix)
		auditID = strings.TrimSuffix(auditID, ".json")

		// Filter by date if specified
		if date != nil {
			// Parse date from audit ID (format: plan-YYYYMMDD-HHMMSS or exec-YYYYMMDD-HHMMSS)
			parts := strings.Split(auditID, "-")
			if len(parts) >= 2 {
				auditDate := parts[1] // YYYYMMDD
				filterDate := date.Format("20060102")
				if auditDate != filterDate {
					continue
				}
			}
		}

		audits = append(audits, AuditMetadata{
			AuditID:   auditID,
			CreatedAt: attrs.Created,
			UpdatedAt: attrs.Updated,
		})
	}

	// Sort by created time (newest first)
	sort.Slice(audits, func(i, j int) bool {
		return audits[i].CreatedAt.After(audits[j].CreatedAt)
	})

	// Apply limit
	if limit > 0 && len(audits) > limit {
		audits = audits[:limit]
	}

	return audits, nil
}

// SavePRPlan stores a PR plan file in GCS.
// Path format: pr-plans/{prNumber}/{filename}
func (s *Storage) SavePRPlan(ctx context.Context, prNumber int, filename string, data []byte) error {
	objectPath := path.Join("pr-plans", fmt.Sprintf("%d", prNumber), filename)
	return s.writeObject(ctx, objectPath, data)
}

// LoadPRPlan retrieves a PR plan file from GCS.
func (s *Storage) LoadPRPlan(ctx context.Context, prNumber int, filename string) ([]byte, error) {
	objectPath := path.Join("pr-plans", fmt.Sprintf("%d", prNumber), filename)
	return s.readObject(ctx, objectPath)
}

// ListPRPlans returns all plan files for a given PR.
func (s *Storage) ListPRPlans(ctx context.Context, prNumber int) ([]string, error) {
	prefix := path.Join("pr-plans", fmt.Sprintf("%d", prNumber)) + "/"
	var filenames []string

	it := s.client.Bucket(s.bucketName).Objects(ctx, &storage.Query{
		Prefix: prefix,
	})

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to list PR plans: %w", err)
		}

		filename := strings.TrimPrefix(attrs.Name, prefix)
		if filename != "" {
			filenames = append(filenames, filename)
		}
	}

	return filenames, nil
}

// writeObject writes data to a GCS object.
func (s *Storage) writeObject(ctx context.Context, objectPath string, data []byte) error {
	obj := s.client.Bucket(s.bucketName).Object(objectPath)
	w := obj.NewWriter(ctx)
	w.ContentType = "application/json"

	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("failed to write to GCS: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close GCS writer: %w", err)
	}

	return nil
}

// readObject reads data from a GCS object.
func (s *Storage) readObject(ctx context.Context, objectPath string) ([]byte, error) {
	obj := s.client.Bucket(s.bucketName).Object(objectPath)
	r, err := obj.NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, fmt.Errorf("object not found: %s", objectPath)
		}
		return nil, fmt.Errorf("failed to read from GCS: %w", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	return data, nil
}

// deleteObject deletes a GCS object.
func (s *Storage) deleteObject(ctx context.Context, objectPath string) error {
	obj := s.client.Bucket(s.bucketName).Object(objectPath)
	if err := obj.Delete(ctx); err != nil {
		if err == storage.ErrObjectNotExist {
			return fmt.Errorf("object not found: %s", objectPath)
		}
		return fmt.Errorf("failed to delete from GCS: %w", err)
	}
	return nil
}

// ObjectExists checks if an object exists in GCS.
func (s *Storage) ObjectExists(ctx context.Context, objectPath string) (bool, error) {
	obj := s.client.Bucket(s.bucketName).Object(objectPath)
	_, err := obj.Attrs(ctx)
	if err == storage.ErrObjectNotExist {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to check object existence: %w", err)
	}
	return true, nil
}

// SaveJSON is a generic helper to save any JSON-serializable object.
func (s *Storage) SaveJSON(ctx context.Context, objectPath string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return s.writeObject(ctx, objectPath, data)
}

// LoadJSON is a generic helper to load and unmarshal JSON from GCS.
func (s *Storage) LoadJSON(ctx context.Context, objectPath string, v interface{}) error {
	data, err := s.readObject(ctx, objectPath)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return nil
}
