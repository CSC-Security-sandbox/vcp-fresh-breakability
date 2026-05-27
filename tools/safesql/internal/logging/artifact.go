// Package logging provides artifact logging capabilities.
package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ArtifactType represents the type of artifact being logged.
type ArtifactType string

const (
	ArtifactTypePlan      ArtifactType = "plan"
	ArtifactTypeRollback  ArtifactType = "rollback"
	ArtifactTypeAudit     ArtifactType = "audit"
	ArtifactTypeExecution ArtifactType = "execution"
)

// Artifact represents a logged artifact (plan, rollback, etc.).
type Artifact struct {
	ID        string                 `json:"id"`
	Type      ArtifactType           `json:"type"`
	Timestamp time.Time              `json:"timestamp"`
	Operator  string                 `json:"operator"`
	PlanID    string                 `json:"plan_id,omitempty"`
	AuditID   string                 `json:"audit_id,omitempty"`
	Ticket    string                 `json:"ticket,omitempty"`
	FilePath  string                 `json:"file_path"`
	Data      map[string]interface{} `json:"data"`
}

// ArtifactLogger logs artifacts to both filesystem and structured logs.
type ArtifactLogger struct {
	logger    *Logger
	storePath string
}

// NewArtifactLogger creates a new artifact logger.
func NewArtifactLogger(logger *Logger, storePath string) *ArtifactLogger {
	return &ArtifactLogger{
		logger:    logger,
		storePath: storePath,
	}
}

// LogArtifact logs an artifact to both filesystem and structured logs.
func (a *ArtifactLogger) LogArtifact(artifact Artifact) error {
	// Ensure timestamp
	if artifact.Timestamp.IsZero() {
		artifact.Timestamp = time.Now().UTC()
	}

	// Create storage directory
	if err := os.MkdirAll(a.storePath, 0755); err != nil {
		return fmt.Errorf("failed to create artifact directory: %w", err)
	}

	// Generate filename if not provided
	if artifact.FilePath == "" {
		filename := fmt.Sprintf("%s-%s-%s.json",
			artifact.Type,
			artifact.ID,
			artifact.Timestamp.Format("20060102-150405"))
		artifact.FilePath = filepath.Join(a.storePath, filename)
	}

	// Write to filesystem
	data, err := json.MarshalIndent(artifact, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal artifact: %w", err)
	}

	if err := os.WriteFile(artifact.FilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write artifact: %w", err)
	}

	// Log to structured logging
	a.logger.Log(Entry{
		Severity: LevelInfo,
		Message:  fmt.Sprintf("Artifact logged: %s/%s", artifact.Type, artifact.ID),
		PlanID:   artifact.PlanID,
		AuditID:  artifact.AuditID,
		Ticket:   artifact.Ticket,
		Operator: artifact.Operator,
		Attributes: map[string]interface{}{
			"artifact_type": artifact.Type,
			"artifact_id":   artifact.ID,
			"file_path":     artifact.FilePath,
		},
	})

	return nil
}

// LogPlan logs a plan artifact.
func (a *ArtifactLogger) LogPlan(planID, operator, ticket string, planData interface{}) error {
	artifact := Artifact{
		ID:       planID,
		Type:     ArtifactTypePlan,
		Operator: operator,
		PlanID:   planID,
		Ticket:   ticket,
		Data: map[string]interface{}{
			"plan": planData,
		},
	}
	return a.LogArtifact(artifact)
}

// LogExecution logs an execution artifact.
func (a *ArtifactLogger) LogExecution(auditID, planID, operator, ticket string, execData interface{}) error {
	artifact := Artifact{
		ID:       auditID,
		Type:     ArtifactTypeExecution,
		Operator: operator,
		PlanID:   planID,
		AuditID:  auditID,
		Ticket:   ticket,
		Data: map[string]interface{}{
			"execution": execData,
		},
	}
	return a.LogArtifact(artifact)
}

// LogRollback logs a rollback artifact.
func (a *ArtifactLogger) LogRollback(rollbackID, auditID, operator string, rollbackData interface{}) error {
	artifact := Artifact{
		ID:       rollbackID,
		Type:     ArtifactTypeRollback,
		Operator: operator,
		AuditID:  auditID,
		Data: map[string]interface{}{
			"rollback": rollbackData,
		},
	}
	return a.LogArtifact(artifact)
}
