package logging

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestArtifactTypeConstants(t *testing.T) {
	tests := []struct {
		artType  ArtifactType
		expected string
	}{
		{ArtifactTypePlan, "plan"},
		{ArtifactTypeRollback, "rollback"},
		{ArtifactTypeAudit, "audit"},
		{ArtifactTypeExecution, "execution"},
	}

	for _, tt := range tests {
		if string(tt.artType) != tt.expected {
			t.Errorf("expected artifact type %q, got %q", tt.expected, tt.artType)
		}
	}
}

func TestNewArtifactLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	al := NewArtifactLogger(logger, "/tmp/artifacts")

	if al == nil {
		t.Fatal("expected non-nil artifact logger")
	}
	if al.logger != logger {
		t.Error("expected logger to be set")
	}
	if al.storePath != "/tmp/artifacts" {
		t.Errorf("expected storePath '/tmp/artifacts', got %q", al.storePath)
	}
}

func TestLogArtifact(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	artifact := Artifact{
		ID:        "test-artifact-123",
		Type:      ArtifactTypePlan,
		Operator:  "test-operator",
		PlanID:    "plan-123",
		Ticket:    "JIRA-456",
		Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Data: map[string]interface{}{
			"key": "value",
		},
	}

	err := al.LogArtifact(artifact)
	if err != nil {
		t.Fatalf("failed to log artifact: %v", err)
	}

	// Verify file was created
	expectedFilename := "plan-test-artifact-123-20240115-103000.json"
	filePath := filepath.Join(tempDir, expectedFilename)

	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read artifact file: %v", err)
	}

	var savedArtifact Artifact
	if err := json.Unmarshal(data, &savedArtifact); err != nil {
		t.Fatalf("failed to parse artifact file: %v", err)
	}

	if savedArtifact.ID != artifact.ID {
		t.Errorf("expected ID %q, got %q", artifact.ID, savedArtifact.ID)
	}
	if savedArtifact.Type != artifact.Type {
		t.Errorf("expected Type %q, got %q", artifact.Type, savedArtifact.Type)
	}
	if savedArtifact.Operator != artifact.Operator {
		t.Errorf("expected Operator %q, got %q", artifact.Operator, savedArtifact.Operator)
	}

	// Verify structured log was written
	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse log entry: %v", err)
	}

	if !strings.Contains(entry.Message, "Artifact logged") {
		t.Errorf("expected log message to contain 'Artifact logged', got %q", entry.Message)
	}
}

func TestLogArtifactWithCustomFilePath(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	customPath := filepath.Join(tempDir, "custom-artifact.json")
	artifact := Artifact{
		ID:       "custom-123",
		Type:     ArtifactTypeAudit,
		FilePath: customPath,
		Data:     map[string]interface{}{"custom": true},
	}

	err := al.LogArtifact(artifact)
	if err != nil {
		t.Fatalf("failed to log artifact: %v", err)
	}

	// Verify custom file path was used
	if _, err := os.Stat(customPath); err != nil {
		t.Errorf("expected file at custom path %q: %v", customPath, err)
	}
}

func TestLogArtifactSetsTimestamp(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	before := time.Now().UTC()
	artifact := Artifact{
		ID:   "timestamp-test",
		Type: ArtifactTypePlan,
	}

	err := al.LogArtifact(artifact)
	if err != nil {
		t.Fatalf("failed to log artifact: %v", err)
	}
	after := time.Now().UTC()

	// Find the artifact file
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected at least one artifact file")
	}

	data, err := os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("failed to read artifact: %v", err)
	}

	var saved Artifact
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatalf("failed to parse artifact: %v", err)
	}

	if saved.Timestamp.Before(before) || saved.Timestamp.After(after) {
		t.Errorf("timestamp %v not in expected range [%v, %v]", saved.Timestamp, before, after)
	}
}

func TestLogPlan(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	planData := map[string]interface{}{
		"statements": []string{"UPDATE users SET active = true"},
	}

	err := al.LogPlan("plan-123", "operator-1", "JIRA-456", planData)
	if err != nil {
		t.Fatalf("failed to log plan: %v", err)
	}

	// Find and read the artifact file
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected plan artifact file")
	}

	data, err := os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("failed to read artifact: %v", err)
	}

	var artifact Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("failed to parse artifact: %v", err)
	}

	if artifact.Type != ArtifactTypePlan {
		t.Errorf("expected type 'plan', got %q", artifact.Type)
	}
	if artifact.ID != "plan-123" {
		t.Errorf("expected ID 'plan-123', got %q", artifact.ID)
	}
	if artifact.Operator != "operator-1" {
		t.Errorf("expected operator 'operator-1', got %q", artifact.Operator)
	}
	if artifact.Ticket != "JIRA-456" {
		t.Errorf("expected ticket 'JIRA-456', got %q", artifact.Ticket)
	}
}

func TestLogExecution(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	execData := map[string]interface{}{
		"status":       "completed",
		"rowsAffected": 42,
	}

	err := al.LogExecution("audit-789", "plan-123", "operator-1", "JIRA-456", execData)
	if err != nil {
		t.Fatalf("failed to log execution: %v", err)
	}

	// Find and read the artifact file
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected execution artifact file")
	}

	data, err := os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("failed to read artifact: %v", err)
	}

	var artifact Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("failed to parse artifact: %v", err)
	}

	if artifact.Type != ArtifactTypeExecution {
		t.Errorf("expected type 'execution', got %q", artifact.Type)
	}
	if artifact.ID != "audit-789" {
		t.Errorf("expected ID 'audit-789', got %q", artifact.ID)
	}
	if artifact.PlanID != "plan-123" {
		t.Errorf("expected planID 'plan-123', got %q", artifact.PlanID)
	}
	if artifact.AuditID != "audit-789" {
		t.Errorf("expected auditID 'audit-789', got %q", artifact.AuditID)
	}
}

func TestLogRollback(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	al := NewArtifactLogger(logger, tempDir)

	rollbackData := map[string]interface{}{
		"status":        "completed",
		"rowsRestored":  42,
		"originalAudit": "audit-789",
	}

	err := al.LogRollback("rollback-001", "audit-789", "operator-1", rollbackData)
	if err != nil {
		t.Fatalf("failed to log rollback: %v", err)
	}

	// Find and read the artifact file
	files, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("failed to read temp dir: %v", err)
	}

	if len(files) == 0 {
		t.Fatal("expected rollback artifact file")
	}

	data, err := os.ReadFile(filepath.Join(tempDir, files[0].Name()))
	if err != nil {
		t.Fatalf("failed to read artifact: %v", err)
	}

	var artifact Artifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		t.Fatalf("failed to parse artifact: %v", err)
	}

	if artifact.Type != ArtifactTypeRollback {
		t.Errorf("expected type 'rollback', got %q", artifact.Type)
	}
	if artifact.ID != "rollback-001" {
		t.Errorf("expected ID 'rollback-001', got %q", artifact.ID)
	}
	if artifact.AuditID != "audit-789" {
		t.Errorf("expected auditID 'audit-789', got %q", artifact.AuditID)
	}
}

func TestLogArtifactCreatesDirectory(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	tempDir := t.TempDir()
	nestedDir := filepath.Join(tempDir, "nested", "deep", "artifacts")
	al := NewArtifactLogger(logger, nestedDir)

	artifact := Artifact{
		ID:   "nested-test",
		Type: ArtifactTypePlan,
	}

	err := al.LogArtifact(artifact)
	if err != nil {
		t.Fatalf("failed to log artifact: %v", err)
	}

	// Verify nested directory was created
	if _, err := os.Stat(nestedDir); err != nil {
		t.Errorf("expected nested directory to be created: %v", err)
	}
}

func TestArtifactJSONMarshalling(t *testing.T) {
	artifact := Artifact{
		ID:        "test-123",
		Type:      ArtifactTypePlan,
		Timestamp: time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Operator:  "test-operator",
		PlanID:    "plan-123",
		AuditID:   "audit-456",
		Ticket:    "JIRA-789",
		FilePath:  "/tmp/test.json",
		Data: map[string]interface{}{
			"key":  "value",
			"num":  42,
			"bool": true,
		},
	}

	data, err := json.Marshal(artifact)
	if err != nil {
		t.Fatalf("failed to marshal artifact: %v", err)
	}

	var parsed Artifact
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal artifact: %v", err)
	}

	if parsed.ID != artifact.ID {
		t.Errorf("ID mismatch: %q vs %q", parsed.ID, artifact.ID)
	}
	if parsed.Type != artifact.Type {
		t.Errorf("Type mismatch: %q vs %q", parsed.Type, artifact.Type)
	}
	if parsed.Operator != artifact.Operator {
		t.Errorf("Operator mismatch: %q vs %q", parsed.Operator, artifact.Operator)
	}
	if parsed.Data["key"] != "value" {
		t.Errorf("Data key mismatch: %v", parsed.Data["key"])
	}
}
