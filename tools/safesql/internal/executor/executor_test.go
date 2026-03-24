package executor

import (
	"errors"
	"testing"
	"time"
)

func TestExtractOwner(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"owner/repo", "owner"},
		{"netapp/vsa-control-plane", "netapp"},
		{"singleword", "singleword"},
		{"multiple/slashes/here", "multiple"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractOwner(tt.input)
			if result != tt.expected {
				t.Errorf("extractOwner(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractRepo(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"owner/repo", "repo"},
		{"netapp/vsa-control-plane", "vsa-control-plane"},
		{"singleword", "singleword"},
		{"multiple/slashes/here", "slashes/here"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractRepo(tt.input)
			if result != tt.expected {
				t.Errorf("extractRepo(%q) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestVerificationResult(t *testing.T) {
	// Test that VerificationResult properly holds error states
	result := &VerificationResult{
		Valid:            true,
		PlanExpired:      false,
		CommitMismatch:   false,
		StateDrift:       false,
		RowCountMismatch: false,
		SignatureInvalid: false,
		Errors:           []string{},
		Details:          make(map[string]interface{}),
	}

	if !result.Valid {
		t.Error("expected result to be valid")
	}

	// Add an error
	result.Valid = false
	result.PlanExpired = true
	result.Errors = append(result.Errors, "Plan expired")

	if result.Valid {
		t.Error("expected result to be invalid after adding error")
	}
	if !result.PlanExpired {
		t.Error("expected PlanExpired to be true")
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}
}

func TestExecutionResult(t *testing.T) {
	// Test ExecutionResult fields
	now := time.Now()
	result := &ExecutionResult{
		Success:      true,
		RowsAffected: []int64{5, 10, 3},
		TotalRows:    18,
		ExecutedAt:   now,
		Timestamp:    now,
		Duration:     500 * time.Millisecond,
		RolledBack:   false,
		Error:        nil,
	}

	if !result.Success {
		t.Error("expected Success to be true")
	}
	if result.TotalRows != 18 {
		t.Errorf("expected TotalRows to be 18, got %d", result.TotalRows)
	}
	if result.Duration != 500*time.Millisecond {
		t.Errorf("expected Duration to be 500ms, got %v", result.Duration)
	}
	if result.RolledBack {
		t.Error("expected RolledBack to be false")
	}

	// Test failure case
	result.Success = false
	result.RolledBack = true
	result.Error = errors.New("execution failed")

	if result.Success {
		t.Error("expected Success to be false")
	}
	if !result.RolledBack {
		t.Error("expected RolledBack to be true")
	}
	if result.Error == nil {
		t.Error("expected Error to be set")
	}
}

// Note: Full integration tests for VerifyPlan, Execute, and ExecuteRollback
// require database mocks which are complex. These tests focus on helper functions
// and data structures.

func TestVerificationResultDetails(t *testing.T) {
	result := &VerificationResult{
		Valid:   false,
		Details: make(map[string]interface{}),
	}

	// Add various types of details
	result.Details["expected_commit"] = "abc123"
	result.Details["current_commit"] = "def456"
	result.Details["stmt_0_expected_rows"] = int64(10)
	result.Details["stmt_0_current_rows"] = int64(15)

	if result.Details["expected_commit"] != "abc123" {
		t.Error("expected expected_commit to be 'abc123'")
	}
	if result.Details["stmt_0_expected_rows"] != int64(10) {
		t.Error("expected stmt_0_expected_rows to be 10")
	}
}

func TestExecutorNew(t *testing.T) {
	// Test that New creates a valid Executor
	// We can't fully test without mocks, but we can verify nil handling
	exec := New(nil, nil, nil)
	if exec == nil {
		t.Error("expected non-nil executor")
	}
	if exec.db != nil {
		t.Error("expected db to be nil when not provided")
	}
	if exec.githubClient != nil {
		t.Error("expected githubClient to be nil when not provided")
	}
	if exec.planBuilder != nil {
		t.Error("expected planBuilder to be nil when not provided")
	}
}
