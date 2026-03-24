package logging

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestNewIntegratedLogger(t *testing.T) {
	logger := NewIntegratedLogger(false, "test-component")

	if logger == nil {
		t.Fatal("expected non-nil integrated logger")
	}

	if logger.slogLogger == nil {
		t.Error("expected slog logger to be set")
	}

	if logger.logger == nil {
		t.Error("expected internal logger to be set")
	}

	if logger.logger.component != "test-component" {
		t.Errorf("expected component 'test-component', got %q", logger.logger.component)
	}

	// Internal logger should have stdout disabled (nil)
	if logger.logger.stdoutWriter != nil {
		t.Error("expected internal logger stdout to be nil")
	}
}

func TestNewIntegratedLoggerWithJournald(t *testing.T) {
	logger := NewIntegratedLogger(true, "test-component")

	if logger == nil {
		t.Fatal("expected non-nil integrated logger")
	}

	if !logger.logger.enableJournald {
		t.Error("expected journald to be enabled")
	}

	// journaldWriter should be set when journald is enabled
	if logger.logger.journaldWriter == nil {
		t.Error("expected journald writer to be set")
	}
}

func TestIntegratedLoggerInfo(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := NewIntegratedLogger(false, "test")
	logger.Info("Test message")

	// Restore stdout and read capture
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	if output != "Test message" {
		t.Errorf("expected 'Test message', got %q", output)
	}
}

func TestIntegratedLoggerInfof(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := NewIntegratedLogger(false, "test")
	logger.Infof("Formatted %s %d", "message", 42)

	// Restore stdout and read capture
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	if output != "Formatted message 42" {
		t.Errorf("expected 'Formatted message 42', got %q", output)
	}
}

func TestIntegratedLoggerWithContext(t *testing.T) {
	logger := NewIntegratedLogger(false, "test")
	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789", "operator-1")

	if ctx == nil {
		t.Fatal("expected non-nil context logger")
	}

	if ctx.planID != "plan-123" {
		t.Errorf("expected planID 'plan-123', got %q", ctx.planID)
	}
	if ctx.auditID != "audit-456" {
		t.Errorf("expected auditID 'audit-456', got %q", ctx.auditID)
	}
	if ctx.ticket != "JIRA-789" {
		t.Errorf("expected ticket 'JIRA-789', got %q", ctx.ticket)
	}
	if ctx.operator != "operator-1" {
		t.Errorf("expected operator 'operator-1', got %q", ctx.operator)
	}
}

func TestIntegratedContextLoggerInfo(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := NewIntegratedLogger(false, "test")
	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789", "operator-1")
	ctx.Info("Context message")

	// Restore stdout and read capture
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	if output != "Context message" {
		t.Errorf("expected 'Context message', got %q", output)
	}
}

func TestIntegratedContextLoggerInfof(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := NewIntegratedLogger(false, "test")
	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789", "operator-1")
	ctx.Infof("Formatted %s", "message")

	// Restore stdout and read capture
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()
	if output != "Formatted message" {
		t.Errorf("expected 'Formatted message', got %q", output)
	}
}

func TestIntegratedLoggerInternalLoggerSettings(t *testing.T) {
	logger := NewIntegratedLogger(true, "my-component")

	// Verify internal logger settings
	if logger.logger.enableStructured {
		t.Error("expected enableStructured to be false")
	}

	if logger.logger.component != "my-component" {
		t.Errorf("expected component 'my-component', got %q", logger.logger.component)
	}
}

func TestIntegratedContextLoggerStoresContext(t *testing.T) {
	logger := NewIntegratedLogger(false, "test")
	ctx := logger.WithContext("plan-1", "audit-2", "JIRA-3", "op-4")

	if ctx.integrated != logger {
		t.Error("expected integrated logger reference to be preserved")
	}

	// Verify all context fields
	if ctx.planID != "plan-1" {
		t.Errorf("planID mismatch")
	}
	if ctx.auditID != "audit-2" {
		t.Errorf("auditID mismatch")
	}
	if ctx.ticket != "JIRA-3" {
		t.Errorf("ticket mismatch")
	}
	if ctx.operator != "op-4" {
		t.Errorf("operator mismatch")
	}
}

func TestIntegratedLoggerWithMockJournald(t *testing.T) {
	// Create logger with journald enabled
	logger := NewIntegratedLogger(true, "test-component")

	// Replace journald writer with mock for testing
	mock := &mockJournaldWriter{}
	logger.logger.journaldWriter = mock

	// Log a message
	logger.Info("Test journald message")

	// Verify mock received the entry
	if len(mock.entries) != 1 {
		t.Errorf("expected 1 entry in mock, got %d", len(mock.entries))
	}

	if len(mock.entries) > 0 {
		entry := mock.entries[0]
		if entry.Message != "Test journald message" {
			t.Errorf("expected message 'Test journald message', got %q", entry.Message)
		}
		if entry.Severity != LevelInfo {
			t.Errorf("expected severity INFO, got %s", entry.Severity)
		}
	}
}

func TestIntegratedContextLoggerWithMockJournald(t *testing.T) {
	logger := NewIntegratedLogger(true, "test-component")

	// Replace journald writer with mock
	mock := &mockJournaldWriter{}
	logger.logger.journaldWriter = mock

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789", "operator-1")
	ctx.Info("Context journald message")

	// Verify mock received the entry with context
	if len(mock.entries) != 1 {
		t.Errorf("expected 1 entry in mock, got %d", len(mock.entries))
	}

	if len(mock.entries) > 0 {
		entry := mock.entries[0]
		if entry.PlanID != "plan-123" {
			t.Errorf("expected planID 'plan-123', got %q", entry.PlanID)
		}
		if entry.AuditID != "audit-456" {
			t.Errorf("expected auditID 'audit-456', got %q", entry.AuditID)
		}
		if entry.Ticket != "JIRA-789" {
			t.Errorf("expected ticket 'JIRA-789', got %q", entry.Ticket)
		}
		if entry.Operator != "operator-1" {
			t.Errorf("expected operator 'operator-1', got %q", entry.Operator)
		}
	}
}
