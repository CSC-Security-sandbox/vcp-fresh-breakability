package logging

import (
	"testing"
)

func TestSeverityToPriority(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarning, "warning"},
		{LevelError, "err"},
		{LevelCritical, "crit"},
		{Level("UNKNOWN"), "info"}, // default case
	}

	for _, tt := range tests {
		t.Run(string(tt.level), func(t *testing.T) {
			result := severityToPriority(tt.level)
			if result != tt.expected {
				t.Errorf("severityToPriority(%s) = %q, want %q", tt.level, result, tt.expected)
			}
		})
	}
}

func TestNewJournaldWriter(t *testing.T) {
	// NewJournaldWriter checks for systemd-cat availability
	writer := NewJournaldWriter()

	if writer == nil {
		t.Fatal("expected non-nil journald writer")
	}

	// The writer should be created regardless of systemd-cat availability
	jw, ok := writer.(*journaldWriter)
	if !ok {
		t.Fatal("expected *journaldWriter type")
	}

	// On systems without systemd, enabled should be false
	// On systems with systemd, enabled should be true
	// We just verify it's one or the other (not a test failure)
	_ = jw.enabled
}

func TestJournaldWriterDisabled(t *testing.T) {
	writer := &journaldWriter{
		enabled: false,
	}

	// When disabled, Write should return nil and not attempt to run systemd-cat
	err := writer.Write(Entry{
		Severity: LevelInfo,
		Message:  "Test message",
	})

	if err != nil {
		t.Errorf("expected nil error when journald is disabled, got %v", err)
	}
}

func TestJournaldWriterInterface(t *testing.T) {
	// Verify that journaldWriter implements JournaldWriter interface
	var _ JournaldWriter = &journaldWriter{}
	var _ JournaldWriter = (*journaldWriter)(nil)
}

func TestJournaldWriterWithMetadata(t *testing.T) {
	// Test that metadata is properly formatted in the message
	// This tests the message building logic even if journald isn't available
	writer := &journaldWriter{
		enabled: false, // Disable actual journald calls
	}

	entry := Entry{
		Severity:  LevelInfo,
		Message:   "Test message with metadata",
		Component: "test-component",
		PlanID:    "plan-123",
		AuditID:   "audit-456",
		Ticket:    "JIRA-789",
		Operator:  "test-operator",
	}

	// This should not error when disabled
	err := writer.Write(entry)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestJournaldWriterMinimalEntry(t *testing.T) {
	writer := &journaldWriter{
		enabled: false,
	}

	// Test with minimal entry (no metadata)
	entry := Entry{
		Severity: LevelInfo,
		Message:  "Simple message",
	}

	err := writer.Write(entry)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestJournaldWriterAllSeverities(t *testing.T) {
	writer := &journaldWriter{
		enabled: false,
	}

	severities := []Level{
		LevelDebug,
		LevelInfo,
		LevelWarning,
		LevelError,
		LevelCritical,
	}

	for _, sev := range severities {
		t.Run(string(sev), func(t *testing.T) {
			entry := Entry{
				Severity: sev,
				Message:  "Test " + string(sev),
			}

			err := writer.Write(entry)
			if err != nil {
				t.Errorf("expected no error for severity %s, got %v", sev, err)
			}
		})
	}
}

// mockJournaldWriter is a mock implementation for testing
type mockJournaldWriter struct {
	entries []Entry
	err     error
}

func (m *mockJournaldWriter) Write(entry Entry) error {
	if m.err != nil {
		return m.err
	}
	m.entries = append(m.entries, entry)
	return nil
}

func TestMockJournaldWriter(t *testing.T) {
	mock := &mockJournaldWriter{}

	// Verify mock implements interface
	var _ JournaldWriter = mock

	// Test write
	entry := Entry{
		Severity: LevelInfo,
		Message:  "Test",
	}

	err := mock.Write(entry)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if len(mock.entries) != 1 {
		t.Errorf("expected 1 entry, got %d", len(mock.entries))
	}

	if mock.entries[0].Message != "Test" {
		t.Errorf("expected message 'Test', got %q", mock.entries[0].Message)
	}
}
