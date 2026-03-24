package logging

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLevelConstants(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "DEBUG"},
		{LevelInfo, "INFO"},
		{LevelWarning, "WARNING"},
		{LevelError, "ERROR"},
		{LevelCritical, "CRITICAL"},
	}

	for _, tt := range tests {
		if string(tt.level) != tt.expected {
			t.Errorf("expected level %q, got %q", tt.expected, tt.level)
		}
	}
}

func TestNewLogger(t *testing.T) {
	cfg := Config{
		EnableJournald:   false,
		EnableStructured: true,
		Component:        "test-component",
		Operator:         "test-operator",
	}

	logger := New(cfg)

	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	if logger.component != cfg.Component {
		t.Errorf("expected component %q, got %q", cfg.Component, logger.component)
	}
	if logger.operator != cfg.Operator {
		t.Errorf("expected operator %q, got %q", cfg.Operator, logger.operator)
	}
	if logger.enableStructured != cfg.EnableStructured {
		t.Errorf("expected enableStructured %v, got %v", cfg.EnableStructured, logger.enableStructured)
	}
}

func TestLoggerPlainTextOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: false,
	}

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	logger.Log(Entry{
		Timestamp: testTime,
		Severity:  LevelInfo,
		Message:   "Test message",
	})

	output := buf.String()
	if !strings.Contains(output, "2024-01-15 10:30:00") {
		t.Errorf("expected timestamp in output, got %q", output)
	}
	if !strings.Contains(output, "INFO") {
		t.Errorf("expected INFO severity in output, got %q", output)
	}
	if !strings.Contains(output, "Test message") {
		t.Errorf("expected message in output, got %q", output)
	}
}

func TestLoggerStructuredOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
		component:        "test-component",
		operator:         "test-operator",
	}

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	logger.Log(Entry{
		Timestamp: testTime,
		Severity:  LevelInfo,
		Message:   "Structured message",
	})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if entry.Severity != LevelInfo {
		t.Errorf("expected severity INFO, got %s", entry.Severity)
	}
	if entry.Message != "Structured message" {
		t.Errorf("expected message 'Structured message', got %q", entry.Message)
	}
	if entry.Component != "test-component" {
		t.Errorf("expected component 'test-component', got %q", entry.Component)
	}
	if entry.Operator != "test-operator" {
		t.Errorf("expected operator 'test-operator', got %q", entry.Operator)
	}
}

func TestLoggerSetDefaultTimestamp(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	before := time.Now().UTC()
	logger.Log(Entry{
		Severity: LevelInfo,
		Message:  "Test",
	})
	after := time.Now().UTC()

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if entry.Timestamp.Before(before) || entry.Timestamp.After(after) {
		t.Errorf("timestamp %v not in expected range [%v, %v]", entry.Timestamp, before, after)
	}
}

func TestLoggerDebug(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Debug("Debug message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelDebug {
		t.Errorf("expected DEBUG severity, got %s", entry.Severity)
	}
	if entry.Message != "Debug message" {
		t.Errorf("expected 'Debug message', got %q", entry.Message)
	}
}

func TestLoggerInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Info("Info message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelInfo {
		t.Errorf("expected INFO severity, got %s", entry.Severity)
	}
}

func TestLoggerInfof(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Infof("Formatted %s %d", "message", 42)

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Formatted message 42" {
		t.Errorf("expected 'Formatted message 42', got %q", entry.Message)
	}
}

func TestLoggerWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Warning("Warning message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelWarning {
		t.Errorf("expected WARNING severity, got %s", entry.Severity)
	}
}

func TestLoggerWarningf(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Warningf("Warning %v", "test")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Warning test" {
		t.Errorf("expected 'Warning test', got %q", entry.Message)
	}
}

func TestLoggerError(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Error("Error message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelError {
		t.Errorf("expected ERROR severity, got %s", entry.Severity)
	}
}

func TestLoggerErrorf(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Errorf("Error: %s", "something went wrong")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Error: something went wrong" {
		t.Errorf("expected 'Error: something went wrong', got %q", entry.Message)
	}
}

func TestLoggerCritical(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Critical("Critical message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelCritical {
		t.Errorf("expected CRITICAL severity, got %s", entry.Severity)
	}
}

func TestLoggerCriticalf(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Criticalf("Critical: %d errors", 5)

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Critical: 5 errors" {
		t.Errorf("expected 'Critical: 5 errors', got %q", entry.Message)
	}
}

func TestLoggerWithContext(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")

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
}

func TestContextLoggerLog(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")
	ctx.Log(Entry{
		Severity: LevelInfo,
		Message:  "Context message",
	})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.PlanID != "plan-123" {
		t.Errorf("expected planID 'plan-123', got %q", entry.PlanID)
	}
	if entry.AuditID != "audit-456" {
		t.Errorf("expected auditID 'audit-456', got %q", entry.AuditID)
	}
	if entry.Ticket != "JIRA-789" {
		t.Errorf("expected ticket 'JIRA-789', got %q", entry.Ticket)
	}
}

func TestContextLoggerDoesNotOverwrite(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")
	ctx.Log(Entry{
		Severity: LevelInfo,
		Message:  "Message",
		PlanID:   "explicit-plan",
		AuditID:  "explicit-audit",
		Ticket:   "explicit-ticket",
	})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	// Explicit values should be preserved
	if entry.PlanID != "explicit-plan" {
		t.Errorf("expected planID 'explicit-plan', got %q", entry.PlanID)
	}
	if entry.AuditID != "explicit-audit" {
		t.Errorf("expected auditID 'explicit-audit', got %q", entry.AuditID)
	}
	if entry.Ticket != "explicit-ticket" {
		t.Errorf("expected ticket 'explicit-ticket', got %q", entry.Ticket)
	}
}

func TestContextLoggerInfo(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "", "")
	ctx.Info("Info message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelInfo {
		t.Errorf("expected INFO severity, got %s", entry.Severity)
	}
	if entry.PlanID != "plan-123" {
		t.Errorf("expected planID 'plan-123', got %q", entry.PlanID)
	}
}

func TestContextLoggerInfof(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("", "audit-456", "")
	ctx.Infof("Formatted %s", "message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Formatted message" {
		t.Errorf("expected 'Formatted message', got %q", entry.Message)
	}
	if entry.AuditID != "audit-456" {
		t.Errorf("expected auditID 'audit-456', got %q", entry.AuditID)
	}
}

func TestContextLoggerWarning(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("", "", "JIRA-789")
	ctx.Warning("Warning message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelWarning {
		t.Errorf("expected WARNING severity, got %s", entry.Severity)
	}
	if entry.Ticket != "JIRA-789" {
		t.Errorf("expected ticket 'JIRA-789', got %q", entry.Ticket)
	}
}

func TestContextLoggerWarningf(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")
	ctx.Warningf("Warning: %d", 42)

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Warning: 42" {
		t.Errorf("expected 'Warning: 42', got %q", entry.Message)
	}
}

func TestContextLoggerError(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")
	ctx.Error("Error message")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Severity != LevelError {
		t.Errorf("expected ERROR severity, got %s", entry.Severity)
	}
}

func TestContextLoggerErrorf(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	ctx := logger.WithContext("plan-123", "audit-456", "JIRA-789")
	ctx.Errorf("Error: %v", "failed")

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Message != "Error: failed" {
		t.Errorf("expected 'Error: failed', got %q", entry.Message)
	}
}

func TestEntryWithAttributes(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	logger.Log(Entry{
		Severity: LevelInfo,
		Message:  "With attributes",
		Attributes: map[string]interface{}{
			"key1": "value1",
			"key2": 42,
			"key3": true,
		},
	})

	var entry Entry
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if entry.Attributes["key1"] != "value1" {
		t.Errorf("expected key1='value1', got %v", entry.Attributes["key1"])
	}
	if entry.Attributes["key2"] != float64(42) { // JSON unmarshals numbers as float64
		t.Errorf("expected key2=42, got %v", entry.Attributes["key2"])
	}
	if entry.Attributes["key3"] != true {
		t.Errorf("expected key3=true, got %v", entry.Attributes["key3"])
	}
}

func TestLoggerNilStdoutWriter(t *testing.T) {
	// Should not panic with nil stdout writer
	logger := &Logger{
		stdoutWriter:     nil,
		enableStructured: true,
	}

	// This should not panic
	logger.Log(Entry{
		Severity: LevelInfo,
		Message:  "Test",
	})
}

func TestLoggerConcurrency(t *testing.T) {
	var buf bytes.Buffer
	logger := &Logger{
		stdoutWriter:     &buf,
		enableStructured: true,
	}

	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(n int) {
			logger.Infof("Message %d", n)
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify output contains messages (not checking order due to concurrency)
	output := buf.String()
	if len(output) == 0 {
		t.Error("expected some output from concurrent logging")
	}
}

func TestEntryJSONMarshalling(t *testing.T) {
	entry := Entry{
		Timestamp:  time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
		Severity:   LevelInfo,
		Message:    "Test message",
		Component:  "component",
		Operator:   "operator",
		PlanID:     "plan-123",
		AuditID:    "audit-456",
		Ticket:     "JIRA-789",
		Attributes: map[string]interface{}{"key": "value"},
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("failed to marshal entry: %v", err)
	}

	var parsed Entry
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("failed to unmarshal entry: %v", err)
	}

	if parsed.Message != entry.Message {
		t.Errorf("message mismatch: %q vs %q", parsed.Message, entry.Message)
	}
	if parsed.Severity != entry.Severity {
		t.Errorf("severity mismatch: %s vs %s", parsed.Severity, entry.Severity)
	}
	if parsed.Component != entry.Component {
		t.Errorf("component mismatch: %q vs %q", parsed.Component, entry.Component)
	}
}
