// Package logging provides journald integration.
package logging

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// JournaldWriter writes log entries to systemd journald.
type JournaldWriter interface {
	Write(entry Entry) error
}

// journaldWriter implements JournaldWriter using systemd-cat.
type journaldWriter struct {
	enabled bool
}

// NewJournaldWriter creates a new journald writer.
func NewJournaldWriter() JournaldWriter {
	// Check if systemd-cat is available
	_, err := exec.LookPath("systemd-cat")
	return &journaldWriter{
		enabled: err == nil,
	}
}

// Write sends a log entry to journald.
func (w *journaldWriter) Write(entry Entry) error {
	if !w.enabled {
		return nil // Silently skip if journald is not available
	}

	// Convert entry to journald format
	// Use systemd-cat with priority and identifier
	priority := severityToPriority(entry.Severity)
	identifier := "safesql"
	// Note: Don't use brackets in identifier as systemd-cat treats them specially
	// Component will be included in the message metadata instead

	// Build structured message with metadata
	message := entry.Message
	metadata := make(map[string]string)
	
	// Always include component if present
	if entry.Component != "" {
		metadata["component"] = entry.Component
	}
	if entry.PlanID != "" {
		metadata["plan_id"] = entry.PlanID
	}
	if entry.AuditID != "" {
		metadata["audit_id"] = entry.AuditID
	}
	if entry.Ticket != "" {
		metadata["ticket"] = entry.Ticket
	}
	if entry.Operator != "" {
		metadata["operator"] = entry.Operator
	}
	
	// Add metadata to message if any exists
	if len(metadata) > 0 {
		metaJSON, _ := json.Marshal(metadata)
		message = fmt.Sprintf("%s | metadata=%s", message, string(metaJSON))
	}

	// Execute systemd-cat
	cmd := exec.Command("systemd-cat", "-t", identifier, "-p", priority)
	cmd.Stdin = strings.NewReader(message)

	return cmd.Run()
}

// severityToPriority converts log level to syslog priority for journald.
func severityToPriority(level Level) string {
	switch level {
	case LevelDebug:
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarning:
		return "warning"
	case LevelError:
		return "err"
	case LevelCritical:
		return "crit"
	default:
		return "info"
	}
}
