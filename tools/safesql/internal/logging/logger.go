// Package logging provides comprehensive audit and logging capabilities.
// It supports concurrent output to stdout and systemd journald, with
// structured logging compatible with Google Cloud Logging.
package logging

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

// Level represents log severity levels compatible with Cloud Logging.
type Level string

const (
	LevelDebug    Level = "DEBUG"
	LevelInfo     Level = "INFO"
	LevelWarning  Level = "WARNING"
	LevelError    Level = "ERROR"
	LevelCritical Level = "CRITICAL"
)

// Entry represents a structured log entry.
type Entry struct {
	Timestamp  time.Time              `json:"timestamp"`
	Severity   Level                  `json:"severity"`
	Message    string                 `json:"message"`
	Component  string                 `json:"component,omitempty"`
	Operator   string                 `json:"operator,omitempty"`
	PlanID     string                 `json:"plan_id,omitempty"`
	AuditID    string                 `json:"audit_id,omitempty"`
	Ticket     string                 `json:"ticket,omitempty"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// Logger provides concurrent logging to multiple outputs.
type Logger struct {
	mu               sync.Mutex
	stdoutWriter     io.Writer
	journaldWriter   JournaldWriter
	enableJournald   bool
	enableStructured bool
	component        string
	operator         string
}

// Config configures the logger.
type Config struct {
	EnableJournald   bool
	EnableStructured bool // Output JSON to stdout instead of plain text
	Component        string
	Operator         string
}

// New creates a new Logger.
func New(cfg Config) *Logger {
	l := &Logger{
		stdoutWriter:     os.Stdout,
		enableJournald:   cfg.EnableJournald,
		enableStructured: cfg.EnableStructured,
		component:        cfg.Component,
		operator:         cfg.Operator,
	}

	if cfg.EnableJournald {
		l.journaldWriter = NewJournaldWriter()
	}

	return l
}

// Log writes a log entry to all configured outputs.
func (l *Logger) Log(entry Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Set defaults
	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now().UTC()
	}
	if entry.Component == "" && l.component != "" {
		entry.Component = l.component
	}
	if entry.Operator == "" && l.operator != "" {
		entry.Operator = l.operator
	}

	// Write to stdout (only if stdoutWriter is set)
	if l.stdoutWriter != nil {
		if l.enableStructured {
			// JSON output
			data, _ := json.Marshal(entry)
			fmt.Fprintln(l.stdoutWriter, string(data))
		} else {
			// Plain text output
			timestamp := entry.Timestamp.Format("2006-01-02 15:04:05")
			fmt.Fprintf(l.stdoutWriter, "[%s] %s: %s\n", timestamp, entry.Severity, entry.Message)
		}
	}

	// Write to journald
	if l.enableJournald && l.journaldWriter != nil {
		l.journaldWriter.Write(entry)
	}
}

// Debug logs a debug message.
func (l *Logger) Debug(message string) {
	l.Log(Entry{
		Severity: LevelDebug,
		Message:  message,
	})
}

// Info logs an info message.
func (l *Logger) Info(message string) {
	l.Log(Entry{
		Severity: LevelInfo,
		Message:  message,
	})
}

// Infof logs a formatted info message.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

// Warning logs a warning message.
func (l *Logger) Warning(message string) {
	l.Log(Entry{
		Severity: LevelWarning,
		Message:  message,
	})
}

// Warningf logs a formatted warning message.
func (l *Logger) Warningf(format string, args ...interface{}) {
	l.Warning(fmt.Sprintf(format, args...))
}

// Error logs an error message.
func (l *Logger) Error(message string) {
	l.Log(Entry{
		Severity: LevelError,
		Message:  message,
	})
}

// Errorf logs a formatted error message.
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.Error(fmt.Sprintf(format, args...))
}

// Critical logs a critical message.
func (l *Logger) Critical(message string) {
	l.Log(Entry{
		Severity: LevelCritical,
		Message:  message,
	})
}

// Criticalf logs a formatted critical message.
func (l *Logger) Criticalf(format string, args ...interface{}) {
	l.Critical(fmt.Sprintf(format, args...))
}

// WithContext creates a new logger with additional context fields.
func (l *Logger) WithContext(planID, auditID, ticket string) *ContextLogger {
	return &ContextLogger{
		logger:  l,
		planID:  planID,
		auditID: auditID,
		ticket:  ticket,
	}
}

// ContextLogger wraps Logger with additional context.
type ContextLogger struct {
	logger  *Logger
	planID  string
	auditID string
	ticket  string
}

// Log writes a log entry with context.
func (c *ContextLogger) Log(entry Entry) {
	if entry.PlanID == "" {
		entry.PlanID = c.planID
	}
	if entry.AuditID == "" {
		entry.AuditID = c.auditID
	}
	if entry.Ticket == "" {
		entry.Ticket = c.ticket
	}
	c.logger.Log(entry)
}

// Info logs an info message with context.
func (c *ContextLogger) Info(message string) {
	c.Log(Entry{
		Severity: LevelInfo,
		Message:  message,
	})
}

// Infof logs a formatted info message with context.
func (c *ContextLogger) Infof(format string, args ...interface{}) {
	c.Info(fmt.Sprintf(format, args...))
}

// Warning logs a warning message with context.
func (c *ContextLogger) Warning(message string) {
	c.Log(Entry{
		Severity: LevelWarning,
		Message:  message,
	})
}

// Warningf logs a formatted warning message with context.
func (c *ContextLogger) Warningf(format string, args ...interface{}) {
	c.Warning(fmt.Sprintf(format, args...))
}

// Error logs an error message with context.
func (c *ContextLogger) Error(message string) {
	c.Log(Entry{
		Severity: LevelError,
		Message:  message,
	})
}

// Errorf logs a formatted error message with context.
func (c *ContextLogger) Errorf(format string, args ...interface{}) {
	c.Error(fmt.Sprintf(format, args...))
}
