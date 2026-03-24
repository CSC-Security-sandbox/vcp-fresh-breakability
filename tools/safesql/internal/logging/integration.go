// Package logging provides integration with existing slog logger.
package logging

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
)

// IntegratedLogger wraps slog.Logger and adds concurrent journald/cloud logging.
type IntegratedLogger struct {
	slogLogger *slog.Logger
	logger     *Logger
}

// NewIntegratedLogger creates a logger that outputs to both slog and the new logging system.
func NewIntegratedLogger(enableJournald bool, component string) *IntegratedLogger {
	// Create slog logger (for stdout)
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	slogLogger := slog.New(handler)

	// Create new logger (for journald only - no stdout duplication)
	logger := &Logger{
		mu:               sync.Mutex{},
		stdoutWriter:     nil, // Disable stdout in this logger
		journaldWriter:   NewJournaldWriter(),
		enableJournald:   enableJournald,
		enableStructured: false,
		component:        component,
	}

	return &IntegratedLogger{
		slogLogger: slogLogger,
		logger:     logger,
	}
}

// Info logs an info message to both outputs.
func (l *IntegratedLogger) Info(msg string) {
	// Output to stdout via slog (preserves existing behavior)
	fmt.Print(msg)
	
	// Also log to journald (logger will handle journald availability internally)
	l.logger.Log(Entry{
		Severity: LevelInfo,
		Message:  msg,
	})
}

// Infof logs a formatted info message.
func (l *IntegratedLogger) Infof(format string, args ...interface{}) {
	l.Info(fmt.Sprintf(format, args...))
}

// WithContext returns a context logger.
func (l *IntegratedLogger) WithContext(planID, auditID, ticket, operator string) *IntegratedContextLogger {
	return &IntegratedContextLogger{
		integrated: l,
		planID:     planID,
		auditID:    auditID,
		ticket:     ticket,
		operator:   operator,
	}
}

// IntegratedContextLogger wraps IntegratedLogger with context.
type IntegratedContextLogger struct {
	integrated *IntegratedLogger
	planID     string
	auditID    string
	ticket     string
	operator   string
}

// Info logs an info message with context.
func (c *IntegratedContextLogger) Info(msg string) {
	// Output to stdout
	fmt.Print(msg)
	
	// Log to journald with context (logger will handle journald availability internally)
	c.integrated.logger.Log(Entry{
		Severity: LevelInfo,
		Message:  msg,
		PlanID:   c.planID,
		AuditID:  c.auditID,
		Ticket:   c.ticket,
		Operator: c.operator,
	})
}

// Infof logs a formatted info message with context.
func (c *IntegratedContextLogger) Infof(format string, args ...interface{}) {
	c.Info(fmt.Sprintf(format, args...))
}
