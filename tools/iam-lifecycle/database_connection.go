package main

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/lib/pq"
)

func connectAdmin(cfg config, dbName string) (*sql.DB, error) {
	dsn := adminDSN(cfg, dbName)
	return openWithRetry(dsn)
}

func connectIAM(cfg config, dbName, iamUser, port string) (*sql.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
		cfg.dbHost, port, iamUser, dbName, cfg.dbSSLMode)
	return openWithRetry(dsn)
}

func adminDSN(cfg config, dbName string) string {
	if cfg.adminPass == "" {
		return fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=%s",
			cfg.dbHost, cfg.dbPort, cfg.adminUser, dbName, cfg.dbSSLMode)
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.dbHost, cfg.dbPort, cfg.adminUser, cfg.adminPass, dbName, cfg.dbSSLMode)
}

func openWithRetry(dsn string) (*sql.DB, error) {
	return openWithRetryN(dsn, 10)
}

func openWithRetryN(dsn string, maxRetries int) (*sql.DB, error) {
	// Add connect_timeout if not present (prevents indefinite hangs)
	if !containsTimeout(dsn) {
		dsn += " connect_timeout=10"
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)

	var lastErr error
	for i := range maxRetries {
		if err := db.Ping(); err == nil {
			slog.Info("database connection established", "attempt", i+1)
			return db, nil
		} else {
			lastErr = err
			delay := min(time.Duration(500*(1<<uint(i)))*time.Millisecond, 30*time.Second)
			slog.Warn("db connect retry", "attempt", i+1, "delay", delay, "error", sanitizeError(err))
			time.Sleep(delay)
		}
	}
	_ = db.Close()
	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, sanitizeError(lastErr))
}

func containsTimeout(dsn string) bool {
	return containsString(dsn, "connect_timeout") || containsString(dsn, "timeout")
}

func containsString(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	// Remove password from error messages
	if containsString(msg, "password") {
		return fmt.Errorf("authentication error (credentials redacted)")
	}
	return err
}

func execSQL(db *sql.DB, stmt string) error {
	preview := stmt
	if len(preview) > 120 {
		preview = preview[:120] + "..."
	}
	// Sanitize any potential sensitive data from logs
	preview = sanitizeSQL(preview)
	slog.Debug("SQL", "stmt", preview)
	if _, err := db.Exec(stmt); err != nil {
		slog.Error("SQL failed", "error", err, "stmt", preview)
		return err
	}
	return nil
}

func sanitizeSQL(sql string) string {
	// Simple check - if it contains password-like keywords, redact
	if containsString(sql, "PASSWORD") || containsString(sql, "password") {
		return "[SQL statement with credentials redacted]"
	}
	return sql
}
