// Package database provides database connectivity for SafeSQL.
package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/lib/pq"
)

// Client handles database operations.
type Client struct {
	db *sql.DB
}

// Config holds database connection settings.
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
	UseIAM   bool
}

// New creates a new database client.
func New(cfg Config) (*Client, error) {
	password := cfg.Password
	if cfg.UseIAM {
		password = ""
	}

	// Quote values that may contain special characters (e.g. "@" in IAM emails).
	// pq key=value format: single-quote values, escape interior single-quotes as \'.
	dsn := fmt.Sprintf("host=%s port=%s user='%s' password='%s' dbname='%s' sslmode=%s",
		cfg.Host, cfg.Port,
		escapePQValue(cfg.User),
		escapePQValue(password),
		escapePQValue(cfg.DBName),
		cfg.SSLMode)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Verify connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &Client{db: db}, nil
}

// Close closes the database connection.
func (c *Client) Close() error {
	return c.db.Close()
}

// CountAffectedRows executes a COUNT query to determine rows that would be affected.
func (c *Client) CountAffectedRows(ctx context.Context, table, whereClause string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return count, nil
}

// GetRowsSnapshot fetches the current state of rows that would be affected.
func (c *Client) GetRowsSnapshot(ctx context.Context, table, whereClause string, limit int) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = convertValue(values[i])
		}
		result = append(result, rowMap)
	}

	return result, rows.Err()
}

// escapePQValue escapes a value for use inside single-quoted pq DSN fields.
func escapePQValue(s string) string {
	// In pq key=value format, single-quoted values escape ' as \' and \ as \\.
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return s
}

func convertValue(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format(time.RFC3339)
	default:
		return val
	}
}

// GetExplainPlan returns the execution plan for a query.
func (c *Client) GetExplainPlan(ctx context.Context, query string) (string, error) {
	explainQuery := "EXPLAIN (FORMAT JSON) " + query

	var planJSON string
	err := c.db.QueryRowContext(ctx, explainQuery).Scan(&planJSON)
	if err != nil {
		return "", fmt.Errorf("failed to get explain plan: %w", err)
	}

	return planJSON, nil
}

// GetEstimatedRows extracts estimated row count from EXPLAIN output.
func (c *Client) GetEstimatedRows(ctx context.Context, query string) (int64, error) {
	planJSON, err := c.GetExplainPlan(ctx, query)
	if err != nil {
		return 0, err
	}

	var plan []struct {
		Plan struct {
			PlanRows int64 `json:"Plan Rows"`
		} `json:"Plan"`
	}

	if err := json.Unmarshal([]byte(planJSON), &plan); err != nil {
		return 0, fmt.Errorf("failed to parse explain plan: %w", err)
	}

	if len(plan) > 0 {
		return plan[0].Plan.PlanRows, nil
	}

	return 0, nil
}

// ExecuteInTransaction executes a query within a transaction.
func (c *Client) ExecuteInTransaction(ctx context.Context, query string, confirm func(rowsAffected int64) bool) (int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the query
	result, err := tx.ExecContext(ctx, query)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	// Ask for confirmation
	if !confirm(rowsAffected) {
		tx.Rollback()
		return rowsAffected, fmt.Errorf("execution cancelled by user")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return rowsAffected, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return rowsAffected, nil
}

// ExecuteMultipleInTransaction executes multiple queries in a single transaction.
func (c *Client) ExecuteMultipleInTransaction(ctx context.Context, queries []string, confirm func(results []int64) bool) ([]int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	var results []int64

	// Execute all queries
	for i, query := range queries {
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to execute query %d: %w", i+1, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to get rows affected for query %d: %w", i+1, err)
		}
		results = append(results, rowsAffected)
	}

	// Ask for confirmation
	if !confirm(results) {
		tx.Rollback()
		return results, fmt.Errorf("execution cancelled by user")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return results, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return results, nil
}

// TableExists checks if a table exists in the database.
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)`

	var exists bool
	err := c.db.QueryRowContext(ctx, query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return exists, nil
}

// GetTableColumns returns column names and types for a table.
func (c *Client) GetTableColumns(ctx context.Context, tableName string) (map[string]string, error) {
	query := `
		SELECT column_name, data_type 
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1`

	rows, err := c.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var name, dtype string
		if err := rows.Scan(&name, &dtype); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		columns[name] = dtype
	}

	return columns, rows.Err()
}

// VerificationQueryInfo contains verification query information.
type VerificationQueryInfo struct {
	StatementIndex int
	Type           string // "pre" or "post"
	SQL            string
	ExpectedCount  int64
}

// ExecuteWithVerification executes queries with pre/post verification SELECT queries.
// Verification queries are executed before and after each UPDATE/DELETE statement.
func (c *Client) ExecuteWithVerification(
	ctx context.Context,
	queries []string,
	verificationQueries []VerificationQueryInfo, // Pre/post queries from plan
	confirm func(results []int64) bool,
) ([]int64, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	var results []int64

	// Map verification queries by statement index and type
	preQueries := make(map[int]VerificationQueryInfo)  // statementIndex -> pre query
	postQueries := make(map[int]VerificationQueryInfo) // statementIndex -> post query

	for _, vq := range verificationQueries {
		if vq.Type == "pre" {
			preQueries[vq.StatementIndex] = vq
		} else if vq.Type == "post" {
			postQueries[vq.StatementIndex] = vq
		}
	}

	// Execute queries with verification
	for i, query := range queries {
		// Pre-execution verification
		if preQuery, exists := preQueries[i]; exists {
			preCount, err := c.executeVerificationQuery(ctx, tx, preQuery.SQL)
			if err != nil {
				tx.Rollback()
				return nil, fmt.Errorf("failed to execute pre-verification query for statement %d: %w", i+1, err)
			}

			// Verify count matches expected
			if preCount != preQuery.ExpectedCount {
				tx.Rollback()
				return nil, fmt.Errorf(
					"pre-execution verification failed for statement %d - Expected: %d, Actual: %d. Data may have changed since plan creation",
					i+1, preQuery.ExpectedCount, preCount,
				)
			}
		}

		// Execute the actual query
		result, err := tx.ExecContext(ctx, query)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to execute query %d: %w", i+1, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to get rows affected for query %d: %w", i+1, err)
		}
		results = append(results, rowsAffected)

		// Post-execution verification
		if postQuery, exists := postQueries[i]; exists {
			postCount, err := c.executeVerificationQuery(ctx, tx, postQuery.SQL)
			if err != nil {
				tx.Rollback()
				return nil, fmt.Errorf("failed to execute post-verification query for statement %d: %w", i+1, err)
			}

			// For UPDATE: count should remain the same
			// For DELETE: count should be 0 (all matching rows deleted)
			if postCount != postQuery.ExpectedCount {
				tx.Rollback()
				return nil, fmt.Errorf(
					"post-execution verification failed for statement %d - Expected remaining rows: %d, Actual: %d, RowsAffected: %d",
					i+1, postQuery.ExpectedCount, postCount, rowsAffected,
				)
			}
		}

		// CRITICAL: Verify rowsAffected matches pre-execution count
		// This ensures the UPDATE/DELETE affected exactly the number of rows we expected
		if preQuery, preExists := preQueries[i]; preExists {
			if rowsAffected != preQuery.ExpectedCount {
				tx.Rollback()
				return nil, fmt.Errorf(
					"rows affected mismatch for statement %d - Pre-execution COUNT(*): %d, PostgreSQL RowsAffected: %d. Operation aborted for safety",
					i+1, preQuery.ExpectedCount, rowsAffected,
				)
			}
		}
	}

	// Ask for confirmation
	if !confirm(results) {
		tx.Rollback()
		return results, fmt.Errorf("execution cancelled by user")
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return results, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return results, nil
}

// executeVerificationQuery executes a SELECT COUNT(*) query within a transaction.
func (c *Client) executeVerificationQuery(ctx context.Context, tx *sql.Tx, query string) (int64, error) {
	var count int64
	err := tx.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to execute verification query '%s': %w", query, err)
	}
	return count, nil
}
