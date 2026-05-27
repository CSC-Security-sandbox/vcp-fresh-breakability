package parser

import (
	"testing"
)

func TestNew(t *testing.T) {
	// Test with nil allowed tables
	p := New(nil)
	if p == nil {
		t.Fatal("expected non-nil parser")
	}
	if len(p.allowedTables) != 0 {
		t.Errorf("expected empty allowedTables, got %d entries", len(p.allowedTables))
	}

	// Test with allowed tables
	tables := []string{"users", "Jobs", "ORDERS"}
	p = New(tables)
	if len(p.allowedTables) != 3 {
		t.Errorf("expected 3 allowedTables, got %d", len(p.allowedTables))
	}
	// Check lowercase conversion
	if !p.allowedTables["users"] {
		t.Error("expected 'users' in allowedTables")
	}
	if !p.allowedTables["jobs"] {
		t.Error("expected 'jobs' (lowercase) in allowedTables")
	}
	if !p.allowedTables["orders"] {
		t.Error("expected 'orders' (lowercase) in allowedTables")
	}
}

func TestParseSelectStatement(t *testing.T) {
	p := New(nil)

	sql := `SELECT * FROM users WHERE id = 1`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Statements))
	}

	stmt := result.Statements[0]
	if stmt.Type != StatementSelect {
		t.Errorf("expected StatementSelect, got %s", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "users" {
		t.Errorf("expected table 'users', got %v", stmt.Tables)
	}
	if !stmt.HasWhereClause {
		t.Error("expected HasWhereClause to be true")
	}
	if stmt.WhereClause != "id = 1" {
		t.Errorf("expected WhereClause 'id = 1', got %q", stmt.WhereClause)
	}
}

func TestParseUpdateStatement(t *testing.T) {
	p := New(nil)

	sql := `UPDATE jobs SET status = 'completed' WHERE id = 123`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := result.Statements[0]
	if stmt.Type != StatementUpdate {
		t.Errorf("expected StatementUpdate, got %s", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "jobs" {
		t.Errorf("expected table 'jobs', got %v", stmt.Tables)
	}
	if !stmt.HasWhereClause {
		t.Error("expected HasWhereClause to be true")
	}
}

func TestParseDeleteStatement(t *testing.T) {
	p := New(nil)

	sql := `DELETE FROM orders WHERE created_at < '2024-01-01'`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := result.Statements[0]
	if stmt.Type != StatementDelete {
		t.Errorf("expected StatementDelete, got %s", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "orders" {
		t.Errorf("expected table 'orders', got %v", stmt.Tables)
	}
	if !stmt.HasWhereClause {
		t.Error("expected HasWhereClause to be true")
	}
}

func TestParseInsertStatement(t *testing.T) {
	p := New(nil)

	sql := `INSERT INTO users (name, email) VALUES ('John', 'john@example.com')`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := result.Statements[0]
	if stmt.Type != StatementInsert {
		t.Errorf("expected StatementInsert, got %s", stmt.Type)
	}
	if len(stmt.Tables) != 1 || stmt.Tables[0] != "users" {
		t.Errorf("expected table 'users', got %v", stmt.Tables)
	}
}

func TestParseMultipleStatements(t *testing.T) {
	p := New(nil)

	sql := `
		SELECT * FROM users WHERE id = 1;
		UPDATE jobs SET status = 'done' WHERE id = 2;
		DELETE FROM logs WHERE age > 30
	`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.Statements) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(result.Statements))
	}

	if result.Statements[0].Type != StatementSelect {
		t.Errorf("expected first statement to be SELECT, got %s", result.Statements[0].Type)
	}
	if result.Statements[1].Type != StatementUpdate {
		t.Errorf("expected second statement to be UPDATE, got %s", result.Statements[1].Type)
	}
	if result.Statements[2].Type != StatementDelete {
		t.Errorf("expected third statement to be DELETE, got %s", result.Statements[2].Type)
	}
}

func TestParseStatementWithoutWhere(t *testing.T) {
	p := New(nil)

	sql := `UPDATE users SET status = 'active'`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := result.Statements[0]
	if stmt.HasWhereClause {
		t.Error("expected HasWhereClause to be false")
	}
	if stmt.WhereClause != "" {
		t.Errorf("expected empty WhereClause, got %q", stmt.WhereClause)
	}
}

func TestExtractMetadata(t *testing.T) {
	p := New(nil)

	sql := `
-- TICKET: JIRA-1234
-- AUTHOR: john.doe@example.com
-- DESCRIPTION: Clean up stale jobs
-- AFFECTED_ROWS_EXPECTED: 100

DELETE FROM jobs WHERE status = 'stale'
`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Metadata.Ticket != "JIRA-1234" {
		t.Errorf("expected Ticket 'JIRA-1234', got %q", result.Metadata.Ticket)
	}
	if result.Metadata.Author != "john.doe@example.com" {
		t.Errorf("expected Author 'john.doe@example.com', got %q", result.Metadata.Author)
	}
	if result.Metadata.Description != "Clean up stale jobs" {
		t.Errorf("expected Description 'Clean up stale jobs', got %q", result.Metadata.Description)
	}
	if result.Metadata.AffectedRowsExpected != 100 {
		t.Errorf("expected AffectedRowsExpected 100, got %d", result.Metadata.AffectedRowsExpected)
	}
}

func TestExtractMetadataCaseInsensitive(t *testing.T) {
	p := New(nil)

	sql := `
-- ticket: LOWER-123
-- author: jane.doe
-- description: Test

SELECT 1
`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Metadata.Ticket != "LOWER-123" {
		t.Errorf("expected Ticket 'LOWER-123', got %q", result.Metadata.Ticket)
	}
	if result.Metadata.Author != "jane.doe" {
		t.Errorf("expected Author 'jane.doe', got %q", result.Metadata.Author)
	}
}

func TestRemoveComments(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single line comment",
			input:    "SELECT * FROM users -- this is a comment",
			expected: "SELECT * FROM users ",
		},
		{
			name:     "multiple single line comments",
			input:    "-- comment 1\nSELECT * -- comment 2\nFROM users",
			expected: "\nSELECT * \nFROM users",
		},
		{
			name:     "block comment",
			input:    "SELECT /* inline comment */ * FROM users",
			expected: "SELECT  * FROM users",
		},
		{
			name:     "multiline block comment",
			input:    "SELECT /*\n  multiline\n  comment\n*/ * FROM users",
			expected: "SELECT  * FROM users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := removeComments(tt.input)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestSplitStatements(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{
			name:     "single statement",
			input:    "SELECT * FROM users",
			expected: 1,
		},
		{
			name:     "two statements",
			input:    "SELECT * FROM users; DELETE FROM logs",
			expected: 2,
		},
		{
			name:     "statement with string containing semicolon",
			input:    "INSERT INTO users (name) VALUES ('test; value')",
			expected: 1,
		},
		{
			name:     "multiple statements with newlines",
			input:    "SELECT 1;\n\nSELECT 2;\nSELECT 3",
			expected: 3,
		},
		{
			name:     "escaped quote in string",
			input:    "INSERT INTO users (name) VALUES ('test''s value')",
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitStatements(tt.input)
			if len(result) != tt.expected {
				t.Errorf("expected %d statements, got %d: %v", tt.expected, len(result), result)
			}
		})
	}
}

func TestDetectStatementType(t *testing.T) {
	tests := []struct {
		sql      string
		expected StatementType
	}{
		{"SELECT * FROM users", StatementSelect},
		{"select * from users", StatementSelect},
		{"  SELECT * FROM users", StatementSelect},
		{"INSERT INTO users VALUES (1)", StatementInsert},
		{"insert into users values (1)", StatementInsert},
		{"UPDATE users SET name = 'x'", StatementUpdate},
		{"update users set name = 'x'", StatementUpdate},
		{"DELETE FROM users", StatementDelete},
		{"delete from users", StatementDelete},
		{"CREATE TABLE users", StatementOther},
		{"DROP TABLE users", StatementOther},
		{"ALTER TABLE users", StatementOther},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := detectStatementType(tt.sql)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestExtractTables(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		stmtType StatementType
		expected []string
	}{
		{
			name:     "select simple",
			sql:      "SELECT * FROM users",
			stmtType: StatementSelect,
			expected: []string{"users"},
		},
		{
			name:     "select quoted table",
			sql:      `SELECT * FROM "Users"`,
			stmtType: StatementSelect,
			expected: []string{"users"},
		},
		{
			name:     "update simple",
			sql:      "UPDATE jobs SET status = 'done'",
			stmtType: StatementUpdate,
			expected: []string{"jobs"},
		},
		{
			name:     "update quoted table",
			sql:      `UPDATE "Jobs" SET status = 'done'`,
			stmtType: StatementUpdate,
			expected: []string{"jobs"},
		},
		{
			name:     "delete simple",
			sql:      "DELETE FROM logs",
			stmtType: StatementDelete,
			expected: []string{"logs"},
		},
		{
			name:     "insert simple",
			sql:      "INSERT INTO orders (id) VALUES (1)",
			stmtType: StatementInsert,
			expected: []string{"orders"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractTables(tt.sql, tt.stmtType)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d tables, got %d: %v", len(tt.expected), len(result), result)
			}
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("expected table %q at position %d, got %q", expected, i, result[i])
				}
			}
		})
	}
}

func TestExtractWhereClause(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectedSQL string
		expectedHas bool
	}{
		{
			name:        "simple where",
			sql:         "SELECT * FROM users WHERE id = 1",
			expectedSQL: "id = 1",
			expectedHas: true,
		},
		{
			name:        "where with order by",
			sql:         "SELECT * FROM users WHERE id = 1 ORDER BY name",
			expectedSQL: "id = 1",
			expectedHas: true,
		},
		{
			name:        "where with limit",
			sql:         "SELECT * FROM users WHERE id = 1 LIMIT 10",
			expectedSQL: "id = 1",
			expectedHas: true,
		},
		{
			name:        "where with group by",
			sql:         "SELECT * FROM users WHERE id = 1 GROUP BY name",
			expectedSQL: "id = 1",
			expectedHas: true,
		},
		{
			name:        "no where clause",
			sql:         "SELECT * FROM users",
			expectedSQL: "",
			expectedHas: false,
		},
		{
			name:        "complex where",
			sql:         "SELECT * FROM users WHERE status = 'active' AND age > 18",
			expectedSQL: "status = 'active' AND age > 18",
			expectedHas: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clause, hasWhere := extractWhereClause(tt.sql)
			if hasWhere != tt.expectedHas {
				t.Errorf("expected hasWhere %v, got %v", tt.expectedHas, hasWhere)
			}
			if clause != tt.expectedSQL {
				t.Errorf("expected clause %q, got %q", tt.expectedSQL, clause)
			}
		})
	}
}

func TestStatementIsMutatingStatement(t *testing.T) {
	tests := []struct {
		stmtType StatementType
		expected bool
	}{
		{StatementSelect, false},
		{StatementInsert, true},
		{StatementUpdate, true},
		{StatementDelete, true},
		{StatementOther, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.stmtType), func(t *testing.T) {
			stmt := Statement{Type: tt.stmtType}
			if stmt.IsMutatingStatement() != tt.expected {
				t.Errorf("expected IsMutatingStatement() to return %v for %s", tt.expected, tt.stmtType)
			}
		})
	}
}

func TestParseResultFileHash(t *testing.T) {
	p := New(nil)

	sql := "SELECT * FROM users"
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.FileHash == "" {
		t.Error("expected non-empty FileHash")
	}
	if len(result.FileHash) < 10 {
		t.Error("expected FileHash to be a reasonable length")
	}
	if result.FileHash[:7] != "sha256:" {
		t.Errorf("expected FileHash to start with 'sha256:', got %q", result.FileHash[:7])
	}
}

func TestComputeHash(t *testing.T) {
	hash1 := computeHash("test content")
	hash2 := computeHash("test content")
	hash3 := computeHash("different content")

	if hash1 != hash2 {
		t.Error("expected same content to produce same hash")
	}
	if hash1 == hash3 {
		t.Error("expected different content to produce different hash")
	}
	if hash1[:7] != "sha256:" {
		t.Errorf("expected hash to start with 'sha256:', got %q", hash1[:7])
	}
}

func TestIsAlphaNum(t *testing.T) {
	tests := []struct {
		input    byte
		expected bool
	}{
		{'a', true},
		{'z', true},
		{'A', true},
		{'Z', true},
		{'0', true},
		{'9', true},
		{'_', true},
		{' ', false},
		{'.', false},
		{'-', false},
		{'@', false},
	}

	for _, tt := range tests {
		t.Run(string(tt.input), func(t *testing.T) {
			if isAlphaNum(tt.input) != tt.expected {
				t.Errorf("expected isAlphaNum(%q) to be %v", tt.input, tt.expected)
			}
		})
	}
}

func TestWhereClauseWithSubquery(t *testing.T) {
	p := New(nil)

	sql := `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100) ORDER BY name`
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stmt := result.Statements[0]
	if !stmt.HasWhereClause {
		t.Error("expected HasWhereClause to be true")
	}
	// The WHERE clause should include the subquery but exclude the top-level ORDER BY
	if stmt.WhereClause != "id IN (SELECT user_id FROM orders WHERE total > 100)" {
		t.Errorf("unexpected WhereClause: %q", stmt.WhereClause)
	}
}
