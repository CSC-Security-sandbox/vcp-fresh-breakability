package parser

import (
	"testing"
)

func TestNewValidator(t *testing.T) {
	// Test default validator
	v := NewValidator()
	if !v.requireWhere {
		t.Error("expected requireWhere to be true by default")
	}
	if !v.blockDangerous {
		t.Error("expected blockDangerous to be true by default")
	}
	if len(v.allowedTables) != 0 {
		t.Error("expected allowedTables to be empty by default")
	}
}

func TestNewValidatorWithOptions(t *testing.T) {
	tables := []string{"users", "Jobs"}
	v := NewValidator(
		WithAllowedTables(tables),
		WithRequireWhere(false),
		WithBlockDangerous(false),
	)

	if v.requireWhere {
		t.Error("expected requireWhere to be false")
	}
	if v.blockDangerous {
		t.Error("expected blockDangerous to be false")
	}
	if len(v.allowedTables) != 2 {
		t.Errorf("expected 2 allowed tables, got %d", len(v.allowedTables))
	}
	if !v.allowedTables["users"] {
		t.Error("expected 'users' in allowedTables")
	}
	if !v.allowedTables["jobs"] {
		t.Error("expected 'jobs' (lowercase) in allowedTables")
	}
}

func TestValidateRequireWhere(t *testing.T) {
	v := NewValidator(WithRequireWhere(true))

	tests := []struct {
		name    string
		sql     string
		isValid bool
	}{
		{
			name:    "UPDATE with WHERE",
			sql:     "UPDATE users SET status = 'active' WHERE id = 1",
			isValid: true,
		},
		{
			name:    "UPDATE without WHERE",
			sql:     "UPDATE users SET status = 'active'",
			isValid: false,
		},
		{
			name:    "DELETE with WHERE",
			sql:     "DELETE FROM users WHERE id = 1",
			isValid: true,
		},
		{
			name:    "DELETE without WHERE",
			sql:     "DELETE FROM users",
			isValid: false,
		},
		{
			name:    "SELECT without WHERE is OK",
			sql:     "SELECT * FROM users",
			isValid: true,
		},
		{
			name:    "INSERT without WHERE is OK",
			sql:     "INSERT INTO users (name) VALUES ('test')",
			isValid: true,
		},
	}

	p := New(nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseResult, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)
			if result.Valid != tt.isValid {
				t.Errorf("expected Valid=%v, got %v, errors: %v", tt.isValid, result.Valid, result.Errors)
			}
		})
	}
}

func TestValidateDangerousWhere(t *testing.T) {
	v := NewValidator(WithBlockDangerous(true))
	p := New(nil)

	tests := []struct {
		name    string
		sql     string
		isValid bool
	}{
		{
			name:    "1=1 is dangerous",
			sql:     "UPDATE users SET status = 'active' WHERE 1=1",
			isValid: false,
		},
		{
			name:    "true is dangerous",
			sql:     "DELETE FROM users WHERE true",
			isValid: false,
		},
		{
			name:    "0=0 is dangerous",
			sql:     "UPDATE users SET status = 'x' WHERE 0=0",
			isValid: false,
		},
		{
			name:    "normal where is OK",
			sql:     "UPDATE users SET status = 'active' WHERE id = 1",
			isValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseResult, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)
			if result.Valid != tt.isValid {
				t.Errorf("expected Valid=%v, got %v", tt.isValid, result.Valid)
			}
		})
	}
}

func TestValidateTableWhitelist(t *testing.T) {
	v := NewValidator(
		WithAllowedTables([]string{"users", "jobs"}),
		WithRequireWhere(false),
	)
	p := New(nil)

	tests := []struct {
		name    string
		sql     string
		isValid bool
	}{
		{
			name:    "allowed table users",
			sql:     "SELECT * FROM users",
			isValid: true,
		},
		{
			name:    "allowed table jobs",
			sql:     "UPDATE jobs SET status = 'done'",
			isValid: true,
		},
		{
			name:    "disallowed table",
			sql:     "SELECT * FROM secrets",
			isValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseResult, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)
			if result.Valid != tt.isValid {
				t.Errorf("expected Valid=%v, got %v", tt.isValid, result.Valid)
			}
		})
	}
}

func TestValidateDDLBlocked(t *testing.T) {
	v := NewValidator()
	p := New(nil)

	ddlStatements := []string{
		"CREATE TABLE users (id INT)",
		"ALTER TABLE users ADD COLUMN name VARCHAR(100)",
		"DROP TABLE users",
		"TRUNCATE TABLE users",
		"GRANT SELECT ON users TO admin",
		"REVOKE SELECT ON users FROM admin",
	}

	for _, sql := range ddlStatements {
		t.Run(sql, func(t *testing.T) {
			parseResult, err := p.Parse(sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)
			if result.Valid {
				t.Errorf("expected DDL statement to be invalid: %s", sql)
			}

			// Check for DDL_NOT_ALLOWED error
			found := false
			for _, e := range result.Errors {
				if e.Rule == "DDL_NOT_ALLOWED" {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected DDL_NOT_ALLOWED error for: %s", sql)
			}
		})
	}
}

func TestValidateBroadWhereWarning(t *testing.T) {
	v := NewValidator()
	p := New(nil)

	tests := []struct {
		name        string
		sql         string
		wantWarning bool
	}{
		{
			name:        "no specific identifier warns",
			sql:         "UPDATE users SET status = 'x' WHERE name = 'John'",
			wantWarning: true,
		},
		{
			name:        "id in WHERE is OK",
			sql:         "UPDATE users SET status = 'x' WHERE id = 1",
			wantWarning: false,
		},
		{
			name:        "uuid in WHERE is OK",
			sql:         "UPDATE users SET status = 'x' WHERE uuid = 'abc-123'",
			wantWarning: false,
		},
		{
			name:        "user_id in WHERE is OK",
			sql:         "DELETE FROM orders WHERE user_id = 1",
			wantWarning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseResult, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)

			hasBroadWarning := false
			for _, w := range result.Warnings {
				if w.Rule == "BROAD_WHERE" {
					hasBroadWarning = true
					break
				}
			}

			if hasBroadWarning != tt.wantWarning {
				t.Errorf("expected BROAD_WHERE warning=%v, got %v", tt.wantWarning, hasBroadWarning)
			}
		})
	}
}

func TestValidateLimitWithoutOrderByWarning(t *testing.T) {
	v := NewValidator()
	p := New(nil)

	tests := []struct {
		name        string
		sql         string
		wantWarning bool
	}{
		{
			name:        "LIMIT without ORDER BY warns",
			sql:         "DELETE FROM users WHERE status = 'x' LIMIT 10",
			wantWarning: true,
		},
		{
			name:        "LIMIT with ORDER BY is OK",
			sql:         "DELETE FROM users WHERE status = 'x' ORDER BY id LIMIT 10",
			wantWarning: false,
		},
		{
			name:        "no LIMIT is OK",
			sql:         "DELETE FROM users WHERE status = 'x'",
			wantWarning: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parseResult, err := p.Parse(tt.sql)
			if err != nil {
				t.Fatalf("failed to parse: %v", err)
			}

			result := v.Validate(parseResult)

			hasWarning := false
			for _, w := range result.Warnings {
				if w.Rule == "LIMIT_WITHOUT_ORDER" {
					hasWarning = true
					break
				}
			}

			if hasWarning != tt.wantWarning {
				t.Errorf("expected LIMIT_WITHOUT_ORDER warning=%v, got %v", tt.wantWarning, hasWarning)
			}
		})
	}
}

func TestValidationErrorString(t *testing.T) {
	err := ValidationError{
		Rule:        "TEST_RULE",
		Description: "test description",
		Severity:    "error",
	}

	expected := "[error] TEST_RULE: test description"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

func TestValidateMetadata(t *testing.T) {
	v := NewValidator()

	tests := []struct {
		name        string
		metadata    QueryMetadata
		wantErrors  int
		wantTicket  bool
		wantAuthor  bool
	}{
		{
			name:        "valid metadata",
			metadata:    QueryMetadata{Ticket: "JIRA-123", Author: "john@example.com"},
			wantErrors:  0,
			wantTicket:  false,
			wantAuthor:  false,
		},
		{
			name:        "missing ticket",
			metadata:    QueryMetadata{Author: "john@example.com"},
			wantErrors:  1,
			wantTicket:  true,
			wantAuthor:  false,
		},
		{
			name:        "missing author (warning only)",
			metadata:    QueryMetadata{Ticket: "JIRA-123"},
			wantErrors:  1,
			wantTicket:  false,
			wantAuthor:  true,
		},
		{
			name:        "missing both",
			metadata:    QueryMetadata{},
			wantErrors:  2,
			wantTicket:  true,
			wantAuthor:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errors := v.ValidateMetadata(&tt.metadata)
			if len(errors) != tt.wantErrors {
				t.Errorf("expected %d errors, got %d: %v", tt.wantErrors, len(errors), errors)
			}

			hasTicketError := false
			hasAuthorError := false
			for _, e := range errors {
				if e.Rule == "MISSING_TICKET" {
					hasTicketError = true
				}
				if e.Rule == "MISSING_AUTHOR" {
					hasAuthorError = true
				}
			}

			if hasTicketError != tt.wantTicket {
				t.Errorf("expected ticket error=%v, got %v", tt.wantTicket, hasTicketError)
			}
			if hasAuthorError != tt.wantAuthor {
				t.Errorf("expected author error=%v, got %v", tt.wantAuthor, hasAuthorError)
			}
		})
	}
}

func TestIsDangerousWhere(t *testing.T) {
	tests := []struct {
		whereClause string
		isDangerous bool
	}{
		{"1=1", true},
		{" 1 = 1 ", true},
		{"true", true},
		{" TRUE ", true},
		{"'1'='1'", true},
		{"0=0", true},
		{"1", true},
		{"id = 1", false},
		{"status = 'active'", false},
		{"age > 18", false},
	}

	for _, tt := range tests {
		t.Run(tt.whereClause, func(t *testing.T) {
			if isDangerousWhere(tt.whereClause) != tt.isDangerous {
				t.Errorf("expected isDangerousWhere(%q) = %v", tt.whereClause, tt.isDangerous)
			}
		})
	}
}

func TestIsDDLStatement(t *testing.T) {
	tests := []struct {
		sql   string
		isDDL bool
	}{
		{"CREATE TABLE users", true},
		{"create table users", true},
		{"  CREATE TABLE users", true},
		{"ALTER TABLE users", true},
		{"DROP TABLE users", true},
		{"TRUNCATE TABLE users", true},
		{"GRANT SELECT ON users", true},
		{"REVOKE SELECT ON users", true},
		{"SELECT * FROM users", false},
		{"INSERT INTO users", false},
		{"UPDATE users SET", false},
		{"DELETE FROM users", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if isDDLStatement(tt.sql) != tt.isDDL {
				t.Errorf("expected isDDLStatement(%q) = %v", tt.sql, tt.isDDL)
			}
		})
	}
}

func TestHasSpecificIdentifier(t *testing.T) {
	tests := []struct {
		whereClause string
		hasID       bool
	}{
		{"id = 1", true},
		{"uuid = 'abc'", true},
		{"user_id = 1", true},
		{"order_uuid = 'x'", true},
		{"name = 'John'", false},
		{"status = 'active'", false},
		{"age > 18", false},
	}

	for _, tt := range tests {
		t.Run(tt.whereClause, func(t *testing.T) {
			if hasSpecificIdentifier(tt.whereClause) != tt.hasID {
				t.Errorf("expected hasSpecificIdentifier(%q) = %v", tt.whereClause, tt.hasID)
			}
		})
	}
}

func TestHasLimit(t *testing.T) {
	tests := []struct {
		sql      string
		hasLimit bool
	}{
		{"SELECT * FROM users LIMIT 10", true},
		{"SELECT * FROM users limit 10", true},
		{"DELETE FROM users WHERE id = 1 LIMIT 1", true},
		{"SELECT * FROM users", false},
		{"SELECT * FROM users WHERE name LIKE '%limit%'", true}, // false positive but OK
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if hasLimit(tt.sql) != tt.hasLimit {
				t.Errorf("expected hasLimit(%q) = %v", tt.sql, tt.hasLimit)
			}
		})
	}
}

func TestHasOrderBy(t *testing.T) {
	tests := []struct {
		sql        string
		hasOrderBy bool
	}{
		{"SELECT * FROM users ORDER BY id", true},
		{"SELECT * FROM users order by id", true},
		{"SELECT * FROM users ORDER  BY id", true},
		{"SELECT * FROM users", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if hasOrderBy(tt.sql) != tt.hasOrderBy {
				t.Errorf("expected hasOrderBy(%q) = %v", tt.sql, tt.hasOrderBy)
			}
		})
	}
}

func TestValidationResultCollectsAllErrors(t *testing.T) {
	v := NewValidator(
		WithRequireWhere(true),
		WithBlockDangerous(true),
	)
	p := New(nil)

	// This should trigger multiple errors
	sql := "CREATE TABLE test"
	parseResult, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("failed to parse: %v", err)
	}

	result := v.Validate(parseResult)
	if result.Valid {
		t.Error("expected validation to fail")
	}
	if len(result.Errors) == 0 {
		t.Error("expected at least one error")
	}
}
