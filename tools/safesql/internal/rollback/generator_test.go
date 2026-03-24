package rollback

import (
	"strings"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
)

func TestNew(t *testing.T) {
	g := New()
	if g == nil {
		t.Error("expected non-nil generator")
	}
}

func TestGenerateForUpdate(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET status = 'inactive' WHERE id = 1",
		Type:   parser.StatementUpdate,
		Tables: []string{"users"},
	}

	preState := []map[string]interface{}{
		{"uuid": "abc-123", "status": "active", "name": "John"},
		{"uuid": "def-456", "status": "active", "name": "Jane"},
	}

	rollbackStmts, err := g.GenerateForUpdate(stmt, preState, "uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rollbackStmts) != 2 {
		t.Fatalf("expected 2 rollback statements, got %d", len(rollbackStmts))
	}

	// Check that rollback restores original values
	for _, rb := range rollbackStmts {
		if !strings.HasPrefix(rb, "UPDATE users SET") {
			t.Errorf("expected rollback to start with 'UPDATE users SET', got %q", rb)
		}
		if !strings.Contains(rb, "status = 'active'") {
			t.Errorf("expected rollback to restore status to 'active', got %q", rb)
		}
		if !strings.Contains(rb, "WHERE uuid =") {
			t.Errorf("expected rollback to have WHERE clause with uuid, got %q", rb)
		}
	}
}

func TestGenerateForUpdateEmptyPreState(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET status = 'inactive'",
		Type:   parser.StatementUpdate,
		Tables: []string{"users"},
	}

	rollbackStmts, err := g.GenerateForUpdate(stmt, nil, "uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rollbackStmts != nil {
		t.Errorf("expected nil for empty preState, got %v", rollbackStmts)
	}
}

func TestGenerateForUpdateNoTable(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET status = 'inactive'",
		Type:   parser.StatementUpdate,
		Tables: []string{},
	}

	preState := []map[string]interface{}{
		{"uuid": "abc-123", "status": "active"},
	}

	_, err := g.GenerateForUpdate(stmt, preState, "uuid")
	if err == nil {
		t.Error("expected error for missing table")
	}
}

func TestGenerateForUpdateFallbackPrimaryKey(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET status = 'inactive'",
		Type:   parser.StatementUpdate,
		Tables: []string{"users"},
	}

	// preState with 'id' instead of 'uuid'
	preState := []map[string]interface{}{
		{"id": 1, "status": "active"},
	}

	rollbackStmts, err := g.GenerateForUpdate(stmt, preState, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rollbackStmts) != 1 {
		t.Fatalf("expected 1 rollback statement, got %d", len(rollbackStmts))
	}

	if !strings.Contains(rollbackStmts[0], "WHERE id =") {
		t.Errorf("expected fallback to use 'id' as primary key, got %q", rollbackStmts[0])
	}
}

func TestGenerateForDelete(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "DELETE FROM users WHERE status = 'inactive'",
		Type:   parser.StatementDelete,
		Tables: []string{"users"},
	}

	preState := []map[string]interface{}{
		{"id": 1, "status": "inactive", "name": "John"},
		{"id": 2, "status": "inactive", "name": "Jane"},
	}

	rollbackStmts, err := g.GenerateForDelete(stmt, preState)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rollbackStmts) != 2 {
		t.Fatalf("expected 2 rollback statements, got %d", len(rollbackStmts))
	}

	// Check that rollback is INSERT
	for _, rb := range rollbackStmts {
		if !strings.HasPrefix(rb, "INSERT INTO users") {
			t.Errorf("expected rollback to be INSERT INTO users, got %q", rb)
		}
	}
}

func TestGenerateForDeleteEmptyPreState(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "DELETE FROM users",
		Type:   parser.StatementDelete,
		Tables: []string{"users"},
	}

	rollbackStmts, err := g.GenerateForDelete(stmt, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rollbackStmts != nil {
		t.Errorf("expected nil for empty preState, got %v", rollbackStmts)
	}
}

func TestGenerateForDeleteNoTable(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "DELETE FROM users",
		Type:   parser.StatementDelete,
		Tables: []string{},
	}

	preState := []map[string]interface{}{
		{"id": 1, "status": "inactive"},
	}

	_, err := g.GenerateForDelete(stmt, preState)
	if err == nil {
		t.Error("expected error for missing table")
	}
}

func TestGenerateForInsert(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "INSERT INTO users (name) VALUES ('John')",
		Type:   parser.StatementInsert,
		Tables: []string{"users"},
	}

	insertedIDs := []interface{}{1, 2, 3}

	rollbackStmts, err := g.GenerateForInsert(stmt, insertedIDs, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rollbackStmts) != 3 {
		t.Fatalf("expected 3 rollback statements, got %d", len(rollbackStmts))
	}

	// Check that rollback is DELETE
	for _, rb := range rollbackStmts {
		if !strings.HasPrefix(rb, "DELETE FROM users WHERE id =") {
			t.Errorf("expected rollback to be DELETE FROM users WHERE id =, got %q", rb)
		}
	}
}

func TestGenerateForInsertEmptyIDs(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "INSERT INTO users (name) VALUES ('John')",
		Type:   parser.StatementInsert,
		Tables: []string{"users"},
	}

	rollbackStmts, err := g.GenerateForInsert(stmt, nil, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if rollbackStmts != nil {
		t.Errorf("expected nil for empty insertedIDs, got %v", rollbackStmts)
	}
}

func TestGenerateForInsertNoTable(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "INSERT INTO users (name) VALUES ('John')",
		Type:   parser.StatementInsert,
		Tables: []string{},
	}

	insertedIDs := []interface{}{1}

	_, err := g.GenerateForInsert(stmt, insertedIDs, "id")
	if err == nil {
		t.Error("expected error for missing table")
	}
}

func TestGenerateConsolidatedDelete(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "DELETE FROM users WHERE status = 'inactive'",
		Type:   parser.StatementDelete,
		Tables: []string{"users"},
	}

	preState := []map[string]interface{}{
		{"id": 1, "name": "John"},
		{"id": 2, "name": "Jane"},
	}

	consolidated, err := g.GenerateConsolidated(stmt, preState, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if consolidated == "" {
		t.Error("expected non-empty consolidated rollback")
	}

	// Should contain multiple INSERT statements separated by ;\n
	parts := strings.Split(consolidated, ";\n")
	if len(parts) != 2 {
		t.Errorf("expected 2 consolidated statements, got %d", len(parts))
	}
}

func TestGenerateConsolidatedUpdate(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET status = 'inactive' WHERE id IN (1, 2)",
		Type:   parser.StatementUpdate,
		Tables: []string{"users"},
	}

	preState := []map[string]interface{}{
		{"uuid": "abc", "status": "active"},
		{"uuid": "def", "status": "active"},
	}

	consolidated, err := g.GenerateConsolidated(stmt, preState, "uuid")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if consolidated == "" {
		t.Error("expected non-empty consolidated rollback")
	}

	parts := strings.Split(consolidated, ";\n")
	if len(parts) != 2 {
		t.Errorf("expected 2 consolidated statements, got %d", len(parts))
	}
}

func TestGenerateConsolidatedOtherType(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "SELECT * FROM users",
		Type:   parser.StatementSelect,
		Tables: []string{"users"},
	}

	consolidated, err := g.GenerateConsolidated(stmt, nil, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if consolidated != "" {
		t.Errorf("expected empty consolidated for SELECT, got %q", consolidated)
	}
}

func TestFormatValue(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected string
	}{
		{nil, "NULL"},
		{"test", "'test'"},
		{"test's value", "'test''s value'"}, // escaped quote
		{123, "123"},
		{int32(456), "456"},
		{int64(789), "789"},
		{float32(1.5), "1.5"},
		{float64(2.5), "2.5"},
		{true, "TRUE"},
		{false, "FALSE"},
		{struct{ Name string }{"test"}, "'{test}'"}, // default handling
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := formatValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatValue(%v) = %q, expected %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestExtractUpdatedColumns(t *testing.T) {
	tests := []struct {
		sql      string
		expected []string
	}{
		{
			sql:      "UPDATE users SET status = 'active'",
			expected: []string{"status"},
		},
		{
			sql:      "UPDATE users SET status = 'active', name = 'John'",
			expected: []string{"status", "name"},
		},
		{
			sql:      "UPDATE users SET status = 'active' WHERE id = 1",
			expected: []string{"status"},
		},
		{
			sql:      `UPDATE users SET "Status" = 'active'`,
			expected: []string{"Status"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := extractUpdatedColumns(tt.sql)
			if len(result) != len(tt.expected) {
				t.Fatalf("expected %d columns, got %d: %v", len(tt.expected), len(result), result)
			}
			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("expected column %q at position %d, got %q", expected, i, result[i])
				}
			}
		})
	}
}

func TestExtractUpdatedColumnsRegex(t *testing.T) {
	// Test the regex fallback for PostgreSQL-specific syntax
	sql := "UPDATE users SET status = 'active', updated_at = NOW() WHERE id = 1"
	result := extractUpdatedColumnsRegex(sql)

	if len(result) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(result), result)
	}

	expected := []string{"status", "updated_at"}
	for i, exp := range expected {
		if result[i] != exp {
			t.Errorf("expected column %q at position %d, got %q", exp, i, result[i])
		}
	}
}

func TestExtractUpdatedColumnsNoSetClause(t *testing.T) {
	sql := "SELECT * FROM users"
	result := extractUpdatedColumnsRegex(sql)

	if result != nil {
		t.Errorf("expected nil for non-UPDATE statement, got %v", result)
	}
}

func TestGenerateRollbackWithSpecialCharacters(t *testing.T) {
	g := New()

	stmt := &parser.Statement{
		SQL:    "UPDATE users SET bio = 'new bio'",
		Type:   parser.StatementUpdate,
		Tables: []string{"users"},
	}

	preState := []map[string]interface{}{
		{"id": 1, "bio": "John's \"special\" bio\nwith newline"},
	}

	rollbackStmts, err := g.GenerateForUpdate(stmt, preState, "id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rollbackStmts) != 1 {
		t.Fatalf("expected 1 rollback statement, got %d", len(rollbackStmts))
	}

	// Check proper escaping
	if !strings.Contains(rollbackStmts[0], "''") {
		t.Errorf("expected escaped single quote in rollback: %q", rollbackStmts[0])
	}
}
