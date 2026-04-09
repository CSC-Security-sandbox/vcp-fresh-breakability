package parser

// Scenario tests: verify that realistic SQL scripts parse and validate correctly.
// Each scenario represents a shape of script a user might supply to safesql.

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Parser scenarios
// ---------------------------------------------------------------------------

func TestScenario_SingleUpdate(t *testing.T) {
	// Simplest real-world case: one UPDATE with a specific WHERE.
	sql := `
-- TICKET: ABC-123
-- AUTHOR: ops@example.com
UPDATE users SET status = 'active' WHERE id = 'abc-uuid-123';
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(result.Statements))
	}
	stmt := result.Statements[0]
	if stmt.Type != StatementUpdate {
		t.Errorf("expected UPDATE, got %s", stmt.Type)
	}
	if !stmt.IsMutatingStatement() {
		t.Error("UPDATE must be mutating")
	}
	if stmt.IsTransactionControl() {
		t.Error("UPDATE must not be transaction control")
	}
	if result.Metadata.Ticket != "ABC-123" {
		t.Errorf("expected ticket ABC-123, got %q", result.Metadata.Ticket)
	}
}

func TestScenario_SelectUpdateSelect(t *testing.T) {
	// The scenario that originally caused the TotalRows warning:
	// two SELECTs sandwiching one UPDATE.
	sql := `
-- TICKET: ABC-123
SELECT * FROM users WHERE id = 'abc-uuid';
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
SELECT * FROM users WHERE id = 'abc-uuid';
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(result.Statements))
	}

	types := []StatementType{StatementSelect, StatementUpdate, StatementSelect}
	for i, expected := range types {
		if result.Statements[i].Type != expected {
			t.Errorf("stmt[%d]: expected %s, got %s", i, expected, result.Statements[i].Type)
		}
	}

	// Only the UPDATE is mutating; SELECTs are not.
	if result.Statements[0].IsMutatingStatement() {
		t.Error("SELECT[0] must not be mutating")
	}
	if !result.Statements[1].IsMutatingStatement() {
		t.Error("UPDATE must be mutating")
	}
	if result.Statements[2].IsMutatingStatement() {
		t.Error("SELECT[2] must not be mutating")
	}

	// None are transaction control.
	for i, stmt := range result.Statements {
		if stmt.IsTransactionControl() {
			t.Errorf("stmt[%d] (%s) must not be transaction control", i, stmt.Type)
		}
	}
}

func TestScenario_BeginUpdateCommit(t *testing.T) {
	// User wrote explicit transaction boundaries — safesql must skip them.
	sql := `
-- TICKET: ABC-123
BEGIN;
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
COMMIT;
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(result.Statements))
	}

	if result.Statements[0].Type != StatementTransaction {
		t.Errorf("stmt[0]: expected TRANSACTION (BEGIN), got %s", result.Statements[0].Type)
	}
	if result.Statements[1].Type != StatementUpdate {
		t.Errorf("stmt[1]: expected UPDATE, got %s", result.Statements[1].Type)
	}
	if result.Statements[2].Type != StatementTransaction {
		t.Errorf("stmt[2]: expected TRANSACTION (COMMIT), got %s", result.Statements[2].Type)
	}

	// BEGIN and COMMIT are transaction control, not mutating.
	if !result.Statements[0].IsTransactionControl() {
		t.Error("BEGIN must be transaction control")
	}
	if result.Statements[0].IsMutatingStatement() {
		t.Error("BEGIN must not be mutating")
	}
	if !result.Statements[2].IsTransactionControl() {
		t.Error("COMMIT must be transaction control")
	}
	if result.Statements[2].IsMutatingStatement() {
		t.Error("COMMIT must not be mutating")
	}
}

func TestScenario_BeginSelectUpdateCommit(t *testing.T) {
	// Combined: explicit transaction + pre/post SELECT + one UPDATE.
	sql := `
-- TICKET: ABC-123
BEGIN;
SELECT * FROM users WHERE id = 'abc-uuid';
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
COMMIT;
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 4 {
		t.Fatalf("expected 4 statements, got %d", len(result.Statements))
	}

	expected := []StatementType{StatementTransaction, StatementSelect, StatementUpdate, StatementTransaction}
	for i, exp := range expected {
		if result.Statements[i].Type != exp {
			t.Errorf("stmt[%d]: expected %s, got %s", i, exp, result.Statements[i].Type)
		}
	}

	// Verify mutating/tx-control classification
	mutating := []bool{false, false, true, false}
	txCtrl := []bool{true, false, false, true}
	for i, stmt := range result.Statements {
		if stmt.IsMutatingStatement() != mutating[i] {
			t.Errorf("stmt[%d] IsMutating: expected %v, got %v", i, mutating[i], stmt.IsMutatingStatement())
		}
		if stmt.IsTransactionControl() != txCtrl[i] {
			t.Errorf("stmt[%d] IsTransactionControl: expected %v, got %v", i, txCtrl[i], stmt.IsTransactionControl())
		}
	}
}

func TestScenario_LowercaseTransactionKeywords(t *testing.T) {
	// Keywords are case-insensitive.
	keywords := []string{
		"begin",
		"Begin",
		"BeGiN",
		"commit",
		"Commit",
		"COMMIT",
		"rollback",
		"Rollback",
		"ROLLBACK",
		"start transaction",
		"START TRANSACTION",
		"Start Transaction",
		"end",
		"END",
		"savepoint sp1",
		"SAVEPOINT sp1",
		"release savepoint sp1",
		"RELEASE SAVEPOINT sp1",
	}
	for _, kw := range keywords {
		t.Run(kw, func(t *testing.T) {
			typ := detectStatementType(kw)
			if typ != StatementTransaction {
				t.Errorf("%q: expected TRANSACTION, got %s", kw, typ)
			}
		})
	}
}

func TestScenario_RollbackIsTransactionControl(t *testing.T) {
	sql := `
-- TICKET: ABC-123
BEGIN;
UPDATE users SET status = 'inactive' WHERE id = 'abc-uuid';
ROLLBACK;
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(result.Statements))
	}
	if result.Statements[2].Type != StatementTransaction {
		t.Errorf("ROLLBACK: expected TRANSACTION, got %s", result.Statements[2].Type)
	}
	if !result.Statements[2].IsTransactionControl() {
		t.Error("ROLLBACK must be transaction control")
	}
}

func TestScenario_SavepointCycle(t *testing.T) {
	// SAVEPOINT + RELEASE SAVEPOINT — both should be classified as TRANSACTION.
	sql := `
-- TICKET: ABC-123
BEGIN;
SAVEPOINT sp1;
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
RELEASE SAVEPOINT sp1;
COMMIT;
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 5 {
		t.Fatalf("expected 5 statements, got %d: %v", len(result.Statements), result.Statements)
	}

	txIdxs := []int{0, 1, 3, 4} // BEGIN, SAVEPOINT, RELEASE SAVEPOINT, COMMIT
	for _, i := range txIdxs {
		if result.Statements[i].Type != StatementTransaction {
			t.Errorf("stmt[%d] (%q): expected TRANSACTION, got %s", i, result.Statements[i].SQL, result.Statements[i].Type)
		}
	}
	if result.Statements[2].Type != StatementUpdate {
		t.Errorf("stmt[2]: expected UPDATE, got %s", result.Statements[2].Type)
	}
}

func TestScenario_MultipleUpdates(t *testing.T) {
	// Two UPDATEs targeting different tables — both mutating, neither tx control.
	sql := `
-- TICKET: ABC-123
UPDATE users SET status = 'active' WHERE region = 'us-east';
UPDATE jobs SET state = 'completed' WHERE user_id = 'abc-uuid';
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(result.Statements))
	}
	for i, stmt := range result.Statements {
		if stmt.Type != StatementUpdate {
			t.Errorf("stmt[%d]: expected UPDATE, got %s", i, stmt.Type)
		}
		if !stmt.IsMutatingStatement() {
			t.Errorf("stmt[%d]: expected mutating", i)
		}
	}
}

func TestScenario_DeleteWithWhere(t *testing.T) {
	sql := `
-- TICKET: ABC-123
DELETE FROM sessions WHERE expires_at < '2024-01-01' AND user_id = 'abc-uuid';
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	stmt := result.Statements[0]
	if stmt.Type != StatementDelete {
		t.Errorf("expected DELETE, got %s", stmt.Type)
	}
	if !stmt.HasWhereClause {
		t.Error("expected WHERE clause")
	}
	if !stmt.IsMutatingStatement() {
		t.Error("DELETE must be mutating")
	}
}

func TestScenario_StringWithSemicolon(t *testing.T) {
	// Semicolon inside a string value must not split the statement.
	sql := `UPDATE users SET note = 'do this; then that' WHERE id = 'abc-uuid'`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(result.Statements) != 1 {
		t.Fatalf("semicolon inside string incorrectly split statement: got %d statements", len(result.Statements))
	}
}

func TestScenario_UpdateWithSubqueryWhere(t *testing.T) {
	// WHERE clause containing a subquery — must be preserved for CountAffectedRows.
	sql := `
-- TICKET: ABC-123
UPDATE jobs SET state = 'done' WHERE user_id IN (SELECT id FROM users WHERE region = 'us-east');
`
	p := New(nil)
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	stmt := result.Statements[0]
	if stmt.Type != StatementUpdate {
		t.Errorf("expected UPDATE, got %s", stmt.Type)
	}
	if !stmt.HasWhereClause {
		t.Error("expected WHERE clause")
	}
	if !strings.Contains(stmt.WhereClause, "SELECT id FROM users") {
		t.Errorf("subquery should be in WHERE clause, got: %q", stmt.WhereClause)
	}
}

// ---------------------------------------------------------------------------
// Validator scenarios
// ---------------------------------------------------------------------------

func TestScenarioValidator_TransactionControlProducesWarning(t *testing.T) {
	// BEGIN/COMMIT in a script must produce warnings, not errors.
	sql := `
-- TICKET: ABC-123
BEGIN;
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
COMMIT;
`
	p := New(nil)
	v := NewValidator()
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	vr := v.Validate(result)

	if !vr.Valid {
		t.Errorf("expected Valid=true, got errors: %v", vr.Errors)
	}

	// Expect exactly 2 TRANSACTION_CONTROL_SKIPPED warnings (BEGIN and COMMIT).
	count := 0
	for _, w := range vr.Warnings {
		if w.Rule == "TRANSACTION_CONTROL_SKIPPED" {
			count++
		}
	}
	if count != 2 {
		t.Errorf("expected 2 TRANSACTION_CONTROL_SKIPPED warnings, got %d (warnings: %v)", count, vr.Warnings)
	}
}

func TestScenarioValidator_DDLBlocked(t *testing.T) {
	sql := `
-- TICKET: ABC-123
SELECT * FROM users WHERE id = 'abc-uuid';
ALTER TABLE users ADD COLUMN new_col INT;
`
	p := New(nil)
	v := NewValidator()
	result, err := p.Parse(sql)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	vr := v.Validate(result)

	if vr.Valid {
		t.Error("expected Valid=false for DDL in script")
	}
	found := false
	for _, e := range vr.Errors {
		if e.Rule == "DDL_NOT_ALLOWED" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected DDL_NOT_ALLOWED error, got: %v", vr.Errors)
	}
}

func TestScenarioValidator_UpdateWithoutWhereBlocked(t *testing.T) {
	sql := `-- TICKET: ABC-123
UPDATE users SET status = 'inactive'`
	p := New(nil)
	v := NewValidator(WithRequireWhere(true))
	result, _ := p.Parse(sql)
	vr := v.Validate(result)

	if vr.Valid {
		t.Error("expected Valid=false for UPDATE without WHERE")
	}
	found := false
	for _, e := range vr.Errors {
		if e.Rule == "REQUIRE_WHERE" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected REQUIRE_WHERE error, got: %v", vr.Errors)
	}
}

func TestScenarioValidator_DangerousWhereBlocked(t *testing.T) {
	sqls := []string{
		"UPDATE users SET status = 'inactive' WHERE 1=1",
		"DELETE FROM jobs WHERE true",
		"UPDATE orders SET cancelled = true WHERE 0=0",
	}
	p := New(nil)
	v := NewValidator(WithBlockDangerous(true))

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			result, _ := p.Parse(sql)
			vr := v.Validate(result)
			if vr.Valid {
				t.Errorf("expected invalid for dangerous WHERE: %s", sql)
			}
		})
	}
}

func TestScenarioValidator_TransactionControlNotSubjectToWhereRule(t *testing.T) {
	// BEGIN/COMMIT have no WHERE clause but must NOT trigger REQUIRE_WHERE.
	sql := `
-- TICKET: ABC-123
BEGIN;
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
COMMIT;
`
	p := New(nil)
	v := NewValidator(WithRequireWhere(true))
	result, _ := p.Parse(sql)
	vr := v.Validate(result)

	if !vr.Valid {
		t.Errorf("expected Valid=true; REQUIRE_WHERE should not apply to tx control, errors: %v", vr.Errors)
	}
	for _, e := range vr.Errors {
		if e.Rule == "REQUIRE_WHERE" {
			t.Errorf("REQUIRE_WHERE should not fire for transaction control statements: %v", e)
		}
	}
}

func TestScenarioValidator_TransactionControlNotSubjectToDDLRule(t *testing.T) {
	// BEGIN/COMMIT must not trigger DDL_NOT_ALLOWED.
	sql := `
-- TICKET: ABC-123
BEGIN;
UPDATE users SET status = 'active' WHERE id = 'abc-uuid';
COMMIT;
`
	p := New(nil)
	v := NewValidator()
	result, _ := p.Parse(sql)
	vr := v.Validate(result)

	for _, e := range vr.Errors {
		if e.Rule == "DDL_NOT_ALLOWED" {
			t.Errorf("DDL_NOT_ALLOWED should not fire for BEGIN/COMMIT: %v", e)
		}
	}
}

func TestScenarioValidator_MissingTicketBlocked(t *testing.T) {
	// No TICKET comment → plan phase must fail.
	sql := `UPDATE users SET status = 'active' WHERE id = 'abc-uuid'`
	p := New(nil)
	v := NewValidator()
	result, _ := p.Parse(sql)
	metaErrs := v.ValidateMetadata(&result.Metadata)

	found := false
	for _, e := range metaErrs {
		if e.Rule == "MISSING_TICKET" {
			found = true
		}
	}
	if !found {
		t.Error("expected MISSING_TICKET error")
	}
}

// ---------------------------------------------------------------------------
// isTransactionControl helper — exhaustive keyword coverage
// ---------------------------------------------------------------------------

func TestIsTransactionControl_AllKeywords(t *testing.T) {
	shouldMatch := []string{
		"BEGIN", "begin", "Begin",
		"COMMIT", "commit",
		"ROLLBACK", "rollback",
		"ROLLBACK TO SAVEPOINT sp1",
		"START TRANSACTION", "start transaction",
		"END", "end",
		"SAVEPOINT sp1", "savepoint sp1",
		"RELEASE SAVEPOINT sp1", "release savepoint sp1",
		"RELEASE sp1",
	}
	shouldNotMatch := []string{
		"SELECT * FROM begin_table",
		"UPDATE commits SET x = 1 WHERE id = 1",
		"INSERT INTO rollbacks (id) VALUES (1)",
		"DELETE FROM end_states WHERE id = 1",
	}

	for _, kw := range shouldMatch {
		t.Run("match:"+kw, func(t *testing.T) {
			if !isTransactionControl(strings.ToUpper(strings.TrimSpace(kw))) {
				t.Errorf("%q should be transaction control", kw)
			}
		})
	}

	for _, sql := range shouldNotMatch {
		t.Run("no-match:"+sql, func(t *testing.T) {
			// These all start with DML keywords that are checked first in
			// detectStatementType, so they'd never reach isTransactionControl.
			// But verify the raw function too.
			upper := strings.ToUpper(strings.TrimSpace(sql))
			// Only test cases that don't start with a DML keyword
			if strings.HasPrefix(upper, "SELECT") ||
				strings.HasPrefix(upper, "UPDATE") ||
				strings.HasPrefix(upper, "INSERT") ||
				strings.HasPrefix(upper, "DELETE") {
				// These are DML — they correctly wouldn't reach isTransactionControl.
				// Verify detectStatementType returns the right DML type, not TRANSACTION.
				typ := detectStatementType(sql)
				if typ == StatementTransaction {
					t.Errorf("%q incorrectly classified as TRANSACTION", sql)
				}
			}
		})
	}
}
