package parser

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidationError represents a validation failure.
type ValidationError struct {
	Statement   *Statement
	Rule        string
	Description string
	Severity    string // "error" or "warning"
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("[%s] %s: %s", e.Severity, e.Rule, e.Description)
}

// ValidationResult contains all validation results.
type ValidationResult struct {
	Valid    bool
	Errors   []ValidationError
	Warnings []ValidationError
}

// Validator validates SQL statements against safety rules.
type Validator struct {
	allowedTables   map[string]bool
	requireWhere    bool
	blockDangerous  bool
	maxRowThreshold int
}

// ValidatorOption configures validator behavior.
type ValidatorOption func(*Validator)

// WithAllowedTables sets the whitelist of allowed tables.
func WithAllowedTables(tables []string) ValidatorOption {
	return func(v *Validator) {
		v.allowedTables = make(map[string]bool)
		for _, t := range tables {
			v.allowedTables[strings.ToLower(t)] = true
		}
	}
}

// WithRequireWhere requires WHERE clause for UPDATE/DELETE.
func WithRequireWhere(require bool) ValidatorOption {
	return func(v *Validator) {
		v.requireWhere = require
	}
}

// WithBlockDangerous blocks known dangerous patterns.
func WithBlockDangerous(block bool) ValidatorOption {
	return func(v *Validator) {
		v.blockDangerous = block
	}
}

// NewValidator creates a new Validator with options.
func NewValidator(opts ...ValidatorOption) *Validator {
	v := &Validator{
		requireWhere:   true,
		blockDangerous: true,
	}
	for _, opt := range opts {
		opt(v)
	}
	return v
}

// Validate checks all statements in a parse result.
func (v *Validator) Validate(result *ParseResult) *ValidationResult {
	vr := &ValidationResult{Valid: true}

	for i := range result.Statements {
		stmt := &result.Statements[i]
		v.validateStatement(stmt, vr)
	}

	if len(vr.Errors) > 0 {
		vr.Valid = false
	}

	return vr
}

func (v *Validator) validateStatement(stmt *Statement, vr *ValidationResult) {
	// Rule 1: Mutating statements require WHERE clause
	if v.requireWhere && stmt.IsMutatingStatement() && stmt.Type != StatementInsert {
		if !stmt.HasWhereClause {
			vr.Errors = append(vr.Errors, ValidationError{
				Statement:   stmt,
				Rule:        "REQUIRE_WHERE",
				Description: fmt.Sprintf("%s statement without WHERE clause - this would affect ALL rows", stmt.Type),
				Severity:    "error",
			})
		}
	}

	// Rule 2: Check for dangerous WHERE patterns
	if v.blockDangerous && stmt.HasWhereClause {
		if isDangerousWhere(stmt.WhereClause) {
			vr.Errors = append(vr.Errors, ValidationError{
				Statement:   stmt,
				Rule:        "DANGEROUS_WHERE",
				Description: fmt.Sprintf("WHERE clause '%s' is equivalent to no WHERE clause (matches all rows)", stmt.WhereClause),
				Severity:    "error",
			})
		}
	}

	// Rule 3: Check table whitelist if configured
	if len(v.allowedTables) > 0 {
		for _, table := range stmt.Tables {
			if !v.allowedTables[strings.ToLower(table)] {
				vr.Errors = append(vr.Errors, ValidationError{
					Statement:   stmt,
					Rule:        "TABLE_NOT_ALLOWED",
					Description: fmt.Sprintf("table '%s' is not in the allowed tables list", table),
					Severity:    "error",
				})
			}
		}
	}

	// Rule 4: Block DDL statements
	if isDDLStatement(stmt.SQL) {
		vr.Errors = append(vr.Errors, ValidationError{
			Statement:   stmt,
			Rule:        "DDL_NOT_ALLOWED",
			Description: "DDL statements (CREATE, ALTER, DROP, TRUNCATE) are not allowed via SafeSQL",
			Severity:    "error",
		})
	}

	// Rule 7: Warn about transaction control statements
	// SafeSQL wraps execution in its own transaction; explicit BEGIN/COMMIT/ROLLBACK in
	// the SQL file will be skipped automatically to prevent early commit/rollback of
	// SafeSQL's transaction wrapper.
	if stmt.Type == StatementTransaction {
		vr.Warnings = append(vr.Warnings, ValidationError{
			Statement:   stmt,
			Rule:        "TRANSACTION_CONTROL_SKIPPED",
			Description: fmt.Sprintf("transaction control statement (%s) will be skipped — SafeSQL manages the transaction boundary", stmt.SQL),
			Severity:    "warning",
		})
	}

	// Rule 5: Warn about statements without specific identifiers
	if stmt.IsMutatingStatement() && stmt.HasWhereClause && !hasSpecificIdentifier(stmt.WhereClause) {
		vr.Warnings = append(vr.Warnings, ValidationError{
			Statement:   stmt,
			Rule:        "BROAD_WHERE",
			Description: "WHERE clause may match multiple rows - consider using a specific identifier (uuid, id)",
			Severity:    "warning",
		})
	}

	// Rule 6: Block UPDATE/DELETE with LIMIT but no ORDER BY
	if stmt.IsMutatingStatement() && hasLimit(stmt.SQL) && !hasOrderBy(stmt.SQL) {
		vr.Warnings = append(vr.Warnings, ValidationError{
			Statement:   stmt,
			Rule:        "LIMIT_WITHOUT_ORDER",
			Description: "LIMIT without ORDER BY may produce non-deterministic results",
			Severity:    "warning",
		})
	}
}

func isDangerousWhere(whereClause string) bool {
	// Patterns that effectively match all rows
	dangerousPatterns := []string{
		`^\s*1\s*=\s*1\s*$`,
		`^\s*true\s*$`,
		`^\s*'1'\s*=\s*'1'\s*$`,
		`^\s*1\s*$`,
		`^\s*0\s*=\s*0\s*$`,
	}

	lower := strings.ToLower(strings.TrimSpace(whereClause))
	for _, pattern := range dangerousPatterns {
		if matched, _ := regexp.MatchString(pattern, lower); matched {
			return true
		}
	}
	return false
}

func isDDLStatement(sql string) bool {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	ddlPrefixes := []string{"CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "GRANT ", "REVOKE "}
	for _, prefix := range ddlPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

func hasSpecificIdentifier(whereClause string) bool {
	// Check if WHERE clause contains common identifier columns
	identifierPatterns := []string{
		`(?i)\buuid\s*=`,
		`(?i)\bid\s*=`,
		`(?i)\b\w+_id\s*=`,
		`(?i)\b\w+_uuid\s*=`,
	}

	for _, pattern := range identifierPatterns {
		if matched, _ := regexp.MatchString(pattern, whereClause); matched {
			return true
		}
	}
	return false
}

func hasLimit(sql string) bool {
	return regexp.MustCompile(`(?i)\bLIMIT\b`).MatchString(sql)
}

func hasOrderBy(sql string) bool {
	return regexp.MustCompile(`(?i)\bORDER\s+BY\b`).MatchString(sql)
}

// ValidateMetadata checks if required metadata is present.
func (v *Validator) ValidateMetadata(metadata *QueryMetadata) []ValidationError {
	var errors []ValidationError

	if metadata.Ticket == "" {
		errors = append(errors, ValidationError{
			Rule:        "MISSING_TICKET",
			Description: "TICKET metadata is required (add -- TICKET: JIRA-XXXX)",
			Severity:    "error",
		})
	}

	if metadata.Author == "" {
		errors = append(errors, ValidationError{
			Rule:        "MISSING_AUTHOR",
			Description: "AUTHOR metadata is recommended (add -- AUTHOR: your-email)",
			Severity:    "warning",
		})
	}

	return errors
}
