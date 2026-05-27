// Package rollback generates rollback SQL statements.
package rollback

import (
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/tools/safesql/internal/parser"
	"github.com/xwb1989/sqlparser"
)

// Generator creates rollback SQL from original statements and their pre-state.
type Generator struct{}

// New creates a new rollback Generator.
func New() *Generator {
	return &Generator{}
}

// GenerateForUpdate generates rollback SQL for an UPDATE statement.
func (g *Generator) GenerateForUpdate(stmt *parser.Statement, preState []map[string]interface{}, primaryKey string) ([]string, error) {
	if len(preState) == 0 {
		return nil, nil
	}

	if len(stmt.Tables) == 0 {
		return nil, fmt.Errorf("no table found in statement")
	}
	table := stmt.Tables[0]

	// Determine which columns were updated
	updatedCols := extractUpdatedColumns(stmt.SQL)
	if len(updatedCols) == 0 {
		return nil, fmt.Errorf("could not determine updated columns")
	}

	var rollbackStmts []string

	for _, row := range preState {
		// Get primary key value
		pkValue, ok := row[primaryKey]
		if !ok {
			// Try common alternatives
			for _, pk := range []string{"id", "uuid"} {
				if v, found := row[pk]; found {
					pkValue = v
					primaryKey = pk
					ok = true
					break
				}
			}
		}
		if !ok {
			continue // Skip rows without identifiable primary key
		}

		// Build SET clause with original values
		var setClauses []string
		for _, col := range updatedCols {
			if origValue, exists := row[col]; exists {
				setClauses = append(setClauses, fmt.Sprintf("%s = %s", col, formatValue(origValue)))
			}
		}

		if len(setClauses) > 0 {
			rollback := fmt.Sprintf("UPDATE %s SET %s WHERE %s = %s",
				table,
				strings.Join(setClauses, ", "),
				primaryKey,
				formatValue(pkValue),
			)
			rollbackStmts = append(rollbackStmts, rollback)
		}
	}

	return rollbackStmts, nil
}

// GenerateForDelete generates rollback SQL for a DELETE statement.
func (g *Generator) GenerateForDelete(stmt *parser.Statement, preState []map[string]interface{}) ([]string, error) {
	if len(preState) == 0 {
		return nil, nil
	}

	if len(stmt.Tables) == 0 {
		return nil, fmt.Errorf("no table found in statement")
	}
	table := stmt.Tables[0]

	var rollbackStmts []string

	for _, row := range preState {
		// Get column names and values
		var columns []string
		var values []string

		for col, val := range row {
			columns = append(columns, col)
			values = append(values, formatValue(val))
		}

		if len(columns) > 0 {
			rollback := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				table,
				strings.Join(columns, ", "),
				strings.Join(values, ", "),
			)
			rollbackStmts = append(rollbackStmts, rollback)
		}
	}

	return rollbackStmts, nil
}

// GenerateForInsert generates rollback SQL for an INSERT statement.
func (g *Generator) GenerateForInsert(stmt *parser.Statement, insertedIDs []interface{}, primaryKey string) ([]string, error) {
	if len(insertedIDs) == 0 {
		return nil, nil
	}

	if len(stmt.Tables) == 0 {
		return nil, fmt.Errorf("no table found in statement")
	}
	table := stmt.Tables[0]

	var rollbackStmts []string

	for _, id := range insertedIDs {
		rollback := fmt.Sprintf("DELETE FROM %s WHERE %s = %s",
			table,
			primaryKey,
			formatValue(id),
		)
		rollbackStmts = append(rollbackStmts, rollback)
	}

	return rollbackStmts, nil
}

// GenerateConsolidated creates a single consolidated rollback statement when possible.
func (g *Generator) GenerateConsolidated(stmt *parser.Statement, preState []map[string]interface{}, primaryKey string) (string, error) {
	if stmt.Type == parser.StatementDelete {
		// For DELETE, we need individual INSERTs
		stmts, err := g.GenerateForDelete(stmt, preState)
		if err != nil {
			return "", err
		}
		return strings.Join(stmts, ";\n"), nil
	}

	if stmt.Type == parser.StatementUpdate {
		// Check if all rows have the same original values for updated columns
		// If so, we can consolidate
		stmts, err := g.GenerateForUpdate(stmt, preState, primaryKey)
		if err != nil {
			return "", err
		}
		return strings.Join(stmts, ";\n"), nil
	}

	return "", nil
}

func extractUpdatedColumns(sql string) []string {
	// Use proper SQL parser to extract updated columns
	// Note: We only extract column names, not the WHERE clause.
	// Rollback SQL uses primary key-based WHERE clauses (WHERE id = X)
	// instead of replaying the original WHERE clause, ensuring each
	// affected row is reverted individually.
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		// Fallback to regex-based extraction for PostgreSQL-specific syntax
		return extractUpdatedColumnsRegex(sql)
	}

	updateStmt, ok := stmt.(*sqlparser.Update)
	if !ok {
		return nil
	}

	var columns []string
	for _, expr := range updateStmt.Exprs {
		// expr.Name is the column being updated
		colName := strings.Trim(expr.Name.Name.String(), "\"'`")
		if colName != "" {
			columns = append(columns, colName)
		}
	}

	return columns
}

// extractUpdatedColumnsRegex is a fallback regex-based extraction for PostgreSQL
func extractUpdatedColumnsRegex(sql string) []string {
	upper := strings.ToUpper(sql)
	setIdx := strings.Index(upper, "SET")
	if setIdx == -1 {
		return nil
	}

	// Find WHERE clause to isolate the SET clause content
	// (We need to find WHERE to avoid parsing WHERE conditions as column names,
	// but we don't preserve the WHERE clause - rollback uses primary key WHERE)
	whereIdx := -1
	for _, pattern := range []string{" WHERE", "\nWHERE", "\tWHERE"} {
		idx := strings.Index(upper[setIdx:], pattern)
		if idx != -1 {
			whereIdx = idx
			break
		}
	}

	var setClause string
	if whereIdx == -1 {
		setClause = sql[setIdx+3:]
	} else {
		setClause = sql[setIdx+3 : setIdx+whereIdx]
	}

	var columns []string
	parts := strings.Split(setClause, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx > 0 {
			col := strings.TrimSpace(part[:eqIdx])
			col = strings.Trim(col, "\"'`")
			if col != "" {
				columns = append(columns, col)
			}
		}
	}

	return columns
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		// Escape single quotes
		escaped := strings.ReplaceAll(val, "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	case int, int32, int64, float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		// Default to string representation
		escaped := strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''")
		return fmt.Sprintf("'%s'", escaped)
	}
}
