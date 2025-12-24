// Package parser provides SQL parsing and validation for SafeSQL.
package parser

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"regexp"
	"strings"
)

// StatementType represents the type of SQL statement.
type StatementType string

const (
	StatementSelect StatementType = "SELECT"
	StatementInsert StatementType = "INSERT"
	StatementUpdate StatementType = "UPDATE"
	StatementDelete StatementType = "DELETE"
	StatementOther  StatementType = "OTHER"
)

// Statement represents a parsed SQL statement.
type Statement struct {
	SQL            string
	Type           StatementType
	Tables         []string
	WhereClause    string
	HasWhereClause bool
	Hash           string
	LineNumber     int
}

// QueryMetadata holds metadata extracted from SQL file comments.
type QueryMetadata struct {
	Ticket               string
	Author               string
	Description          string
	AffectedRowsExpected int
}

// ParseResult contains all parsed information from a SQL file.
type ParseResult struct {
	Statements []Statement
	Metadata   QueryMetadata
	RawSQL     string
	FileHash   string
}

// Parser handles SQL parsing and validation.
type Parser struct {
	allowedTables map[string]bool
}

// New creates a new Parser instance.
func New(allowedTables []string) *Parser {
	tableMap := make(map[string]bool)
	for _, t := range allowedTables {
		tableMap[strings.ToLower(t)] = true
	}
	return &Parser{allowedTables: tableMap}
}

// Parse parses SQL content and extracts statements and metadata.
func (p *Parser) Parse(content string) (*ParseResult, error) {
	result := &ParseResult{
		RawSQL:   content,
		FileHash: computeHash(content),
	}

	// Extract metadata from comments
	result.Metadata = p.extractMetadata(content)

	// Parse individual statements
	statements, err := p.parseStatements(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse statements: %w", err)
	}
	result.Statements = statements

	return result, nil
}

func (p *Parser) extractMetadata(content string) QueryMetadata {
	metadata := QueryMetadata{}
	scanner := bufio.NewScanner(strings.NewReader(content))

	ticketRe := regexp.MustCompile(`(?i)--\s*TICKET:\s*(.+)`)
	authorRe := regexp.MustCompile(`(?i)--\s*AUTHOR:\s*(.+)`)
	descRe := regexp.MustCompile(`(?i)--\s*DESCRIPTION:\s*(.+)`)
	rowsRe := regexp.MustCompile(`(?i)--\s*AFFECTED_ROWS_EXPECTED:\s*(\d+)`)

	for scanner.Scan() {
		line := scanner.Text()
		if matches := ticketRe.FindStringSubmatch(line); len(matches) > 1 {
			metadata.Ticket = strings.TrimSpace(matches[1])
		}
		if matches := authorRe.FindStringSubmatch(line); len(matches) > 1 {
			metadata.Author = strings.TrimSpace(matches[1])
		}
		if matches := descRe.FindStringSubmatch(line); len(matches) > 1 {
			metadata.Description = strings.TrimSpace(matches[1])
		}
		if matches := rowsRe.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &metadata.AffectedRowsExpected)
		}
	}

	return metadata
}

func (p *Parser) parseStatements(content string) ([]Statement, error) {
	// Remove comments for parsing but keep for reference
	cleanContent := removeComments(content)

	// Split by semicolons (simplified - doesn't handle strings with semicolons)
	rawStatements := splitStatements(cleanContent)

	var statements []Statement
	lineNum := 1

	for _, raw := range rawStatements {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		stmt := Statement{
			SQL:        raw,
			Hash:       computeHash(raw),
			LineNumber: lineNum,
		}

		// Determine statement type
		stmt.Type = detectStatementType(raw)

		// Extract tables
		stmt.Tables = extractTables(raw, stmt.Type)

		// Check for WHERE clause
		stmt.WhereClause, stmt.HasWhereClause = extractWhereClause(raw)

		statements = append(statements, stmt)
		lineNum += strings.Count(raw, "\n") + 1
	}

	return statements, nil
}

func removeComments(sql string) string {
	// Remove single-line comments
	lineCommentRe := regexp.MustCompile(`--.*$`)
	lines := strings.Split(sql, "\n")
	var cleanLines []string
	for _, line := range lines {
		cleanLines = append(cleanLines, lineCommentRe.ReplaceAllString(line, ""))
	}
	result := strings.Join(cleanLines, "\n")

	// Remove multi-line comments
	blockCommentRe := regexp.MustCompile(`/\*[\s\S]*?\*/`)
	result = blockCommentRe.ReplaceAllString(result, "")

	return result
}

func splitStatements(sql string) []string {
	// Simple split by semicolon - for production, use a proper SQL parser
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			current.WriteByte(c)
		} else if inString && c == stringChar {
			// Check for escaped quote
			if i+1 < len(sql) && sql[i+1] == stringChar {
				current.WriteByte(c)
				current.WriteByte(c)
				i++
			} else {
				inString = false
				current.WriteByte(c)
			}
		} else if !inString && c == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}

	// Add remaining statement if any
	stmt := strings.TrimSpace(current.String())
	if stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}

func detectStatementType(sql string) StatementType {
	upper := strings.ToUpper(strings.TrimSpace(sql))
	switch {
	case strings.HasPrefix(upper, "SELECT"):
		return StatementSelect
	case strings.HasPrefix(upper, "INSERT"):
		return StatementInsert
	case strings.HasPrefix(upper, "UPDATE"):
		return StatementUpdate
	case strings.HasPrefix(upper, "DELETE"):
		return StatementDelete
	default:
		return StatementOther
	}
}

func extractTables(sql string, stmtType StatementType) []string {
	var tables []string
	upper := strings.ToUpper(sql)

	switch stmtType {
	case StatementUpdate:
		// UPDATE table_name SET ...
		re := regexp.MustCompile(`(?i)UPDATE\s+(\w+)`)
		if matches := re.FindStringSubmatch(sql); len(matches) > 1 {
			tables = append(tables, strings.ToLower(matches[1]))
		}
	case StatementDelete:
		// DELETE FROM table_name ...
		re := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)`)
		if matches := re.FindStringSubmatch(sql); len(matches) > 1 {
			tables = append(tables, strings.ToLower(matches[1]))
		}
	case StatementInsert:
		// INSERT INTO table_name ...
		re := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`)
		if matches := re.FindStringSubmatch(sql); len(matches) > 1 {
			tables = append(tables, strings.ToLower(matches[1]))
		}
	case StatementSelect:
		// SELECT ... FROM table_name ...
		re := regexp.MustCompile(`(?i)FROM\s+(\w+)`)
		matches := re.FindAllStringSubmatch(upper, -1)
		for _, m := range matches {
			if len(m) > 1 {
				tables = append(tables, strings.ToLower(m[1]))
			}
		}
	}

	return tables
}

func extractWhereClause(sql string) (string, bool) {
	upper := strings.ToUpper(sql)
	idx := strings.Index(upper, "WHERE")
	if idx == -1 {
		return "", false
	}

	// Extract everything after WHERE
	whereClause := strings.TrimSpace(sql[idx+5:])

	// Remove trailing clauses (ORDER BY, LIMIT, etc.)
	for _, keyword := range []string{"ORDER BY", "LIMIT", "GROUP BY", "HAVING"} {
		if keyIdx := strings.Index(strings.ToUpper(whereClause), keyword); keyIdx != -1 {
			whereClause = strings.TrimSpace(whereClause[:keyIdx])
		}
	}

	return whereClause, true
}

func computeHash(content string) string {
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", hash)
}

// IsMutatingStatement returns true if the statement modifies data.
func (s *Statement) IsMutatingStatement() bool {
	return s.Type == StatementUpdate || s.Type == StatementDelete || s.Type == StatementInsert
}
