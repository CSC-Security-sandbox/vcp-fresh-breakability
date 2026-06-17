package cli

import (
	"regexp"
	"strings"
)

// keyValuePattern matches key-value lines such as "Field Name: value" or
// "Field Name = value", including indented fields. The field name can contain
// letters, numbers, spaces, dots, dashes, and underscores. Compiled once at
// package init to avoid recompiling on every invocation.
var keyValuePattern = regexp.MustCompile(`^\s*([\w\s\.\-_]+?)\s*[:=]\s*(.*)$`)

// RemoveFieldsFromCLIOutput removes specified fields from CLI text output.
// It supports two formats:
//
// 1. Key-Value format (lines like "Field Name: value" or "Field Name = value")
//   - Entire line is removed if field name matches
//
// 2. Tabular format (columns with headers)
//   - Column data is removed if header matches field name
//
// Field matching is case-insensitive and supports partial matching:
// - "physical_used" matches "Physical Used", "physical_used_space", etc.
// - Underscores and spaces are normalized for matching
func RemoveFieldsFromCLIOutput(output string, fieldsToRemove []string) string {
	if output == "" || len(fieldsToRemove) == 0 {
		return output
	}

	// Normalize field names for matching
	normalizedFields := make([]string, len(fieldsToRemove))
	for i, field := range fieldsToRemove {
		normalizedFields[i] = normalizeFieldName(field)
	}

	lines := strings.Split(output, "\n")

	// Detect if output is tabular (has header line with multiple columns)
	if isTabularOutput(lines) {
		return removeFieldsFromTabular(lines, normalizedFields)
	}

	// Key-value format
	return removeFieldsFromKeyValue(lines, normalizedFields)
}

// normalizeFieldName normalizes a field name for comparison.
// Converts to lowercase and replaces underscores/spaces with a common separator.
func normalizeFieldName(field string) string {
	field = strings.ToLower(field)
	field = strings.ReplaceAll(field, "_", " ")
	field = strings.ReplaceAll(field, ".", " ")
	// Collapse multiple spaces
	space := regexp.MustCompile(`\s+`)
	field = space.ReplaceAllString(field, " ")
	return strings.TrimSpace(field)
}

// fieldMatches checks if a field name matches any of the fields to remove.
// Uses normalized comparison and partial matching.
func fieldMatches(fieldName string, fieldsToRemove []string) bool {
	normalized := normalizeFieldName(fieldName)
	for _, field := range fieldsToRemove {
		// Check exact match or if normalized field contains the target
		if normalized == field || strings.Contains(normalized, field) {
			return true
		}
	}
	return false
}

// isTabularOutput detects if the output is in tabular format.
// Tabular output typically has:
// - A header line with column names separated by spaces
// - A separator line (dashes) or
// - Multiple rows with aligned columns
//
// Key-value format has lines like "Field Name: value". Output may start with
// message lines (e.g. "This is your first recorded login.") before key-value lines;
// we scan the first portion and prefer key-value if any line contains ":" or "=".
func isTabularOutput(lines []string) bool {
	if len(lines) < 2 {
		return false
	}

	// Scan first portion: if any line looks like key-value (has ":" or "="),
	// treat as key-value so RemoveFields runs (e.g. "volume show -instance" with login message).
	scanLimit := len(lines)
	if scanLimit > 30 {
		scanLimit = 30
	}
	for i := 0; i < scanLimit; i++ {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if strings.Contains(line, ":") || strings.Contains(line, "=") {
			return false
		}
	}

	// Look for separator line (dashes) indicating a table
	for i := 0; i < len(lines) && i < 5; i++ {
		line := strings.TrimSpace(lines[i])
		if len(line) > 10 && strings.Count(line, "-") > len(line)/2 {
			return true
		}
	}

	// No key-value lines in first portion and no separator: first non-empty line with
	// multiple words and no colon may be a table header (e.g. "Volume Aggregate Physical_Used")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		words := strings.Fields(line)
		if len(words) >= 3 {
			return true
		}
		break
	}

	return false
}

// removeFieldsFromKeyValue removes fields from key-value formatted output.
// Lines matching "Field Name: value" or "Field Name = value" are removed.
func removeFieldsFromKeyValue(lines []string, fieldsToRemove []string) string {
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		// Empty lines pass through
		if trimmed == "" {
			result = append(result, line)
			continue
		}

		// Check if this is a key-value line
		matches := keyValuePattern.FindStringSubmatch(trimmed)
		if matches != nil {
			fieldName := matches[1]
			if fieldMatches(fieldName, fieldsToRemove) {
				continue
			}
		}

		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

// removeFieldsFromTabular removes columns from tabular formatted output.
func removeFieldsFromTabular(lines []string, fieldsToRemove []string) string {
	if len(lines) == 0 {
		return ""
	}

	// Find header line and separator line
	headerIdx := -1
	separatorIdx := -1

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		// Check for separator line (dashes)
		if strings.Count(trimmed, "-") > len(trimmed)/2 {
			separatorIdx = i
			if headerIdx == -1 && i > 0 {
				// Header is likely the line before separator
				headerIdx = i - 1
			}
			break
		}

		// If no separator found yet and this looks like a header
		if headerIdx == -1 && !strings.Contains(trimmed, ":") {
			headerIdx = i
		}
	}

	if headerIdx == -1 {
		// Can't identify header, return as-is
		return strings.Join(lines, "\n")
	}

	// Parse header to find column positions
	headerLine := lines[headerIdx]
	columns := parseTableColumns(headerLine)

	// Find columns to remove
	columnsToRemove := make(map[int]bool)
	for i, col := range columns {
		if fieldMatches(col.name, fieldsToRemove) {
			columnsToRemove[i] = true
		}
	}

	if len(columnsToRemove) == 0 {
		return strings.Join(lines, "\n")
	}

	// Process each line
	var result []string
	for i, line := range lines {
		if i == headerIdx || (separatorIdx >= 0 && i == separatorIdx) || i > headerIdx {
			// This is header, separator, or data line - remove columns
			newLine := removeColumnsFromLine(line, columns, columnsToRemove)
			result = append(result, newLine)
		} else {
			// Lines before header (title, etc.) - keep as-is
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// tableColumn represents a column in tabular output
type tableColumn struct {
	name  string
	start int
	end   int
}

// parseTableColumns parses the header line to find column positions.
func parseTableColumns(headerLine string) []tableColumn {
	var columns []tableColumn

	// Find word boundaries in header
	inWord := false
	wordStart := 0

	for i := 0; i < len(headerLine); i++ {
		isSpace := headerLine[i] == ' ' || headerLine[i] == '\t'

		if !isSpace && !inWord {
			// Start of a word
			wordStart = i
			inWord = true
		} else if isSpace && inWord {
			// End of a word
			word := headerLine[wordStart:i]

			// Find the end of this column (next word start or end of line)
			colEnd := len(headerLine)
			for j := i; j < len(headerLine); j++ {
				if headerLine[j] != ' ' && headerLine[j] != '\t' {
					colEnd = j
					break
				}
			}

			columns = append(columns, tableColumn{
				name:  strings.TrimSpace(word),
				start: wordStart,
				end:   colEnd,
			})
			inWord = false
		}
	}

	// Handle word that extends to end of line
	if inWord {
		word := headerLine[wordStart:]
		columns = append(columns, tableColumn{
			name:  strings.TrimSpace(word),
			start: wordStart,
			end:   len(headerLine),
		})
	}

	return columns
}

// removeColumnsFromLine removes specified columns from a line.
func removeColumnsFromLine(line string, columns []tableColumn, columnsToRemove map[int]bool) string {
	if len(line) == 0 {
		return line
	}

	// Build list of character ranges to keep
	var result strings.Builder
	lastEnd := 0

	for i, col := range columns {
		if columnsToRemove[i] {
			// Add characters before this column
			if col.start > lastEnd && lastEnd < len(line) {
				end := col.start
				if end > len(line) {
					end = len(line)
				}
				result.WriteString(line[lastEnd:end])
			}
			lastEnd = col.end
			if lastEnd > len(line) {
				lastEnd = len(line)
			}
		}
	}

	// Add remaining characters after last removed column
	if lastEnd < len(line) {
		result.WriteString(line[lastEnd:])
	}

	return result.String()
}

// KeepFieldsInCLIOutput is the allow-list counterpart of RemoveFieldsFromCLIOutput:
// it keeps only the specified fields and drops everything else. It is used where the
// safe default is to hide all fields except an explicit set (for example, exposing only
// the tiering footprint rows of "volume show-footprint").
//
// Unlike RemoveFieldsFromCLIOutput, matching here is EXACT on normalized field names (no
// partial/substring matching). This prevents an entry like "Total Footprint" from also
// keeping "Total Footprint Percent" or "Total Footprint Data Reduction". Blank lines are
// preserved for readability; any non-matching or non key-value line is dropped.
func KeepFieldsInCLIOutput(output string, fieldsToKeep []string) string {
	if output == "" || len(fieldsToKeep) == 0 {
		return output
	}

	// normalizeFieldName collapses underscores/dots/spaces but intentionally does not touch
	// hyphens (changing the global normalizer could alter unrelated RemoveFields matching).
	// Many ONTAP tabular headers are hyphenated (e.g. "Total-Footprint"), so register a
	// hyphenated variant of each allowed field alongside the space-separated form. This keeps
	// exact-match filtering working for both "Total Footprint" and "Total-Footprint".
	keep := make(map[string]bool, len(fieldsToKeep)*2)
	for _, field := range fieldsToKeep {
		normalized := normalizeFieldName(field)
		keep[normalized] = true
		keep[strings.ReplaceAll(normalized, " ", "-")] = true
	}

	lines := strings.Split(output, "\n")
	if isTabularOutput(lines) {
		return keepFieldsInTabular(lines, keep)
	}
	return keepFieldsInKeyValue(lines, keep)
}

// keepFieldsInKeyValue keeps only key-value lines whose (normalized) field name exactly
// matches one of the allowed fields. Blank lines are preserved; everything else is dropped.
func keepFieldsInKeyValue(lines []string, fieldsToKeep map[string]bool) string {
	var result []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			result = append(result, line)
			continue
		}

		matches := keyValuePattern.FindStringSubmatch(trimmed)
		if matches != nil && fieldsToKeep[normalizeFieldName(matches[1])] {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// keepFieldsInTabular keeps only the columns whose (normalized) header exactly matches one
// of the allowed fields and removes the rest. Best-effort: multi-word headers are parsed by
// whitespace, so this is primarily intended for the key-value (-instance) form.
func keepFieldsInTabular(lines []string, fieldsToKeep map[string]bool) string {
	if len(lines) == 0 {
		return ""
	}

	headerIdx := -1
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.Count(trimmed, "-") > len(trimmed)/2 {
			if headerIdx == -1 && i > 0 {
				headerIdx = i - 1
			}
			break
		}
		if headerIdx == -1 && !strings.Contains(trimmed, ":") {
			headerIdx = i
		}
	}

	if headerIdx == -1 {
		return strings.Join(lines, "\n")
	}

	columns := parseTableColumns(lines[headerIdx])
	columnsToRemove := make(map[int]bool)
	for i, col := range columns {
		if !fieldsToKeep[normalizeFieldName(col.name)] {
			columnsToRemove[i] = true
		}
	}

	if len(columnsToRemove) == 0 {
		return strings.Join(lines, "\n")
	}

	var result []string
	for i, line := range lines {
		if i >= headerIdx {
			result = append(result, removeColumnsFromLine(line, columns, columnsToRemove))
		} else {
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

// MaskFieldsInCLIOutput replaces field values with asterisks for sensitive data.
// This is an alternative to removing fields entirely.
func MaskFieldsInCLIOutput(output string, fieldsToMask []string) string {
	if output == "" || len(fieldsToMask) == 0 {
		return output
	}

	normalizedFields := make([]string, len(fieldsToMask))
	for i, field := range fieldsToMask {
		normalizedFields[i] = normalizeFieldName(field)
	}

	lines := strings.Split(output, "\n")
	kvPattern := regexp.MustCompile(`^(\s*)([\w\s\.\-]+?)(\s*[:=]\s*)(.*)$`)

	var result []string
	for _, line := range lines {
		matches := kvPattern.FindStringSubmatch(line)
		if matches != nil {
			fieldName := matches[2]
			if fieldMatches(fieldName, normalizedFields) {
				// Replace value with asterisks
				maskedLine := matches[1] + matches[2] + matches[3] + "***"
				result = append(result, maskedLine)
				continue
			}
		}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}
