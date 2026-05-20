package cli

import (
	"strings"
	"testing"
)

func TestRemoveFieldsFromCLIOutput_KeyValue(t *testing.T) {
	t.Run("basic key-value removal", func(t *testing.T) {
		input := `Volume Name: vol1
Aggregate: aggr1
Physical Used: 50GB
Available: 100GB
State: online`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "Physical Used") {
			t.Error("Expected 'Physical Used' to be removed")
		}
		if !strings.Contains(result, "Volume Name") {
			t.Error("Expected 'Volume Name' to remain")
		}
		if !strings.Contains(result, "Aggregate") {
			t.Error("Expected 'Aggregate' to remain")
		}
		if !strings.Contains(result, "Available") {
			t.Error("Expected 'Available' to remain")
		}
	})

	t.Run("multiple fields removal", func(t *testing.T) {
		input := `Volume Name: vol1
Aggregate: aggr1
Physical Used: 50GB
Efficiency: enabled
Available: 100GB`

		fieldsToRemove := []string{"physical_used", "efficiency"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "Physical Used") {
			t.Error("Expected 'Physical Used' to be removed")
		}
		if strings.Contains(result, "Efficiency") {
			t.Error("Expected 'Efficiency' to be removed")
		}
		if !strings.Contains(result, "Volume Name") {
			t.Error("Expected 'Volume Name' to remain")
		}
	})

	t.Run("partial field name matching", func(t *testing.T) {
		input := `Volume Name: vol1
Physical Used Space: 50GB
Logical Used Space: 40GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "Physical Used Space") {
			t.Error("Expected 'Physical Used Space' to be removed (partial match)")
		}
		if !strings.Contains(result, "Logical Used Space") {
			t.Error("Expected 'Logical Used Space' to remain")
		}
	})

	t.Run("case insensitive matching", func(t *testing.T) {
		input := `PHYSICAL USED: 50GB
physical used: 40GB
Physical_Used: 30GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		lines := strings.Split(strings.TrimSpace(result), "\n")
		for _, line := range lines {
			if strings.Contains(strings.ToLower(line), "physical") {
				t.Errorf("Expected line containing 'physical' to be removed: %s", line)
			}
		}
	})

	t.Run("equals sign format", func(t *testing.T) {
		input := `Volume Name = vol1
Physical Used = 50GB
Available = 100GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "Physical Used") {
			t.Error("Expected 'Physical Used' (with = sign) to be removed")
		}
		if !strings.Contains(result, "Volume Name") {
			t.Error("Expected 'Volume Name' to remain")
		}
	})

	t.Run("indented nested fields", func(t *testing.T) {
		input := `Volume Name: vol1
Space:
    Total: 100GB
    Physical Used: 50GB
    Available: 50GB
State: online`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "Physical Used") {
			t.Error("Expected 'Physical Used' to be removed")
		}
		if !strings.Contains(result, "Total") {
			t.Error("Expected 'Total' to remain")
		}
		if !strings.Contains(result, "State") {
			t.Error("Expected 'State' to remain")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		result := RemoveFieldsFromCLIOutput("", []string{"field"})
		if result != "" {
			t.Errorf("Expected empty output, got %q", result)
		}
	})

	t.Run("no fields to remove", func(t *testing.T) {
		input := "Volume Name: vol1"
		result := RemoveFieldsFromCLIOutput(input, []string{})
		if result != input {
			t.Errorf("Expected unchanged output, got %q", result)
		}
	})

	t.Run("dotted field names", func(t *testing.T) {
		input := `Volume Name: vol1
space.logical_space.enforcement: true
space.logical_space.reporting: true
Available: 100GB`

		fieldsToRemove := []string{"space.logical_space.enforcement"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(result, "enforcement") {
			t.Error("Expected 'space.logical_space.enforcement' to be removed")
		}
		if !strings.Contains(result, "reporting") {
			t.Error("Expected 'reporting' to remain")
		}
	})
}

func TestRemoveFieldsFromCLIOutput_Tabular(t *testing.T) {
	t.Run("basic tabular removal", func(t *testing.T) {
		input := `Volume     Aggregate    Physical_Used    Available
------     ---------    -------------    ---------
vol1       aggr1        50GB             50GB
vol2       aggr2        30GB             70GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		// Check that Physical_Used column is removed
		lines := strings.Split(result, "\n")
		for _, line := range lines {
			// The column name should be removed
			if strings.Contains(strings.ToLower(line), "physical_used") {
				t.Errorf("Expected 'Physical_Used' column to be removed, line: %s", line)
			}
		}

		// Other columns should remain
		if !strings.Contains(result, "Volume") {
			t.Error("Expected 'Volume' column to remain")
		}
		if !strings.Contains(result, "Aggregate") {
			t.Error("Expected 'Aggregate' column to remain")
		}
	})

	t.Run("multiple column removal", func(t *testing.T) {
		input := `Volume     Physical_Used    Efficiency    Available
------     -------------    ----------    ---------
vol1       50GB             enabled       50GB`

		fieldsToRemove := []string{"physical_used", "efficiency"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(strings.ToLower(result), "physical_used") {
			t.Error("Expected 'Physical_Used' column to be removed")
		}
		if strings.Contains(strings.ToLower(result), "efficiency") {
			t.Error("Expected 'Efficiency' column to be removed")
		}
		if !strings.Contains(result, "Volume") {
			t.Error("Expected 'Volume' column to remain")
		}
	})
}

func TestNormalizeFieldName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"physical_used", "physical used"},
		{"Physical_Used", "physical used"},
		{"PHYSICAL_USED", "physical used"},
		{"Physical Used", "physical used"},
		{"physical.used", "physical used"},
		{"space.logical_space.enforcement", "space logical space enforcement"},
		{"  physical_used  ", "physical used"},
		{"physical__used", "physical used"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := normalizeFieldName(tt.input)
			if got != tt.want {
				t.Errorf("normalizeFieldName(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFieldMatches(t *testing.T) {
	tests := []struct {
		name           string
		fieldName      string
		fieldsToRemove []string
		want           bool
	}{
		{
			name:           "exact match",
			fieldName:      "Physical Used",
			fieldsToRemove: []string{"physical used"},
			want:           true,
		},
		{
			name:           "partial match",
			fieldName:      "Physical Used Space",
			fieldsToRemove: []string{"physical used"},
			want:           true,
		},
		{
			name:           "underscore match",
			fieldName:      "Physical_Used",
			fieldsToRemove: []string{"physical used"},
			want:           true,
		},
		{
			name:           "no match",
			fieldName:      "Available",
			fieldsToRemove: []string{"physical used"},
			want:           false,
		},
		{
			name:           "multiple fields match",
			fieldName:      "Efficiency",
			fieldsToRemove: []string{"physical used", "efficiency"},
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Normalize fields to remove
			normalized := make([]string, len(tt.fieldsToRemove))
			for i, f := range tt.fieldsToRemove {
				normalized[i] = normalizeFieldName(f)
			}

			got := fieldMatches(tt.fieldName, normalized)
			if got != tt.want {
				t.Errorf("fieldMatches(%q, %v) = %v, want %v", tt.fieldName, tt.fieldsToRemove, got, tt.want)
			}
		})
	}
}

func TestIsTabularOutput(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		want  bool
	}{
		{
			name: "tabular with separator",
			lines: []string{
				"Volume     Aggregate    Size",
				"------     ---------    ----",
				"vol1       aggr1        100GB",
			},
			want: true,
		},
		{
			name: "key-value format",
			lines: []string{
				"Volume Name: vol1",
				"Aggregate: aggr1",
				"Size: 100GB",
			},
			want: false,
		},
		{
			name:  "empty",
			lines: []string{},
			want:  false,
		},
		{
			name: "single line",
			lines: []string{
				"Volume Name: vol1",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTabularOutput(tt.lines)
			if got != tt.want {
				t.Errorf("isTabularOutput() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMaskFieldsInCLIOutput(t *testing.T) {
	t.Run("basic masking", func(t *testing.T) {
		input := `Volume Name: vol1
Password: secret123
Available: 100GB`

		fieldsToMask := []string{"password"}

		result := MaskFieldsInCLIOutput(input, fieldsToMask)

		if !strings.Contains(result, "Password: ***") {
			t.Error("Expected 'Password' value to be masked")
		}
		if !strings.Contains(result, "Volume Name: vol1") {
			t.Error("Expected 'Volume Name' to remain unchanged")
		}
		if strings.Contains(result, "secret123") {
			t.Error("Expected password value to be removed")
		}
	})

	t.Run("empty input", func(t *testing.T) {
		result := MaskFieldsInCLIOutput("", []string{"field"})
		if result != "" {
			t.Errorf("Expected empty output, got %q", result)
		}
	})

	t.Run("WhenNoFieldsToMask_ShouldReturnUnchanged", func(t *testing.T) {
		input := "Volume Name: vol1"
		result := MaskFieldsInCLIOutput(input, []string{})
		if result != input {
			t.Errorf("Expected unchanged output, got %q", result)
		}
	})
}

func TestIsTabularOutput_EdgeCases(t *testing.T) {
	t.Run("WhenEmptyLinesBeforeHeader_ShouldDetectTabular", func(t *testing.T) {
		lines := []string{
			"",
			"",
			"Volume     Aggregate    Size",
			"------     ---------    ----",
			"vol1       aggr1        100GB",
		}
		if !isTabularOutput(lines) {
			t.Error("Expected tabular output with empty lines at start")
		}
	})

	t.Run("WhenTabularWithoutSeparator_ShouldDetectFromColumns", func(t *testing.T) {
		lines := []string{
			"Volume Name Aggregate Size State",
			"vol1 aggr1 100GB online",
		}
		if !isTabularOutput(lines) {
			t.Error("Expected tabular output detected from multiple columns")
		}
	})

	t.Run("WhenEqualsSignPresent_ShouldDetectKeyValue", func(t *testing.T) {
		lines := []string{
			"Volume Name = vol1",
			"Aggregate = aggr1",
		}
		if isTabularOutput(lines) {
			t.Error("Expected key-value format detected from equals sign")
		}
	})

	t.Run("WhenAllLinesEmpty_ShouldReturnFalse", func(t *testing.T) {
		lines := []string{"", "", ""}
		if isTabularOutput(lines) {
			t.Error("Expected false for all empty lines")
		}
	})
}

func TestRemoveFieldsFromKeyValue_EdgeCases(t *testing.T) {
	t.Run("WhenNestedFieldsWithIndentation_ShouldRemoveMatchedLine", func(t *testing.T) {
		input := `Volume Name: vol1
Space:
    Total: 100GB
    Physical Used: 50GB
        Sub Field: nested value
    Available: 50GB
State: online`

		fieldsToRemove := []string{"physical used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		// Physical Used line should be removed; sub-fields are independent lines and kept
		if strings.Contains(result, "Physical Used") {
			t.Error("Expected 'Physical Used' to be removed")
		}
		if !strings.Contains(result, "Total") {
			t.Error("Expected 'Total' to remain")
		}
		if !strings.Contains(result, "State") {
			t.Error("Expected 'State' to remain")
		}
	})

	t.Run("WhenEmptyLinesPresent_ShouldPreserveThem", func(t *testing.T) {
		input := `Volume Name: vol1

Physical Used: 50GB

Available: 100GB`

		fieldsToRemove := []string{"physical used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		// Empty lines should be preserved
		lines := strings.Split(result, "\n")
		emptyCount := 0
		for _, line := range lines {
			if strings.TrimSpace(line) == "" {
				emptyCount++
			}
		}
		if emptyCount < 1 {
			t.Error("Expected empty lines to be preserved")
		}
	})

	t.Run("WhenNonKeyValueLinesPresent_ShouldPreserveThem", func(t *testing.T) {
		input := `Title Line Without Colon
Volume Name: vol1
Physical Used: 50GB
Another plain line
Available: 100GB`

		fieldsToRemove := []string{"physical used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if !strings.Contains(result, "Title Line Without Colon") {
			t.Error("Expected non-kv lines to be preserved")
		}
		if !strings.Contains(result, "Another plain line") {
			t.Error("Expected non-kv lines to be preserved")
		}
	})
}

func TestRemoveFieldsFromTabular_EdgeCases(t *testing.T) {
	t.Run("WhenInputIsEmpty_ShouldReturnEmpty", func(t *testing.T) {
		result := removeFieldsFromTabular([]string{}, []string{"field"})
		if result != "" {
			t.Errorf("Expected empty result for empty input, got %q", result)
		}
	})

	t.Run("WhenCannotIdentifyHeader_ShouldReturnUnchanged", func(t *testing.T) {
		// All lines contain ":" so header can't be identified
		lines := []string{
			"Field: value",
			"Another: value",
		}
		result := removeFieldsFromTabular(lines, []string{"field"})
		if result != "Field: value\nAnother: value" {
			t.Errorf("Expected unchanged output when header can't be identified, got %q", result)
		}
	})

	t.Run("WhenNoColumnsMatch_ShouldReturnUnchanged", func(t *testing.T) {
		input := `Volume     Aggregate    Size
------     ---------    ----
vol1       aggr1        100GB`

		result := RemoveFieldsFromCLIOutput(input, []string{"nonexistent"})

		if !strings.Contains(result, "Volume") {
			t.Error("Expected all columns to remain when no match")
		}
		if !strings.Contains(result, "Aggregate") {
			t.Error("Expected all columns to remain when no match")
		}
	})

	t.Run("WhenSeparatorPresent_ShouldIdentifyHeaderFromIt", func(t *testing.T) {
		// When there's a separator line, the line immediately before it is the header
		input := `Volume     Physical_Used    Available
------     -------------    ---------
vol1       50GB             50GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if !strings.Contains(result, "Volume") {
			t.Error("Expected Volume column to remain")
		}
		if strings.Contains(strings.ToLower(result), "physical_used") {
			t.Error("Expected Physical_Used column to be removed")
		}
		if !strings.Contains(result, "Available") {
			t.Error("Expected Available column to remain")
		}
	})

	t.Run("WhenNoSeparator_ShouldIdentifyHeaderFromFirstLine", func(t *testing.T) {
		input := `Volume Aggregate Physical_Used Available
vol1 aggr1 50GB 50GB
vol2 aggr2 30GB 70GB`

		fieldsToRemove := []string{"physical_used"}

		result := RemoveFieldsFromCLIOutput(input, fieldsToRemove)

		if strings.Contains(strings.ToLower(result), "physical_used") {
			t.Error("Expected Physical_Used column to be removed")
		}
	})
}

func TestParseTableColumns(t *testing.T) {
	t.Run("WhenBasicHeader_ShouldParseColumns", func(t *testing.T) {
		headerLine := "Volume     Aggregate    Size"
		columns := parseTableColumns(headerLine)

		if len(columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(columns))
		}
		if columns[0].name != "Volume" {
			t.Errorf("Expected first column 'Volume', got %q", columns[0].name)
		}
	})

	t.Run("WhenWordAtEndOfLine_ShouldExtendToEnd", func(t *testing.T) {
		headerLine := "Volume Aggregate Size"
		columns := parseTableColumns(headerLine)

		if len(columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(columns))
		}
		// Last column should extend to end of line
		if columns[2].name != "Size" {
			t.Errorf("Expected last column 'Size', got %q", columns[2].name)
		}
	})

	t.Run("WhenTabsAndSpaces_ShouldParseCorrectly", func(t *testing.T) {
		headerLine := "Volume\t\tAggregate\t\tSize"
		columns := parseTableColumns(headerLine)

		if len(columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(columns))
		}
	})
}

func TestRemoveColumnsFromLine(t *testing.T) {
	t.Run("WhenLineIsEmpty_ShouldReturnEmpty", func(t *testing.T) {
		columns := []tableColumn{
			{name: "Vol", start: 0, end: 5},
		}
		result := removeColumnsFromLine("", columns, map[int]bool{0: true})
		if result != "" {
			t.Errorf("Expected empty result for empty line, got %q", result)
		}
	})

	t.Run("WhenLineShorterThanColumnPositions_ShouldHandleGracefully", func(t *testing.T) {
		// Column positions exceed line length
		columns := []tableColumn{
			{name: "Vol", start: 0, end: 10},
			{name: "Aggr", start: 15, end: 25},
		}
		line := "vol1"
		result := removeColumnsFromLine(line, columns, map[int]bool{1: true})

		// Should handle gracefully
		if result != "vol1" {
			t.Errorf("Expected 'vol1', got %q", result)
		}
	})

	t.Run("WhenRemovingFirstColumn_ShouldRemoveIt", func(t *testing.T) {
		columns := []tableColumn{
			{name: "Vol", start: 0, end: 10},
			{name: "Aggr", start: 10, end: 20},
		}
		line := "vol1      aggr1     "
		result := removeColumnsFromLine(line, columns, map[int]bool{0: true})

		if strings.Contains(result, "vol1") {
			t.Error("Expected first column to be removed")
		}
		if !strings.Contains(result, "aggr1") {
			t.Error("Expected second column to remain")
		}
	})

	t.Run("WhenRemovingMiddleColumn_ShouldRemoveIt", func(t *testing.T) {
		columns := []tableColumn{
			{name: "Vol", start: 0, end: 10},
			{name: "Used", start: 10, end: 20},
			{name: "Avail", start: 20, end: 30},
		}
		line := "vol1      50GB      100GB     "
		result := removeColumnsFromLine(line, columns, map[int]bool{1: true})

		if !strings.Contains(result, "vol1") {
			t.Error("Expected first column to remain")
		}
		if !strings.Contains(result, "100GB") {
			t.Error("Expected third column to remain")
		}
	})

	t.Run("WhenColumnEndExceedsLineLength_ShouldHandleGracefully", func(t *testing.T) {
		columns := []tableColumn{
			{name: "Vol", start: 0, end: 10},
			{name: "Used", start: 10, end: 100}, // End exceeds line
		}
		line := "vol1      50GB"
		result := removeColumnsFromLine(line, columns, map[int]bool{1: true})

		if !strings.Contains(result, "vol1") {
			t.Error("Expected first column to remain")
		}
	})
}
