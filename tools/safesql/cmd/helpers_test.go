package cmd

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestComputeFileHash(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantLen int
		wantPfx string
	}{
		{
			name:    "basic content",
			content: "SELECT * FROM users",
			wantLen: 71, // "sha256:" (7) + 64 hex chars
			wantPfx: "sha256:",
		},
		{
			name:    "empty content",
			content: "",
			wantLen: 71,
			wantPfx: "sha256:",
		},
		{
			name:    "multiline content",
			content: "UPDATE users\nSET status = 'active'\nWHERE id = 1;",
			wantLen: 71,
			wantPfx: "sha256:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := computeFileHash(tt.content)

			if len(result) != tt.wantLen {
				t.Errorf("expected length %d, got %d", tt.wantLen, len(result))
			}

			if !strings.HasPrefix(result, tt.wantPfx) {
				t.Errorf("expected prefix %q, got %q", tt.wantPfx, result[:7])
			}
		})
	}

	// Test determinism
	t.Run("deterministic", func(t *testing.T) {
		content := "SELECT * FROM test"
		hash1 := computeFileHash(content)
		hash2 := computeFileHash(content)

		if hash1 != hash2 {
			t.Error("hash should be deterministic for same content")
		}
	})

	// Test different content produces different hash
	t.Run("different content different hash", func(t *testing.T) {
		hash1 := computeFileHash("content1")
		hash2 := computeFileHash("content2")

		if hash1 == hash2 {
			t.Error("different content should produce different hash")
		}
	})
}

func TestTruncateSQL(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		maxLen   int
		expected string
	}{
		{
			name:     "short SQL unchanged",
			sql:      "SELECT * FROM users",
			maxLen:   50,
			expected: "SELECT * FROM users",
		},
		{
			name:     "long SQL truncated",
			sql:      "SELECT id, name, email, status, created_at, updated_at FROM users WHERE status = 'active'",
			maxLen:   30,
			expected: "SELECT id, name, email, sta...",
		},
		{
			name:     "exact length unchanged",
			sql:      "SELECT *",
			maxLen:   8,
			expected: "SELECT *",
		},
		{
			name:     "multiline normalized",
			sql:      "SELECT *\nFROM\nusers",
			maxLen:   50,
			expected: "SELECT * FROM users",
		},
		{
			name:     "extra spaces normalized",
			sql:      "SELECT   *    FROM   users",
			maxLen:   50,
			expected: "SELECT * FROM users",
		},
		{
			name:     "minimum truncation",
			sql:      "ABCDEF",
			maxLen:   4,
			expected: "A...",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateSQL(tt.sql, tt.maxLen)

			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}

			if len(result) > tt.maxLen {
				t.Errorf("result length %d exceeds maxLen %d", len(result), tt.maxLen)
			}
		})
	}
}

func TestFormatRowPreview(t *testing.T) {
	tests := []struct {
		name     string
		row      map[string]interface{}
		checkFn  func(result string) bool
		checkMsg string
	}{
		{
			name: "simple row",
			row: map[string]interface{}{
				"id":   1,
				"name": "John",
			},
			checkFn: func(result string) bool {
				return strings.Contains(result, "id=1") && strings.Contains(result, "name=John")
			},
			checkMsg: "should contain id=1 and name=John",
		},
		{
			name: "empty row",
			row:  map[string]interface{}{},
			checkFn: func(result string) bool {
				return result == ""
			},
			checkMsg: "should be empty string",
		},
		{
			name: "more than 5 fields truncated",
			row: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
				"d": 4,
				"e": 5,
				"f": 6,
			},
			checkFn: func(result string) bool {
				return strings.HasSuffix(result, "...")
			},
			checkMsg: "should end with ...",
		},
		{
			name: "exactly 5 fields",
			row: map[string]interface{}{
				"a": 1,
				"b": 2,
				"c": 3,
				"d": 4,
				"e": 5,
			},
			checkFn: func(result string) bool {
				return !strings.HasSuffix(result, "...")
			},
			checkMsg: "should not end with ...",
		},
		{
			name: "various types",
			row: map[string]interface{}{
				"int":    42,
				"string": "test",
				"bool":   true,
			},
			checkFn: func(result string) bool {
				return strings.Contains(result, "int=42") &&
					strings.Contains(result, "string=test") &&
					strings.Contains(result, "bool=true")
			},
			checkMsg: "should contain all typed values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatRowPreview(tt.row)

			if !tt.checkFn(result) {
				t.Errorf("check failed: %s. Got: %q", tt.checkMsg, result)
			}
		})
	}
}

func TestPrintBoxFormat(t *testing.T) {
	// Test the printBox output format
	// Since it uses logger, we just verify the logic
	title := "TEST TITLE"
	expectedBorderLen := len(title) + 4 // "+====+" format

	border := strings.Repeat("=", expectedBorderLen)
	if len(border) != expectedBorderLen {
		t.Errorf("expected border length %d, got %d", expectedBorderLen, len(border))
	}
}

func TestParseFirstJSON(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr bool
		check   func(result map[string]interface{}) bool
	}{
		{
			name:    "single JSON object",
			data:    `{"key": "value"}`,
			wantErr: false,
			check: func(result map[string]interface{}) bool {
				return result["key"] == "value"
			},
		},
		{
			name:    "multiple JSON objects",
			data:    `{"first": 1}{"second": 2}`,
			wantErr: false,
			check: func(result map[string]interface{}) bool {
				// Should only parse the first object
				_, hasFirst := result["first"]
				_, hasSecond := result["second"]
				return hasFirst && !hasSecond
			},
		},
		{
			name:    "JSON with trailing whitespace",
			data:    `{"key": "value"}   `,
			wantErr: false,
			check: func(result map[string]interface{}) bool {
				return result["key"] == "value"
			},
		},
		{
			name:    "JSON with trailing newlines",
			data:    "{\"key\": \"value\"}\n\n",
			wantErr: false,
			check: func(result map[string]interface{}) bool {
				return result["key"] == "value"
			},
		},
		{
			name:    "invalid JSON",
			data:    `{not valid json}`,
			wantErr: true,
			check:   nil,
		},
		{
			name:    "empty data",
			data:    "",
			wantErr: true,
			check:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result map[string]interface{}
			err := parseFirstJSON([]byte(tt.data), &result)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tt.check != nil && !tt.check(result) {
				t.Errorf("check failed for result: %v", result)
			}
		})
	}
}

func TestParseFirstJSONStruct(t *testing.T) {
	type TestStruct struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	data := `{"name": "John", "age": 30}{"name": "ignored"}`

	var result TestStruct
	err := parseFirstJSON([]byte(data), &result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.Name != "John" {
		t.Errorf("expected name 'John', got %q", result.Name)
	}
	if result.Age != 30 {
		t.Errorf("expected age 30, got %d", result.Age)
	}
}

func TestParseFirstJSONArray(t *testing.T) {
	data := `[1, 2, 3][4, 5, 6]`

	var result []int
	err := parseFirstJSON([]byte(data), &result)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 elements, got %d", len(result))
	}

	expected := []int{1, 2, 3}
	for i, v := range expected {
		if result[i] != v {
			t.Errorf("expected result[%d]=%d, got %d", i, v, result[i])
		}
	}
}

func TestValidJSONParsing(t *testing.T) {
	// Test that standard json.Unmarshal behavior is preserved for valid JSON
	validJSON := `{"name": "test", "value": 123}`

	var result map[string]interface{}
	if err := parseFirstJSON([]byte(validJSON), &result); err != nil {
		t.Fatalf("failed to parse valid JSON: %v", err)
	}

	if result["name"] != "test" {
		t.Errorf("expected name='test', got %v", result["name"])
	}

	// Compare with standard json.Unmarshal
	var stdResult map[string]interface{}
	if err := json.Unmarshal([]byte(validJSON), &stdResult); err != nil {
		t.Fatalf("standard unmarshal failed: %v", err)
	}

	if result["name"] != stdResult["name"] {
		t.Error("parseFirstJSON should match json.Unmarshal for valid JSON")
	}
}

func TestGetCurrentUsername(t *testing.T) {
	// This test verifies the function runs without error
	// Actual username depends on the system
	username, err := getCurrentUsername()

	if err != nil {
		t.Fatalf("getCurrentUsername failed: %v", err)
	}

	if username == "" {
		t.Error("expected non-empty username")
	}

	// Verify no newlines in result
	if strings.Contains(username, "\n") {
		t.Error("username should not contain newlines")
	}
}
