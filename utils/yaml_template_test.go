package utils

import (
	"strings"
	"testing"
)

func TestSubstituteYAMLTemplate_Basic(t *testing.T) {
	yaml := `
Exporters:
  prometheus:
    port: {{.PROM_PORT}}
  service_control:
    url: "{{.SERVICE_URL}}"
`

	tokens := map[string]string{
		"PROM_PORT":   "12991",
		"SERVICE_URL": "https://servicecontrol.googleapis.com",
	}

	// Act
	out, err := SubstituteYAMLTemplate(yaml, tokens)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(out, "12991") || !strings.Contains(out, "https://servicecontrol.googleapis.com") {
		t.Errorf("token substitution failed: got %q", out)
	}
}

func TestSubstituteYAMLTemplate_MissingToken(t *testing.T) {
	yaml := `port: {{.MISSING}}`
	_, err := SubstituteYAMLTemplate(yaml, map[string]string{})
	if err == nil {
		t.Error("expected error for missing token, got nil")
	}
}

func TestSubstituteYAMLTemplate_EmptyInput(t *testing.T) {
	out, err := SubstituteYAMLTemplate("", map[string]string{"FOO": "bar"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Errorf("expected empty output, got %q", out)
	}
}
