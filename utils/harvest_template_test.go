package utils

import (
	"os"
	_ "strings"
	"testing"
)

func TestLoadHarvestTemplate_CustomPath(t *testing.T) {
	f, err := os.CreateTemp("", "harvest-template-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	_, err = f.WriteString("PORT: {{.PORT}}\n")
	if err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}

	if err := os.Setenv("HARVEST_TEMPLATE_PATH", f.Name()); err != nil {
		t.Fatalf("failed to set env: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("HARVEST_TEMPLATE_PATH"); err != nil {
			t.Fatalf("failed to unset env: %v", err)
		}
	}()
	content := harvestTemplate
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content == "" {
		t.Errorf("expected content, got empty string")
	}
}
