package utils

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"os"
	_ "strings"
	"testing"
)

func TestLoadHarvestTemplate_DefaultPath(t *testing.T) {
	if err := os.Unsetenv("HARVEST_TEMPLATE_PATH"); err != nil {
		t.Fatalf("failed to unset env: %v", err)
	}
	_, err := LoadHarvestTemplate()
	// File may not exist in test env, just check for error type
	if err == nil {
		t.Errorf("expected error for missing default template file, got nil")
	}
}

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
	content, err := LoadHarvestTemplate()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if content == "" {
		t.Errorf("expected content, got empty string")
	}
}

func TestRenderHarvestTemplate_Integration(t *testing.T) {
	// This test will fail if the default template file does not exist
	if err := os.Unsetenv("HARVEST_TEMPLATE_PATH"); err != nil {
		t.Fatalf("failed to unset env: %v", err)
	}
	tokens := &datamodel.HarvestConfig{PORT: "9999"}
	_, err := RenderHarvestTemplate(tokens)
	if err == nil {
		t.Errorf("expected error for missing template file, got nil")
	}
}
