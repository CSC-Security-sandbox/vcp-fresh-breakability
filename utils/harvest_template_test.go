package utils

import (
	"os"
	"strings"
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

func TestHarvestTemplateSHA_MatchesEmbeddedTemplateBytes(t *testing.T) {
	// harvestTemplate is the same bytes embedded at build time that init() hashes into HarvestTemplateSHA.
	want := computeTemplateHash(harvestTemplate)
	if HarvestTemplateSHA != want {
		t.Errorf("HarvestTemplateSHA = %q, want computeTemplateHash(embed) = %q", HarvestTemplateSHA, want)
	}
}

func TestHarvestTemplateSHA_IsSet(t *testing.T) {
	if HarvestTemplateSHA == "" {
		t.Error("HarvestTemplateSHA should be set at init time, got empty string")
	}
	if !strings.HasPrefix(HarvestTemplateSHA, "sha256:") {
		t.Errorf("HarvestTemplateSHA should start with 'sha256:', got %q", HarvestTemplateSHA)
	}
	// sha256 hex = 64 chars + "sha256:" prefix = 71 chars
	if len(HarvestTemplateSHA) != 71 {
		t.Errorf("HarvestTemplateSHA length = %d, want 71 (sha256: + 64 hex chars)", len(HarvestTemplateSHA))
	}
}

func TestComputeTemplateHash(t *testing.T) {
	t.Run("DeterministicForSameContent", func(tt *testing.T) {
		content := "Exporters:\n  prometheus:\n    port: 9090\n"
		h1 := computeTemplateHash(content)
		h2 := computeTemplateHash(content)
		if h1 != h2 {
			tt.Errorf("same content produced different hashes: %q vs %q", h1, h2)
		}
	})

	t.Run("DifferentForDifferentContent", func(tt *testing.T) {
		h1 := computeTemplateHash("version: 1")
		h2 := computeTemplateHash("version: 2")
		if h1 == h2 {
			tt.Error("different content should produce different hashes")
		}
	})

	t.Run("HasSHA256Prefix", func(tt *testing.T) {
		h := computeTemplateHash("any content")
		if !strings.HasPrefix(h, "sha256:") {
			tt.Errorf("hash should start with 'sha256:', got %q", h)
		}
	})

	t.Run("EmptyContentProducesValidHash", func(tt *testing.T) {
		h := computeTemplateHash("")
		if !strings.HasPrefix(h, "sha256:") {
			tt.Errorf("hash should start with 'sha256:', got %q", h)
		}
		if len(h) != 71 {
			tt.Errorf("hash length = %d, want 71", len(h))
		}
	})

	t.Run("SingleCharChangeProducesDifferentHash", func(tt *testing.T) {
		base := "Exporters:\n  prometheus:\n    port: 9090\n"
		modified := "Exporters:\n  prometheus:\n    port: 9091\n"
		h1 := computeTemplateHash(base)
		h2 := computeTemplateHash(modified)
		if h1 == h2 {
			tt.Error("single char change should produce a different hash")
		}
	})
}
