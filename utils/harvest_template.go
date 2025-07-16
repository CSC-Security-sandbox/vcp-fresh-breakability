package utils

import (
	"bytes"
	_ "embed"
	"os"
	"path/filepath"
	"text/template"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

// RenderTemplate takes a HarvestTemplateTokens struct and a template string, and returns the substituted YAML string.
func RenderTemplate(tmplStr string, tokens *datamodel.HarvestConfig) (string, error) {
	tmpl, err := template.New("harvest").Option("missingkey=error").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, tokens)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func RenderHarvestTemplate(tokens *datamodel.HarvestConfig) (string, error) {
	harvestTemplate, err := LoadHarvestTemplate()
	if err != nil {
		return "", err
	}
	return RenderTemplate(harvestTemplate, tokens)
}

// LoadHarvestTemplate loads the harvest-template.yaml file and returns its contents as a string.
func LoadHarvestTemplate() (string, error) {
	path := os.Getenv("HARVEST_TEMPLATE_PATH")
	if path == "" {
		path = filepath.Join("core", "harvest-template.yaml") // default fallback
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
