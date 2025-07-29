package utils

import (
	"bytes"
	_ "embed"
	"text/template"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

//go:embed harvest-template.yaml
var harvestTemplate string

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
	return RenderTemplate(harvestTemplate, tokens)
}
