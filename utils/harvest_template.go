package utils

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"fmt"
	"text/template"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

//go:embed harvest-template.yaml
var harvestTemplate string

// HarvestTemplateSHA is a SHA256 hash of the embedded harvest-template.yaml,
// computed at init time. Any change to the template produces a new hash, which
// triggers HarvestPollerUpgradeWorkFlow on the next Core API startup when workflows.LaunchHarvestRefreshIfNeeded runs.
var HarvestTemplateSHA string

func init() {
	HarvestTemplateSHA = computeTemplateHash(harvestTemplate)
}

func computeTemplateHash(content string) string {
	h := sha256.Sum256([]byte(content))
	return fmt.Sprintf("sha256:%x", h)
}

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
