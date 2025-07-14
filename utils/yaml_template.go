package utils

import (
	"bytes"
	"text/template"
)

// SubstituteYAMLTemplate takes a YAML string with Go template tokens (e.g., {{.TOKEN}})
// and a map of string substitutions, and returns the substituted YAML string.
func SubstituteYAMLTemplate(yamlInput string, tokens map[string]string) (string, error) {
	tmpl, err := template.New("yaml").Option("missingkey=error").Parse(yamlInput)
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
