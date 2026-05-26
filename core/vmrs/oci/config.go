package oci

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/goccy/go-yaml"
)

// LoadConfig reads and decodes the OCI VMRS YAML at configFilePath.
//
// Structural validation (positive capacities, well-formed flex_/vpu_ keys,
// non-empty cells) is deferred to NewSelector so callers see a single
// canonical error path. Call LoadConfig once at startup and cache the
// returned *Config; do not invoke from hot paths.
func LoadConfig(configFilePath string) (*Config, error) {
	file, err := os.Open(configFilePath)
	if err != nil {
		readErr := &ConfigParseError{
			Message: fmt.Sprintf("failed to read config file due to error: %s", err.Error()),
			Path:    configFilePath,
		}
		slog.Error(readErr.Error())
		return nil, readErr
	}
	defer func() { _ = file.Close() }()

	var cfg Config
	dec := yaml.NewDecoder(file, yaml.Strict())
	if err := dec.Decode(&cfg); err != nil {
		parseErr := &ConfigParseError{
			Message: fmt.Sprintf("failed to parse config file due to error: %s", err.Error()),
			Path:    configFilePath,
		}
		slog.Error(parseErr.Error())
		return nil, parseErr
	}

	return &cfg, nil
}
