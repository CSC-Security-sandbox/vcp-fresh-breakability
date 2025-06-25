// This file defines the configuration required for VMRS to make the right decisions.
//
// The configuration is parsed from a YAML file and provides methods to retrieve performance limits for different VM and disk types that are supported by various hyperscalers.

package config

import (
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/goccy/go-yaml"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"golang.org/x/exp/slog"
)

// LoadConfig loads the VMRS configuration from the specified YAML file.
//
// Ideally, this function should be called once at the start of the application to load the configuration.
// It would seem like an ideal candidate to be invoked as part of package initialization. But, package initialization with side-effects is difficult to test, and can lead to unexpected behavior.
func LoadConfig(configFilePath string) (*vmrs.VMRSConfig, error) {
	// Load the config file.
	file, err := os.Open(configFilePath)
	if err != nil {
		readErr := vmrs.ConfigParseError{
			Message: fmt.Sprintf("failed to read config file due to error: %s", err.Error()),
			Path:    configFilePath,
		}
		slog.Error(readErr.Error())
		return nil, &readErr
	}

	var config vmrs.VMRSConfig

	// Unmarshal the YAML content into the VMRSConfig struct, and validate it.
	validate := validator.New()
	dec := yaml.NewDecoder(
		file,
		yaml.Validator(validate),
		yaml.Strict(),
	)
	err = dec.Decode(&config)
	if err != nil {
		parseErr := vmrs.ConfigParseError{
			Message: fmt.Sprintf("failed to parse config file due to error: %s", err.Error()),
			Path:    configFilePath,
		}
		slog.Error(parseErr.Error())
		return nil, &parseErr
	}

	return &config, nil
}
