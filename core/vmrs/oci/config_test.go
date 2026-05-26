// config_test.go covers LoadConfig's two error branches that the
// positive-path tests in single_vm_test.go don't reach:
//
//   - File-open failure (e.g. typo'd path): exercised by pointing
//     LoadConfig at a path that doesn't exist. Anchors the wrapping
//     into ConfigParseError so callers can errors.As against the
//     package's public error type instead of bare os.PathError.
//
//   - YAML decode failure (file present but malformed): exercised by
//     dropping malformed YAML in t.TempDir(). The decoder runs in
//     yaml.Strict() mode, so unknown top-level fields are equivalent
//     to malformed YAML for this purpose.
//
// Both branches must return *ConfigParseError (not a bare error) and
// the formatted message must echo the offending path so an operator
// can see at a glance which file failed.
package oci_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/oci"
)

func TestLoadConfig_FileMissing_ReturnsConfigParseError(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist.yaml")

	cfg, err := oci.LoadConfig(missing)

	require.Error(t, err)
	assert.Nil(t, cfg)

	var parseErr *oci.ConfigParseError
	require.True(t, errors.As(err, &parseErr),
		"LoadConfig must wrap os.Open failures as *ConfigParseError so callers can branch on the package's error type")
	assert.Equal(t, missing, parseErr.Path,
		"ConfigParseError.Path must echo the offending path for operator diagnosis")
	assert.Contains(t, err.Error(), "failed to read config file")
}

func TestLoadConfig_MalformedYAML_ReturnsConfigParseError(t *testing.T) {
	// yaml.Strict() rejects unknown top-level fields; the OCI Config
	// struct only knows `throughput:`, so a GCP-style top-level `vmrs:`
	// key is a clean way to provoke a decode failure that's stable
	// across YAML library upgrades.
	path := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("vmrs:\n  foo: bar\n"), 0o600))

	cfg, err := oci.LoadConfig(path)

	require.Error(t, err)
	assert.Nil(t, cfg)

	var parseErr *oci.ConfigParseError
	require.True(t, errors.As(err, &parseErr),
		"LoadConfig must wrap YAML decode failures as *ConfigParseError")
	assert.Equal(t, path, parseErr.Path)
	assert.Contains(t, err.Error(), "failed to parse config file")
}
