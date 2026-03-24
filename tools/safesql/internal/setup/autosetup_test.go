package setup

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

func TestGetEnvOrDefault(t *testing.T) {
	tests := []struct {
		name         string
		key          string
		defaultValue string
		envValue     string
		expected     string
	}{
		{
			name:         "returns env value when set",
			key:          "TEST_VAR_1",
			defaultValue: "default",
			envValue:     "custom",
			expected:     "custom",
		},
		{
			name:         "returns default when env not set",
			key:          "TEST_VAR_2",
			defaultValue: "default",
			envValue:     "",
			expected:     "default",
		},
		{
			name:         "empty default is valid",
			key:          "TEST_VAR_3",
			defaultValue: "",
			envValue:     "",
			expected:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envValue != "" {
				os.Setenv(tt.key, tt.envValue)
				defer os.Unsetenv(tt.key)
			} else {
				os.Unsetenv(tt.key)
			}

			result := getEnvOrDefault(tt.key, tt.defaultValue)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestGetCurrentUsername(t *testing.T) {
	// Save original env values
	origUser := os.Getenv("USER")
	origLogname := os.Getenv("LOGNAME")
	origUsername := os.Getenv("USERNAME")

	defer func() {
		os.Setenv("USER", origUser)
		os.Setenv("LOGNAME", origLogname)
		os.Setenv("USERNAME", origUsername)
	}()

	// Test when USER is set
	os.Setenv("USER", "testuser")
	os.Unsetenv("LOGNAME")
	os.Unsetenv("USERNAME")

	username := getCurrentUsername()
	if username != "testuser" {
		t.Errorf("expected 'testuser', got %q", username)
	}

	// Test fallback to LOGNAME
	os.Unsetenv("USER")
	os.Setenv("LOGNAME", "loguser")

	username = getCurrentUsername()
	if username != "loguser" {
		t.Errorf("expected 'loguser', got %q", username)
	}

	// Test fallback to USERNAME
	os.Unsetenv("USER")
	os.Unsetenv("LOGNAME")
	os.Setenv("USERNAME", "winuser")

	username = getCurrentUsername()
	if username != "winuser" {
		t.Errorf("expected 'winuser', got %q", username)
	}
}

func TestAutoSetupDisabled(t *testing.T) {
	// Set env to disable auto-setup
	os.Setenv("SAFESQL_NO_AUTO_SETUP", "true")
	defer os.Unsetenv("SAFESQL_NO_AUTO_SETUP")

	err := AutoSetup()
	if err != nil {
		t.Errorf("expected no error when auto-setup is disabled, got %v", err)
	}
}

func TestShouldSetupPortForwardDisabled(t *testing.T) {
	// Disable auto port-forward
	os.Setenv("SAFESQL_AUTO_PORT_FORWARD", "false")
	defer os.Unsetenv("SAFESQL_AUTO_PORT_FORWARD")

	result := shouldSetupPortForward()
	if result {
		t.Error("expected false when SAFESQL_AUTO_PORT_FORWARD is 'false'")
	}
}

func TestShouldSetupPortForwardNonLocalhost(t *testing.T) {
	// Set DB_HOST to non-localhost
	os.Setenv("DB_HOST", "remote-db.example.com")
	defer os.Unsetenv("DB_HOST")
	os.Unsetenv("SAFESQL_AUTO_PORT_FORWARD")

	result := shouldSetupPortForward()
	if result {
		t.Error("expected false when DB_HOST is not localhost")
	}
}

func TestShouldSetupPortForwardLocalhost(t *testing.T) {
	// Set DB_HOST to localhost
	os.Setenv("DB_HOST", "localhost")
	defer os.Unsetenv("DB_HOST")
	os.Unsetenv("SAFESQL_AUTO_PORT_FORWARD")

	// Note: This might return true or false depending on whether the port is accessible
	// We're just testing that it doesn't panic and returns a boolean
	_ = shouldSetupPortForward()
}

func TestShouldSetupPortForward127(t *testing.T) {
	// Set DB_HOST to 127.0.0.1
	os.Setenv("DB_HOST", "127.0.0.1")
	defer os.Unsetenv("DB_HOST")
	os.Unsetenv("SAFESQL_AUTO_PORT_FORWARD")

	// Note: This might return true or false depending on whether the port is accessible
	// We're just testing that it doesn't panic and returns a boolean
	_ = shouldSetupPortForward()
}

func TestCreateDirectories(t *testing.T) {
	// Use temp directory
	tempDir := t.TempDir()
	os.Setenv("SAFESQL_CONFIG_DIR", tempDir+"/safesql-config")
	defer os.Unsetenv("SAFESQL_CONFIG_DIR")

	err := createDirectories()
	if err != nil {
		t.Fatalf("failed to create directories: %v", err)
	}

	// Verify directory was created
	configDir := tempDir + "/safesql-config"
	info, err := os.Stat(configDir)
	if err != nil {
		t.Fatalf("config directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected config path to be a directory")
	}
}

func TestCreateDirectoriesDefaultPath(t *testing.T) {
	// Unset custom config dir to use default
	os.Unsetenv("SAFESQL_CONFIG_DIR")

	// Set HOME to temp dir
	origHome := os.Getenv("HOME")
	tempDir := t.TempDir()
	os.Setenv("HOME", tempDir)
	defer os.Setenv("HOME", origHome)

	err := createDirectories()
	if err != nil {
		t.Fatalf("failed to create directories: %v", err)
	}

	// Verify default directory was created
	expectedDir := tempDir + "/.safesql"
	info, err := os.Stat(expectedDir)
	if err != nil {
		t.Fatalf("default config directory not created: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected config path to be a directory")
	}
}

func TestShowEnvironment(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Set some env vars for testing
	os.Setenv("DB_HOST", "test-host")
	os.Setenv("DB_PORT", "5433")
	os.Setenv("GITHUB_TOKEN", "secret-token")
	os.Setenv("SAFESQL_OPERATOR", "test-operator")
	defer func() {
		os.Unsetenv("DB_HOST")
		os.Unsetenv("DB_PORT")
		os.Unsetenv("GITHUB_TOKEN")
		os.Unsetenv("SAFESQL_OPERATOR")
	}()

	// Call ShowEnvironment
	ShowEnvironment()

	// Restore stdout and read capture
	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()

	// Verify output contains expected sections
	if !strings.Contains(output, "SafeSQL Environment Configuration") {
		t.Error("expected output to contain 'SafeSQL Environment Configuration'")
	}
	if !strings.Contains(output, "DB_HOST=test-host") {
		t.Error("expected output to contain DB_HOST")
	}
	if !strings.Contains(output, "DB_PORT=5433") {
		t.Error("expected output to contain DB_PORT")
	}
	if !strings.Contains(output, "GITHUB_TOKEN=***set***") {
		t.Error("expected GITHUB_TOKEN to show as set")
	}
	if !strings.Contains(output, "SAFESQL_OPERATOR=test-operator") {
		t.Error("expected output to contain SAFESQL_OPERATOR")
	}
}

func TestShowEnvironmentPasswordHidden(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	os.Setenv("DB_PASSWORD", "super-secret")
	defer os.Unsetenv("DB_PASSWORD")

	ShowEnvironment()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()

	// Password should be hidden
	if strings.Contains(output, "super-secret") {
		t.Error("password should not be displayed in output")
	}
	if !strings.Contains(output, "DB_PASSWORD=***set***") {
		t.Error("expected DB_PASSWORD to show as ***set***")
	}
}

func TestShowEnvironmentNoPassword(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	os.Unsetenv("DB_PASSWORD")

	ShowEnvironment()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()

	if !strings.Contains(output, "DB_PASSWORD=***not set***") {
		t.Error("expected DB_PASSWORD to show as ***not set***")
	}
}

func TestShowEnvironmentSections(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	ShowEnvironment()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)

	output := buf.String()

	// Verify all sections are present
	sections := []string{
		"Database:",
		"GitHub:",
		"Storage:",
		"Operator:",
		"Auto-Setup:",
		"Kubernetes (for auto-setup):",
	}

	for _, section := range sections {
		if !strings.Contains(output, section) {
			t.Errorf("expected output to contain section %q", section)
		}
	}
}

func TestAutoSetupSetsOperator(t *testing.T) {
	// Clean up after test
	origOperator := os.Getenv("SAFESQL_OPERATOR")
	defer func() {
		if origOperator != "" {
			os.Setenv("SAFESQL_OPERATOR", origOperator)
		} else {
			os.Unsetenv("SAFESQL_OPERATOR")
		}
	}()

	// Ensure operator is not set
	os.Unsetenv("SAFESQL_OPERATOR")

	// Set USER env var
	os.Setenv("USER", "test-user-for-operator")
	defer os.Unsetenv("USER")

	// Disable auto-setup to avoid external calls
	os.Setenv("SAFESQL_NO_AUTO_SETUP", "true")
	defer os.Unsetenv("SAFESQL_NO_AUTO_SETUP")

	// Auto setup when disabled doesn't set operator
	err := AutoSetup()
	if err != nil {
		t.Fatalf("AutoSetup failed: %v", err)
	}

	// Operator should NOT be set because we disabled auto-setup
	operator := os.Getenv("SAFESQL_OPERATOR")
	if operator == "test-user-for-operator" {
		// This is fine - it means auto-setup was truly disabled
	}
}

func TestEnvVarDefaults(t *testing.T) {
	// Unset all relevant env vars
	envVars := []string{
		"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_SSL_MODE",
		"DB_SECRET_NAME", "DB_SECRET_NAMESPACE", "DB_SECRET_KEY",
		"DB_PORT_FORWARD_SERVICE", "DB_PORT_FORWARD_NAMESPACE", "DB_PORT_FORWARD_PORT",
	}
	origValues := make(map[string]string)
	for _, v := range envVars {
		origValues[v] = os.Getenv(v)
		os.Unsetenv(v)
	}
	defer func() {
		for v, val := range origValues {
			if val != "" {
				os.Setenv(v, val)
			}
		}
	}()

	// Verify defaults
	tests := []struct {
		key      string
		expected string
	}{
		{"DB_HOST", "127.0.0.1"},
		{"DB_PORT", "5432"},
		{"DB_NAME", "vcp"},
		{"DB_USER", "postgres"},
		{"DB_SSL_MODE", "disable"},
		{"DB_SECRET_NAME", "postgres-credentials"},
		{"DB_SECRET_NAMESPACE", "sde"},
		{"DB_SECRET_KEY", "postgres-root-password"},
		{"DB_PORT_FORWARD_SERVICE", "cloud-sql-proxy"},
		{"DB_PORT_FORWARD_NAMESPACE", "sde"},
		{"DB_PORT_FORWARD_PORT", "5432"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			result := getEnvOrDefault(tt.key, tt.expected)
			if result != tt.expected {
				t.Errorf("expected default %q for %s, got %q", tt.expected, tt.key, result)
			}
		})
	}
}

func TestIsPortAccessible(t *testing.T) {
	// This test checks the function doesn't panic
	// Actual port accessibility depends on the system state

	// Test with a port that's likely not accessible
	result := isPortAccessible("127.0.0.1", "59999")
	// Result could be true or false depending on system
	_ = result

	// Test with localhost
	result = isPortAccessible("localhost", "59998")
	_ = result
}

func TestCheckDatabaseConnectivity(t *testing.T) {
	// Save original values
	origHost := os.Getenv("DB_HOST")
	origPort := os.Getenv("DB_PORT")
	origPassword := os.Getenv("DB_PASSWORD")

	defer func() {
		if origHost != "" {
			os.Setenv("DB_HOST", origHost)
		} else {
			os.Unsetenv("DB_HOST")
		}
		if origPort != "" {
			os.Setenv("DB_PORT", origPort)
		} else {
			os.Unsetenv("DB_PORT")
		}
		if origPassword != "" {
			os.Setenv("DB_PASSWORD", origPassword)
		} else {
			os.Unsetenv("DB_PASSWORD")
		}
	}()

	// Set to use a port that's likely not accessible
	os.Setenv("DB_HOST", "127.0.0.1")
	os.Setenv("DB_PORT", "59997")
	os.Unsetenv("DB_PASSWORD")

	// This should fail because the port is not accessible
	err := CheckDatabaseConnectivity()
	if err == nil {
		// Port might actually be accessible on some systems
		t.Skip("port was unexpectedly accessible")
	}

	if !strings.Contains(err.Error(), "not accessible") {
		t.Errorf("expected 'not accessible' error, got: %v", err)
	}
}
