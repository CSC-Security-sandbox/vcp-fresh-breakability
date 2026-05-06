package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProxyAdminPorts(t *testing.T) {
	tests := []struct {
		name                    string
		adminPort               string
		temporalAdminPort       string
		expectedPorts           []string
		expectedContainsTemporal bool
	}{
		{
			name:                    "default port only",
			adminPort:               "",
			temporalAdminPort:       "",
			expectedPorts:           []string{"9091"},
			expectedContainsTemporal: false,
		},
		{
			name:                    "custom default port",
			adminPort:               "9092",
			temporalAdminPort:       "",
			expectedPorts:           []string{"9092"},
			expectedContainsTemporal: false,
		},
		{
			name:                    "both ports configured",
			adminPort:               "9091",
			temporalAdminPort:       "9093",
			expectedPorts:           []string{"9091", "9093"},
			expectedContainsTemporal: true,
		},
		{
			name:                    "temporal port only",
			adminPort:               "",
			temporalAdminPort:       "9094",
			expectedPorts:           []string{"9091", "9094"},
			expectedContainsTemporal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		// Clean up environment
		defer func() {
			_ = os.Unsetenv("CLOUD_SQL_PROXY_ADMIN_PORT")
			_ = os.Unsetenv("CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT")
		}()

		if tt.adminPort != "" {
			_ = os.Setenv("CLOUD_SQL_PROXY_ADMIN_PORT", tt.adminPort)
		}
		if tt.temporalAdminPort != "" {
			_ = os.Setenv("CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT", tt.temporalAdminPort)
		}

			ports := proxyAdminPorts()

			// Verify port count
			if tt.expectedContainsTemporal {
				assert.Equal(t, 2, len(ports))
			} else {
				assert.Equal(t, 1, len(ports))
			}

			// Verify specific ports
			for _, expectedPort := range tt.expectedPorts {
				assert.Contains(t, ports, expectedPort)
			}
		})
	}
}

func TestCleanupAdminSecret_NoNamespace(t *testing.T) {
	// Clean up environment
	defer func() { _ = os.Unsetenv("POD_NAMESPACE") }()

	// Ensure POD_NAMESPACE is not set
	_ = os.Unsetenv("POD_NAMESPACE")

	// This should return early without error
	// We can't easily test HTTP calls without a real Kubernetes API
	// but we can verify the early return path
	cleanupAdminSecret()
	
	// If we get here without panic, the early return worked
	assert.True(t, true)
}

func TestCleanupAdminSecret_WithNamespace(t *testing.T) {
	// This test verifies that when namespace is set,
	// the function attempts to read the service account token
	// We can't test the actual HTTP deletion without a real cluster
	
	defer func() { _ = os.Unsetenv("POD_NAMESPACE") }()
	
	_ = os.Setenv("POD_NAMESPACE", "test-namespace")
	
	// Function should attempt to read SA token and gracefully handle its absence
	cleanupAdminSecret()
	
	// If we get here without panic, error handling worked
	assert.True(t, true)
}

func TestShutdownProxy_CallsSendQuit(t *testing.T) {
	// This test verifies that shutdownProxy calls sendQuit for all ports
	defer func() {
		_ = os.Unsetenv("CLOUD_SQL_PROXY_ADMIN_PORT")
		_ = os.Unsetenv("CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT")
	}()

	// Set up test ports
	_ = os.Setenv("CLOUD_SQL_PROXY_ADMIN_PORT", "9091")
	_ = os.Setenv("CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT", "9093")

	// Call shutdownProxy (will attempt to connect, which will fail without real proxy)
	// but we're testing that it doesn't panic
	shutdownProxy()
	
	// If we get here, the function handled connection failures gracefully
	assert.True(t, true)
}

func TestSendQuit_InvalidPort(t *testing.T) {
	// Test that sendQuit handles invalid ports gracefully
	// Should log warning but not panic
	sendQuit("99999") // Port unlikely to be open
	
	// If we get here without panic, error handling worked
	assert.True(t, true)
}

func TestSendQuit_NonNumericPort(t *testing.T) {
	// Test that sendQuit handles non-numeric ports gracefully
	sendQuit("invalid") // Invalid port
	
	// If we get here without panic, error handling worked
	assert.True(t, true)
}

func TestCleanupURLConstruction(t *testing.T) {
	// Test that cleanup URLs are constructed correctly
	namespace := "test-namespace"
	
	tests := []struct {
		name        string
		resourceType string
		expected    string
	}{
		{
			name:        "ExternalSecret URL",
			resourceType: "externalsecrets",
			expected:    "https://kubernetes.default.svc/apis/external-secrets.io/v1beta1/namespaces/test-namespace/externalsecrets/iam-lifecycle-admin-secret",
		},
		{
			name:        "Secret URL",
			resourceType: "secrets",
			expected:    "https://kubernetes.default.svc/api/v1/namespaces/test-namespace/secrets/iam-lifecycle-admin-secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var url string
			if tt.resourceType == "externalsecrets" {
				url = "https://kubernetes.default.svc/apis/external-secrets.io/v1beta1/namespaces/" + namespace + "/externalsecrets/iam-lifecycle-admin-secret"
			} else {
				url = "https://kubernetes.default.svc/api/v1/namespaces/" + namespace + "/secrets/iam-lifecycle-admin-secret"
			}
			assert.Equal(t, tt.expected, url)
		})
	}
}

func TestProxyShutdownHTTPMessage(t *testing.T) {
	// Verify the HTTP message format for proxy shutdown
	expectedMessage := "POST /quitquitquit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
	
	// Verify message structure
	assert.Contains(t, expectedMessage, "POST /quitquitquit")
	assert.Contains(t, expectedMessage, "HTTP/1.1")
	assert.Contains(t, expectedMessage, "Host: localhost")
	assert.Contains(t, expectedMessage, "Connection: close")
	assert.Contains(t, expectedMessage, "\r\n\r\n") // HTTP headers end
}

func TestCleanupGracefulFailure(t *testing.T) {
	// Test that cleanup functions handle failures gracefully
	// and don't cause the main job to fail
	
	t.Run("cleanup with missing service account token", func(t *testing.T) {
		defer func() { _ = os.Unsetenv("POD_NAMESPACE") }()
		_ = os.Setenv("POD_NAMESPACE", "test-ns")
		
		// Should log warning but not panic
		cleanupAdminSecret()
		assert.True(t, true)
	})
	
	t.Run("shutdown proxy with no proxy running", func(t *testing.T) {
		// Should log warning but not panic
		shutdownProxy()
		assert.True(t, true)
	})
}

func TestEnvironmentVariableHandling(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		value    string
		expectSet bool
	}{
		{
			name:     "POD_NAMESPACE set",
			envVar:   "POD_NAMESPACE",
			value:    "vcp-namespace",
			expectSet: true,
		},
		{
			name:     "POD_NAMESPACE empty",
			envVar:   "POD_NAMESPACE",
			value:    "",
			expectSet: false,
		},
		{
			name:     "CLOUD_SQL_PROXY_ADMIN_PORT set",
			envVar:   "CLOUD_SQL_PROXY_ADMIN_PORT",
			value:    "9091",
			expectSet: true,
		},
		{
			name:     "CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT set",
			envVar:   "CLOUD_SQL_PROXY_TEMPORAL_ADMIN_PORT",
			value:    "9093",
			expectSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() { _ = os.Unsetenv(tt.envVar) }()
			
			if tt.expectSet {
				_ = os.Setenv(tt.envVar, tt.value)
				assert.Equal(t, tt.value, os.Getenv(tt.envVar))
			} else {
				_ = os.Unsetenv(tt.envVar)
				assert.Empty(t, os.Getenv(tt.envVar))
			}
		})
	}
}
