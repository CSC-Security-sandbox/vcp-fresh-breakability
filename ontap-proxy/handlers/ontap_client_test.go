package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

// mockBackendMetricsRecorder captures backend metric calls for tests. Avoids process-wide Prometheus/OTel state.
type mockBackendMetricsRecorder struct {
	mu sync.Mutex

	RecordBackendRequestCalls   []struct{ Method, ProjectID, PoolID, Path string; StatusCode int }
	RecordBackendDurationCalls  []struct{ Duration float64; Method, ProjectID, PoolID, Path string; StatusCode int }
	RecordBackendErrorCalls     []struct{ Method, ProjectID, PoolID, Path, ErrorCode string }
}

func (m *mockBackendMetricsRecorder) RecordBackendRequest(_ context.Context, method, projectID, poolID, path string, statusCode int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RecordBackendRequestCalls = append(m.RecordBackendRequestCalls, struct{ Method, ProjectID, PoolID, Path string; StatusCode int }{method, projectID, poolID, path, statusCode})
}

func (m *mockBackendMetricsRecorder) RecordBackendDuration(_ context.Context, durationSec float64, method, projectID, poolID, path string, statusCode int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RecordBackendDurationCalls = append(m.RecordBackendDurationCalls, struct{ Duration float64; Method, ProjectID, PoolID, Path string; StatusCode int }{durationSec, method, projectID, poolID, path, statusCode})
}

func (m *mockBackendMetricsRecorder) RecordBackendError(_ context.Context, method, projectID, poolID, path, errorCode string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RecordBackendErrorCalls = append(m.RecordBackendErrorCalls, struct{ Method, ProjectID, PoolID, Path, ErrorCode string }{method, projectID, poolID, path, errorCode})
}

func TestOntapClient_GetVolume_RecordsBackendMetrics(t *testing.T) {
	t.Run("WhenSuccessRecordsBackendRequestTotalAndRequestDurationSeconds", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(VolumeInfo{UUID: "u", Name: "v", SVM: struct {
				Name string `json:"name"`
				UUID string `json:"uuid"`
			}{Name: "svm", UUID: "s"}})
		}))
		defer server.Close()

		rec := &mockBackendMetricsRecorder{}
		client := &RestOntapClient{
			httpClient:      server.Client(),
			endpoint:        server.Listener.Addr().String(),
			authData:        &models.AuthData{AuthType: models.USERNAME_PWD, Username: "u", Password: "p"},
			backendRecorder: rec,
		}
		ctx := middleware.AddBackendMetricsToContext(context.Background(), "proj-1", "pool-1", "/api/storage/volumes")

		_, err := client.GetVolume(ctx, "uuid")
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(rec.RecordBackendRequestCalls), 1)
		require.GreaterOrEqual(t, len(rec.RecordBackendDurationCalls), 1)
		assert.Equal(t, "GET", rec.RecordBackendRequestCalls[0].Method)
		assert.Equal(t, "proj-1", rec.RecordBackendRequestCalls[0].ProjectID)
		assert.Equal(t, "pool-1", rec.RecordBackendRequestCalls[0].PoolID)
		assert.Equal(t, "/api/storage/volumes", rec.RecordBackendRequestCalls[0].Path)
		assert.Equal(t, 200, rec.RecordBackendRequestCalls[0].StatusCode)
	})

	t.Run("WhenErrorOr4xxRecordsBackendErrorsTotal", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		rec := &mockBackendMetricsRecorder{}
		client := &RestOntapClient{
			httpClient:      server.Client(),
			endpoint:        server.Listener.Addr().String(),
			authData:        &models.AuthData{AuthType: models.USERNAME_PWD, Username: "u", Password: "p"},
			backendRecorder: rec,
		}
		ctx := middleware.AddBackendMetricsToContext(context.Background(), "proj-2", "pool-2", "/api/storage/volumes")

		_, err := client.GetVolume(ctx, "missing")
		assert.Error(t, err)

		require.GreaterOrEqual(t, len(rec.RecordBackendErrorCalls), 1)
		assert.Equal(t, "GET", rec.RecordBackendErrorCalls[0].Method)
		assert.Equal(t, "proj-2", rec.RecordBackendErrorCalls[0].ProjectID)
		assert.Equal(t, "pool-2", rec.RecordBackendErrorCalls[0].PoolID)
		assert.Equal(t, "/api/storage/volumes", rec.RecordBackendErrorCalls[0].Path)
		assert.Equal(t, "404", rec.RecordBackendErrorCalls[0].ErrorCode)
	})
}

func TestOntapClient_ExecuteCLI_RecordsBackendMetrics(t *testing.T) {
	t.Run("WhenSuccessRecordsBackendRequestTotalAndRequestDurationSeconds", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(CLIResponse{Output: "ok"})
		}))
		defer server.Close()

		rec := &mockBackendMetricsRecorder{}
		client := &RestOntapClient{
			httpClient:      server.Client(),
			endpoint:        server.Listener.Addr().String(),
			authData:        &models.AuthData{AuthType: models.USERNAME_PWD, Username: "u", Password: "p"},
			backendRecorder: rec,
		}
		ctx := middleware.AddBackendMetricsToContext(context.Background(), "proj-3", "pool-3", "/api/private/cli")

		_, err := client.ExecuteCLI(ctx, "version", "admin")
		require.NoError(t, err)

		require.GreaterOrEqual(t, len(rec.RecordBackendRequestCalls), 1)
		assert.Equal(t, "POST", rec.RecordBackendRequestCalls[0].Method)
		assert.Equal(t, "proj-3", rec.RecordBackendRequestCalls[0].ProjectID)
		assert.Equal(t, "pool-3", rec.RecordBackendRequestCalls[0].PoolID)
		assert.Equal(t, "/api/private/cli", rec.RecordBackendRequestCalls[0].Path)
		assert.Equal(t, 200, rec.RecordBackendRequestCalls[0].StatusCode)
	})

	t.Run("When4xxResponseRecordsBackendErrorsTotal", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": {"message": "bad"}}`))
		}))
		defer server.Close()

		rec := &mockBackendMetricsRecorder{}
		client := &RestOntapClient{
			httpClient:      server.Client(),
			endpoint:        server.Listener.Addr().String(),
			authData:        &models.AuthData{AuthType: models.USERNAME_PWD, Username: "u", Password: "p"},
			backendRecorder: rec,
		}
		ctx := middleware.AddBackendMetricsToContext(context.Background(), "proj-4", "pool-4", "/api/private/cli")

		_, err := client.ExecuteCLI(ctx, "invalid", "admin")
		assert.Error(t, err)

		require.GreaterOrEqual(t, len(rec.RecordBackendErrorCalls), 1)
		assert.Equal(t, "POST", rec.RecordBackendErrorCalls[0].Method)
		assert.Equal(t, "proj-4", rec.RecordBackendErrorCalls[0].ProjectID)
		assert.Equal(t, "pool-4", rec.RecordBackendErrorCalls[0].PoolID)
		assert.Equal(t, "/api/private/cli", rec.RecordBackendErrorCalls[0].Path)
		assert.Equal(t, "400", rec.RecordBackendErrorCalls[0].ErrorCode)
	})
}

func TestNewOntapClientFromContext(t *testing.T) {
	t.Run("returns error when cache key not in context", func(t *testing.T) {
		ctx := context.Background()

		client, err := NewOntapClientFromContext(ctx)

		assert.Nil(t, client)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no auth data cache key found in context")
	})

	t.Run("returns error when auth data not in cache", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), models.AuthDataKey, "non-existent-key")

		client, err := NewOntapClientFromContext(ctx)

		assert.Nil(t, client)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "auth data not found in cache")
	})

	t.Run("successfully creates client when auth data is in cache", func(t *testing.T) {
		// Setup mock ONTAP server
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Return 200 OK for connection pool health check
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		// Extract host:port from server URL (remove https:// prefix)
		endpoint := strings.TrimPrefix(server.URL, "https://")

		// Create auth data with the mock server endpoint
		authData := &models.AuthData{
			AuthType:    models.USERNAME_PWD,
			Username:    "testuser",
			Password:    "testpass",
			PoolID:      "test-pool-success",
			AccountName: "test-account-success",
			OntapEndpoints: []models.OntapEndpoint{
				{DNS: endpoint, IP: endpoint}, // DNS is used for reachability check
			},
		}

		// Add to cache
		cacheKey := "auth:test-pool-success"
		cache.AddToAuthDataCache(cacheKey, authData)
		defer cache.RemoveFromAuthDataCache(cacheKey)

		// Create context with cache key
		ctx := context.WithValue(context.Background(), models.AuthDataKey, cacheKey)

		// Execute
		client, err := NewOntapClientFromContext(ctx)

		// Verify
		require.NoError(t, err)
		assert.NotNil(t, client)
	})
}

func TestOntapClient_GetVolume(t *testing.T) {
	t.Run("successfully retrieves volume info", func(t *testing.T) {
		// Setup mock server
		expectedVolume := VolumeInfo{
			UUID: "test-uuid-123",
			Name: "test-volume",
			SVM: struct {
				Name string `json:"name"`
				UUID string `json:"uuid"`
			}{
				Name: "test-svm",
				UUID: "svm-uuid-456",
			},
		}

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			assert.Contains(t, r.URL.Path, "/api/storage/volumes/test-uuid-123")
			assert.Contains(t, r.URL.RawQuery, "fields=name,svm.name,svm.uuid")

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(expectedVolume)
		}))
		defer server.Close()

		// Create client with mock server
		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		// Execute
		volume, err := client.GetVolume(context.Background(), "test-uuid-123")

		// Verify
		require.NoError(t, err)
		assert.Equal(t, expectedVolume.UUID, volume.UUID)
		assert.Equal(t, expectedVolume.Name, volume.Name)
		assert.Equal(t, expectedVolume.SVM.Name, volume.SVM.Name)
	})

	t.Run("returns error when volume not found", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		volume, err := client.GetVolume(context.Background(), "non-existent-uuid")

		assert.Nil(t, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "volume not found")
	})

	t.Run("returns error on ONTAP error response", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": {"message": "Internal error"}}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		volume, err := client.GetVolume(context.Background(), "test-uuid")

		assert.Nil(t, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ONTAP error")
	})
}

func TestOntapClient_ExecuteCLI(t *testing.T) {
	t.Run("successfully executes CLI command", func(t *testing.T) {
		expectedResponse := CLIResponse{
			Output: "Command executed successfully",
		}

		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/api/private/cli", r.URL.Path)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

			// Verify request body
			var reqBody CLIRequest
			err := json.NewDecoder(r.Body).Decode(&reqBody)
			require.NoError(t, err)
			assert.Equal(t, "test command", reqBody.Input)
			assert.Equal(t, "admin", reqBody.Privilege)

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(expectedResponse)
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "test command", "admin")

		require.NoError(t, err)
		assert.Equal(t, expectedResponse.Output, response.Output)
	})

	t.Run("handles plain text response", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("Plain text output"))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "test command", "admin")

		require.NoError(t, err)
		assert.Equal(t, "Plain text output", response.Output)
	})

	t.Run("returns error on CLI failure", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": {"message": "Invalid command"}}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "invalid command", "admin")

		assert.Nil(t, response)
		assert.Error(t, err)
		// Error should be an OntapCLIError with the message from ONTAP
		assert.Contains(t, err.Error(), "Invalid command")
	})
}

func TestOntapCLIError(t *testing.T) {
	t.Run("Error method includes code when present", func(t *testing.T) {
		err := &OntapCLIError{
			StatusCode: 400,
			Code:       "ERR001",
			Message:    "Invalid parameter",
		}

		errString := err.Error()
		assert.Contains(t, errString, "Invalid parameter")
		assert.Contains(t, errString, "ERR001")
	})

	t.Run("Error method returns message only when code is empty", func(t *testing.T) {
		err := &OntapCLIError{
			StatusCode: 500,
			Message:    "Internal server error",
		}

		errString := err.Error()
		assert.Equal(t, "Internal server error", errString)
		assert.NotContains(t, errString, "code:")
	})
}

func TestOntapClient_GetVolume_AdditionalCases(t *testing.T) {
	t.Run("returns error on invalid JSON response", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{invalid json`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		volume, err := client.GetVolume(context.Background(), "test-uuid")

		assert.Nil(t, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to parse volume response")
	})

	t.Run("returns error when request fails due to network error", func(t *testing.T) {
		// Create client pointing to non-existent server
		client := &RestOntapClient{
			httpClient: &http.Client{},
			endpoint:   "127.0.0.1:1", // Non-routable port
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		volume, err := client.GetVolume(context.Background(), "test-uuid")

		assert.Nil(t, volume)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "request failed")
	})
}

func TestOntapClient_ExecuteCLI_AdditionalCases(t *testing.T) {
	t.Run("returns structured error with code", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": {"code": "ERR_INVALID", "message": "Invalid input"}}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "bad command", "admin")

		assert.Nil(t, response)
		assert.Error(t, err)

		// Check it's an OntapCLIError
		var cliErr *OntapCLIError
		assert.ErrorAs(t, err, &cliErr)
		assert.Equal(t, "ERR_INVALID", cliErr.Code)
		assert.Equal(t, "Invalid input", cliErr.Message)
	})

	t.Run("returns fallback error for non-JSON error response", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`Plain text error message`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "bad command", "admin")

		assert.Nil(t, response)
		assert.Error(t, err)

		// Check it's an OntapCLIError with the plain text message
		var cliErr *OntapCLIError
		assert.ErrorAs(t, err, &cliErr)
		assert.Empty(t, cliErr.Code)
		assert.Contains(t, cliErr.Message, "Plain text error message")
	})

	t.Run("returns error when CLI request fails due to network error", func(t *testing.T) {
		// Create client pointing to non-existent server
		client := &RestOntapClient{
			httpClient: &http.Client{},
			endpoint:   "127.0.0.1:1", // Non-routable port
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		response, err := client.ExecuteCLI(context.Background(), "test command", "admin")

		assert.Nil(t, response)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "CLI request failed")
	})
}

func TestOntapClient_setAuthHeaders(t *testing.T) {
	t.Run("sets basic auth for username/password auth type", func(t *testing.T) {
		client := &RestOntapClient{
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "testuser",
				Password: "testpass",
			},
		}

		req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
		client.setAuthHeaders(req)

		username, password, ok := req.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "testuser", username)
		assert.Equal(t, "testpass", password)
	})

	t.Run("does not set auth header for certificate auth type", func(t *testing.T) {
		client := &RestOntapClient{
			authData: &models.AuthData{
				AuthType: models.USER_CERTIFICATE,
			},
		}

		req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
		client.setAuthHeaders(req)

		assert.Empty(t, req.Header.Get("Authorization"))
	})
}

func TestOntapClient_ExecuteAPI(t *testing.T) {
	t.Run("successfully returns 200 and body", func(t *testing.T) {
		body := []byte(`{"access_token":"tok123","expires_in":3600}`)
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			assert.Equal(t, "/api/cluster/licensing/access_tokens", r.URL.Path)
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
			assert.Equal(t, "application/json", r.Header.Get("Accept"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(body)
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "u",
				Password: "p",
			},
		}

		respBody, statusCode, err := client.ExecuteAPI(context.Background(), http.MethodPost, "/api/cluster/licensing/access_tokens", []byte(`{"client_id":"x"}`))

		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)
		assert.Equal(t, body, respBody)
	})

	t.Run("GET with nil body", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodGet, r.Method)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "u",
				Password: "p",
			},
		}

		respBody, statusCode, err := client.ExecuteAPI(context.Background(), http.MethodGet, "/api/foo", nil)

		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)
		assert.Equal(t, []byte(`{}`), respBody)
	})

	t.Run("returns status code and body on non-200", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"bad"}}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "u",
				Password: "p",
			},
		}

		respBody, statusCode, err := client.ExecuteAPI(context.Background(), http.MethodPost, "/api/bar", []byte("{}"))

		require.NoError(t, err)
		assert.Equal(t, 400, statusCode)
		assert.Equal(t, []byte(`{"error":{"message":"bad"}}`), respBody)
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		client := &RestOntapClient{
			httpClient: &http.Client{},
			endpoint:   "127.0.0.1:1",
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "u",
				Password: "p",
			},
		}

		respBody, statusCode, err := client.ExecuteAPI(context.Background(), http.MethodGet, "/api/foo", nil)

		assert.Error(t, err)
		assert.Nil(t, respBody)
		assert.Equal(t, 0, statusCode)
		assert.Contains(t, err.Error(), "request failed")
	})

	t.Run("POST with non-empty body sets Content-Type and reads then closes response body", func(t *testing.T) {
		body := []byte(`{"key":"value"}`)
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "application/json", r.Header.Get("Content-Type"), "Content-Type must be set when body is non-empty")
			assert.Equal(t, "application/json", r.Header.Get("Accept"))
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client := &RestOntapClient{
			httpClient: server.Client(),
			endpoint:   server.Listener.Addr().String(),
			authData: &models.AuthData{
				AuthType: models.USERNAME_PWD,
				Username: "u",
				Password: "p",
			},
		}

		respBody, statusCode, err := client.ExecuteAPI(context.Background(), http.MethodPost, "/api/test", body)

		require.NoError(t, err)
		assert.Equal(t, 200, statusCode)
		assert.Equal(t, []byte(`{"ok":true}`), respBody)
	})
}
