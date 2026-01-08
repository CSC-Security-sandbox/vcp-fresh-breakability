// Package handlers provides custom API handlers for the ONTAP proxy.
// These handlers translate incoming API requests to different ONTAP APIs
// and transform responses back to expected formats.
package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/cache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/reverseproxy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// OntapClient provides methods to interact with ONTAP REST APIs.
// It reuses the connection pool from the reverse proxy for efficiency.
type OntapClient struct {
	httpClient *http.Client
	endpoint   string
	authData   *models.AuthData
}

// VolumeInfo represents the volume information retrieved from ONTAP.
// Used to get volume name and SVM name for CLI commands.
type VolumeInfo struct {
	UUID string `json:"uuid"`
	Name string `json:"name"`
	SVM  struct {
		Name string `json:"name"`
		UUID string `json:"uuid"`
	} `json:"svm"`
}

// CLIRequest represents the request body for POST /api/private/cli
type CLIRequest struct {
	Input     string `json:"input"`
	Privilege string `json:"privilege"`
}

// CLIResponse represents the response from POST /api/private/cli
type CLIResponse struct {
	Output string `json:"output"`
}

// OntapErrorResponse represents the error response from ONTAP REST API
type OntapErrorResponse struct {
	Error *OntapError `json:"error,omitempty"`
}

// OntapError represents the error details from ONTAP
type OntapError struct {
	Message string `json:"message"`
	Code    string `json:"code"`
}

// CLIErrorResponse represents the full error response from ONTAP private CLI
type CLIErrorResponse struct {
	Error  *OntapError `json:"error,omitempty"`
	Output string      `json:"output,omitempty"`
}

// OntapCLIError is a custom error type that preserves ONTAP error details
type OntapCLIError struct {
	StatusCode int
	Code       string
	Message    string
	Output     string
}

func (e *OntapCLIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s (code: %s)", e.Message, e.Code)
	}
	return e.Message
}

// NewOntapClientFromContext creates an OntapClient using auth data from the request context.
// The auth data is set by CredentialMiddleware and cached in the auth data cache.
// Returns an error if auth data is not found in the context.
func NewOntapClientFromContext(ctx context.Context) (*OntapClient, error) {
	cacheKey := cache.GetAuthDataKeyFromContext(ctx)
	if cacheKey == "" {
		return nil, fmt.Errorf("no auth data cache key found in context")
	}

	authData, exists := cache.GetFromAuthDataCache(cacheKey)
	if !exists || authData == nil {
		return nil, fmt.Errorf("auth data not found in cache for key: %s", cacheKey)
	}

	// Get a pooled HTTP client from the global connection pool
	client, endpoint, err := reverseproxy.GetGlobalConnectionPool().GetClient(ctx, authData)
	if err != nil {
		return nil, fmt.Errorf("failed to get pooled client: %w", err)
	}

	return &OntapClient{
		httpClient: client,
		endpoint:   endpoint,
		authData:   authData,
	}, nil
}

// GetVolume retrieves volume information by UUID from ONTAP.
// Returns volume name and SVM name needed for CLI commands.
func (c *OntapClient) GetVolume(ctx context.Context, volumeUUID string) (*VolumeInfo, error) {
	logger := util.GetLogger(ctx)

	url := fmt.Sprintf("https://%s/api/storage/volumes/%s?fields=name,svm.name,svm.uuid", c.endpoint, volumeUUID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeaders(req)
	req.Header.Set("Accept", "application/json")

	logger.DebugContext(ctx, "Fetching volume info from ONTAP",
		"volumeUUID", volumeUUID,
		"endpoint", c.endpoint,
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("volume not found: %s", volumeUUID)
	}

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ONTAP error (status %d): %s", resp.StatusCode, string(body))
	}

	var volumeInfo VolumeInfo
	if err := json.NewDecoder(resp.Body).Decode(&volumeInfo); err != nil {
		return nil, fmt.Errorf("failed to parse volume response: %w", err)
	}

	logger.DebugContext(ctx, "Retrieved volume info",
		"volumeUUID", volumeUUID,
		"volumeName", volumeInfo.Name,
		"svmName", volumeInfo.SVM.Name,
	)

	return &volumeInfo, nil
}

// ExecuteCLI executes an ONTAP CLI command via the private CLI endpoint.
// The command is sent as POST /api/private/cli with the specified privilege level.
func (c *OntapClient) ExecuteCLI(ctx context.Context, command, privilege string) (*CLIResponse, error) {
	logger := util.GetLogger(ctx)

	url := fmt.Sprintf("https://%s/api/private/cli", c.endpoint)

	reqBody := CLIRequest{
		Input:     command,
		Privilege: privilege,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CLI request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create CLI request: %w", err)
	}

	c.setAuthHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Log the command (sanitized - no credentials in command)
	logger.InfoContext(ctx, "Executing ONTAP CLI command",
		"endpoint", c.endpoint,
		"privilege", privilege,
		// Note: Be careful not to log sensitive commands
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("CLI request failed: %w", err)
	}
	defer func() {
		if resp.Body != nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read CLI response: %w", err)
	}

	if resp.StatusCode >= 400 {
		// Try to parse as structured ONTAP error response
		var cliError CLIErrorResponse
		if jsonErr := json.Unmarshal(body, &cliError); jsonErr == nil && cliError.Error != nil {
			return nil, &OntapCLIError{
				StatusCode: resp.StatusCode,
				Code:       cliError.Error.Code,
				Message:    cliError.Error.Message,
				Output:     cliError.Output,
			}
		}
		// Fallback to generic error
		return nil, &OntapCLIError{
			StatusCode: resp.StatusCode,
			Message:    string(body),
		}
	}

	var cliResp CLIResponse
	if err := json.Unmarshal(body, &cliResp); err != nil {
		// Some CLI commands return plain text, not JSON
		// In that case, treat the body as the output
		cliResp.Output = string(body)
	}

	logger.DebugContext(ctx, "CLI command executed successfully")

	return &cliResp, nil
}

// setAuthHeaders sets the appropriate authentication headers on the request
// based on the auth type (certificate or basic auth).
func (c *OntapClient) setAuthHeaders(req *http.Request) {
	// Certificate auth is handled by the TLS transport, no header needed
	if c.authData.AuthType == models.USER_CERTIFICATE {
		return
	}

	// For basic auth, set the Authorization header
	if c.authData.Username != "" && c.authData.Password != "" {
		req.SetBasicAuth(c.authData.Username, c.authData.Password)
	}
}
