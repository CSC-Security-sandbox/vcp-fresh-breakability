// Package ccfe provides an internal client for listing storage pools from CCFE.
// Used only by the leaked-resources pipeline; not exposed to users.
package ccfe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	listStoragePoolsPath = "/v1beta1/projects/%s/locations/%s/storagePools"
)

// listStoragePoolsResponse represents a minimal CCFE list storage pools response.
// Name is typically "projects/{project}/locations/{location}/storagePools/{poolResourceId}".
type listStoragePoolsResponse struct {
	StoragePools []ccfeStoragePoolItem `json:"storagePools"`
}

type ccfeStoragePoolItem struct {
	Name   string `json:"name"`
	PoolID string `json:"poolId,omitempty"`
}

// Client performs internal CCFE API calls for leaked-resources detection only.
type Client struct {
	baseURL    string
	httpClient *http.Client
	getToken   func(context.Context) (string, error)
}

// ClientOption configures the CCFE client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client (for tests).
func WithHTTPClient(c *http.Client) ClientOption {
	return func(client *Client) {
		client.httpClient = c
	}
}

// WithTokenGetter sets a custom token getter (for tests).
func WithTokenGetter(fn func(context.Context) (string, error)) ClientOption {
	return func(client *Client) {
		client.getToken = fn
	}
}

// WithBaseURL sets the base URL (for tests). When empty, ListStoragePools returns nil, nil.
func WithBaseURL(url string) ClientOption {
	return func(client *Client) {
		client.baseURL = url
	}
}

// NewClient returns a client that uses GCP_HYDRATE_BASE_URL and default auth.
func NewClient(getToken func(context.Context) (string, error), opts ...ClientOption) *Client {
	c := &Client{
		baseURL:    env.GetString("GCP_HYDRATE_BASE_URL", ""),
		httpClient: http.DefaultClient,
		getToken:   getToken,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ListStoragePools returns pool resource names (last segment of name path) for the given project and location.
// Location is typically a region (e.g. us-central1) or zone. Returns nil slice and nil error if base URL is empty (CCFE disabled).
func (c *Client) ListStoragePools(ctx context.Context, projectID, location string) ([]string, error) {
	if c.baseURL == "" {
		return nil, nil
	}
	token, err := c.getToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	url := c.baseURL + fmt.Sprintf(listStoragePoolsPath, projectID, location)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		util.GetLogger(ctx).Warnf("CCFE list storage pools returned status %d for project=%s location=%s", resp.StatusCode, projectID, location)
		return nil, fmt.Errorf("ccfe list storage pools: status %d", resp.StatusCode)
	}

	var listResp listStoragePoolsResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	names := make([]string, 0, len(listResp.StoragePools))
	for _, p := range listResp.StoragePools {
		resourceName := poolResourceNameFromCCFEItem(p)
		if resourceName != "" {
			names = append(names, resourceName)
		}
	}
	return names, nil
}

// poolResourceNameFromCCFEItem returns the pool resource ID/name for comparison with VCP.
// Prefers the last segment of name path (projects/.../storagePools/<id>); falls back to poolId if set.
func poolResourceNameFromCCFEItem(p ccfeStoragePoolItem) string {
	if p.Name != "" {
		const prefix = "storagePools/"
		if i := strings.LastIndex(p.Name, prefix); i >= 0 {
			return strings.TrimSpace(p.Name[i+len(prefix):])
		}
		// Fallback: last path segment
		if parts := strings.Split(p.Name, "/"); len(parts) > 0 {
			return strings.TrimSpace(parts[len(parts)-1])
		}
	}
	return strings.TrimSpace(p.PoolID)
}
