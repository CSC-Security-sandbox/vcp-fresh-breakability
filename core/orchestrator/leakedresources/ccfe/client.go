// Package ccfe provides an internal client for listing storage pools, backup vaults, and backups from CCFE.
// Used only by the leaked-resources pipeline; not exposed to users.
// ListStoragePools defaults to the same /v1internal/... path style as CCFE hydration; set LEAKED_RESOURCES_CCFE_LIST_STORAGE_POOLS_PATH to override.
package ccfe

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

const (
	// envListStoragePoolsPath is LEAKED_RESOURCES_CCFE_LIST_STORAGE_POOLS_PATH: printf template with two verbs (project ID, location).
	envListStoragePoolsPath = "LEAKED_RESOURCES_CCFE_LIST_STORAGE_POOLS_PATH"
	// defaultListStoragePoolsPathTemplate is the CCFE internal (hydration) list path.
	defaultListStoragePoolsPathTemplate = "/v1internal/projects/%s/locations/%s/storagePools"

	listBackupVaultsPath = "/v1beta1/projects/%s/locations/%s/backupVaults"
)

// listStoragePoolsResponse represents a minimal CCFE list storage pools response.
// CCFE's internal list endpoint returns pools under "internalStoragePools".
// Name is typically "projects/{project}/locations/{location}/storagePools/{poolResourceId}".
// NetappUUID is the value VCP wrote when CCFE first hydrated the pool and is the
// identifier we compare against datamodel.Pool.UUID — names can be reused across
// recreations, UUIDs cannot, so the leaked-resources diff keys off UUID.
type listStoragePoolsResponse struct {
	StoragePools []ccfeStoragePoolItem `json:"internalStoragePools"`
}

type ccfeStoragePoolItem struct {
	Name       string `json:"name"`
	NetappUUID string `json:"netappUuid,omitempty"`
	PoolID     string `json:"poolId,omitempty"`
	State      string `json:"state,omitempty"`
}

// listBackupVaultsResponse represents a minimal CCFE list backup vaults response.
// Name (when present) is typically "projects/{project}/locations/{location}/backupVaults/{resourceId}".
type listBackupVaultsResponse struct {
	BackupVaults []ccfeBackupVaultItem `json:"backupVaults"`
}

type ccfeBackupVaultItem struct {
	Name          string `json:"name,omitempty"`
	BackupVaultID string `json:"backupVaultId,omitempty"`
	ResourceID    string `json:"resourceId,omitempty"`
}

// Client performs internal CCFE API calls for leaked-resources detection only.
type Client struct {
	baseURL                      string
	listStoragePoolsPathTemplate string
	httpClient                   *http.Client
	getToken                     func(context.Context) (string, error)
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

// WithBaseURL sets the base URL (for tests). When empty, list methods return nil, nil (CCFE disabled).
func WithBaseURL(url string) ClientOption {
	return func(client *Client) {
		client.baseURL = url
	}
}

// WithListStoragePoolsPathTemplate sets the list-pools relative path template (two "%s": project, location). Overrides LEAKED_RESOURCES_CCFE_LIST_STORAGE_POOLS_PATH / default.
func WithListStoragePoolsPathTemplate(template string) ClientOption {
	return func(client *Client) {
		client.listStoragePoolsPathTemplate = template
	}
}

// NewClient returns a client that uses GCP_HYDRATE_BASE_URL and default auth.
// ListStoragePools uses a v1internal-style path by default; override with LEAKED_RESOURCES_CCFE_LIST_STORAGE_POOLS_PATH or WithListStoragePoolsPathTemplate.
func NewClient(getToken func(context.Context) (string, error), opts ...ClientOption) *Client {
	c := &Client{
		baseURL:                      env.GetString("GCP_HYDRATE_BASE_URL", ""),
		listStoragePoolsPathTemplate: env.GetString(envListStoragePoolsPath, defaultListStoragePoolsPathTemplate),
		httpClient:                   http.DefaultClient,
		getToken:                     getToken,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ListStoragePools returns one poolpairs.CachedPool per pool that CCFE knows about
// for the given project and location. Each element carries the pool's netappUuid
// (the comparison key, equal to VCP's Pool.UUID) and its short resource name (the
// last segment of CCFE's "name" field, kept for human-readable leak records).
// Pools missing a netappUuid — typically those still being created — are dropped
// with a debug log so they cannot drive false leak signals before CCFE has a
// stable identifier for them.
//
// Location is typically a region (e.g. us-central1) or zone. Returns (nil, nil)
// if the base URL is empty (CCFE disabled), so callers can distinguish "CCFE
// returned no pools" from "we never asked CCFE". The activity layer relies on
// that distinction to avoid clobbering a previously-good cache snapshot.
func (c *Client) ListStoragePools(ctx context.Context, projectID, location string) ([]poolpairs.CachedPool, error) {
	logger := util.GetLogger(ctx)
	relPath := fmt.Sprintf(c.listStoragePoolsPathTemplate, projectID, location)

	if c.baseURL == "" {
		logger.Infof("leaked resources CCFE: ListStoragePools skipped (GCP_HYDRATE_BASE_URL empty) project=%s location=%s path=%s",
			projectID, location, relPath)
		return nil, nil
	}

	logger.Infof("leaked resources CCFE: ListStoragePools request project=%s location=%s path=%s", projectID, location, relPath)

	token, err := c.getToken(ctx)
	if err != nil {
		logger.Warnf("leaked resources CCFE: ListStoragePools token error project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("get token: %w", err)
	}
	url := c.baseURL + relPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Warnf("leaked resources CCFE: ListStoragePools HTTP error project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		logger.Warnf("leaked resources CCFE: ListStoragePools non-OK status=%d project=%s location=%s", resp.StatusCode, projectID, location)
		return nil, fmt.Errorf("ccfe list storage pools: status %d", resp.StatusCode)
	}

	var listResp listStoragePoolsResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	pools := make([]poolpairs.CachedPool, 0, len(listResp.StoragePools))
	skippedNoUUID := 0
	for _, p := range listResp.StoragePools {
		uuid := strings.TrimSpace(p.NetappUUID)
		if uuid == "" {
			skippedNoUUID++
			logger.Debugf("leaked resources CCFE: skipping pool without netappUuid project=%s location=%s name=%q state=%q",
				projectID, location, p.Name, p.State)
			continue
		}
		pools = append(pools, poolpairs.CachedPool{
			UUID: uuid,
			Name: poolResourceNameFromCCFEItem(p),
		})
	}
	logger.Infof("leaked resources CCFE: ListStoragePools ok project=%s location=%s pool_count=%d skipped_no_uuid=%d",
		projectID, location, len(pools), skippedNoUUID)
	return pools, nil
}

// ListBackupVaults returns backup vault resource identifiers for the given project and location
// (last segment after backupVaults/ in name, else resourceId, else backupVaultId). Returns nil slice
// and nil error if base URL is empty (CCFE disabled).
func (c *Client) ListBackupVaults(ctx context.Context, projectID, location string) ([]string, error) {
	logger := util.GetLogger(ctx)
	logger.Infof("CCFE client: ListBackupVaults started project=%s location=%s", projectID, location)
	if c.baseURL == "" {
		logger.Infof("CCFE client: ListBackupVaults skipped, base URL empty (CCFE disabled) project=%s location=%s", projectID, location)
		return nil, nil
	}
	token, err := c.getToken(ctx)
	if err != nil {
		logger.Infof("CCFE client: ListBackupVaults get token failed project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("get token: %w", err)
	}
	logger.Infof("CCFE client: ListBackupVaults obtained token project=%s location=%s", projectID, location)
	reqURL := c.baseURL + fmt.Sprintf(listBackupVaultsPath, projectID, location)
	logger.Infof("CCFE client: ListBackupVaults request URL built project=%s location=%s", projectID, location)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		logger.Infof("CCFE client: ListBackupVaults create request failed project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	logger.Infof("CCFE client: ListBackupVaults sending GET project=%s location=%s", projectID, location)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Infof("CCFE client: ListBackupVaults HTTP Do failed project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Infof("CCFE client: ListBackupVaults read body failed project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		util.GetLogger(ctx).Warnf("CCFE list backup vaults returned status %d for project=%s location=%s", resp.StatusCode, projectID, location)
		return nil, fmt.Errorf("ccfe list backup vaults: status %d", resp.StatusCode)
	}

	var listResp listBackupVaultsResponse
	if err := json.Unmarshal(body, &listResp); err != nil {
		logger.Infof("CCFE client: ListBackupVaults JSON parse failed project=%s location=%s: %v", projectID, location, err)
		return nil, fmt.Errorf("parse response: %w", err)
	}
	logger.Infof("CCFE client: ListBackupVaults parsed %d raw backupVault item(s) project=%s location=%s", len(listResp.BackupVaults), projectID, location)
	names := make([]string, 0, len(listResp.BackupVaults))
	for _, v := range listResp.BackupVaults {
		id := backupVaultResourceNameFromCCFEItem(ctx, v)
		if id != "" {
			names = append(names, id)
			logger.Infof("CCFE client: ListBackupVaults extracted vault id=%q project=%s location=%s", id, projectID, location)
		}
	}
	logger.Infof("CCFE client: ListBackupVaults finished project=%s location=%s count=%d", projectID, location, len(names))
	return names, nil
}

// // ListAllBackupsAcrossBackupVaults lists backup vaults for the project and location, then lists backups
// // in each vault (GET .../backupVaults/{vault}/backups) and returns one flattened slice of backup identifiers.
// // Identifier extraction matches backupResourceNameFromCCFEItem. Returns nil, nil if base URL is empty; empty
// // slice if there are no vaults or no backups.
// func (c *Client) ListAllBackupsAcrossBackupVaults(ctx context.Context, projectID, location string) ([]string, error) {
// 	logger := util.GetLogger(ctx)
// 	logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults started project=%s location=%s", projectID, location)
// 	if c.baseURL == "" {
// 		logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults skipped, base URL empty project=%s location=%s", projectID, location)
// 		return nil, nil
// 	}
// 	vaults, err := c.ListBackupVaults(ctx, projectID, location)
// 	if err != nil {
// 		logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults ListBackupVaults failed project=%s location=%s: %v", projectID, location, err)
// 		return nil, err
// 	}
// 	if vaults == nil {
// 		logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults no vault list (nil), exiting project=%s location=%s", projectID, location)
// 		return nil, nil
// 	}
// 	logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults listing backups in %d vault(s) project=%s location=%s", len(vaults), projectID, location)
// 	out := make([]string, 0)
// 	for _, vault := range vaults {
// 		logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults fetching backups for vault=%q project=%s location=%s", vault, projectID, location)
// 		backups, err := c.listBackupsInVault(ctx, projectID, location, vault)
// 		if err != nil {
// 			logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults listBackupsInVault failed vault=%q project=%s location=%s: %v", vault, projectID, location, err)
// 			return nil, fmt.Errorf("list backups in vault %q: %w", vault, err)
// 		}
// 		logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults vault=%q returned %d backup id(s) project=%s location=%s", vault, len(backups), projectID, location)
// 		out = append(out, backups...)
// 	}
// 	logger.Infof("CCFE client: ListAllBackupsAcrossBackupVaults finished project=%s location=%s total_backup_ids=%d", projectID, location, len(out))
// 	return out, nil
// }

// func (c *Client) listBackupsInVault(ctx context.Context, projectID, location, backupVaultName string) ([]string, error) {
// 	logger := util.GetLogger(ctx)
// 	logger.Infof("CCFE client: listBackupsInVault started vault=%q project=%s location=%s", backupVaultName, projectID, location)
// 	token, err := c.getToken(ctx)
// 	if err != nil {
// 		logger.Infof("CCFE client: listBackupsInVault get token failed vault=%q project=%s location=%s: %v", backupVaultName, projectID, location, err)
// 		return nil, fmt.Errorf("get token: %w", err)
// 	}
// 	logger.Infof("CCFE client: listBackupsInVault obtained token vault=%q project=%s location=%s", backupVaultName, projectID, location)
// 	escapedVault := url.PathEscape(backupVaultName)
// 	urlStr := c.baseURL + fmt.Sprintf(listBackupsInVaultPath, projectID, location, escapedVault)
// 	logger.Infof("CCFE client: listBackupsInVault request URL built vault=%q escaped=%q project=%s location=%s", backupVaultName, escapedVault, projectID, location)
// 	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
// 	if err != nil {
// 		logger.Infof("CCFE client: listBackupsInVault create request failed vault=%q project=%s location=%s: %v", backupVaultName, projectID, location, err)
// 		return nil, fmt.Errorf("create request: %w", err)
// 	}
// 	req.Header.Set("Authorization", "Bearer "+token)
// 	req.Header.Set("Content-Type", "application/json")
// 	logger.Infof("CCFE client: listBackupsInVault sending GET vault=%q project=%s location=%s", backupVaultName, projectID, location)

// 	resp, err := c.httpClient.Do(req)
// 	if err != nil {
// 		logger.Infof("CCFE client: listBackupsInVault HTTP Do failed vault=%q project=%s location=%s: %v", backupVaultName, projectID, location, err)
// 		return nil, fmt.Errorf("do request: %w", err)
// 	}
// 	defer func() { _ = resp.Body.Close() }()

// 	body, err := io.ReadAll(resp.Body)
// 	if err != nil {
// 		logger.Infof("CCFE client: listBackupsInVault read body failed vault=%q project=%s location=%s: %v", backupVaultName, projectID, location, err)
// 		return nil, fmt.Errorf("read response: %w", err)
// 	}
// 	if resp.StatusCode != http.StatusOK {
// 		util.GetLogger(ctx).Warnf("CCFE list backups returned status %d for project=%s location=%s vault=%s", resp.StatusCode, projectID, location, backupVaultName)
// 		return nil, fmt.Errorf("ccfe list backups: status %d", resp.StatusCode)
// 	}

// 	var listResp listBackupsInVaultResponse
// 	if err := json.Unmarshal(body, &listResp); err != nil {
// 		logger.Infof("CCFE client: listBackupsInVault JSON parse failed vault=%q project=%s location=%s: %v", backupVaultName, projectID, location, err)
// 		return nil, fmt.Errorf("parse response: %w", err)
// 	}
// 	logger.Infof("CCFE client: listBackupsInVault parsed %d raw backup item(s) vault=%q project=%s location=%s", len(listResp.Backups), backupVaultName, projectID, location)
// 	names := make([]string, 0, len(listResp.Backups))
// 	for _, b := range listResp.Backups {
// 		id := backupResourceNameFromCCFEItem(ctx, b)
// 		if id != "" {
// 			names = append(names, id)
// 			logger.Infof("CCFE client: listBackupsInVault extracted backup id=%q vault=%q project=%s location=%s", id, backupVaultName, projectID, location)
// 		}
// 	}
// 	logger.Infof("CCFE client: listBackupsInVault finished vault=%q project=%s location=%s count=%d", backupVaultName, projectID, location, len(names))
// 	return names, nil
// }

// poolResourceNameFromCCFEItem returns the pool's short resource name (the
// last segment of CCFE's "name" path, e.g. "satya1-tcase-24-a1"). It is used
// purely for human-readable leak records and operational logs — the leaked
// resources diff itself keys off NetappUUID. Falls back to poolId when the
// "name" field is missing.
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

// backupVaultResourceNameFromCCFEItem returns a vault identifier for comparison with VCP.
// Prefers the segment after backupVaults/ in name; then last path segment of name; then resourceId; then backupVaultId.
func backupVaultResourceNameFromCCFEItem(ctx context.Context, v ccfeBackupVaultItem) string {
	logger := util.GetLogger(ctx)
	logger.Infof("CCFE client: backupVaultResourceNameFromCCFEItem input name=%q resourceId=%q backupVaultId=%q", v.Name, v.ResourceID, v.BackupVaultID)
	if v.Name != "" {
		const prefix = "backupVaults/"
		if i := strings.LastIndex(v.Name, prefix); i >= 0 {
			out := strings.TrimSpace(v.Name[i+len(prefix):])
			logger.Infof("CCFE client: backupVaultResourceNameFromCCFEItem chose segment after %q -> %q", prefix, out)
			return out
		}
		if parts := strings.Split(v.Name, "/"); len(parts) > 0 {
			out := strings.TrimSpace(parts[len(parts)-1])
			logger.Infof("CCFE client: backupVaultResourceNameFromCCFEItem chose last path segment of name -> %q", out)
			return out
		}
	}
	if s := strings.TrimSpace(v.ResourceID); s != "" {
		logger.Infof("CCFE client: backupVaultResourceNameFromCCFEItem chose resourceId -> %q", s)
		return s
	}
	out := strings.TrimSpace(v.BackupVaultID)
	logger.Infof("CCFE client: backupVaultResourceNameFromCCFEItem chose backupVaultId fallback -> %q", out)
	return out
}

// // backupResourceNameFromCCFEItem returns a backup identifier for comparison with VCP.
// // Prefers the segment after backups/ in name (full resource path); then last path segment of name;
// // then resourceId; then backupId.
// func backupResourceNameFromCCFEItem(ctx context.Context, b ccfeBackupItem) string {
// 	logger := util.GetLogger(ctx)
// 	logger.Infof("CCFE client: backupResourceNameFromCCFEItem input name=%q resourceId=%q backupId=%q", b.Name, b.ResourceID, b.BackupID)
// 	if b.Name != "" {
// 		const prefix = "backups/"
// 		if i := strings.LastIndex(b.Name, prefix); i >= 0 {
// 			out := strings.TrimSpace(b.Name[i+len(prefix):])
// 			logger.Infof("CCFE client: backupResourceNameFromCCFEItem chose segment after %q -> %q", prefix, out)
// 			return out
// 		}
// 		if parts := strings.Split(b.Name, "/"); len(parts) > 0 {
// 			out := strings.TrimSpace(parts[len(parts)-1])
// 			logger.Infof("CCFE client: backupResourceNameFromCCFEItem chose last path segment of name -> %q", out)
// 			return out
// 		}
// 	}
// 	if s := strings.TrimSpace(b.ResourceID); s != "" {
// 		logger.Infof("CCFE client: backupResourceNameFromCCFEItem chose resourceId -> %q", s)
// 		return s
// 	}
// 	out := strings.TrimSpace(b.BackupID)
// 	logger.Infof("CCFE client: backupResourceNameFromCCFEItem chose backupId fallback -> %q", out)
// 	return out
// }
