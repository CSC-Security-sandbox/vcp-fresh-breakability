package ccfe

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
)

func TestClient_ListStoragePools_EmptyBaseURL(t *testing.T) {
	ctx := context.Background()
	c := NewClient(nil, WithBaseURL(""))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.NoError(t, err)
	assert.Nil(t, pools)
}

func TestClient_ListStoragePools_GetTokenFails(t *testing.T) {
	ctx := context.Background()
	getToken := func(context.Context) (string, error) { return "", errors.New("token error") }
	c := NewClient(getToken, WithBaseURL("https://ccfe.example.com"))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, pools)
	assert.Contains(t, err.Error(), "get token")
}

// TestClient_ListStoragePools_Success exercises the happy-path UUID extraction:
// pools with netappUuid come through as CachedPool entries (UUID + short name),
// pools missing netappUuid are dropped because UUID is the leak comparison key
// and a missing UUID would either collide with another empty-UUID entry or
// match nothing in VCP.
func TestClient_ListStoragePools_Success(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1internal/projects/proj1/locations/us-central1/storagePools", r.URL.Path)
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"internalStoragePools":[
			{"name":"projects/p/locations/l/storagePools/pool-a","netappUuid":"uuid-a"},
			{"name":"projects/p/locations/l/storagePools/pool-b","netappUuid":"uuid-b"},
			{"name":"projects/p/locations/l/storagePools/pool-c"}
		]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, pools, 2) // pool-c is dropped (no netappUuid)
	assert.Equal(t, poolpairs.CachedPool{UUID: "uuid-a", Name: "pool-a"}, pools[0])
	assert.Equal(t, poolpairs.CachedPool{UUID: "uuid-b", Name: "pool-b"}, pools[1])
}

func TestClient_ListStoragePools_NonOKStatus(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("error"))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, pools)
	assert.Contains(t, err.Error(), "status 500")
}

func TestClient_ListStoragePools_InvalidJSON(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("not json"))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, pools)
	assert.Contains(t, err.Error(), "parse")
}

// TestClient_ListStoragePools_WithTokenGetter covers the WithTokenGetter option
// and the poolResourceNameFromCCFEItem branches that produce the CachedPool.Name
// field: name without "storagePools/" (last path segment), and empty name with
// poolId fallback. Each item carries a netappUuid so it survives the no-UUID
// drop filter.
func TestClient_ListStoragePools_WithTokenGetter(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// name without storagePools/ -> last path segment "segment-z";
		// empty name + poolId -> "id-only"; entry with no UUID -> dropped.
		_, _ = w.Write([]byte(`{"internalStoragePools":[
			{"name":"projects/p/locations/l/custom/segment-z","netappUuid":"uuid-z"},
			{"name":"","poolId":"id-only","netappUuid":"uuid-only"},
			{"name":"projects/p/locations/l/storagePools/no-uuid-pool"}
		]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(nil, WithBaseURL(server.URL), WithHTTPClient(server.Client()), WithTokenGetter(getToken))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, pools, 2) // third item has no netappUuid -> skipped
	assert.Equal(t, poolpairs.CachedPool{UUID: "uuid-z", Name: "segment-z"}, pools[0])
	assert.Equal(t, poolpairs.CachedPool{UUID: "uuid-only", Name: "id-only"}, pools[1])
}

// TestClient_ListStoragePools_RealCCFEPayload mirrors the actual CCFE internal API response shape
// (key "internalStoragePools" plus richer fields like state, capacityGib, labels) to guard
// against silent regressions where unknown JSON keys would unmarshal into an empty slice and let
// the leaked-resources detector flag every VCP pool as missing in CCFE. Asserts both the UUID
// (the comparison key) and the short name (kept for human-readable leak records).
func TestClient_ListStoragePools_RealCCFEPayload(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{
			"internalStoragePools": [
				{
					"name": "projects/g1p-atom-dev-qa-so1/locations/australia-southeast1-a/storagePools/satya1-tcase-24-a1",
					"netappUuid": "46368dba-d92f-fe16-e5a5-8c065f246cdf",
					"serviceLevel": "FLEX",
					"capacityGib": "1024",
					"state": "DELETING",
					"network": "projects/602798798049/global/networks/satya1-vsa-vcp",
					"customPerformanceEnabled": true,
					"totalThroughputMibps": "64",
					"totalIops": "1024",
					"hotTierSizeGib": "1024",
					"qosType": "AUTO",
					"unifiedPool": true,
					"type": "UNIFIED"
				},
				{
					"name": "projects/g1p-atom-dev-qa-so1/locations/australia-southeast1-a/storagePools/satya1-tcase-1-a1",
					"netappUuid": "65d83a87-624f-f16a-7e76-ae120fdfc54b",
					"serviceLevel": "FLEX",
					"capacityGib": "3096",
					"state": "READY",
					"labels": {"environment":"production","team":"storage"},
					"customPerformanceEnabled": true,
					"qosType": "AUTO",
					"unifiedPool": true,
					"type": "UNIFIED"
				}
			]
		}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	pools, err := c.ListStoragePools(ctx, "602798798049", "australia-southeast1-a")
	require.NoError(t, err)
	require.Len(t, pools, 2)
	assert.Equal(t, poolpairs.CachedPool{UUID: "46368dba-d92f-fe16-e5a5-8c065f246cdf", Name: "satya1-tcase-24-a1"}, pools[0])
	assert.Equal(t, poolpairs.CachedPool{UUID: "65d83a87-624f-f16a-7e76-ae120fdfc54b", Name: "satya1-tcase-1-a1"}, pools[1])
}

// TestClient_ListStoragePools_CustomPathTemplate verifies that
// WithListStoragePoolsPathTemplate overrides the default list-pools path
// and the client requests the supplied template instead.
func TestClient_ListStoragePools_CustomPathTemplate(t *testing.T) {
	ctx := context.Background()
	customPath := "/custom/v9/projects/%s/locations/%s/storagePools"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/custom/v9/projects/proj1/locations/us-central1/storagePools", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"internalStoragePools":[{"name":"projects/p/locations/l/storagePools/custom-pool","netappUuid":"uuid-custom"}]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()), WithListStoragePoolsPathTemplate(customPath))
	pools, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, pools, 1)
	assert.Equal(t, poolpairs.CachedPool{UUID: "uuid-custom", Name: "custom-pool"}, pools[0])
}

func TestClient_ListBackupVaults_EmptyBaseURL(t *testing.T) {
	ctx := context.Background()
	c := NewClient(nil, WithBaseURL(""))
	names, err := c.ListBackupVaults(ctx, "proj1", "us-central1")
	assert.NoError(t, err)
	assert.Nil(t, names)
}

func TestClient_ListBackupVaults_Success(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1beta1/projects/proj1/locations/us-central1/backupVaults", r.URL.Path)
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"backupVaults":[
			{"name":"projects/p/locations/l/backupVaults/vault-a"},
			{"resourceId":"res-b"},
			{"backupVaultId":"id-c"}
		]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	names, err := c.ListBackupVaults(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, names, 3)
	assert.Equal(t, "vault-a", names[0])
	assert.Equal(t, "res-b", names[1])
	assert.Equal(t, "id-c", names[2])
}
