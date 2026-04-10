package ccfe

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_ListStoragePools_EmptyBaseURL(t *testing.T) {
	ctx := context.Background()
	c := NewClient(nil, WithBaseURL(""))
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.NoError(t, err)
	assert.Nil(t, names)
}

func TestClient_ListStoragePools_GetTokenFails(t *testing.T) {
	ctx := context.Background()
	getToken := func(context.Context) (string, error) { return "", errors.New("token error") }
	c := NewClient(getToken, WithBaseURL("https://ccfe.example.com"))
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, names)
	assert.Contains(t, err.Error(), "get token")
}

func TestClient_ListStoragePools_Success(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1beta1/projects/proj1/locations/us-central1/storagePools", r.URL.Path)
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"storagePools":[{"name":"projects/p/locations/l/storagePools/pool-a"},{"name":"projects/p/locations/l/storagePools/pool-b"},{"poolId":"fallback-id"}]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(getToken, WithBaseURL(server.URL), WithHTTPClient(server.Client()))
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, names, 3)
	assert.Equal(t, "pool-a", names[0])
	assert.Equal(t, "pool-b", names[1])
	assert.Equal(t, "fallback-id", names[2])
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
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, names)
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
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	assert.Error(t, err)
	assert.Nil(t, names)
	assert.Contains(t, err.Error(), "parse")
}

// TestClient_ListStoragePools_WithTokenGetter covers the WithTokenGetter option and poolResourceNameFromCCFEItem branches:
// name without "storagePools/" (last path segment), empty name with poolId, and skipped empty resource names.
func TestClient_ListStoragePools_WithTokenGetter(t *testing.T) {
	ctx := context.Background()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// name without storagePools/ -> last path segment "segment-z"; empty name + poolId -> "id-only"; empty name+poolId skipped
		_, _ = w.Write([]byte(`{"storagePools":[
			{"name":"projects/p/locations/l/custom/segment-z"},
			{"name":"","poolId":"id-only"},
			{"name":"","poolId":""}
		]}`))
	}))
	defer server.Close()

	getToken := func(context.Context) (string, error) { return "tok", nil }
	c := NewClient(nil, WithBaseURL(server.URL), WithHTTPClient(server.Client()), WithTokenGetter(getToken))
	names, err := c.ListStoragePools(ctx, "proj1", "us-central1")
	require.NoError(t, err)
	require.Len(t, names, 2) // third item has empty name and empty poolId -> skipped
	assert.Equal(t, "segment-z", names[0])
	assert.Equal(t, "id-only", names[1])
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
