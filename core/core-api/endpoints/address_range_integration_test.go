package api

// Integration tests for address range API endpoints.
// Wires: SQLite in-memory DB → real GCPOrchestrator → real Handler → Ogen HTTP server → httptest.
// No mocks — exercises the full stack from HTTP request to database and back.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	gcporch "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory/gcp"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// setupIntegrationServer wires a real in-memory DB → GCPOrchestrator → Handler → Ogen HTTP server.
func setupIntegrationServer(t *testing.T) *httptest.Server {
	t.Helper()
	store, err := database.NewTestStorage(log.NewLogger())
	require.NoError(t, err)

	orch := gcporch.NewGCPOrchestrator(store, nil)
	handler := NewHandler(orch)
	srv, err := oasgenserver.NewServer(handler)
	require.NoError(t, err)
	return httptest.NewServer(srv)
}

func doJSON(t *testing.T, srv *httptest.Server, method, path string, body interface{}) *http.Response {
	t.Helper()
	var reqBody *bytes.Buffer
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewBuffer(b)
	} else {
		reqBody = bytes.NewBuffer(nil)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, srv.URL+path, reqBody)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func decodeBody(t *testing.T, resp *http.Response, out interface{}) {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
}

// TestAddressRangeAPI_Integration exercises the full CRUD + update lifecycle via HTTP.
func TestAddressRangeAPI_CreateAndGet(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// --- CREATE ---
	createBody := map[string]interface{}{
		"addressRange":     "my-range",
		"addressRangeCidr": "10.0.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])
	assert.NotEmpty(t, arID)
	assert.Equal(t, "my-range", created["addressRange"])
	assert.Equal(t, "10.0.0.0/16", created["addressRangeCidr"])
	assert.Equal(t, "CREATED", created["lifeCycleState"])

	// --- GET ---
	resp = doJSON(t, srv, http.MethodGet, "/v1/addressRange/"+arID, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]interface{}
	decodeBody(t, resp, &got)
	assert.Equal(t, arID, got["addressRangeId"])
	assert.Equal(t, "my-range", got["addressRange"])
}

func TestAddressRangeAPI_UpdateImmutableFields(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create
	createBody := map[string]interface{}{
		"addressRange":     "range-immutable",
		"addressRangeCidr": "10.1.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// Attempt to update Name, CIDR, and Network — all should be silently ignored.
	updateBody := map[string]interface{}{
		"addressRange":     "new-name",
		"addressRangeCidr": "10.99.0.0/16",
		"network":          "projects/999/global/networks/other-vpc",
	}
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, updateBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updated map[string]interface{}
	decodeBody(t, resp, &updated)
	// All identity fields must be unchanged.
	assert.Equal(t, "range-immutable", updated["addressRange"], "Name must be immutable")
	assert.Equal(t, "10.1.0.0/16", updated["addressRangeCidr"], "CIDR must be immutable")
	assert.Equal(t, "projects/123456/global/networks/my-vpc", updated["network"], "Network must be immutable")
}

func TestAddressRangeAPI_UpdateApplyRouteAggregation(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create
	createBody := map[string]interface{}{
		"addressRange":     "range-routeagg",
		"addressRangeCidr": "10.2.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// applyRouteAggregation IS mutable — should change.
	updateBody := map[string]interface{}{
		"applyRouteAggregation": true,
	}
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, updateBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updated map[string]interface{}
	decodeBody(t, resp, &updated)
	assert.Equal(t, true, updated["applyRouteAggregation"], "applyRouteAggregation must update")
	// Identity fields still unchanged.
	assert.Equal(t, "range-routeagg", updated["addressRange"])
	assert.Equal(t, "10.2.0.0/16", updated["addressRangeCidr"])
}

func TestAddressRangeAPI_UpdateLifeCycleStateToDisabled(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create
	createBody := map[string]interface{}{
		"addressRange":     "range-disable",
		"addressRangeCidr": "10.3.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// Transition to DISABLED via update.
	updateBody := map[string]interface{}{
		"lifeCycleState": "DISABLED",
	}
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, updateBody)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var updated map[string]interface{}
	decodeBody(t, resp, &updated)
	assert.Equal(t, "DISABLED", updated["lifeCycleState"])
}

func TestAddressRangeAPI_UpdateBlockedAfterDisabled(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create and disable.
	createBody := map[string]interface{}{
		"addressRange":     "range-disabled-block",
		"addressRangeCidr": "10.4.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, map[string]interface{}{"lifeCycleState": "DISABLED"})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Further update should be rejected with 409.
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, map[string]interface{}{"applyRouteAggregation": true})
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestAddressRangeAPI_DuplicateNameRejected(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	body := map[string]interface{}{
		"addressRange":     "dup-name",
		"addressRangeCidr": "10.5.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", body)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Second create with same name — should conflict.
	body["addressRangeCidr"] = "10.6.0.0/16"
	resp = doJSON(t, srv, http.MethodPost, "/v1/addressRange", body)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestAddressRangeAPI_DuplicateCIDRRejected(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	body := map[string]interface{}{
		"addressRange":     "cidr-first",
		"addressRangeCidr": "10.7.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", body)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Second create with same CIDR — should conflict.
	body["addressRange"] = "cidr-second"
	resp = doJSON(t, srv, http.MethodPost, "/v1/addressRange", body)
	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestAddressRangeAPI_Delete(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create
	createBody := map[string]interface{}{
		"addressRange":     "range-to-delete",
		"addressRangeCidr": "10.8.0.0/16",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// Delete
	resp = doJSON(t, srv, http.MethodDelete, "/v1/addressRange/"+arID, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	// Get after delete should 404.
	resp = doJSON(t, srv, http.MethodGet, "/v1/addressRange/"+arID, nil)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestAddressRangeAPI_List(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	// Create two ranges in the same VPC.
	for _, name := range []string{"list-range-1", "list-range-2"} {
		cidr := "10.9.0.0/24"
		if name == "list-range-2" {
			cidr = "10.9.1.0/24"
		}
		resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", map[string]interface{}{
			"addressRange":     name,
			"addressRangeCidr": cidr,
			"network":          "projects/123456/global/networks/list-vpc",
			"lifType":          "dataLIF",
		})
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}

	// List with VPC filter.
	resp := doJSON(t, srv, http.MethodGet, "/v1/addressRange?vpcName=list-vpc&hostProjectNumber=123456", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var list []interface{}
	decodeBody(t, resp, &list)
	assert.Len(t, list, 2)
}

// TestAddressRangeAPI_DeleteReturnsDeletedAt verifies the delete response includes deletedAt.
func TestAddressRangeAPI_DeleteReturnsDeletedAt(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	createBody := map[string]interface{}{
		"addressRange":     "range-del-at",
		"addressRangeCidr": "10.20.0.0/24",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	resp = doJSON(t, srv, http.MethodDelete, "/v1/addressRange/"+arID, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var deleted map[string]interface{}
	decodeBody(t, resp, &deleted)
	// deletedAt must be present in the response.
	assert.NotEmpty(t, deleted["deletedAt"], "expected deletedAt to be set in delete response")
}

// TestAddressRangeAPI_UpdateStateWithRouteAggregation verifies routeAggregationAppliedAt is set.
func TestAddressRangeAPI_UpdateStateWithRouteAggregation(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	createBody := map[string]interface{}{
		"addressRange":     "range-route-agg",
		"addressRangeCidr": "10.21.0.0/24",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// Transition to IN_USE with routeAggregationApplied=true via the CVN update endpoint.
	stateBody := map[string]interface{}{
		"lifeCycleState":          "IN_USE",
		"routeAggregationApplied": true,
	}
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID+"/updateState", stateBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var updated map[string]interface{}
	decodeBody(t, resp, &updated)
	assert.Equal(t, "IN_USE", updated["lifeCycleState"])
	assert.NotEmpty(t, updated["routeAggregationAppliedAt"], "expected routeAggregationAppliedAt to be set")
}

// TestAddressRangeAPI_UpdateLifeCycleStateViaUpdateStateEndpoint covers line 110 via CVN endpoint.
func TestAddressRangeAPI_UpdateLifeCycleStateViaUpdateEndpoint(t *testing.T) {
	srv := setupIntegrationServer(t)
	defer srv.Close()

	createBody := map[string]interface{}{
		"addressRange":     "range-state-cvn",
		"addressRangeCidr": "10.22.0.0/24",
		"network":          "projects/123456/global/networks/my-vpc",
		"lifType":          "dataLIF",
	}
	resp := doJSON(t, srv, http.MethodPost, "/v1/addressRange", createBody)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var created map[string]interface{}
	decodeBody(t, resp, &created)
	arID := fmt.Sprintf("%v", created["addressRangeId"])

	// Use update (PUT /v1/addressRange/{id}) with lifeCycleState set to cover line 110.
	updateBody := map[string]interface{}{
		"lifeCycleState": "DISABLED",
	}
	resp = doJSON(t, srv, http.MethodPut, "/v1/addressRange/"+arID, updateBody)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var updated map[string]interface{}
	decodeBody(t, resp, &updated)
	assert.Equal(t, "DISABLED", updated["lifeCycleState"])
}
