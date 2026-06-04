package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestAPIRequestsTotal_Increments(t *testing.T) {
	before := testutil.ToFloat64(APIRequestsTotal.WithLabelValues("/pools", "POST", "202", "us-east"))
	APIRequestsTotal.WithLabelValues("/pools", "POST", "202", "us-east").Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(APIRequestsTotal.WithLabelValues("/pools", "POST", "202", "us-east")))
}

func TestAPIRequestDurationSeconds_Observes(t *testing.T) {
	assert.NotPanics(t, func() {
		APIRequestDurationSeconds.WithLabelValues("POST", "/pools", "us-east").Observe(0.05)
	})
}

func TestNormalizeRoute_PoolOCID(t *testing.T) {
	assert.Equal(t, "/v1beta/pools/{poolOCID}", NormalizeRoute("/v1beta/pools/ocid1.pool.oc1..abc123"))
}

func TestNormalizeRoute_CreateSvmByPool(t *testing.T) {
	assert.Equal(t, "/v1beta/pools/{poolOCID}/svms", NormalizeRoute("/v1beta/pools/ocid1.pool.oc1..abc123/svms"))
}

func TestNormalizeRoute_DeleteSvm(t *testing.T) {
	assert.Equal(t, "/v1beta/pools/{poolOCID}/svms/{svmOCID}", NormalizeRoute("/v1beta/pools/ocid1.pool.oc1..abc/svms/ocid1.svm.oc1..xyz"))
}

func TestNormalizeRoute_WorkRequest(t *testing.T) {
	assert.Equal(t, "/v1beta/workRequests/{workRequestId}", NormalizeRoute("/v1beta/workRequests/wf-abc123"))
}

func TestNormalizeRoute_StaticPath(t *testing.T) {
	assert.Equal(t, "/v1beta/pools", NormalizeRoute("/v1beta/pools"))
}

func TestNormalizeRoute_Health(t *testing.T) {
	assert.Equal(t, "/health", NormalizeRoute("/health"))
}

func TestRegion_ReturnsString(t *testing.T) {
	got := Region()
	assert.IsType(t, "", got)
}
