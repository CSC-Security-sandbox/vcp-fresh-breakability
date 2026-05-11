package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsEndpoint_ShowsRegisteredMetrics(t *testing.T) {
	APIRequestsTotal.WithLabelValues("/v1beta/pools", "POST", "202", "us-ashburn-1").Inc()
	APIRequestDurationSeconds.WithLabelValues("POST", "/v1beta/pools", "us-ashburn-1").Observe(0.045)

	ts := httptest.NewServer(Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	assert.Contains(t, output, "api_requests_total")
	assert.Contains(t, output, "api_request_duration_seconds")
	assert.Contains(t, output, `endpoint="/v1beta/pools"`)
	assert.Contains(t, output, `status_code="202"`)
}

func TestMetricsEndpoint_ExcludesDefaultMetrics(t *testing.T) {
	ts := httptest.NewServer(Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	for _, prefix := range []string{"go_", "process_", "temporal_", "otel_scope_", "promhttp_"} {
		for _, line := range strings.Split(output, "\n") {
			if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
				continue
			}
			assert.False(t, strings.HasPrefix(line, prefix),
				"custom registry should not contain %s metrics, found: %s", prefix, line)
		}
	}
}

func TestMetricsEndpoint_ContainsOnlyExpectedMetricFamilies(t *testing.T) {
	APIRequestsTotal.WithLabelValues("/v1beta/integ", "GET", "200", "test-region").Inc()
	APIRequestDurationSeconds.WithLabelValues("GET", "/v1beta/integ", "test-region").Observe(0.01)

	ts := httptest.NewServer(Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	allowed := map[string]bool{
		"api_requests_total":           false,
		"api_request_duration_seconds": false,
		"oci_workflow_stage_total":         false,
		"oci_workflow_duration_seconds":    false,
	}

	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "# HELP ") {
			continue
		}
		parts := strings.SplitN(line, " ", 4)
		if len(parts) < 3 {
			continue
		}
		metricName := parts[2]
		_, ok := allowed[metricName]
		assert.True(t, ok, "unexpected metric family in output: %s", metricName)
		allowed[metricName] = true
	}

	assert.True(t, allowed["api_requests_total"], "api_requests_total should be present")
	assert.True(t, allowed["api_request_duration_seconds"], "api_request_duration_seconds should be present")
}
