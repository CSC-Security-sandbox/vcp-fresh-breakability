package oci

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/metrics"
)

func TestWorkflowStageTotal_Increments(t *testing.T) {
	before := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfCreatePool, queueCustomer, stageVLMDeploy, resultSuccess))
	workflowStageTotal.WithLabelValues(wfCreatePool, queueCustomer, stageVLMDeploy, resultSuccess).Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfCreatePool, queueCustomer, stageVLMDeploy, resultSuccess)))
}

func TestWorkflowDurationSeconds_Observes(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowDurationSeconds.WithLabelValues(wfCreatePool, "us-ashburn-1", queueCustomer).Observe(120)
	})
}

func TestWorkflowMetrics_RegisteredOnCustomRegistry(t *testing.T) {
	workflowStageTotal.WithLabelValues(wfCreatePool, queueCustomer, stageVLMDeploy, resultSuccess).Inc()
	workflowDurationSeconds.WithLabelValues(wfCreatePool, "us-ashburn-1", queueCustomer).Observe(60)

	ts := httptest.NewServer(promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	assert.Contains(t, output, "oci_workflow_stage_total")
	assert.Contains(t, output, "oci_workflow_duration_seconds")
	assert.Contains(t, output, `workflow="oci_create_pool"`)
	assert.Contains(t, output, `stage="vlm_deploy"`)
	assert.Contains(t, output, `result="success"`)
}

func TestWorkflowMetrics_NoDefaultRegistryMetrics(t *testing.T) {
	ts := httptest.NewServer(promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{}))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	for _, line := range strings.Split(output, "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		assert.False(t, strings.HasPrefix(line, "go_"),
			"custom registry should not contain go_ metrics, found: %s", line)
		assert.False(t, strings.HasPrefix(line, "process_"),
			"custom registry should not contain process_ metrics, found: %s", line)
	}
}

func TestWorkflowStageTotal_HasCorrectLabels(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowStageTotal.WithLabelValues(wfDeletePool, queueCustomer, stageDBCleanup, resultFailure).Inc()
	})
	val := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfDeletePool, queueCustomer, stageDBCleanup, resultFailure))
	assert.GreaterOrEqual(t, val, float64(1))
}

func TestWorkflowDurationSeconds_HasCorrectLabels(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowDurationSeconds.WithLabelValues(wfDeletePool, "eu-frankfurt-1", queueCustomer).Observe(300)
	})
	count := testutil.CollectAndCount(workflowDurationSeconds)
	assert.GreaterOrEqual(t, count, 1)
}

func TestWorkflowStageTotal_UpdatePoolIncrements(t *testing.T) {
	before := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfUpdatePool, queueCustomer, stageVLMUpdate, resultSuccess))
	workflowStageTotal.WithLabelValues(wfUpdatePool, queueCustomer, stageVLMUpdate, resultSuccess).Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfUpdatePool, queueCustomer, stageVLMUpdate, resultSuccess)))
}

func TestWorkflowStageTotal_UpdatePoolDBPersistLabels(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowStageTotal.WithLabelValues(wfUpdatePool, queueCustomer, stageDBPersistFinal, resultSuccess).Inc()
	})
	finalVal := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfUpdatePool, queueCustomer, stageDBPersistFinal, resultSuccess))
	assert.GreaterOrEqual(t, finalVal, float64(1))
}

func TestWorkflowDurationSeconds_UpdatePoolObserves(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowDurationSeconds.WithLabelValues(wfUpdatePool, "us-ashburn-1", queueCustomer).Observe(180)
	})
	count := testutil.CollectAndCount(workflowDurationSeconds)
	assert.GreaterOrEqual(t, count, 1)
}

func TestWorkflowStageTotal_IncrementsSVMCreate(t *testing.T) {
	before := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfCreateSVM, queueCustomer, stageVLMCreateSVM, resultSuccess))
	workflowStageTotal.WithLabelValues(wfCreateSVM, queueCustomer, stageVLMCreateSVM, resultSuccess).Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfCreateSVM, queueCustomer, stageVLMCreateSVM, resultSuccess)))
}

func TestWorkflowStageTotal_IncrementsSVMDelete(t *testing.T) {
	before := testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfDeleteSVM, queueCustomer, stageSoftDeleteSVM, resultSuccess))
	workflowStageTotal.WithLabelValues(wfDeleteSVM, queueCustomer, stageSoftDeleteSVM, resultSuccess).Inc()
	assert.Equal(t, before+1, testutil.ToFloat64(workflowStageTotal.WithLabelValues(wfDeleteSVM, queueCustomer, stageSoftDeleteSVM, resultSuccess)))
}

func TestWorkflowDurationSeconds_ObservesSVM(t *testing.T) {
	assert.NotPanics(t, func() {
		workflowDurationSeconds.WithLabelValues(wfCreateSVM, "us-ashburn-1", queueCustomer).Observe(45)
		workflowDurationSeconds.WithLabelValues(wfDeleteSVM, "us-ashburn-1", queueCustomer).Observe(30)
	})
}
