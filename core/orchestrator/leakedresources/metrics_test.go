package leakedresources

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
	"go.opentelemetry.io/otel/attribute"
)

type localMockReporter struct {
	mock.Mock
}

func (m *localMockReporter) Report(ctx context.Context, records []model.LeakRecord) error {
	args := m.Called(ctx, records)
	return args.Error(0)
}

func resetLeakedCountsForTest() {
	leakedMetricsMu.Lock()
	defer leakedMetricsMu.Unlock()
	leakedCountsByKey = map[leakedCountKey]int64{}
}

func TestMetricsReporter_Report_AggregatesCountsBySafeDimensions(t *testing.T) {
	resetLeakedCountsForTest()
	r := NewMetricsReporter()

	records := []model.LeakRecord{
		{
			ResourceType: model.ResourceTypePool,
			Reason:       "in_vcp_not_in_ccfe",
			ProjectID:    "p1",
			Region:       "us-central1",
		},
		{
			ResourceType: model.ResourceTypePool,
			Reason:       "in_vcp_not_in_ccfe",
			ProjectID:    "p1",
			Region:       "us-central1",
		},
		{
			ResourceType: model.ResourceTypeVolume,
			Reason:       "volume_orphan_pool_missing",
		},
	}

	err := r.Report(context.Background(), records)
	assert.NoError(t, err)

	leakedMetricsMu.RLock()
	defer leakedMetricsMu.RUnlock()
	assert.Equal(t, int64(2), leakedCountsByKey[leakedCountKey{
		resourceType: "pool",
		reason:       "in_vcp_not_in_ccfe",
		projectID:    "p1",
		region:       "us-central1",
	}])
	assert.Equal(t, int64(1), leakedCountsByKey[leakedCountKey{
		resourceType: "volume",
		reason:       "volume_orphan_pool_missing",
		projectID:    "",
		region:       "",
	}])
}

func TestMetricsReporter_Report_ReplacesPreviousRunState(t *testing.T) {
	resetLeakedCountsForTest()
	r := NewMetricsReporter()

	err := r.Report(context.Background(), []model.LeakRecord{
		{ResourceType: model.ResourceTypePool, Reason: "in_vcp_not_in_ccfe"},
	})
	assert.NoError(t, err)

	err = r.Report(context.Background(), []model.LeakRecord{
		{ResourceType: model.ResourceTypeSnapshot, Reason: "snapshot_orphan_volume_missing"},
	})
	assert.NoError(t, err)

	leakedMetricsMu.RLock()
	defer leakedMetricsMu.RUnlock()
	assert.Len(t, leakedCountsByKey, 1)
	assert.Equal(t, int64(1), leakedCountsByKey[leakedCountKey{
		resourceType: "snapshot",
		reason:       "snapshot_orphan_volume_missing",
		projectID:    "",
		region:       "",
	}])
}

func TestMultiReporter_Report_PropagatesError(t *testing.T) {
	ok := &localMockReporter{}
	fail := &localMockReporter{}
	ok.On("Report", context.Background(), mockAnyLeakRecords()).Return(nil).Once()
	fail.On("Report", context.Background(), mockAnyLeakRecords()).Return(errors.New("boom")).Once()

	r := NewMultiReporter(ok, fail)
	err := r.Report(context.Background(), []model.LeakRecord{{ResourceType: model.ResourceTypePool, Reason: "x"}})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	ok.AssertExpectations(t)
	fail.AssertExpectations(t)
}

func TestRecordMonitoringRun_InitializesCounter(t *testing.T) {
	recordMonitoringRun(context.Background(), "success")
	assert.NotNil(t, monitoringRunsCounter)
}

func TestRecordMonitoringRun_EmptyStatusDefaultsToError(t *testing.T) {
	recordMonitoringRun(context.Background(), "")
	assert.NotNil(t, monitoringRunsCounter)
}

func TestCurrentLeakCountObservations_IncludesOptionalDimensionsConditionally(t *testing.T) {
	resetLeakedCountsForTest()
	updateLeakedCounts([]model.LeakRecord{
		{
			ResourceType: model.ResourceTypePool,
			Reason:       "in_vcp_not_in_ccfe",
			ProjectID:    "proj-1",
			Region:       "us-central1",
		},
		{
			ResourceType: model.ResourceTypeVolume,
			Reason:       "volume_orphan_pool_missing",
		},
	})

	observations := currentLeakCountObservations()
	assert.Len(t, observations, 2)

	containsObs := func(expectedCount int64, attrs map[string]string) bool {
		for _, o := range observations {
			if o.count != expectedCount {
				continue
			}
			m := map[string]string{}
			for _, kv := range o.attrs {
				if kv.Value.Type() == attribute.STRING {
					m[string(kv.Key)] = kv.Value.AsString()
				}
			}
			match := true
			for k, v := range attrs {
				if m[k] != v {
					match = false
					break
				}
			}
			if match && len(m) == len(attrs) {
				return true
			}
		}
		return false
	}

	assert.True(t, containsObs(1, map[string]string{
		"resource_type": "pool",
		"reason":        "in_vcp_not_in_ccfe",
		"project_id":    "proj-1",
		"region":        "us-central1",
	}))
	assert.True(t, containsObs(1, map[string]string{
		"resource_type": "volume",
		"reason":        "volume_orphan_pool_missing",
	}))
}

func mockAnyLeakRecords() []model.LeakRecord {
	return []model.LeakRecord{{ResourceType: model.ResourceTypePool, Reason: "x"}}
}
