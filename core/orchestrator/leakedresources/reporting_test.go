package leakedresources

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/model"
)

func TestLogReporter_Report_NoRecords(t *testing.T) {
	ctx := context.Background()
	var r LogReporter
	err := r.Report(ctx, nil)
	assert.NoError(t, err)
	err = r.Report(ctx, []model.LeakRecord{})
	assert.NoError(t, err)
}

func TestLogReporter_Report_WithRecords(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{ResourceType: model.ResourceTypePool, ResourceID: "p1", ResourceName: "pool-1", Reason: "in_vcp_not_in_ccfe"},
		{ResourceType: model.ResourceTypeVolume, ResourceID: "v1", ResourceName: "vol-1", Reason: "volume_orphan_pool_missing"},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}

func TestLogReporter_Report_WithRecordsWithExtra(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{
			ResourceType: model.ResourceTypeSnapshot,
			ResourceID:   "78e982e1-cafc-ae9b-6b8a-4bb749df4c2e",
			ResourceName: "snapshot22021119108138284f7a1d131",
			ProjectID:    "119108138284",
			Reason:       "snapshot_orphan_volume_missing",
			Extra: map[string]string{
				"volume_id":  "123",
				"account_id": "10",
			},
		},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}

func TestLogReporter_Report_WithRecordWithNilExtra(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{
			ResourceType: model.ResourceTypeSnapshot,
			ResourceID:   "snap-1",
			ResourceName: "snap1",
			Reason:       "snapshot_orphan_volume_missing",
			Extra:        nil,
		},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}

func TestLogReporter_Report_WithRecordWithEmptyExtra(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{
			ResourceType: model.ResourceTypePool,
			ResourceID:   "p1",
			Reason:       "in_vcp_not_in_ccfe",
			Extra:        map[string]string{},
		},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}

func TestLogReporter_Report_MixedRecordsWithAndWithoutExtra(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{ResourceType: model.ResourceTypePool, ResourceID: "p1", Reason: "in_vcp_not_in_ccfe"},
		{
			ResourceType: model.ResourceTypeSnapshot,
			ResourceID:   "snap-2",
			Reason:       "snapshot_orphan_volume_missing",
			Extra:        map[string]string{"volume_id": "456"},
		},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}
