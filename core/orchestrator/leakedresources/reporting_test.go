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

func TestLogReporter_Report_InternalReservedIP_DedicatedLine(t *testing.T) {
	ctx := context.Background()
	records := []model.LeakRecord{
		{
			ResourceType: model.ResourceTypeInternalReservedIP,
			ResourceID:   "https://www.googleapis.com/compute/v1/projects/tp/regions/us-central1/addresses/x",
			ResourceName: "addr-x",
			ProjectID:    "tp",
			Region:       "us-central1",
			Reason:       "internal_reserved_ip_unassigned_capacity",
			Extra: map[string]string{
				"ip":                 "10.0.0.5",
				"subnet":             "sn1",
				"pool_uuids":         "pool-uuid-1",
				"creation_timestamp": "2024-01-01T00:00:00Z",
				"age_basis":          "gcp_creation_timestamp",
			},
		},
	}
	var r LogReporter
	err := r.Report(ctx, records)
	assert.NoError(t, err)
}

func TestFormatLeakCountsByType_Empty(t *testing.T) {
	assert.Equal(t, "", formatLeakCountsByType(map[model.ResourceType]int{}))
}

func TestExtraGet_NilMap(t *testing.T) {
	assert.Equal(t, "", extraGet(nil, "k"))
}

func TestFormatExtraKeyValuesExclude_AllExcluded(t *testing.T) {
	got := formatExtraKeyValuesExclude(map[string]string{"a": "1"}, "a")
	assert.Equal(t, "", got)
}
