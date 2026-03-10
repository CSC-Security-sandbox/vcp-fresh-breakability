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
