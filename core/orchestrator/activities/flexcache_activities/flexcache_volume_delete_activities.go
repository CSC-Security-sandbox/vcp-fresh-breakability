package flexcache_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type FlexCacheVolumeDeleteActivity struct {
	SE database.Storage
}

// DeleteFlexCacheVolumeInOntapActivity deletes a FlexCache volume in ONTAP
func (a FlexCacheVolumeDeleteActivity) DeleteFlexCacheVolumeInOntapActivity(ctx context.Context, result *flexcache.DeleteFlexCacheResult) (*flexcache.DeleteFlexCacheResult, error) {
	// TODO: VSCP-1231 - Implement FlexCache volume deletion in ONTAP
	return nil, nil
}
