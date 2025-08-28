package flexcache_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

type FlexCacheVolumeCreateActivity struct {
	SE database.Storage
}

// CreateFlexCacheVolumeInOntapActivity creates a FlexCache volume in ONTAP
func (a *FlexCacheVolumeCreateActivity) CreateFlexCacheVolumeInOntapActivity(ctx context.Context, dbVolume *datamodel.Volume, node *models.Node) error {
	// TODO: VSCP-1249 - Implement FlexCache volume creation in ONTAP

	return nil
}
