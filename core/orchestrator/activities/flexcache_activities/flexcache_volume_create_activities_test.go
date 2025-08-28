package flexcache_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestFlexCacheVolumeCreateActivity_CreateFlexCacheVolumeInOntap(t *testing.T) {
	var mockStorage database.Storage = nil
	activity := &FlexCacheVolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	dbVolume := &datamodel.Volume{}
	node := &models.Node{}

	err := activity.CreateFlexCacheVolumeInOntapActivity(ctx, dbVolume, node)

	assert.NoError(t, err, "No-op function should return nil")
}
