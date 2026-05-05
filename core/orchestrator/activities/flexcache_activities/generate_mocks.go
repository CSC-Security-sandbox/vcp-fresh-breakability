package flexcache_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
)

type monkeyMethods interface {
	fetchTemporalClientForFlexCacheDelete(ctx context.Context) client.Client
	hyperscalerGetProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error)
	utilGetLogger(ctx interface{}) log.Logger
	verifyAndGetFlexCacheUpdateParams(volume *datamodel.Volume, params *common.UpdateVolumeParams) (*vsa.UpdateFlexCacheVolumeParams, error)
	commonHydrateFlexCacheState(ctx context.Context, logger log.Logger, region, projectId, volumeResourceID, cacheState, state, token string) error
	authGenerateCallbackToken(ctx context.Context) (string, error)
	isHydrationEnabled() bool
}
