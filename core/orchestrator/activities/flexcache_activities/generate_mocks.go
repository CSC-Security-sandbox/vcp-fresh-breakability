package flexcache_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
)

type monkeyMethods interface {
	hyperscalerGetProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error)
}
