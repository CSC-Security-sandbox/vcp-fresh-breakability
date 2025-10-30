package flexcache_activities

import (
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	isHydrationEnabled = mm.isHydrationEnabled
	verifyAndGetFlexCacheUpdateParams = mm.verifyAndGetFlexCacheUpdateParams

	hyperscalerGetProviderByNode = mm.hyperscalerGetProviderByNode
	utilGetLogger = mm.utilGetLogger
	commonHydrateFlexCacheState = mm.commonHydrateFlexCacheState
	authGenerateCallbackToken = mm.authGenerateCallbackToken

	t.Cleanup(func() {
		isHydrationEnabled = _isHydrationEnabled
		verifyAndGetFlexCacheUpdateParams = _verifyAndGetFlexCacheUpdateParams

		hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
		utilGetLogger = util.GetLogger
		commonHydrateFlexCacheState = common.HydrateFlexCacheState
		authGenerateCallbackToken = auth.GenerateCallbackToken
	})

	return mm
}
