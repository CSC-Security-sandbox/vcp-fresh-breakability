package flexcache_activities

import (
	"testing"
	
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	hyperscalerGetProviderByNode = mm.hyperscalerGetProviderByNode
	utilGetLogger = mm.utilGetLogger

	t.Cleanup(func() {
		hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
		utilGetLogger = util.GetLogger
	})

	return mm
}
