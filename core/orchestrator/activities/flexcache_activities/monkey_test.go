package flexcache_activities

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"testing"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	hyperscalerGetProviderByNode = mm.hyperscalerGetProviderByNode

	t.Cleanup(func() {
		hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
	})

	return mm
}
