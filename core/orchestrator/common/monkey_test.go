package common

import (
	"testing"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	hydrateToCffe = mm.hydrateToCffe

	t.Cleanup(func() {
		hydrateToCffe = _hydrateToCffe
	})

	return mm
}
