package vsa

import (
	"testing"
)

func newMonkeyMockAndPatch(t *testing.T) *monkeyMock {
	mm := newMonkeyMock(t)

	getOntapClientFunc = mm.getOntapClientFunc

	t.Cleanup(func() {
		getOntapClientFunc = getOntapClient
	})

	return mm
}
