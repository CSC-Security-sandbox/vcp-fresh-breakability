package gcp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestNewGCPOrchestrator(t *testing.T) {
	t.Run("Success_CreatesOrchestrator", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(tt)

		orch := NewGCPOrchestrator(mockStorage, mockTemporal)

		assert.NotNil(tt, orch)
		assert.Equal(tt, mockStorage, orch.storage)
		assert.Equal(tt, mockTemporal, orch.temporal)
	})
}
