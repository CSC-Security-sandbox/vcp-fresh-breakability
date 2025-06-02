package temporalmanager_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/temporalmanager"
	"go.temporal.io/sdk/client"
)

// Test cases
func TestGetClient(t *testing.T) {
	manager := temporalmanager.TemporalManager{
		Client: nil,
	}
	client := manager.GetClient()
	assert.Nil(t, client)
}

func TestCloseClient(t *testing.T) {
	clientOptions := client.Options{
		HostPort: "localhost:7233",
	}
	mockClient, _ := client.NewLazyClient(clientOptions)
	// Create a new worker
	manager := temporalmanager.TemporalManager{
		Client: mockClient,
	}
	manager.CloseClient()
	assert.NotNil(t, manager.Client)
}
