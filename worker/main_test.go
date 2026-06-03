package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	tManagerPkg "github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/temporalmanager"
	"go.temporal.io/sdk/client"
)

func TestRegisterOCICustomerWorkflowsAndActivities(t *testing.T) {
	clientOptions := client.Options{
		HostPort: "localhost:0000",
	}
	mockClient, err := client.NewLazyClient(clientOptions)
	assert.NoError(t, err)
	defer mockClient.Close()

	worker := tManagerPkg.NewWorker(mockClient, "test-task-queue")
	mockStorage := database.NewMockStorage(t)

	assert.NotPanics(t, func() {
		RegisterOCICustomerWorkflowsAndActivities(*worker, mockStorage, mockClient)
	})
}
