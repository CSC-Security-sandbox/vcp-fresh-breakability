package temporalmanager_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/temporalmanager"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

// SampleWorkflow is a simple workflow that appends a suffix to the input string.
func SampleWorkflow(ctx workflow.Context, input string) (string, error) {
	// Set workflow options (e.g., timeout)
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	// Perform a simple operation
	result := input + "_processed"

	// Simulate some delay
	err := workflow.Sleep(ctx, 2*time.Second)
	if err != nil {
		return "", err
	}

	return result, nil
}

// SampleActivity is a simple activity that appends a suffix to the input string.
func SampleActivity(input string) (string, error) {
	// Perform a simple operation
	result := input + "_processed"
	return result, nil
}

func TestNewWorker(t *testing.T) {
	// Test case for NewWorker
	t.Run("NewWorker", func(t *testing.T) {
		// Mock client and task queue
		clientOptions := client.Options{
			HostPort: "localhost:0000", // Using a non-existent port for testing
		}
		mockClient, _ := client.NewLazyClient(clientOptions)
		taskQueue := "test-task-queue"

		// Create a new worker
		worker := temporalmanager.NewWorker(mockClient, taskQueue)

		// Check if the worker is not nil
		if worker == nil {
			t.Errorf("Expected worker to be not nil, got nil")
		}
		// Check if the worker's client is the same as the mock client
		if worker.GetClient() != mockClient {
			t.Errorf("Expected worker client to be %v, got %v", mockClient, worker.GetClient())
		}
		worker.RegisterWorkflow(SampleWorkflow)
		worker.RegisterActivity(SampleActivity)
		err := worker.Run()
		assert.NotNil(t, err)
		// Check if the worker is running
		if worker.GetWorker() == nil {
			t.Errorf("Expected worker to be running, got nil")
		}
		worker.Stop()
		// close client
		worker.GetClient().Close()
	})
}
