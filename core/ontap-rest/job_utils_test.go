package ontap_rest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	ontapRestModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// TestPollOntapJobDirectly tests PollOntapJobDirectly function
func TestPollOntapJobDirectly(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()

	t.Run("ContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel() // Cancel immediately

		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 1*time.Second, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
	})

	t.Run("Timeout", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		// Mock GetJob to always return running state
		jobState := ontapRestModels.JobStateRunning
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil)

		// Use a very short timeout
		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 100*time.Millisecond, 50*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "timeout")
	})

	t.Run("GetJobError", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		mockClusterClient.On("GetJob", "job-uuid").Return(nil, errors.New("get job error"))

		// Use a short timeout to avoid long test
		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 200*time.Millisecond, 50*time.Millisecond, mockLogger)
		// Should continue polling on error, but will timeout eventually
		assert.Error(tt, err)
	})

	t.Run("JobStateNil", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: nil,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil)

		// Use a short timeout
		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 200*time.Millisecond, 50*time.Millisecond, mockLogger)
		assert.Error(tt, err)
	})

	t.Run("JobStateSuccess", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateSuccess
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil)

		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 10*time.Second, 1*time.Second, mockLogger)
		assert.NoError(tt, err)
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateFailure", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateFailure
		jobMessage := "job failed message"
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State:   &jobState,
				Message: &jobMessage,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil)

		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 10*time.Second, 1*time.Second, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "job failed")
		assert.Contains(tt, err.Error(), jobMessage)
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateQueued", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateQueued
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Once()
		// Second call returns success to avoid timeout
		jobStateSuccess := ontapRestModels.JobStateSuccess
		mockJobResponseSuccess := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobStateSuccess,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponseSuccess, nil)

		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.NoError(tt, err)
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateRunning", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateRunning
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Once()
		// Second call returns success
		jobStateSuccess := ontapRestModels.JobStateSuccess
		mockJobResponseSuccess := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobStateSuccess,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponseSuccess, nil)

		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.NoError(tt, err)
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStatePaused", func(tt *testing.T) {
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStatePaused
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Once()
		// Second call returns success
		jobStateSuccess := ontapRestModels.JobStateSuccess
		mockJobResponseSuccess := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobStateSuccess,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponseSuccess, nil)

		err := PollOntapJobDirectly(ctx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.NoError(tt, err)
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateQueuedWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateQueued
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			// Cancel context after first call
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateRunningWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStateRunning
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStatePausedWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobState := ontapRestModels.JobStatePaused
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobState,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateDefaultWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobStateStr := "invalid_state"
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobStateStr,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("GetJobErrorWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		mockClusterClient.On("GetJob", "job-uuid").Return(nil, errors.New("get job error")).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 50*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})

	t.Run("JobStateNilWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: nil,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})
}

// TestPollOntapJobDirectly_DefaultCase tests the default case in PollOntapJobDirectly
func TestPollOntapJobDirectly_DefaultCase(t *testing.T) {
	ctx := context.Background()
	mockLogger := log.NewLogger()

	t.Run("JobStateDefaultWithContextCancellation", func(tt *testing.T) {
		cancelledCtx, cancel := context.WithCancel(ctx)
		mockClient := new(MockRESTClient)
		mockClusterClient := new(MockClusterClient)
		mockClient.On("Cluster").Return(mockClusterClient)

		jobStateStr := "unknown_state"
		mockJobResponse := &cluster.JobGetOK{
			Payload: &ontapRestModels.Job{
				State: &jobStateStr,
			},
		}
		mockClusterClient.On("GetJob", "job-uuid").Return(mockJobResponse, nil).Run(func(args mock.Arguments) {
			cancel()
		})

		err := PollOntapJobDirectly(cancelledCtx, mockClient, "job-uuid", 10*time.Second, 100*time.Millisecond, mockLogger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cancelled")
		mockClusterClient.AssertExpectations(tt)
	})
}
