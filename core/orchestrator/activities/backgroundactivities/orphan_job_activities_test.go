package backgroundactivities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	temporalclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
)

func TestOrphanJobsActivity(t *testing.T) {
	ctx := context.Background()
	t.Run("WhenGetJobsWithConditionFails", func(tt *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockTemporalClient := &mocks.Client{}
		activity := &OrphanJobActivity{SE: mockSE,
			TemporalClient: mockTemporalClient}

		mockSE.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return(nil, errors.New("some error")).Once()

		err := activity.OrphanJobsActivity(ctx)

		assert.Error(tt, err)
		mockSE.AssertExpectations(tt)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		// Create mock storage
		mockStorage := new(database.MockStorage)
		mockTemporalClient := &mocks.Client{}

		activity := &OrphanJobActivity{
			SE:             mockStorage,
			TemporalClient: mockTemporalClient,
		}
		defer func() {
			// Restore original function after test
			processSingleJob = _processSingleJob
		}()

		processSingleJob = func(ctx context.Context, se database.Storage, job *datamodel.Job, temporalClient temporalclient.Client) error {
			return nil
		}

		// Create test jobs with proper JobAttributes
		testJobs := []*datamodel.Job{
			{
				BaseModel:  datamodel.BaseModel{UUID: "job-1"},
				Type:       string(datamodel.JobTypeCreateKmsConfig),
				State:      string(datamodel.JobsStateWaitForTemporal),
				TrackingID: 0,
				WorkflowID: "workflow-1",
				JobAttributes: &datamodel.JobAttributes{
					ResourceUUID:      "kms-config-1",
					CurrentRetryCount: 0,
				},
			},
			{
				BaseModel:  datamodel.BaseModel{UUID: "job-2"},
				Type:       string(datamodel.JobTypeDeleteKmsConfig),
				State:      string(datamodel.JobsStateWaitForTemporal),
				TrackingID: 1,
				WorkflowID: "workflow-2",
				JobAttributes: &datamodel.JobAttributes{
					ResourceUUID:      "kms-config-2",
					CurrentRetryCount: 0,
				},
			},
		}

		// Mock expectations
		filter := dbutils.CreateFilterWithConditions(
			dbutils.NewFilterCondition("state", "=", datamodel.JobsStateWaitForTemporal),
		)
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return(testJobs, nil)

		// Execute activity directly
		err := activity.OrphanJobsActivity(ctx)

		// Assertions
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockTemporalClient.AssertExpectations(t)
	})
	t.Run("WhenFlagDisabled", func(tt *testing.T) {
		// Create mock storage
		mockStorage := new(database.MockStorage)
		mockTemporalClient := &mocks.Client{}

		activity := &OrphanJobActivity{
			SE:             mockStorage,
			TemporalClient: mockTemporalClient,
		}
		orphanJobProcessingEnabled = false
		defer func() {
			orphanJobProcessingEnabled = env.GetBool("ORPHAN_JOB_PROCESSING_ENABLED", true)
		}()

		// Execute activity directly
		err := activity.OrphanJobsActivity(ctx)

		// Assertions
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockTemporalClient.AssertExpectations(t)
	})
}

func TestProcessSingleJob_Success(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: 1,
		},
	}

	// Mock KMS config data
	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:         datamodel.BaseModel{UUID: "kms-config-1"},
		Account:           &datamodel.Account{Name: "test-account", BaseModel: datamodel.BaseModel{ID: 1}},
		ResourceID:        "resource-1",
		Description:       "Test KMS Config",
		KeyRingLocation:   "us-central1",
		CustomerProjectID: "project-123",
		KmsAttributes: &datamodel.KmsAttributes{
			SdeKmsConfigOperationURI:  "operations/operation-1",
			SdeKmsConfigOperationDone: false,
		},
	}

	// Mock storage calls
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)

	// Mock temporal client
	mockTemporalClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("workflow-1")

	// Create expected parameters
	expectedParams := &common.CreateKmsConfigParams{
		UUID:          "kms-config-1",
		ProjectNumber: "test-account",
		AccountName:   "test-account",
		ResourceID:    "resource-1",
		Description:   "Test KMS Config",
		LocationID:    "us-central1",
		OperationUri:  "operations/operation-1",
		OperationDone: false,
	}

	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.Anything,
		mock.Anything, // workflow function
		expectedParams,
		mockKmsConfig,
	).Return(mockRun, nil)

	// Execute
	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

func TestProcessSingleJob_MaxRetriesExceeded_CreateKmsConfig(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: models.WaitForTemporalJobMaxRetryCount,
		},
	}

	// Mock KMS config data for FailedWorkflowJob
	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:         datamodel.BaseModel{UUID: "kms-config-1"},
		Account:           &datamodel.Account{Name: "test-account"},
		ResourceID:        "resource-1",
		Description:       "Test KMS Config",
		KeyRingLocation:   "us-central1",
		CustomerProjectID: "project-123",
		KmsAttributes:     &datamodel.KmsAttributes{},
	}

	// Mock function calls
	getSignedAuthToken = func(projectID string) (string, error) {
		return "mocked-jwt-token", nil
	}

	failedKmsConfigCreateActivity = func(ctx context.Context, se database.Storage, kmsConfig *datamodel.KmsConfig, reason string, location string) error {
		return nil
	}

	defer func() {
		getSignedAuthToken = auth.GetSignedJwtToken
		failedKmsConfigCreateActivity = kms_activities.FailedKmsConfigCreateActivity
	}()

	// Mock storage calls
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.Anything).Return(nil)
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)
	mockStorage.On("UpdateJob", ctx, "job-1", mock.Anything, 0, mock.Anything).Return(nil)

	// Execute
	mockTemporalClient := &mocks.Client{}
	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestProcessSingleJob_MaxRetriesExceeded_DeleteKmsConfig(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeDeleteKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: models.WaitForTemporalJobMaxRetryCount,
		},
	}

	// Mock storage calls
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.Anything).Return(nil)
	mockStorage.On("UpdateJob", ctx, "job-1", string(datamodel.JobsStateERROR), 0, mock.Anything).Return(nil)

	// Execute
	mockTemporalClient := &mocks.Client{}
	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestProcessSingleJob_UnknownJobType(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       "UNKNOWN_JOB_TYPE", // Invalid job type
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "resource-1",
			CurrentRetryCount: 0,
		},
	}

	// Mock retry count increment
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)

	// Execute
	mockTemporalClient := &mocks.Client{}
	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no workflow mapping found")
	mockStorage.AssertExpectations(t)
}

func TestProcessSingleJob_PrepareWorkflowArgsFails(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "non-existent-kms-config",
			CurrentRetryCount: 1,
		},
	}

	// Mock resource not found
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)
	mockStorage.On("GetKmsConfig", ctx, "non-existent-kms-config").Return(nil, errors.New("kms config not found"))

	// Execute
	mockTemporalClient := &mocks.Client{}
	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare workflow arguments")
	mockStorage.AssertExpectations(t)
}

func TestProcessSingleJob_WorkflowExecutionFails(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: 1,
		},
	}

	// Mock KMS config data
	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:         datamodel.BaseModel{UUID: "kms-config-1"},
		Account:           &datamodel.Account{Name: "test-account"},
		ResourceID:        "resource-1",
		Description:       "Test KMS Config",
		KeyRingLocation:   "us-central1",
		CustomerProjectID: "project-123",
		KmsAttributes:     &datamodel.KmsAttributes{},
	}

	workflowError := errors.New("workflow execution failed")

	// Mock storage calls
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)
	mockStorage.On("UpdateJob", ctx, "job-1", string(datamodel.JobsStateWaitForTemporal), 0, workflowError.Error()).Return(nil)

	// Mock temporal client
	mockTemporalClient := &mocks.Client{}
	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, workflowError)

	// Execute
	err := processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.Error(t, err)
	assert.Equal(t, workflowError, err)
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
}

func TestProcessSingleJob_RetryCountIncrementFails(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: 1,
		},
	}

	// Mock KMS config data
	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:         datamodel.BaseModel{UUID: "kms-config-1"},
		Account:           &datamodel.Account{Name: "test-account"},
		ResourceID:        "resource-1",
		Description:       "Test KMS Config",
		KeyRingLocation:   "us-central1",
		CustomerProjectID: "project-123",
		KmsAttributes:     &datamodel.KmsAttributes{},
	}

	// Mock storage calls - retry count update fails but execution continues
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(errors.New("update failed"))
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)

	// Mock temporal client
	mockTemporalClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("workflow-1")
	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(mockRun, nil)

	// Execute - should succeed even if retry count update fails
	err := processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.NoError(t, err) // The function should continue even if retry count update fails
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

// TestProcessSingleJob_SplitVolume_TaskQueueAndTimeout asserts that a JobTypeSplitVolume
// job is submitted to BackgroundTaskQueue with a WorkflowRunTimeout equal to
// GetSplitVolumeWorkflowTimeout(), not the global default timeout.
func TestProcessSingleJob_SplitVolume_TaskQueueAndTimeout(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-split-1"},
		Type:       string(datamodel.JobTypeSplitVolume),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-split-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "volume-1",
			CurrentRetryCount: 1,
		},
	}

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{ID: 10, UUID: "pool-1"},
		DeploymentName: "test-deployment",
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-1"},
		Pool:      pool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			SplitJobUUID: "ontap-job-1",
		},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{UUID: "node-1"}},
	}

	mockStorage.On("UpdateJobAttributes", ctx, "job-split-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)
	mockStorage.On("GetVolume", ctx, "volume-1").Return(volume, nil)
	mockStorage.On("GetNodesByPoolID", ctx, int64(10)).Return(dbNodes, nil)

	mockTemporalClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("workflow-split-1")

	expectedTimeout := *workflowengine.GetSplitVolumeWorkflowTimeout()
	expectedTaskQueue := workflowengine.BackgroundTaskQueue

	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.MatchedBy(func(opts temporalclient.StartWorkflowOptions) bool {
			return opts.TaskQueue == expectedTaskQueue &&
				opts.WorkflowRunTimeout == expectedTimeout
		}),
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(mockRun, nil)

	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

// TestProcessSingleJob_DefaultJobType_UsesCustomerTaskQueueAndGlobalTimeout asserts that
// job types without an explicit taskQueue or timeoutSeconds (e.g. CreateKmsConfig) fall
// back to CustomerTaskQueue and the global workflow timeout.
func TestProcessSingleJob_DefaultJobType_UsesCustomerTaskQueueAndGlobalTimeout(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-kms-1"},
		Type:       string(datamodel.JobTypeCreateKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-kms-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: 1,
		},
	}

	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:         datamodel.BaseModel{UUID: "kms-config-1"},
		Account:           &datamodel.Account{Name: "test-account", BaseModel: datamodel.BaseModel{ID: 1}},
		ResourceID:        "resource-1",
		Description:       "Test KMS Config",
		KeyRingLocation:   "us-central1",
		CustomerProjectID: "project-123",
		KmsAttributes: &datamodel.KmsAttributes{
			SdeKmsConfigOperationURI:  "operations/op-1",
			SdeKmsConfigOperationDone: false,
		},
	}

	mockStorage.On("UpdateJobAttributes", ctx, "job-kms-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)

	mockTemporalClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("workflow-kms-1")

	expectedTimeout := workflowengine.GetWorkflowGlobalTimeout()
	expectedTaskQueue := workflowengine.CustomerTaskQueue

	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.MatchedBy(func(opts temporalclient.StartWorkflowOptions) bool {
			return opts.TaskQueue == expectedTaskQueue &&
				opts.WorkflowRunTimeout == expectedTimeout
		}),
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(mockRun, nil)

	err := _processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}

func TestProcessSingleJob_DeleteKmsConfig_Success(t *testing.T) {
	ctx := context.Background()

	mockStorage := new(database.MockStorage)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-1"},
		Type:       string(datamodel.JobTypeDeleteKmsConfig),
		State:      string(datamodel.JobsStateWaitForTemporal),
		TrackingID: 0,
		WorkflowID: "workflow-1",
		JobAttributes: &datamodel.JobAttributes{
			ResourceUUID:      "kms-config-1",
			CurrentRetryCount: 1,
		},
	}

	// Mock KMS config data
	mockKmsConfig := &datamodel.KmsConfig{
		BaseModel:       datamodel.BaseModel{UUID: "kms-config-1"},
		Account:         &datamodel.Account{Name: "test-account"},
		ResourceID:      "resource-1",
		Description:     "Test KMS Config",
		KeyRingLocation: "us-central1",
		KmsAttributes:   &datamodel.KmsAttributes{},
	}

	// Mock storage calls
	mockStorage.On("GetKmsConfig", ctx, "kms-config-1").Return(mockKmsConfig, nil)
	mockStorage.On("UpdateJobAttributes", ctx, "job-1", mock.AnythingOfType("*datamodel.JobAttributes")).Return(nil)

	// Mock temporal client
	mockTemporalClient := &mocks.Client{}
	mockRun := &mocks.WorkflowRun{}
	mockRun.On("GetID").Return("workflow-1")

	// Create expected delete parameters
	expectedParams := &common.DeleteKmsConfigParams{
		KmsConfigID: "kms-config-1",
		Region:      "us-central1",
		AccountName: "test-account",
	}

	mockTemporalClient.On("ExecuteWorkflow",
		ctx,
		mock.Anything,
		mock.Anything, // workflow function
		mockKmsConfig,
		expectedParams,
	).Return(mockRun, nil)

	// Execute
	err := processSingleJob(ctx, mockStorage, job, mockTemporalClient)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockTemporalClient.AssertExpectations(t)
	mockRun.AssertExpectations(t)
}
