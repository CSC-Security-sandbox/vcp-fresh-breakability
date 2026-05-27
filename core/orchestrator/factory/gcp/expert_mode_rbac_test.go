package gcp

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	workflowEngineMock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func TestUpdateRbacForPools_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	// Expected job to be created
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}

	// Mock CreateJob
	mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.Type == string(models.JobTypeExpertModeRbacRefresh) &&
			job.State == string(models.JobsStateNEW) &&
			!job.AccountID.Valid
	})).Return(expectedJob, nil).Once()

	// Mock ExecuteWorkflow
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything, // Workflow function
	).Return(nil, nil).Once()

	// Execute
	jobID, err := orchestrator.UpdateRbacForPools(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedJob.UUID, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}

func TestUpdateRbacForPools_CreateJobFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	expectedError := errors.New("failed to create job")

	// Mock CreateJob to fail
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, expectedError).Once()

	// Execute
	jobID, err := orchestrator.UpdateRbacForPools(ctx)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	// Temporal should not be called if job creation fails
	mockTemporal.AssertExpectations(t)
}

func TestUpdateRbacForPools_ExecuteWorkflowFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	// Expected job to be created
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		TrackingID:    123,
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}

	workflowError := errors.New("failed to start workflow")

	// Mock CreateJob
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(expectedJob, nil).Once()

	// Mock ExecuteWorkflow to fail
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything, // Workflow function
	).Return(nil, workflowError).Once()

	// Mock UpdateJob to update job status to ERROR
	mockStorage.On("UpdateJob", ctx, expectedJob.UUID, string(models.JobsStateERROR), expectedJob.TrackingID, workflowError.Error()).Return(nil).Once()

	// Execute
	jobID, err := orchestrator.UpdateRbacForPools(ctx)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start RBAC update workflow")
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}

func TestUpdateRbacForPools_ExecuteWorkflowFails_UpdateJobFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	// Expected job to be created
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		TrackingID:    123,
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}

	workflowError := errors.New("failed to start workflow")
	updateJobError := errors.New("failed to update job")

	// Mock CreateJob
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(expectedJob, nil).Once()

	// Mock ExecuteWorkflow to fail
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything, // Workflow function
	).Return(nil, workflowError).Once()

	// Mock UpdateJob to fail
	mockStorage.On("UpdateJob", ctx, expectedJob.UUID, string(models.JobsStateERROR), expectedJob.TrackingID, workflowError.Error()).Return(updateJobError).Once()

	// Execute
	jobID, err := orchestrator.UpdateRbacForPools(ctx)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start RBAC update workflow")
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}
func TestUpdateRbacForPoolById_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}
	poolId := "pool-uuid-123"
	mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.Type == string(models.JobTypeExpertModeRbacRefresh) &&
			job.State == string(models.JobsStateNEW) &&
			!job.AccountID.Valid &&
			job.ResourceUUID == poolId
	})).Return(expectedJob, nil).Once()
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything, // Workflow function
		poolId,
	).Return(nil, nil).Once()
	jobID, err := orchestrator.UpdateRbacForPoolById(ctx, &commonparams.RefreshRbacForPoolParams{PoolID: poolId})
	assert.NoError(t, err)
	assert.Equal(t, expectedJob.UUID, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}
func TestUpdateRbacForPoolById_CreateJobFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}
	expectedError := errors.New("failed to create job")
	poolId := "pool-uuid-123"
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, expectedError).Once()
	jobID, err := orchestrator.UpdateRbacForPoolById(ctx, &commonparams.RefreshRbacForPoolParams{PoolID: poolId})
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}
func TestUpdateRbacForPoolById_ExecuteWorkflowFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		TrackingID:    123,
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}
	workflowError := errors.New("failed to start workflow")
	poolId := "pool-uuid-123"
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(expectedJob, nil).Once()
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything,
		poolId,
	).Return(nil, workflowError).Once()
	mockStorage.On("UpdateJob", ctx, expectedJob.UUID, string(models.JobsStateERROR), expectedJob.TrackingID, workflowError.Error()).Return(nil).Once()
	jobID, err := orchestrator.UpdateRbacForPoolById(ctx, &commonparams.RefreshRbacForPoolParams{PoolID: poolId})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start single pool RBAC update workflow")
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}
func TestUpdateRbacForPoolById_ExecuteWorkflowFails_UpdateJobFails(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)
	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}
	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		TrackingID:    123,
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}
	workflowError := errors.New("failed to start workflow")
	updateJobError := errors.New("failed to update job")
	poolId := "pool-uuid-123"
	mockStorage.On("CreateJob", ctx, mock.Anything).Return(expectedJob, nil).Once()
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything,
		poolId,
	).Return(nil, workflowError).Once()
	mockStorage.On("UpdateJob", ctx, expectedJob.UUID, string(models.JobsStateERROR), expectedJob.TrackingID, workflowError.Error()).Return(updateJobError).Once()
	jobID, err := orchestrator.UpdateRbacForPoolById(ctx, &commonparams.RefreshRbacForPoolParams{PoolID: poolId})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to start single pool RBAC update workflow")
	assert.Empty(t, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}

func TestUpdateRbacForPools_JobFields(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	mockTemporal := workflowEngineMock.NewMockTemporalTestClient(t)

	orchestrator := &GCPOrchestrator{
		storage:  mockStorage,
		temporal: mockTemporal,
	}

	expectedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:          string(models.JobTypeExpertModeRbacRefresh),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Valid: false},
		WorkflowID:    "test-workflow-id",
		CorrelationID: "test-correlation-id",
		RequestID:     "test-request-id",
	}

	// Mock CreateJob with specific field validation
	mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
		return job.Type == string(models.JobTypeExpertModeRbacRefresh) &&
			job.State == string(models.JobsStateNEW) &&
			!job.AccountID.Valid
	})).Return(expectedJob, nil).Once()

	// Mock ExecuteWorkflow
	mockTemporal.EXPECT().ExecuteWorkflow(
		ctx,
		mock.MatchedBy(func(opts client.StartWorkflowOptions) bool {
			return opts.TaskQueue == workflowengine.BackgroundTaskQueue &&
				opts.ID == expectedJob.WorkflowID &&
				opts.WorkflowIDReusePolicy == enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
		}),
		mock.Anything, // Workflow function
	).Return(nil, nil).Once()

	// Execute
	jobID, err := orchestrator.UpdateRbacForPools(ctx)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedJob.UUID, jobID)
	mockStorage.AssertExpectations(t)
	mockTemporal.AssertExpectations(t)
}
