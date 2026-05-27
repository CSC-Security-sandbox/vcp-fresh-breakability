package gcp

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateOrGetStartProjectEventJob(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("panic")
		}
		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOn,
		}
		_, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "panic", err.Error())
	})
	t.Run("WhenGetJobsWithConditionFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}
		mockStorage.On("GetJobsWithCondition", mock.Anything, mock.Anything).Return(nil, errors.New("panic"))

		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOn,
		}
		_, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "panic", err.Error())
	})
	t.Run("WhenGetJobsWithConditionReturnsJob", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}
		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeStartProjectEventOnState)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{{
			BaseModel:     datamodel.BaseModel{UUID: "jobUUID"},
			JobAttributes: &datamodel.JobAttributes{Location: "us-central1"},
		}}, nil)

		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOn,
		}
		res, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, "jobUUID", res)
	})
	t.Run("WhenCreateJobReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeStartProjectEventOffState)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("panic"))

		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		_, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "panic", err.Error())
	})
	t.Run("WhenTemporalExecuteWorkflowReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeStartProjectEventOffState)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to execute workflow"))

		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		_, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to execute workflow", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeStartProjectEventOffState)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "jobUUID"}}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		params := &commonparams.StartProjectEventParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		res, err := _createOrGetStartProjectEventJob(ctx, mockStorage, mockTemporal, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, "jobUUID", res)
	})
}
func TestUpdateResourceStateJob(t *testing.T) {
	t.Run("WhenGetOrCreateAccountFails", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return nil, errors.New("panic")
		}
		params := &commonparams.UpdateResourceStateParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOn,
		}
		_, err := _updateResourceState(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "panic", err.Error())
	})
	t.Run("WhenCreateJobReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeHandleResourceEvent)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(nil, errors.New("panic"))

		params := &commonparams.UpdateResourceStateParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		_, err := _updateResourceState(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "panic", err.Error())
	})
	t.Run("WhenTemporalExecuteWorkflowReturnsError", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeHandleResourceEvent)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to execute workflow"))

		params := &commonparams.UpdateResourceStateParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		_, err := _updateResourceState(ctx, mockStorage, mockTemporal, params)
		assert.NotNil(tt, err)
		assert.Equal(tt, "failed to execute workflow", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		ctx := context.Background()
		mockLogger := log.NewLogger()
		ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)
		mockStorage := new(database.MockStorage)
		mockTemporal := workflow_engine_mock.NewMockTemporalTestClient(t)

		getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
			return &datamodel.Account{Name: "12345", BaseModel: datamodel.BaseModel{ID: 1}}, nil
		}

		jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
		filter := utils.CreateFilterWithConditions(
			utils.NewFilterCondition("account_id", "=", int64(1)),
			utils.NewFilterCondition("type", "=", string(models.JobTypeHandleResourceEvent)),
			utils.NewFilterCondition("state", "in", jobTransitioningStates))
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{}, nil)
		mockStorage.On("CreateJob", ctx, mock.Anything).Return(&datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "jobUUID"}}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		params := &commonparams.UpdateResourceStateParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: "test-correlation-id",
			State:          models.StateOff,
		}
		res, err := _updateResourceState(ctx, mockStorage, mockTemporal, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)
		assert.Equal(tt, "jobUUID", res)
	})
}

func TestCreateOrGetFinishProjectEventJob(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func(*database.MockStorage, *workflow_engine_mock.MockTemporalTestClient)
		params        *commonparams.FinishProjectEventParams
		expectedJobID string
		expectedError string
	}{
		{
			name: "Error getting account",
			setupMocks: func(storage *database.MockStorage, _ *workflow_engine_mock.MockTemporalTestClient) {
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return nil, errors.New("failed to get account")
				}
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedError: "failed to get account",
		},
		{
			name: "Error getting jobs",
			setupMocks: func(storage *database.MockStorage, _ *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(
					nil, errors.New("failed to fetch jobs"))
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedError: "failed to fetch jobs",
		},
		{
			name: "Existing job found with matching location",
			setupMocks: func(storage *database.MockStorage, _ *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(
					[]*datamodel.Job{{
						BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
						JobAttributes: &datamodel.JobAttributes{Location: "test-location"},
					}}, nil)
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedJobID: "job-uuid",
		},
		{
			name: "Existing jobs found but different location - creates new job",
			setupMocks: func(storage *database.MockStorage, temporalClient *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(
					[]*datamodel.Job{{
						BaseModel:     datamodel.BaseModel{UUID: "existing-job-uuid"},
						JobAttributes: &datamodel.JobAttributes{Location: "different-location"},
					}}, nil)

				createdJob := &datamodel.Job{
					BaseModel: datamodel.BaseModel{
						UUID: "new-job-uuid",
					},
					WorkflowID: "workflow-id",
				}
				storage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(createdJob, nil)
				temporalClient.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedJobID: "new-job-uuid",
		},
		{
			name: "Existing job with empty location matches empty request location",
			setupMocks: func(storage *database.MockStorage, _ *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(
					[]*datamodel.Job{{
						BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
						JobAttributes: &datamodel.JobAttributes{Location: ""},
					}}, nil)
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "",
				State:         "DELETE",
			},
			expectedJobID: "job-uuid",
		},
		{
			name: "No existing jobs - creates new job successfully",
			setupMocks: func(storage *database.MockStorage, temporalClient *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil)

				createdJob := &datamodel.Job{
					BaseModel: datamodel.BaseModel{
						UUID: "new-job-uuid",
					},
					WorkflowID: "workflow-id",
				}
				storage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(createdJob, nil)
				temporalClient.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedJobID: "new-job-uuid",
		},
		{
			name: "Job found with NotFound error - creates new job",
			setupMocks: func(storage *database.MockStorage, temporalClient *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(
					nil, customerrors.NewNotFoundErr("not found", nil))

				createdJob := &datamodel.Job{
					BaseModel: datamodel.BaseModel{
						UUID: "new-job-uuid",
					},
					WorkflowID: "workflow-id",
				}
				storage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(createdJob, nil)
				temporalClient.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedJobID: "new-job-uuid",
		},
		{
			name: "Error creating new job",
			setupMocks: func(storage *database.MockStorage, _ *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil)
				storage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(nil, errors.New("create job error"))
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedError: "create job error",
		},
		{
			name: "Error executing workflow",
			setupMocks: func(storage *database.MockStorage, temporalClient *workflow_engine_mock.MockTemporalTestClient) {
				account := &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						ID: 123,
					},
				}
				getOrCreateAccount = func(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error) {
					return account, nil
				}
				storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil)

				createdJob := &datamodel.Job{
					BaseModel: datamodel.BaseModel{
						UUID: "new-job-uuid",
					},
					WorkflowID: "workflow-id",
				}
				storage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(createdJob, nil)
				temporalClient.EXPECT().ExecuteWorkflow(
					mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(
					nil, errors.New("failed to execute workflow"))
			},
			params: &commonparams.FinishProjectEventParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
				State:         "DELETE",
			},
			expectedError: "failed to execute workflow",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			mockStorage := &database.MockStorage{}
			mockTemporalClient := &workflow_engine_mock.MockTemporalTestClient{}
			tt.setupMocks(mockStorage, mockTemporalClient)

			orchestrator := &GCPOrchestrator{
				storage:  mockStorage,
				temporal: mockTemporalClient,
			}

			// Execute
			jobID, err := orchestrator.CreateOrGetFinishProjectEventJob(context.Background(), tt.params)

			// Assert
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedJobID, jobID)
			}

			mockStorage.AssertExpectations(t)
			mockTemporalClient.AssertExpectations(t)
		})
	}
}
