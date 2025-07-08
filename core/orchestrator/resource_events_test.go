package orchestrator

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflow_engine_mock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"testing"
)

func TestCreateOrGetStartProjectEventJobl(t *testing.T) {
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
			LocationID:     "us-central1",
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
			LocationID:     "us-central1",
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
		mockStorage.On("GetJobsWithCondition", ctx, *filter).Return([]*datamodel.Job{{BaseModel: datamodel.BaseModel{UUID: "jobUUID"}}}, nil)

		params := &commonparams.StartProjectEventParams{
			LocationID:     "us-central1",
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
			LocationID:     "us-central1",
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
			LocationID:     "us-central1",
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
			LocationID:     "us-central1",
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
