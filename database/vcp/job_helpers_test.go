package database

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestGetExistingDeleteJobForDeletingState_ExistingJobInProgress_ReturnsJobUUID(t *testing.T) {
	ctx := context.Background()
	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	deleteJobType := models.JobTypeDeleteVolume

	existingJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid-123"},
		State:     string(models.JobsStatePROCESSING),
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(deleteJobType)).Return(existingJob, nil)
	mockLogger.On("Infof", "Delete job already in progress for %s, returning existing job UUID: %s", resourceUUID, existingJob.UUID).Return()

	result := GetExistingDeleteJobForDeletingState(ctx, mockStorage, resourceUUID, deleteJobType, mockLogger)

	assert.Equal(t, existingJob.UUID, result)
	mockStorage.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestGetExistingDeleteJobForDeletingState_ExistingJobDone_ReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	deleteJobType := models.JobTypeDeleteVolume

	existingJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid-123"},
		State:     string(models.JobsStateDONE),
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(deleteJobType)).Return(existingJob, nil)

	result := GetExistingDeleteJobForDeletingState(ctx, mockStorage, resourceUUID, deleteJobType, mockLogger)

	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
}

func TestGetExistingDeleteJobForDeletingState_ExistingJobError_ReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	deleteJobType := models.JobTypeDeleteVolume

	existingJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "job-uuid-123"},
		State:     string(models.JobsStateERROR),
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(deleteJobType)).Return(existingJob, nil)

	result := GetExistingDeleteJobForDeletingState(ctx, mockStorage, resourceUUID, deleteJobType, mockLogger)

	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
}

func TestGetExistingDeleteJobForDeletingState_NoJobFound_ReturnsEmpty(t *testing.T) {
	ctx := context.Background()
	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	deleteJobType := models.JobTypeDeleteVolume

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(deleteJobType)).Return(nil, errors.New("not found"))

	result := GetExistingDeleteJobForDeletingState(ctx, mockStorage, resourceUUID, deleteJobType, mockLogger)

	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_EmptyCorrelationID_ReturnsError(t *testing.T) {
	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	// Create context without correlation ID
	ctxWithoutCorrID := context.Background()

	mockLogger.On("Warnf", "Correlation ID is empty for delete request on %s %s in CREATING state", resourceType, resourceUUID).Return()

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithoutCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.Error(t, err)
	assert.False(t, isCleanup)
	assert.Equal(t, "", result)
	mockLogger.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_ExistingDeleteJobCorrelationIDMismatch_ReturnsError(t *testing.T) {
	correlationID := "correlation-id-123"
	fields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}
	ctxWithCorrID := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	existingDeleteJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "delete-job-uuid"},
		CorrelationID: "different-correlation-id",
		State:         string(models.JobsStatePROCESSING),
	}

	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(deleteJobType)).Return(existingDeleteJob, nil)
	mockLogger.On("Warnf", "Correlation ID mismatch: create job correlation ID %s does not match delete request correlation ID %s", existingDeleteJob.CorrelationID, correlationID).Return()

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.Error(t, err)
	assert.False(t, isCleanup)
	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_ExistingDeleteJobInProgress_ReturnsJobUUID(t *testing.T) {
	correlationID := "correlation-id-123"
	fields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}
	ctxWithCorrID := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	existingDeleteJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "delete-job-uuid"},
		CorrelationID: correlationID,
		State:         string(models.JobsStatePROCESSING),
	}

	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(deleteJobType)).Return(existingDeleteJob, nil)
	mockLogger.On("Infof", "Delete job already in progress for %s %s, returning existing job UUID: %s", resourceType, resourceUUID, existingDeleteJob.UUID).Return()

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.NoError(t, err)
	assert.False(t, isCleanup)
	assert.Equal(t, existingDeleteJob.UUID, result)
	mockStorage.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_CreateJobNotFound_ReturnsError(t *testing.T) {
	correlationID := "correlation-id-123"
	fields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}
	ctxWithCorrID := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	// No existing delete job
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(deleteJobType)).Return(nil, nil)
	// Create job not found
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(createJobType)).Return(nil, errors.New("not found"))
	mockLogger.On("Warnf", "Create job not found for %s %s in CREATING state", resourceType, resourceUUID).Return()

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.Error(t, err)
	assert.False(t, isCleanup)
	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_CreateJobCorrelationIDMismatch_ReturnsError(t *testing.T) {
	correlationID := "correlation-id-123"
	fields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}
	ctxWithCorrID := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	// No existing delete job
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(deleteJobType)).Return(nil, nil)
	// Create job with different correlation ID
	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "create-job-uuid"},
		CorrelationID: "different-correlation-id",
	}
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(createJobType)).Return(createJob, nil)
	mockLogger.On("Warnf", "Correlation ID mismatch: create job correlation ID %s does not match delete request correlation ID %s", createJob.CorrelationID, correlationID).Return()

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.Error(t, err)
	assert.False(t, isCleanup)
	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
	mockLogger.AssertExpectations(t)
}

func TestValidateCorrelationIDForCreatingResource_ValidCorrelationID_ReturnsCleanupDelete(t *testing.T) {
	correlationID := "correlation-id-123"
	fields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}
	ctxWithCorrID := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, fields)

	mockStorage := NewMockStorage(t)
	mockLogger := &log.MockLogger{}
	resourceUUID := "resource-uuid-123"
	resourceType := "Volume"
	createJobType := models.JobTypeCreateVolume
	deleteJobType := models.JobTypeDeleteVolume

	// No existing delete job
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(deleteJobType)).Return(nil, nil)
	// Create job with matching correlation ID
	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "create-job-uuid"},
		CorrelationID: correlationID,
	}
	mockStorage.On("GetJobByResourceUUID", ctxWithCorrID, resourceUUID, string(createJobType)).Return(createJob, nil)

	result, isCleanup, err := ValidateCorrelationIDForCreatingResource(
		ctxWithCorrID, mockStorage, resourceUUID, resourceType, createJobType, deleteJobType, mockLogger)

	assert.NoError(t, err)
	assert.True(t, isCleanup)
	assert.Equal(t, "", result)
	mockStorage.AssertExpectations(t)
}
