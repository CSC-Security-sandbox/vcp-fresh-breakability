package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

func TestV1RotateGcpKmsConfig_Success(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	// Test data
	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"
	jobUUID := "test-job-uuid"

	// Create mock KMS config response
	mockKmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      kmsConfigUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		State:             models.LifeCycleStateREADY,
		StateDetails:      "ready",
		KeyRing:           "test-keyring",
		KeyRingLocation:   "us-central1",
		KeyName:           "test-key",
		KeyProjectID:      "test-project",
		CustomerProjectID: "customer-project",
		ResourceID:        "test-resource-id",
		KmsAttributes: &models.KmsAttributes{
			SdeServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		},
	}

	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID:      jobUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Type:         models.JobTypeRotateKmsConfig,
		State:        models.JobsStateNEW,
		WorkflowID:   "test-workflow-id",
		ResourceName: "test-resource",
		JobAttributes: &models.JobAttributes{
			ResourceUUID: kmsConfigUUID,
		},
	}

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return(mockKmsConfig, mockJob, nil)

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Check that we got the expected response type
	response, ok := result.(*oasgenserver.GcpKmsConfigV1)
	assert.True(t, ok)

	// Verify response fields
	assert.True(t, response.UUID.IsSet())
	assert.Equal(t, kmsConfigUUID, response.UUID.Value)

	assert.True(t, response.ResourceId.IsSet())
	assert.Equal(t, "test-resource-id", response.ResourceId.Value)

	assert.True(t, response.KeyRing.IsSet())
	assert.Equal(t, "test-keyring", response.KeyRing.Value)

	assert.True(t, response.KeyName.IsSet())
	assert.Equal(t, "test-key", response.KeyName.Value)

	assert.True(t, response.ServiceAccountEmail.IsSet())
	assert.Equal(t, "test-sa@test-project.iam.gserviceaccount.com", response.ServiceAccountEmail.Value)

	assert.Len(t, response.Jobs, 1)
	assert.True(t, response.Jobs[0].JobId.IsSet())
	assert.Equal(t, jobUUID, response.Jobs[0].JobId.Value)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_WithCorrelationID(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	// Test data
	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"
	jobUUID := "test-job-uuid"
	correlationID := "test-correlation-id"

	mockKmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{UUID: kmsConfigUUID},
		Name:      "test-kms-config",
	}

	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID:      jobUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Type:         models.JobTypeRotateKmsConfig,
		State:        models.JobsStateNEW,
		WorkflowID:   "test-workflow-id",
		ResourceName: "test-resource",
		JobAttributes: &models.JobAttributes{
			ResourceUUID: kmsConfigUUID,
		},
	}

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up context with correlation ID using the correct middleware key
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, correlationID)

	// Set up expectations
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: correlationID,
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return(mockKmsConfig, mockJob, nil)

	// Execute
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_KmsConfigNotFound(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "non-existent-uuid"
	accountName := "test-account"

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations - orchestrator returns not found error
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return((*models.KmsConfig)(nil), (*models.Job)(nil), errors.New("KMS config not found"))

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err) // Handler converts error to response
	assert.NotNil(t, result)

	// Check that we got a not found response
	notFoundResponse, ok := result.(*oasgenserver.V1RotateGcpKmsConfigNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), notFoundResponse.Code)
	assert.Equal(t, "KMS config not found", notFoundResponse.Message)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_BadRequestError(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations - orchestrator returns bad request error
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	badRequestErr := utilserrors.NewBadRequestErr("Concerned GCP KMS config is not in a state(ready/in use) to rotate the service account key")
	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return((*models.KmsConfig)(nil), (*models.Job)(nil), badRequestErr)

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err) // Handler converts error to response
	assert.NotNil(t, result)

	// Check that we got a bad request response
	badRequestResponse, ok := result.(*oasgenserver.V1RotateGcpKmsConfigBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequestResponse.Code)
	assert.Equal(t, "Concerned GCP KMS config is not in a state(ready/in use) to rotate the service account key", badRequestResponse.Message)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_ConflictError(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations - orchestrator returns conflict error
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return((*models.KmsConfig)(nil), (*models.Job)(nil), errors.New("rotation conflict: already in progress"))

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err) // Handler converts error to response
	assert.NotNil(t, result)

	// Check that we got a conflict response
	conflictResponse, ok := result.(*oasgenserver.V1RotateGcpKmsConfigConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), conflictResponse.Code)
	assert.Equal(t, "rotation conflict: already in progress", conflictResponse.Message)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_InternalServerError(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations - orchestrator returns generic error
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return((*models.KmsConfig)(nil), (*models.Job)(nil), errors.New("database connection failed"))

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err) // Handler converts error to response
	assert.NotNil(t, result)

	// Check that we got an internal server error response
	serverErrorResponse, ok := result.(*oasgenserver.V1RotateGcpKmsConfigInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), serverErrorResponse.Code)
	assert.Equal(t, "database connection failed", serverErrorResponse.Message)

	// Verify mock expectations
	mockOrch.AssertExpectations(t)
}

func TestV1RotateGcpKmsConfig_RotationDisabled(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = false

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err) // Handler should not return error, but a BadRequest response
	assert.NotNil(t, result)

	// Check that we got a forbidden request response indicating rotation is disabled
	badRequestResponse, ok := result.(*oasgenserver.V1RotateGcpKmsConfigForbidden)
	assert.True(t, ok, "Expected V1RotateGcpKmsConfigBadRequest response when rotation is disabled")
	assert.Equal(t, float64(403), badRequestResponse.Code)
	assert.Equal(t, "KMS rotation feature is currently disabled", badRequestResponse.Message)

	// Verify that orchestrator was NOT called since the feature is disabled
	mockOrch.AssertNotCalled(t, "RotateKmsConfig")
}

func TestV1RotateGcpKmsConfig_RotationEnabled(t *testing.T) {
	// Setup - enable KMS rotation for this test
	defer func() {
		kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
	}()
	kmsRotationEnabled = true

	mockOrch := orchestrator.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	kmsConfigUUID := "test-kms-config-uuid"
	accountName := "test-account"
	jobUUID := "test-job-uuid"

	// Create mock KMS config and job responses
	mockKmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      kmsConfigUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		State:             models.LifeCycleStateREADY,
		StateDetails:      "ready",
		KeyRing:           "test-keyring",
		KeyRingLocation:   "us-central1",
		KeyName:           "test-key",
		KeyProjectID:      "test-project",
		CustomerProjectID: "customer-project",
		ResourceID:        "test-resource-id",
		KmsAttributes: &models.KmsAttributes{
			SdeServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		},
	}

	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID:      jobUUID,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Type:         models.JobTypeRotateKmsConfig,
		State:        models.JobsStateNEW,
		WorkflowID:   "test-workflow-id",
		ResourceName: "test-resource",
		JobAttributes: &models.JobAttributes{
			ResourceUUID: kmsConfigUUID,
		},
	}

	req := &oasgenserver.GcpKmsKeyRotateV1{
		OwnerID: accountName,
	}

	params := oasgenserver.V1RotateGcpKmsConfigParams{
		UUID: kmsConfigUUID,
	}

	// Set up expectations - orchestrator should be called and return success
	expectedParams := &common.RotateKmsConfigParams{
		KmsConfigID:    kmsConfigUUID,
		AccountName:    accountName,
		XCorrelationID: "",
	}

	mockOrch.EXPECT().RotateKmsConfig(mock.Anything, expectedParams).Return(mockKmsConfig, mockJob, nil)

	// Execute
	ctx := context.Background()
	result, err := handler.V1RotateGcpKmsConfig(ctx, req, params)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Check that we got the expected success response
	response, ok := result.(*oasgenserver.GcpKmsConfigV1)
	assert.True(t, ok, "Expected GcpKmsConfigV1 response when rotation is enabled and successful")

	// Verify response fields
	assert.True(t, response.UUID.IsSet())
	assert.Equal(t, kmsConfigUUID, response.UUID.Value)

	assert.True(t, response.ResourceId.IsSet())
	assert.Equal(t, "test-resource-id", response.ResourceId.Value)

	// Verify that orchestrator was called as expected
	mockOrch.AssertExpectations(t)
}

func TestConvertKmsConfigToApiResponse_FullData(t *testing.T) {
	// Test data
	now := time.Now()
	kmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID:      "test-uuid",
			CreatedAt: now,
			UpdatedAt: now,
		},
		Name:              "test-kms-config",
		Description:       "Test description",
		State:             models.LifeCycleStateREADY,
		StateDetails:      "Ready for use",
		KeyRing:           "test-keyring",
		KeyRingLocation:   "us-central1",
		KeyName:           "test-key",
		KeyProjectID:      "test-project",
		CustomerProjectID: "customer-project",
		ResourceID:        "test-resource-id",
		KmsAttributes: &models.KmsAttributes{
			SdeServiceAccountEmail: "test-sa@test-project.iam.gserviceaccount.com",
		},
	}
	jobUUID := "test-job-uuid"
	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID:      jobUUID,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Type:         models.JobTypeRotateKmsConfig,
		State:        models.JobsStateNEW,
		WorkflowID:   "test-workflow-id",
		ResourceName: "test-resource",
		JobAttributes: &models.JobAttributes{
			ResourceUUID: "test-uuid",
		},
	}

	// Execute
	result := convertKmsConfigToApiResponse(kmsConfig, mockJob)

	// Assert
	assert.NotNil(t, result)

	// Verify all fields are set correctly
	assert.True(t, result.UUID.IsSet())
	assert.Equal(t, "test-uuid", result.UUID.Value)

	assert.True(t, result.Description.IsSet())
	assert.Equal(t, "Test description", result.Description.Value)

	assert.True(t, result.ResourceId.IsSet())
	assert.Equal(t, "test-resource-id", result.ResourceId.Value)

	assert.True(t, result.KeyRing.IsSet())
	assert.Equal(t, "test-keyring", result.KeyRing.Value)

	assert.True(t, result.KeyName.IsSet())
	assert.Equal(t, "test-key", result.KeyName.Value)

	assert.True(t, result.KeyProjectID.IsSet())
	assert.Equal(t, "test-project", result.KeyProjectID.Value)

	assert.True(t, result.KeyRingLocation.IsSet())
	assert.Equal(t, "us-central1", result.KeyRingLocation.Value)

	assert.True(t, result.CreatedAt.IsSet())
	assert.Equal(t, now, result.CreatedAt.Value)

	assert.True(t, result.UpdatedAt.IsSet())
	assert.Equal(t, now, result.UpdatedAt.Value)

	assert.True(t, result.State.IsSet())
	assert.Equal(t, oasgenserver.GcpKmsConfigV1State(models.LifeCycleStateREADY), result.State.Value)

	assert.True(t, result.StateDetails.IsSet())
	assert.Equal(t, "Kms config is ready for use", result.StateDetails.Value)

	assert.True(t, result.ServiceAccountEmail.IsSet())
	assert.Equal(t, "test-sa@test-project.iam.gserviceaccount.com", result.ServiceAccountEmail.Value)

	assert.Len(t, result.Jobs, 1)
	assert.True(t, result.Jobs[0].JobId.IsSet())
	assert.Equal(t, jobUUID, result.Jobs[0].JobId.Value)
}

func TestConvertKmsConfigToApiResponse_MinimalData(t *testing.T) {
	// Test data with minimal fields
	kmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID: "test-uuid",
		},
		ResourceID: "test-resource-id",
	}
	jobUUID := "test-job-uuid"
	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID: jobUUID,
		},
		Type:  models.JobTypeRotateKmsConfig,
		State: models.JobsStateNEW,
	}

	// Execute
	result := convertKmsConfigToApiResponse(kmsConfig, mockJob)

	// Assert
	assert.NotNil(t, result)

	// Verify required fields are set
	assert.True(t, result.UUID.IsSet())
	assert.Equal(t, "test-uuid", result.UUID.Value)

	assert.True(t, result.ResourceId.IsSet())
	assert.Equal(t, "test-resource-id", result.ResourceId.Value)

	// Verify optional fields are not set when empty
	assert.False(t, result.Description.IsSet())

	// Job should still be present
	assert.Len(t, result.Jobs, 1)
	assert.Equal(t, jobUUID, result.Jobs[0].JobId.Value)
}

func TestConvertKmsConfigToApiResponse_NilKmsConfig(t *testing.T) {
	// Execute with nil KMS config
	mockJob := &models.Job{
		BaseModel: models.BaseModel{
			UUID: "test-job-uuid",
		},
		Type:  models.JobTypeRotateKmsConfig,
		State: models.JobsStateNEW,
	}
	result := convertKmsConfigToApiResponse(nil, mockJob)

	// Assert
	assert.NotNil(t, result)

	// Should return empty response with just the job
	assert.False(t, result.UUID.IsSet())
	assert.Len(t, result.Jobs, 1)
	assert.Equal(t, "test-job-uuid", result.Jobs[0].JobId.Value)
}

func TestConvertKmsConfigToApiResponse_EmptyJobUUID(t *testing.T) {
	// Test data
	kmsConfig := &models.KmsConfig{
		BaseModel: models.BaseModel{
			UUID: "test-uuid",
		},
	}

	// Execute with nil job
	result := convertKmsConfigToApiResponse(kmsConfig, nil)

	// Assert
	assert.NotNil(t, result)
	assert.True(t, result.UUID.IsSet())

	// Jobs array should be empty when job is nil
	assert.Len(t, result.Jobs, 0)
}
