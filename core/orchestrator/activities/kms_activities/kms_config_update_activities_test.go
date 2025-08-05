package kms_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}

func TestUpdateKmsConfig_Error(t *testing.T) {
	tests := []struct {
		name        string
		kmsConfig   *datamodel.KmsConfig
		params      *common.UpdateKmsConfigParams
		expectedErr string
	}{
		{
			name: "DatabaseUpdateError",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			params: &common.UpdateKmsConfigParams{
				ResourceID: "updated-resource-id",
			},
			expectedErr: "database update failed",
		},
		{
			name: "DatabaseUpdateErrorWithKeyDetails",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			params: &common.UpdateKmsConfigParams{
				KeyName:         "new-key",
				KeyRing:         "new-keyring",
				KeyRingLocation: "us-west1",
				KeyProjectID:    "new-project",
				ResourceID:      "updated-resource-id",
			},
			expectedErr: "database update failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockStorage := database.NewMockStorage(t)
			ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

			// Mock database update to return error
			mockStorage.On("UpdateKmsConfig", ctx, tt.kmsConfig.UUID, mock.Anything).Return(errors.New(tt.expectedErr))

			// Act
			err := UpdateKmsConfig(mockStorage, ctx, tt.kmsConfig, tt.params)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
			mockStorage.AssertExpectations(t)
		})
	}
}

func TestUpdateKmsConfigState_Success(t *testing.T) {
	tests := []struct {
		name         string
		kmsConfig    *datamodel.KmsConfig
		state        string
		stateDetails string
	}{
		{
			name: "UpdateToCreatedState",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateCreated,
			stateDetails: models.LifeCycleStateCreatedDetails,
		},
		{
			name: "UpdateToUpdatingState",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateUpdating,
			stateDetails: models.LifeCycleStateUpdatingDetails,
		},
		{
			name: "UpdateToErrorState",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateError,
			stateDetails: "An error occurred during processing",
		},
		{
			name: "UpdateToReadyState",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateREADY,
			stateDetails: models.LifeCycleStateReadyDetails,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockStorage := database.NewMockStorage(t)
			activity := KmsConfigActivity{SE: mockStorage}
			ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

			expectedKms := &datamodel.KmsConfig{
				BaseModel: tt.kmsConfig.BaseModel,
				Name:      tt.kmsConfig.Name,
				State:     tt.state,
			}

			mockStorage.On("UpdateKmsConfigState", ctx, tt.kmsConfig.UUID, tt.state, tt.stateDetails).Return(expectedKms, nil)

			// Act
			err := activity.UpdateKmsConfigState(ctx, tt.kmsConfig, tt.state, tt.stateDetails)

			// Assert
			assert.NoError(t, err)
			mockStorage.AssertExpectations(t)
		})
	}
}

func TestUpdateKmsConfigState_Error(t *testing.T) {
	tests := []struct {
		name         string
		kmsConfig    *datamodel.KmsConfig
		state        string
		stateDetails string
		dbError      string
	}{
		{
			name: "DatabaseUpdateError",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateUpdating,
			stateDetails: models.LifeCycleStateUpdatingDetails,
			dbError:      "database connection failed",
		},
		{
			name: "InvalidStateTransition",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateError,
			stateDetails: "System error occurred",
			dbError:      "invalid state transition",
		},
		{
			name: "KmsConfigNotFound",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "non-existent-uuid"},
				Name:      "test-kms",
			},
			state:        models.LifeCycleStateCreated,
			stateDetails: models.LifeCycleStateCreatedDetails,
			dbError:      "kms config not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockStorage := database.NewMockStorage(t)
			activity := KmsConfigActivity{SE: mockStorage}
			ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

			mockStorage.On("UpdateKmsConfigState", ctx, tt.kmsConfig.UUID, tt.state, tt.stateDetails).Return(nil, errors.New(tt.dbError))

			// Act
			err := activity.UpdateKmsConfigState(ctx, tt.kmsConfig, tt.state, tt.stateDetails)

			// Assert
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tt.dbError)
			mockStorage.AssertExpectations(t)
		})
	}
}

func TestUpdateKmsConfig_Standalone_Success(t *testing.T) {
	// Test the standalone UpdateKmsConfig function directly
	tests := []struct {
		name           string
		kmsConfig      *datamodel.KmsConfig
		params         *common.UpdateKmsConfigParams
		expectedFields map[string]interface{}
	}{
		{
			name: "UpdateWithKeyNameSetsStateToCreated",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			params: &common.UpdateKmsConfigParams{
				KeyName:         "test-key",
				KeyRing:         "test-ring",
				KeyRingLocation: "us-central1",
				KeyProjectID:    "test-project",
			},
			expectedFields: map[string]interface{}{
				"key_name":          "test-key",
				"key_ring":          "test-ring",
				"key_ring_location": "us-central1",
				"key_project_id":    "test-project",
				"state":             models.LifeCycleStateCreated,
			},
		},
		{
			name: "UpdateWithoutKeyNameDoesNotSetState",
			kmsConfig: &datamodel.KmsConfig{
				BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
				Name:      "test-kms",
			},
			params: &common.UpdateKmsConfigParams{
				ResourceID:  "test-resource",
				Description: stringPtr("test description"),
			},
			expectedFields: map[string]interface{}{
				"resource_id": "test-resource",
				"description": "test description",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockStorage := database.NewMockStorage(t)
			ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

			mockStorage.On("UpdateKmsConfig", ctx, tt.kmsConfig.UUID, mock.MatchedBy(func(updateFields map[string]interface{}) bool {
				// Check if all expected fields are present and have correct values
				for key, expectedValue := range tt.expectedFields {
					actualValue, exists := updateFields[key]
					if !exists || actualValue != expectedValue {
						return false
					}
				}
				// Check if no unexpected fields are present
				return len(updateFields) == len(tt.expectedFields)
			})).Return(nil)

			// Act
			err := UpdateKmsConfig(mockStorage, ctx, tt.kmsConfig, tt.params)

			// Assert
			assert.NoError(t, err)
			mockStorage.AssertExpectations(t)
		})
	}
}

func TestUpdateKmsConfig_Standalone_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kmsConfig := &datamodel.KmsConfig{
		BaseModel: datamodel.BaseModel{UUID: "test-uuid"},
		Name:      "test-kms",
	}
	params := &common.UpdateKmsConfigParams{
		ResourceID: "test-resource",
	}

	mockStorage.On("UpdateKmsConfig", ctx, kmsConfig.UUID, mock.Anything).Return(errors.New("database error"))

	// Act
	err := UpdateKmsConfig(mockStorage, ctx, kmsConfig, params)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}
