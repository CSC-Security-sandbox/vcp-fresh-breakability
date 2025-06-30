package kms_activities

import (
	"context"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestUpdateSDEKmsConfig_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{Name: "test-pool"}

	params := &common.UpdateKmsConfigParams{}
	sdeUpdateSDEKmsConfiguration = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
		return nil, nil
	}

	// Act
	err := activity.UpdateSDEKmsConfig(ctx, kms, params)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	sdeUpdateSDEKmsConfiguration = sde.UpdateSDEKmsConfiguration
}

func TestUpdateSDEKmsConfig_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{Name: "test-pool"}

	params := &common.UpdateKmsConfigParams{}
	sdeUpdateSDEKmsConfiguration = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.UpdateKmsConfigParams) (gcpgenserver.V1betaUpdateKmsConfigurationRes, error) {
		return nil, errors.New("some error")
	}

	// Act
	err := activity.UpdateSDEKmsConfig(ctx, kms, params)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
	sdeUpdateSDEKmsConfiguration = sde.UpdateSDEKmsConfiguration
}

func TestUpdateKmsConfig_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	description := "test description"
	params := &common.UpdateKmsConfigParams{
		KeyName:         "key2",
		KeyRing:         "keyring1",
		KeyRingLocation: "us-central1",
		Name:            "kms",
		Description:     &description,
	}
	updatedkms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "kms", KeyName: params.KeyName, KeyRing: params.KeyRing, KeyRingLocation: params.KeyRingLocation, Description: *params.Description}
	mockStorage.On("UpdateKmsConfig", ctx, updatedkms.UUID, mock.Anything).Return(nil)

	// Act
	err := activity.UpdateKmsConfig(ctx, kms, params)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateKmsConfig_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	description := "test description"
	params := &common.UpdateKmsConfigParams{
		KeyName:         "key2",
		KeyRing:         "keyring1",
		KeyRingLocation: "us-central1",
		Name:            "kms",
		Description:     &description,
	}
	updatedkms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "kms", KeyName: params.KeyName, KeyRing: params.KeyRing, KeyRingLocation: params.KeyRingLocation, Description: *params.Description}
	mockStorage.On("UpdateKmsConfig", ctx, updatedkms.UUID, mock.Anything).Return(errors.New("some error"))

	// Act
	err := activity.UpdateKmsConfig(ctx, kms, params)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateKmsConfigState_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	mockStorage.On("UpdateKmsConfigState", ctx, "uuid", models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(kms, nil)

	// Act
	err := activity.UpdateKmsConfigState(ctx, kms, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateKmsConfigState_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	mockStorage.On("UpdateKmsConfigState", ctx, "uuid", models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails).Return(nil, errors.New("some error"))

	// Act
	err := activity.UpdateKmsConfigState(ctx, kms, models.LifeCycleStateUpdating, models.LifeCycleStateUpdatingDetails)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}
