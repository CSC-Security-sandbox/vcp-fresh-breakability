package kms_activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/sde"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteSDEKmsConfig_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{Name: "test-pool"}

	params := &common.DeleteKmsConfigParams{}
	sdeDeleteSDEKmsConfiguration = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		return nil, nil
	}
	defer func() { sdeDeleteSDEKmsConfiguration = sde.DeleteSDEKmsConfiguration }()

	// Act
	_, err := activity.DeleteSDEKmsConfig(ctx, kms, params)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteSDEKmsConfig_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{Name: "test-pool"}

	params := &common.DeleteKmsConfigParams{}
	sdeDeleteSDEKmsConfiguration = func(ctx context.Context, kmsConfig *datamodel.KmsConfig, params *common.DeleteKmsConfigParams) (gcpgenserver.V1betaDeleteKmsConfigurationRes, error) {
		return nil, errors.New("some error")
	}
	defer func() { sdeDeleteSDEKmsConfiguration = sde.DeleteSDEKmsConfiguration }()
	// Act
	_, err := activity.DeleteSDEKmsConfig(ctx, kms, params)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteKmsConfig_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	params := &common.DeleteKmsConfigParams{
		KmsConfigID: "uuid",
	}
	mockStorage.On("DeleteKmsConfig", ctx, "uuid").Return(kms, nil)

	// Act
	err := activity.DeleteKmsConfig(ctx, kms, params)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteKmsConfig_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	params := &common.DeleteKmsConfigParams{
		KmsConfigID: "uuid",
	}
	mockStorage.On("DeleteKmsConfig", ctx, "uuid").Return(nil, errors.New("some error"))

	// Act
	err := activity.DeleteKmsConfig(ctx, kms, params)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteKmsConfigState_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	mockStorage.On("UpdateKmsConfigState", ctx, "uuid", models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(kms, nil)

	// Act
	err := activity.UpdateKmsConfigState(ctx, kms, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteKmsConfigState_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{BaseModel: datamodel.BaseModel{UUID: "uuid"}, Name: "test-kms", KeyName: "key1"}

	mockStorage.On("UpdateKmsConfigState", ctx, "uuid", models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails).Return(nil, errors.New("some error"))

	// Act
	err := activity.UpdateKmsConfigState(ctx, kms, models.LifeCycleStateDeleting, models.LifeCycleStateDeletingDetails)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateServiceAccountAsDisabled_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{
		ServiceAccount: &datamodel.ServiceAccount{
			BaseModel: datamodel.BaseModel{
				UUID: "sa-uuid",
			},
		},
	}

	mockStorage.On("UpdateServiceAccountState", ctx, "sa-uuid", models.LifeCycleStateDisabled, models.LifeCycleStateDisabledDetails).Return(kms.ServiceAccount, nil)

	err := activity.DisableKmsServiceAccount(ctx, kms)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateServiceAccountAsDisabled_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	kms := &datamodel.KmsConfig{
		ServiceAccount: &datamodel.ServiceAccount{
			BaseModel: datamodel.BaseModel{
				UUID: "sa-uuid",
			},
		},
	}

	mockStorage.On("UpdateServiceAccountState", ctx, "sa-uuid", models.LifeCycleStateDisabled, models.LifeCycleStateDisabledDetails).Return(nil, errors.New("update error"))

	err := activity.DisableKmsServiceAccount(ctx, kms)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteSDEDeleteJob_NilJobUuid(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := &common.DeleteKmsConfigParams{
		Region:         "us-central1",
		AccountName:    "test-account",
		XCorrelationID: "corr-id",
	}

	err := activity.DescribeSDEDeleteJob(ctx, nil, params)
	assert.NoError(t, err)
}

func TestDeleteSDEDeleteJob_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := &common.DeleteKmsConfigParams{
		Region:         "us-central1",
		AccountName:    "test-account",
		XCorrelationID: "corr-id",
	}
	jobUuid := "job-uuid"
	called := false
	describeSDEJob = func(ctx context.Context, jobUuid, region, accountName, xCorrelationID string) error {
		called = true
		assert.Equal(t, "job-uuid", jobUuid)
		assert.Equal(t, "us-central1", region)
		assert.Equal(t, "test-account", accountName)
		assert.Equal(t, "corr-id", xCorrelationID)
		return nil
	}
	defer func() { describeSDEJob = sde.DescribeSDEJob }()

	err := activity.DescribeSDEDeleteJob(ctx, &jobUuid, params)
	assert.NoError(t, err)
	assert.True(t, called)
}

func TestDeleteSDEDeleteJob_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := KmsConfigActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := &common.DeleteKmsConfigParams{
		Region:         "us-central1",
		AccountName:    "test-account",
		XCorrelationID: "corr-id",
	}
	jobUuid := "job-uuid"
	describeSDEJob = func(ctx context.Context, jobUuid, region, accountName, xCorrelationID string) error {
		return errors.New("describe error")
	}
	defer func() { describeSDEJob = sde.DescribeSDEJob }()

	err := activity.DescribeSDEDeleteJob(ctx, &jobUuid, params)
	assert.Error(t, err)
}
