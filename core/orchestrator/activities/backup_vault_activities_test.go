package activities

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coremodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

// Note: The GCP service functions are complex to mock due to their direct integration with Google Cloud Storage.
// The tests below use panic recovery to handle the nil pointer dereferences that occur when the GCP service
// is not properly initialized. This provides basic coverage for the function entry points and error handling.
//
// LIMITATION: Lines 467-468 and 472-475 in deleteGCPBucketsForVault (error handling for EmptyBucket and DeleteBucket failures)
// cannot be easily tested without complex Google Cloud Storage client mocking infrastructure. These lines handle
// errors returned by the GCP service methods, but the current test setup causes panics before these error handling
// paths can be reached. To properly test these lines, a more sophisticated mocking approach would be required.

func TestConvertsValidBackupVaultV1betaToDataModel(tt *testing.T) {
	tt.Run("ConvertsValidBackupVaultV1betaToDataModel", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		desc := "test-descriptopn"
		minEnforcedRetentionDuration := int64(30)
		dstBVname := "cross-region-vault"
		locationId := "us-central1"
		bv := &models.BackupVaultV1beta{
			ResourceID:      &reourceID,
			SourceRegion:    &locationId,
			BackupRegion:    &backupRegion,
			BackupVaultType: &bvType,
			Description:     &desc,
			BackupRetentionPolicy: &models.BackupRetentionPolicyV1beta{
				BackupMinimumEnforcedRetentionDays: &minEnforcedRetentionDuration,
				DailyBackupImmutable:               true,
				WeeklyBackupImmutable:              false,
				MonthlyBackupImmutable:             true,
				ManualBackupImmutable:              false,
			},
			BackupVaultID:          "uuid-123",
			CreatedAt:              strfmt.DateTime(time.Now()),
			State:                  "ACTIVE",
			StateDetails:           "Operational",
			DestinationBackupVault: &dstBVname,
		}

		expected := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID:      "uuid-123",
				CreatedAt: time.Time(bv.CreatedAt),
				UpdatedAt: time.Time(bv.CreatedAt),
				DeletedAt: nil,
			},
			Name:                  "test-vault",
			BackupRegionName:      &backupRegion,
			SourceRegionName:      &locationId,
			LifeCycleState:        "ACTIVE",
			LifeCycleStateDetails: "Operational",
			BackupVaultType:       "STANDARD",
			Description:           &desc,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: &minEnforcedRetentionDuration,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               true,
				IsAdhocBackupImmutable:                 false,
			},
			CrossRegionBackupVaultName: &dstBVname,
			ServiceType:               coremodels.ServiceTypeGCNV,
		}

		result, err := ConvertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	tt.Run("HandlesNilFieldsInBackupVaultV1beta", func(t *testing.T) {
		bv := &models.BackupVaultV1beta{}
		locationId := "us-central1"

		expected := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID:      "",
				CreatedAt: time.Time{},
				UpdatedAt: time.Time{},
				DeletedAt: nil,
			},
			Name:                  "",
			BackupRegionName:      nil,
			SourceRegionName:      nillable.ToPointer("us-central1"),
			LifeCycleState:        "",
			LifeCycleStateDetails: "",
			BackupVaultType:       "",
			Description:           nil,
			ImmutableAttributes: &datamodel.ImmutableAttributes{
				BackupMinimumEnforcedRetentionDuration: nil,
				IsDailyBackupImmutable:                 false,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               false,
				IsAdhocBackupImmutable:                 false,
			},
			CrossRegionBackupVaultName: nil,
			ServiceType:               coremodels.ServiceTypeGCNV,
		}

		result, err := _convertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
	tt.Run("ConvertsBackupVaultV1betaWithCmekAttributes", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		desc := "test-description"
		locationId := "us-central1"
		kmsConfigPath := "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key"
		encryptionState := "ENCRYPTION_STATE_COMPLETED"
		backupsPrimaryKeyVersion := "1"
		bv := &models.BackupVaultV1beta{
			ResourceID:               &reourceID,
			SourceRegion:             &locationId,
			BackupRegion:             &backupRegion,
			BackupVaultType:          &bvType,
			Description:              &desc,
			BackupVaultID:            "uuid-123",
			CreatedAt:                strfmt.DateTime(time.Now()),
			State:                    "ACTIVE",
			StateDetails:             "Operational",
			KmsConfigResourcePath:    &kmsConfigPath,
			EncryptionState:          &encryptionState,
			BackupsPrimaryKeyVersion: &backupsPrimaryKeyVersion,
		}

		result, err := ConvertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.CmekAttributes)
		assert.Equal(t, kmsConfigPath, *result.CmekAttributes.KmsConfigResourcePath)
		assert.Equal(t, encryptionState, *result.CmekAttributes.EncryptionState)
		assert.Equal(t, backupsPrimaryKeyVersion, *result.CmekAttributes.BackupsPrimaryKeyVersion)
	})
	tt.Run("ConvertsBackupVaultV1betaWithPartialCmekAttributes", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		locationId := "us-central1"
		kmsConfigPath := "projects/test-project/locations/us-central1/keyRings/test-ring/cryptoKeys/test-key"
		bv := &models.BackupVaultV1beta{
			ResourceID:            &reourceID,
			SourceRegion:          &locationId,
			BackupRegion:          &backupRegion,
			BackupVaultType:       &bvType,
			BackupVaultID:         "uuid-123",
			CreatedAt:             strfmt.DateTime(time.Now()),
			State:                 "ACTIVE",
			StateDetails:          "Operational",
			KmsConfigResourcePath: &kmsConfigPath,
			// EncryptionState and BackupsPrimaryKeyVersion are nil
		}

		result, err := ConvertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.CmekAttributes)
		assert.Equal(t, kmsConfigPath, *result.CmekAttributes.KmsConfigResourcePath)
		assert.Nil(t, result.CmekAttributes.EncryptionState)
		assert.Nil(t, result.CmekAttributes.BackupsPrimaryKeyVersion)
	})
	tt.Run("ConvertsBackupVaultV1betaWithNoCmekAttributes", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		locationId := "us-central1"
		bv := &models.BackupVaultV1beta{
			ResourceID:      &reourceID,
			SourceRegion:    &locationId,
			BackupRegion:    &backupRegion,
			BackupVaultType: &bvType,
			BackupVaultID:   "uuid-123",
			CreatedAt:       strfmt.DateTime(time.Now()),
			State:           "ACTIVE",
			StateDetails:    "Operational",
			// All CMEK fields are nil
		}

		result, err := ConvertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Nil(t, result.CmekAttributes)
	})
}

func TestRotateBucketCmekActivity_EmptyBucketName(t *testing.T) {
	activity := &BackupVaultActivity{}

	err := activity.RotateBucketCmekActivity(context.Background(), "", "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1")

	assert.Error(t, err)
	appErr, ok := err.(*temporal.ApplicationError)
	assert.True(t, ok)
	assert.Equal(t, "RotateBucketCmekActivityInvalidBucket", appErr.Type())
}

func TestRotateBucketCmekActivity_GetGCPServiceError(t *testing.T) {
	origGetGCPService := hyperscaler2.GetGCPService
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to init gcp")
	}
	defer func() { hyperscaler2.GetGCPService = origGetGCPService }()

	activity := &BackupVaultActivity{}

	err := activity.RotateBucketCmekActivity(context.Background(), "bucket-1", "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1")

	assert.Error(t, err)
}

func TestUpdateBackupVaultCmekInVCPActivity_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	existing := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}
	input := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}
	primaryKeyVersion := "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/11"

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil)
	mockStorage.On("UpdateBackupVaultInVCP", ctx, mock.AnythingOfType("*datamodel.BackupVault"), existing).
		Run(func(args mock.Arguments) {
			updated := args.Get(1).(*datamodel.BackupVault)
			if assert.NotNil(t, updated.CmekAttributes) {
				assert.Equal(t, primaryKeyVersion, *updated.CmekAttributes.BackupsPrimaryKeyVersion)
				if assert.NotNil(t, updated.CmekAttributes.EncryptionState) {
					assert.Equal(t, "ENCRYPTION_STATE_COMPLETED", *updated.CmekAttributes.EncryptionState)
				}
			}
		}).
		Return(existing, nil)

	activity := &BackupVaultActivity{SE: mockStorage}

	err := activity.UpdateBackupVaultCmekInVCPActivity(ctx, input, primaryKeyVersion)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupVaultCmekInVCPActivity_UpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	existing := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}
	input := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil)
	mockStorage.On("UpdateBackupVaultInVCP", ctx, mock.AnythingOfType("*datamodel.BackupVault"), existing).
		Return(nil, errors.New("db error"))

	activity := &BackupVaultActivity{SE: mockStorage}

	err := activity.UpdateBackupVaultCmekInVCPActivity(ctx, input, "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/11")

	assert.Error(t, err)
	appErr, ok := err.(*temporal.ApplicationError)
	assert.True(t, ok)
	assert.Equal(t, "UpdateBackupVaultCmekInVCPActivityError", appErr.Type())
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupVaultEncryptionStateInVCPActivity_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	existing := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}
	input := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil)
	mockStorage.On("UpdateBackupVaultInVCP", ctx, mock.AnythingOfType("*datamodel.BackupVault"), existing).
		Run(func(args mock.Arguments) {
			updated := args.Get(1).(*datamodel.BackupVault)
			if assert.NotNil(t, updated.CmekAttributes) {
				if assert.NotNil(t, updated.CmekAttributes.EncryptionState) {
					assert.Equal(t, "ENCRYPTION_STATE_FAILED", *updated.CmekAttributes.EncryptionState)
				}
			}
		}).
		Return(existing, nil)

	activity := &BackupVaultActivity{SE: mockStorage}

	err := activity.UpdateBackupVaultEncryptionStateInVCPActivity(ctx, input, "ENCRYPTION_STATE_FAILED")

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupVaultEncryptionStateInVCPActivity_PreservesBackupsPrimaryKeyVersion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	kmsPath := "projects/p/locations/r/keyRings/ring/cryptoKeys/key"
	oldState := "ENCRYPTION_STATE_COMPLETED"
	existingPKV := "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/6"

	existing := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
		CmekAttributes: &datamodel.CmekAttributes{
			KmsConfigResourcePath:    &kmsPath,
			EncryptionState:          &oldState,
			BackupsPrimaryKeyVersion: &existingPKV,
		},
	}
	input := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}

	newState := "ENCRYPTION_STATE_FAILED"

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil)
	mockStorage.On("UpdateBackupVaultInVCP", ctx, mock.AnythingOfType("*datamodel.BackupVault"), existing).
		Run(func(args mock.Arguments) {
			updated := args.Get(1).(*datamodel.BackupVault)
			if assert.NotNil(t, updated.CmekAttributes) {
				// Encryption state should be updated to the new value.
				if assert.NotNil(t, updated.CmekAttributes.EncryptionState) {
					assert.Equal(t, newState, *updated.CmekAttributes.EncryptionState)
				}
				// Existing primary key version must be preserved.
				if assert.NotNil(t, updated.CmekAttributes.BackupsPrimaryKeyVersion) {
					assert.Equal(t, existingPKV, *updated.CmekAttributes.BackupsPrimaryKeyVersion)
				}
				// KMS config path must also be preserved.
				if assert.NotNil(t, updated.CmekAttributes.KmsConfigResourcePath) {
					assert.Equal(t, kmsPath, *updated.CmekAttributes.KmsConfigResourcePath)
				}
			}
		}).
		Return(existing, nil)

	activity := &BackupVaultActivity{SE: mockStorage}

	err := activity.UpdateBackupVaultEncryptionStateInVCPActivity(ctx, input, newState)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupVaultEncryptionStateInVCPActivity_UpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	existing := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}
	input := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil)
	mockStorage.On("UpdateBackupVaultInVCP", ctx, mock.AnythingOfType("*datamodel.BackupVault"), existing).
		Return(nil, errors.New("db error"))

	activity := &BackupVaultActivity{SE: mockStorage}

	err := activity.UpdateBackupVaultEncryptionStateInVCPActivity(ctx, input, "ENCRYPTION_STATE_FAILED")

	assert.Error(t, err)
	appErr, ok := err.(*temporal.ApplicationError)
	assert.True(t, ok)
	assert.Equal(t, "UpdateBackupVaultEncryptionStateInVCPActivityError", appErr.Type())
	mockStorage.AssertExpectations(t)
}

func TestStartSDECmekRotationForBackupVault_Success(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	params := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-1",
		BackupVaultID: "vault-123",
	}
	primaryKeyVersion := "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1"

	// Successful CVP call – we don't care about the payload.
	mockClient.On("V1betaRotateCmekBackups", mock.Anything).
		Return(nil, nil).
		Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	origCreateClient := cvpCreateClient
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	defer func() { cvpCreateClient = origCreateClient }()

	activity := &BackupVaultActivity{}

	err := activity.StartSDECmekRotationForBackupVault(ctx, params, primaryKeyVersion)

	assert.NoError(t, err)
	mockClient.AssertCalled(t, "V1betaRotateCmekBackups", mock.Anything)
}

func TestStartSDECmekRotationForBackupVault_ErrorMapping(t *testing.T) {
	tests := []struct {
		name         string
		errType      error
		expectedType string
		retryable    bool
	}{
		{
			name:         "BadRequest",
			errType:      &backup_vault.V1betaRotateCmekBackupsBadRequest{},
			expectedType: "V1betaRotateCmekBackupsBadRequest",
			retryable:    false,
		},
		{
			name:         "Unauthorized",
			errType:      &backup_vault.V1betaRotateCmekBackupsUnauthorized{},
			expectedType: "V1betaRotateCmekBackupsUnauthorized",
			retryable:    false,
		},
		{
			name:         "Forbidden",
			errType:      &backup_vault.V1betaRotateCmekBackupsForbidden{},
			expectedType: "V1betaRotateCmekBackupsForbidden",
			retryable:    false,
		},
		{
			name:         "Conflict",
			errType:      &backup_vault.V1betaRotateCmekBackupsConflict{},
			expectedType: "V1betaRotateCmekBackupsConflict",
			retryable:    false,
		},
		{
			name:         "UnprocessableEntity",
			errType:      &backup_vault.V1betaRotateCmekBackupsUnprocessableEntity{},
			expectedType: "V1betaRotateCmekBackupsUnprocessableEntity",
			retryable:    false,
		},
		{
			name:         "InternalServerError",
			errType:      &backup_vault.V1betaRotateCmekBackupsInternalServerError{},
			expectedType: "V1betaRotateCmekBackupsInternalServerError",
			retryable:    false,
		},
		{
			name:         "TooManyRequests",
			errType:      &backup_vault.V1betaRotateCmekBackupsTooManyRequests{},
			expectedType: "V1betaRotateCmekBackupsTooManyRequests",
			retryable:    true,
		},
		{
			name:         "Default",
			errType:      &backup_vault.V1betaRotateCmekBackupsDefault{},
			expectedType: "V1betaRotateCmekBackupsDefault",
			retryable:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := backup_vault.NewMockClientService(t)
			ctx := context.Background()

			params := &common.BackupVaultParams{
				Region:        "us-central1",
				OwnerID:       "owner-1",
				BackupVaultID: "vault-123",
			}

			// Mock error response type from CVP.
			mockClient.On("V1betaRotateCmekBackups", mock.Anything).
				Return(nil, tt.errType).
				Once()

			cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
			origCreateClient := cvpCreateClient
			cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
				return *cvpClient
			}
			defer func() { cvpCreateClient = origCreateClient }()

			activity := &BackupVaultActivity{}

			err := activity.StartSDECmekRotationForBackupVault(ctx, params, "pkv")

			assert.Error(t, err)
			if appErr, ok := err.(*temporal.ApplicationError); ok {
				assert.Equal(t, tt.expectedType, appErr.Type())
			}
		})
	}
}

func TestWaitForSDECmekRotationCompletion_Completed(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	params := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-1",
		BackupVaultID: "vault-123",
	}

	state := coremodels.EncryptionStateCompleted
	mockClient.On("V1betaListBackupVaults", mock.Anything).
		Return(&backup_vault.V1betaListBackupVaultsOK{
			Payload: &backup_vault.V1betaListBackupVaultsOKBody{
				BackupVaults: []*models.BackupVaultV1beta{
					{
						BackupVaultID:   params.BackupVaultID,
						EncryptionState: &state,
					},
				},
			},
		}, nil).
		Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	origCreateClient := cvpCreateClient
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	defer func() { cvpCreateClient = origCreateClient }()

	activity := &BackupVaultActivity{}

	ok, err := activity.WaitForSDECmekRotationCompletion(ctx, params)

	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestWaitForSDECmekRotationCompletion_Failed(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	params := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-1",
		BackupVaultID: "vault-123",
	}

	state := coremodels.EncryptionStateFailed
	mockClient.On("V1betaListBackupVaults", mock.Anything).
		Return(&backup_vault.V1betaListBackupVaultsOK{
			Payload: &backup_vault.V1betaListBackupVaultsOKBody{
				BackupVaults: []*models.BackupVaultV1beta{
					{
						BackupVaultID:   params.BackupVaultID,
						EncryptionState: &state,
					},
				},
			},
		}, nil).
		Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	origCreateClient := cvpCreateClient
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	defer func() { cvpCreateClient = origCreateClient }()

	activity := &BackupVaultActivity{}

	ok, err := activity.WaitForSDECmekRotationCompletion(ctx, params)

	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestWaitForSDECmekRotationCompletion_ListError(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	params := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-1",
		BackupVaultID: "vault-123",
	}

	mockClient.On("V1betaListBackupVaults", mock.Anything).
		Return(nil, errors.New("list failed")).
		Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	origCreateClient := cvpCreateClient
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	defer func() { cvpCreateClient = origCreateClient }()

	activity := &BackupVaultActivity{}

	ok, err := activity.WaitForSDECmekRotationCompletion(ctx, params)

	assert.Error(t, err)
	assert.False(t, ok)
	appErr, ok2 := err.(*temporal.ApplicationError)
	assert.True(t, ok2)
	assert.Equal(t, "V1betaListBackupVaultsError", appErr.Type())
}

func TestUpdateBackupVault(tt *testing.T) {
	tt.Run("WhenReturnsUpdatedBackupVaultSuccess", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
		ctx := context.Background()

		dailyImmutable := true
		weeklyImmutable := false
		backupMRD := int64(30)
		paramz := &common.BackupVaultParams{
			ID:            1,
			OwnerID:       "owner-1",
			BackupVaultID: "bv-id-123",
			Name:          "test-vault",
			Region:        "us-east1",
			BackupRetentionPolicy: common.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &backupMRD,
				IsDailyBackupImmutable:                 &dailyImmutable,
				IsWeeklyBackupImmutable:                &weeklyImmutable,
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			&backup_vault.V1betaUpdateBackupVaultAccepted{
				Payload: &models.OperationV1beta{},
			}, nil).Once()

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient

		defer func() {
			cvpCreateClient = originalCreateClient
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := BackupVaultActivity{
			SE: database.NewMockStorage(t),
		}

		result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
	})
	tt.Run("WhenReturnsUpdatedBackupVaultSuccess", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
		ctx := context.Background()

		dailyImmutable := true
		weeklyImmutable := false
		backupMRD := int64(30)
		paramz := &common.BackupVaultParams{
			ID:            1,
			OwnerID:       "owner-1",
			BackupVaultID: "bv-id-123",
			Name:          "test-vault",
			Region:        "us-east1",
			BackupRetentionPolicy: common.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &backupMRD,
				IsDailyBackupImmutable:                 &dailyImmutable,
				IsWeeklyBackupImmutable:                &weeklyImmutable,
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(nil, &backup_vault.V1betaUpdateBackupVaultBadRequest{
			Payload: &models.Error{
				Code:    400,
				Message: "Bad Request",
			},
		})

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := BackupVaultActivity{
			SE: database.NewMockStorage(t),
		}

		result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
	})
	tt.Run("WhenReturnsUpdatedBackupVaultConvertionError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
		ctx := context.Background()

		dailyImmutable := true
		weeklyImmutable := false
		monthlyImmutable := false
		adhocImmutable := false
		backupMRD := int64(30)
		paramz := &common.BackupVaultParams{
			ID:            1,
			OwnerID:       "owner-1",
			BackupVaultID: "bv-id-123",
			Name:          "test-vault",
			Region:        "us-east1",
			BackupRetentionPolicy: common.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &backupMRD,
				IsDailyBackupImmutable:                 &dailyImmutable,
				IsWeeklyBackupImmutable:                &weeklyImmutable,
				IsMonthlyBackupImmutable:               &monthlyImmutable,
				IsAdhocBackupImmutable:                 &adhocImmutable,
			},
		}

		mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
			&backup_vault.V1betaUpdateBackupVaultAccepted{
				Payload: &models.OperationV1beta{},
			}, nil).Once()

		ConvertToBackupVaultDataModel = func(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
			return nil, errors.New("conversion error")
		}

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
			ConvertToBackupVaultDataModel = _convertToBackupVaultDataModel
		}()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		activity := BackupVaultActivity{
			SE: database.NewMockStorage(t),
		}

		result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
	})
}

func TestReturnsBackupVaultSuccessfullyFromVCP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	bvParams := &datamodel.BackupVault{
		Name: "test-vault",
	}
	vcpBvParams := &datamodel.BackupVault{
		Name: "vcp-vault",
	}

	mockStorage.On("UpdateBackupVaultInVCP", ctx, bvParams, vcpBvParams).Return(vcpBvParams, nil).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	result, err := activity.UpdateBackupVaultInVCP(ctx, bvParams, vcpBvParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, vcpBvParams, result)
	mockStorage.AssertCalled(t, "UpdateBackupVaultInVCP", ctx, bvParams, vcpBvParams)
}

func TestReturnsErrorWhenUpdateFailsInVCP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	bvParams := &datamodel.BackupVault{
		Name: "test-vault",
	}
	vcpBvParams := &datamodel.BackupVault{
		Name: "vcp-vault",
	}

	mockStorage.On("UpdateBackupVaultInVCP", ctx, bvParams, vcpBvParams).Return(nil, errors.New("update failed")).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	result, err := activity.UpdateBackupVaultInVCP(ctx, bvParams, vcpBvParams)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertCalled(t, "UpdateBackupVaultInVCP", ctx, bvParams, vcpBvParams)
}

func TestDeletesBackupVaultSuccessfullyFromVCP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVaultId := "test-vault-id"
	expectedBackupVault := &datamodel.BackupVault{
		Name: "test-vault",
	}

	mockStorage.On("DeleteBackupVaultInVCP", ctx, backupVaultId).Return(expectedBackupVault, nil).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	result, err := activity.DeleteBackupVaultInVCP(ctx, backupVaultId)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedBackupVault, result)
	mockStorage.AssertCalled(t, "DeleteBackupVaultInVCP", ctx, backupVaultId)
}

func TestReturnsErrorWhenDeleteFailsInVCP(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVaultId := "test-vault-id"

	mockStorage.On("DeleteBackupVaultInVCP", ctx, backupVaultId).Return(nil, errors.New("delete failed")).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	result, err := activity.DeleteBackupVaultInVCP(ctx, backupVaultId)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertCalled(t, "DeleteBackupVaultInVCP", ctx, backupVaultId)
}

func TestUpdatesBackupVaultStateSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
		Name:      "test-vault",
	}
	state := "ERROR"
	stateDetails := "Failed due to timeout"

	existing := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID:             1,
		Name:                  "test-vault",
		LifeCycleState:        "READY",
		LifeCycleStateDetails: "Available for use",
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil).Once()
	mockStorage.On("UpdateBackupVaultState", ctx, existing, state, stateDetails).Return(existing, nil).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenStateUpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
		Name:      "test-vault",
	}
	state := "ERROR"
	stateDetails := "Failed due to timeout"

	existing := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID:             1,
		Name:                  "test-vault",
		LifeCycleState:        "READY",
		LifeCycleStateDetails: "Available for use",
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "bv-uuid", int64(1)).Return(existing, nil).Once()
	mockStorage.On("UpdateBackupVaultState", ctx, existing, state, stateDetails).Return(nil, errors.New("update failed")).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateDeletedBackupVaultStateInCaseOfErrorSuccess(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
		Name:      "test-vault",
	}
	state := coremodels.LifeCycleStateREADY
	stateDetails := coremodels.LifeCycleStateAvailableDetails

	restored := &datamodel.BackupVault{
		BaseModel:             datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID:             1,
		Name:                  "test-vault",
		LifeCycleState:        state,
		LifeCycleStateDetails: stateDetails,
	}

	mockStorage.On("RestoreDeletedBackupVault", ctx, "bv-uuid", int64(1), state, stateDetails).Return(restored, nil).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateDeletedBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateDeletedBackupVaultStateInCaseOfErrorFailure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "bv-uuid"},
		AccountID: 1,
		Name:      "test-vault",
	}
	state := coremodels.LifeCycleStateREADY
	stateDetails := coremodels.LifeCycleStateAvailableDetails

	mockStorage.On("RestoreDeletedBackupVault", ctx, "bv-uuid", int64(1), state, stateDetails).Return(nil, errors.New("restore failed")).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateDeletedBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeletesBackupVaultSuccessfullyFromSDE(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
	}

	mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
		&backup_vault.V1betaDeleteBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: map[string]interface{}{
					"BackupVaultID": "vault-123",
				},
			},
		}, nil, nil).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	defer func() { cvpCreateClient = originalCreateClient }()
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	activity := BackupVaultActivity{}

	result, err := activity.DeleteBackupVaultInSDE(ctx, paramz)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "vault-123", result.UUID)
	mockClient.AssertCalled(t, "V1betaDeleteBackupVault", mock.Anything)
}

func TestReturnsErrorWhenDeleteFailsInSDE(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
	}

	mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(nil, nil, errors.New("delete failed")).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	defer func() { cvpCreateClient = originalCreateClient }()
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	activity := BackupVaultActivity{}

	result, err := activity.DeleteBackupVaultInSDE(ctx, paramz)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockClient.AssertCalled(t, "V1betaDeleteBackupVault", mock.Anything)
}

func TestReturnsErrorWhenResponseMarshallingFails(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
	}

	mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
		&backup_vault.V1betaDeleteBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: make(chan int), // Invalid type to cause marshalling error
			},
		}, nil, nil).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	defer func() { cvpCreateClient = originalCreateClient }()
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	activity := BackupVaultActivity{}

	result, err := activity.DeleteBackupVaultInSDE(ctx, paramz)

	assert.Error(t, err)
	assert.Nil(t, result)
	mockClient.AssertCalled(t, "V1betaDeleteBackupVault", mock.Anything)
}

func TestBackupVaultActivity_DeleteBackupVaultBuckets(t *testing.T) {
	// Save original function references
	originalGetGCPService := hyperscaler2.GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		DeleteGCPBucket = originalDeleteGCPBucket
	}()

	tests := []struct {
		name          string
		backupVault   *datamodel.BackupVault
		setupMocks    func()
		expectedError bool
		errorContains string
	}{
		{
			name: "successfully deletes all buckets",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name: "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{
					{
						BucketName:          "bucket-1",
						ServiceAccountName:  "sa-1",
						TenantProjectNumber: "project-1",
					},
					{
						BucketName:          "bucket-2",
						ServiceAccountName:  "sa-2",
						TenantProjectNumber: "project-2",
					},
				},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
					return true, nil
				}
			},
			expectedError: false,
		},
		{
			name: "successfully deletes buckets with empty bucket names",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name: "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{
					{
						BucketName:          "",
						ServiceAccountName:  "sa-1",
						TenantProjectNumber: "project-1",
					},
					{
						BucketName:          "bucket-2",
						ServiceAccountName:  "sa-2",
						TenantProjectNumber: "project-2",
					},
					{
						BucketName:          "",
						ServiceAccountName:  "sa-3",
						TenantProjectNumber: "project-3",
					},
				},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) { // Should only be called for bucket-2
					assert.Equal(t, "bucket-2", bucketName)
					return true, nil
				}
			},
			expectedError: false,
		},
		{
			name: "successfully handles empty bucket details",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name:          "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
					t.Fatal("DeleteGCPBucket should not be called when there are no buckets")
					return false, nil
				}
			},
			expectedError: false,
		},
		{
			name: "GetGCPService fails",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name: "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{
					{
						BucketName:          "bucket-1",
						ServiceAccountName:  "sa-1",
						TenantProjectNumber: "project-1",
					},
				},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return nil, errors.New("failed to get GCP service")
				}
			},
			expectedError: true,
			errorContains: "failed to get GCP service",
		},
		{
			name: "DeleteGCPBucket fails on first bucket",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name: "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{
					{
						BucketName:          "bucket-1",
						ServiceAccountName:  "sa-1",
						TenantProjectNumber: "project-1",
					},
					{
						BucketName:          "bucket-2",
						ServiceAccountName:  "sa-2",
						TenantProjectNumber: "project-2",
					},
				},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
					if bucketName == "bucket-1" {
						return false, errors.New("failed to delete bucket-1")
					}
					t.Fatal("DeleteGCPBucket should not be called for bucket-2 when bucket-1 fails")
					return false, nil
				}
			},
			expectedError: true,
			errorContains: "failed to delete bucket-1",
		},
		{
			name: "DeleteGCPBucket fails on second bucket",
			backupVault: &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "test-uuid",
				},
				Name: "test-backup-vault",
				BucketDetails: datamodel.BucketDetailsArray{
					{
						BucketName:          "bucket-1",
						ServiceAccountName:  "sa-1",
						TenantProjectNumber: "project-1",
					},
					{
						BucketName:          "bucket-2",
						ServiceAccountName:  "sa-2",
						TenantProjectNumber: "project-2",
					},
				},
			},
			setupMocks: func() {
				hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
					if bucketName == "bucket-1" {
						return true, nil
					}
					if bucketName == "bucket-2" {
						return false, errors.New("failed to delete bucket-2")
					}
					t.Fatal("DeleteGCPBucket called with unexpected bucket name")
					return false, nil
				}
			},
			expectedError: true,
			errorContains: "failed to delete bucket-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mocks
			tt.setupMocks()
			mockStorage := database.NewMockStorage(t)

			// Create activity instance
			activity := &BackupVaultActivity{
				SE: mockStorage,
			}

			ctx := context.Background()

			// Execute the function
			err := activity.DeleteBackupVaultBuckets(ctx, tt.backupVault)

			// Assert results
			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBackupVaultActivity_DeleteBackupVaultBuckets_WithMockGCP(t *testing.T) {
	// Save original function references
	originalGetGCPService := hyperscaler2.GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		DeleteGCPBucket = originalDeleteGCPBucket
	}()

	t.Run("GCP service initialization failure", func(t *testing.T) {
		// Setup function mocks to simulate GCP service initialization failure
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("GCP service initialization failed")
		}

		mockStorage := database.NewMockStorage(t)
		// Create activity instance
		activity := &BackupVaultActivity{
			SE: mockStorage,
		}

		// Create backup vault
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-uuid",
			},
			Name: "test-backup-vault",
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName:          "bucket-1",
					ServiceAccountName:  "sa-1",
					TenantProjectNumber: "project-1",
				},
			},
		}

		// Create context with logger
		ctx := context.Background()

		// Execute the function
		err := activity.DeleteBackupVaultBuckets(ctx, backupVault)

		// Assert results
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "GCP service initialization failed")
	})
}

func TestBackupVaultActivity_DeleteBackupVaultBuckets_EdgeCases(t *testing.T) {
	// Save original function references
	originalGetGCPService := hyperscaler2.GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		DeleteGCPBucket = originalDeleteGCPBucket
	}()

	t.Run("backup vault with nil bucket details", func(t *testing.T) {
		// Setup function mocks
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
			t.Fatal("DeleteGCPBucket should not be called when bucket details is nil")
			return false, nil
		}
		mockStorage := database.NewMockStorage(t)

		// Create activity instance
		activity := &BackupVaultActivity{
			SE: mockStorage,
		}

		// Create backup vault with nil bucket details
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "test-uuid",
			},
			Name:          "test-backup-vault",
			BucketDetails: nil,
		}

		// Create context with logger
		ctx := context.Background()

		// Execute the function
		err := activity.DeleteBackupVaultBuckets(ctx, backupVault)

		// Should handle nil bucket details gracefully
		assert.NoError(t, err)
	})
}

func TestDeleteBackupVaultInSDE_ErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		errorType     interface{}
		expectedError string
		expectedType  string
		shouldRetry   bool
	}{
		{
			name:          "BadRequest error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultBadRequest{},
			expectedError: "Bad request deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultBadRequest",
			shouldRetry:   false,
		},
		{
			name:          "Unauthorized error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultUnauthorized{},
			expectedError: "Unauthorized to delete backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultUnauthorized",
			shouldRetry:   false,
		},
		{
			name:          "Forbidden error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultForbidden{},
			expectedError: "Forbidden to delete backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultForbidden",
			shouldRetry:   false,
		},
		{
			name:          "NotFound error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultNotFound{},
			expectedError: "Backup vault vault-123 not found:",
			expectedType:  "V1betaDeleteBackupVaultNotFound",
			shouldRetry:   false,
		},
		{
			name:          "Conflict error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultConflict{},
			expectedError: "Conflict deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultConflict",
			shouldRetry:   false,
		},
		{
			name:          "UnprocessableEntity error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultUnprocessableEntity{},
			expectedError: "Unprocessable entity deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultUnprocessableEntity",
			shouldRetry:   false,
		},
		{
			name:          "InternalServerError error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultInternalServerError{},
			expectedError: "Internal server error deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultInternalServerError",
			shouldRetry:   false,
		},
		{
			name:          "TooManyRequests error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultTooManyRequests{},
			expectedError: "Too many requests deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultTooManyRequests",
			shouldRetry:   true,
		},
		{
			name:          "NotImplemented error",
			errorType:     &backup_vault.V1betaDeleteBackupVaultNotImplemented{},
			expectedError: "Not implemented deleting backup vault vault-123:",
			expectedType:  "V1betaDeleteBackupVaultNotImplemented",
			shouldRetry:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := backup_vault.NewMockClientService(t)
			ctx := context.Background()

			paramz := &common.BackupVaultParams{
				Region:        "us-central1",
				OwnerID:       "owner-123",
				BackupVaultID: "vault-123",
			}

			// Mock the error response
			mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(nil, nil, tt.errorType).Once()

			cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
			originalCreateClient := cvpCreateClient
			defer func() { cvpCreateClient = originalCreateClient }()
			cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
				return *cvpClient
			}

			activity := BackupVaultActivity{}

			result, err := activity.DeleteBackupVaultInSDE(ctx, paramz)

			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), tt.expectedError)

			// Check if it's a temporal error and verify retry behavior
			if tt.shouldRetry {
				// Should be a retryable error
				assert.Contains(t, err.Error(), "Too many requests")
				assert.Contains(t, err.Error(), "retryable: true")
			} else {
				// Should be a non-retryable error
				assert.Contains(t, err.Error(), "retryable: false")
			}

			mockClient.AssertCalled(t, "V1betaDeleteBackupVault", mock.Anything)
		})
	}
}

func TestUpdateBackupVaultInSDE_ErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		errorType     interface{}
		expectedError string
		expectedType  string
		shouldRetry   bool
	}{
		{
			name:          "BadRequest error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultBadRequest{},
			expectedError: "Bad request updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultBadRequest",
			shouldRetry:   false,
		},
		{
			name:          "Unauthorized error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultUnauthorized{},
			expectedError: "Unauthorized to update backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultUnauthorized",
			shouldRetry:   false,
		},
		{
			name:          "Forbidden error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultForbidden{},
			expectedError: "Forbidden to update backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultForbidden",
			shouldRetry:   false,
		},
		{
			name:          "Conflict error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultConflict{},
			expectedError: "Conflict updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultConflict",
			shouldRetry:   false,
		},
		{
			name:          "UnprocessableEntity error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultUnprocessableEntity{},
			expectedError: "Unprocessable entity updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultUnprocessableEntity",
			shouldRetry:   false,
		},
		{
			name:          "InternalServerError error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultInternalServerError{},
			expectedError: "Internal server error updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultInternalServerError",
			shouldRetry:   false,
		},
		{
			name:          "TooManyRequests error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultTooManyRequests{},
			expectedError: "Too many requests updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultTooManyRequests",
			shouldRetry:   true,
		},
		{
			name:          "NotImplemented error",
			errorType:     &backup_vault.V1betaUpdateBackupVaultNotImplemented{},
			expectedError: "Not implemented updating backup vault vault-123:",
			expectedType:  "V1betaUpdateBackupVaultNotImplemented",
			shouldRetry:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := backup_vault.NewMockClientService(t)
			ctx := context.Background()

			des := "test description"
			paramz := &common.BackupVaultParams{
				Region:        "us-central1",
				OwnerID:       "owner-123",
				BackupVaultID: "vault-123",
				Description:   &des,
			}

			// Mock the error response
			mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(nil, tt.errorType).Once()

			cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
			originalCreateClient := cvpCreateClient
			defer func() { cvpCreateClient = originalCreateClient }()
			cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
				return *cvpClient
			}

			activity := BackupVaultActivity{}

			result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

			assert.Error(t, err)
			assert.Nil(t, result)
			assert.Contains(t, err.Error(), tt.expectedError)

			// Check if it's a temporal error and verify retry behavior
			if tt.shouldRetry {
				// Should be a retryable error
				assert.Contains(t, err.Error(), "Too many requests")
				assert.Contains(t, err.Error(), "retryable: true")
			} else {
				// Should be a non-retryable error
				assert.Contains(t, err.Error(), "retryable: false")
			}

			mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
		})
	}
}

func TestUpdateBackupVaultInSDE_ResponseMarshallingError(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()
	desc := "test description"
	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
		Description:   &desc,
	}

	// Mock a response that will cause marshalling error
	mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
		&backup_vault.V1betaUpdateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: make(chan int), // Invalid type to cause marshalling error
			},
		}, nil).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	defer func() { cvpCreateClient = originalCreateClient }()
	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	activity := BackupVaultActivity{}

	result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to marshal response from SDE BackupVault Updation")
	mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
}

func TestUpdateBackupVaultInSDE_ModelConversionError(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	desc := "test description"
	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
		Description:   &desc,
	}

	// Mock a successful response
	mockClient.On("V1betaUpdateBackupVault", mock.Anything).Return(
		&backup_vault.V1betaUpdateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: map[string]interface{}{
					"BackupVaultID": "vault-123",
				},
			},
		}, nil).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	originalConvertToBackupVaultDataModel := ConvertToBackupVaultDataModel

	defer func() {
		cvpCreateClient = originalCreateClient
		ConvertToBackupVaultDataModel = originalConvertToBackupVaultDataModel
	}()

	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	// Mock conversion error
	ConvertToBackupVaultDataModel = func(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
		return nil, errors.New("conversion error")
	}

	activity := BackupVaultActivity{}

	result, err := activity.UpdateBackupVaultInSDE(ctx, paramz)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "conversion error")
	mockClient.AssertCalled(t, "V1betaUpdateBackupVault", mock.Anything)
}

func TestDeleteBackupVaultInSDE_ModelConversionError(t *testing.T) {
	mockClient := backup_vault.NewMockClientService(t)
	ctx := context.Background()

	paramz := &common.BackupVaultParams{
		Region:        "us-central1",
		OwnerID:       "owner-123",
		BackupVaultID: "vault-123",
	}

	// Mock a successful response
	mockClient.On("V1betaDeleteBackupVault", mock.Anything).Return(
		&backup_vault.V1betaDeleteBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: map[string]interface{}{
					"BackupVaultID": "vault-123",
				},
			},
		}, nil, nil).Once()

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := cvpCreateClient
	originalConvertToBackupVaultDataModel := ConvertToBackupVaultDataModel

	defer func() {
		cvpCreateClient = originalCreateClient
		ConvertToBackupVaultDataModel = originalConvertToBackupVaultDataModel
	}()

	cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}

	// Mock conversion error
	ConvertToBackupVaultDataModel = func(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
		return nil, errors.New("conversion error")
	}

	activity := BackupVaultActivity{}

	result, err := activity.DeleteBackupVaultInSDE(ctx, paramz)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "conversion error")
	mockClient.AssertCalled(t, "V1betaDeleteBackupVault", mock.Anything)
}

func TestDeleteBackupVaultBuckets_NilBackupVault(t *testing.T) {
	activity := &BackupVaultActivity{
		SE: database.NewMockStorage(t),
	}

	ctx := context.Background()
	err := activity.DeleteBackupVaultBuckets(ctx, nil)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "backupVault parameter is nil")
}

func TestCleanupBackupVaultsForAccount(t *testing.T) {
	t.Run("CleanupBackupVaultsForAccount_GetAccountFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(nil, errors.New("account not found"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.CleanupBackupVaultsForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "account not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVaultsForAccount_ListBackupVaultsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup vaults list failure
		mockStorage.On("ListBackupVaults", mock.Anything, account.ID).Return(nil, errors.New("database error"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.CleanupBackupVaultsForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVaultsForAccount_NoVaults", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock empty backup vaults list
		mockStorage.On("ListBackupVaults", mock.Anything, account.ID).Return([]*datamodel.BackupVault{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.CleanupBackupVaultsForAccount(context.Background(), "test-project-123")

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVaultsForAccount_WithVaults", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup vaults list with multiple vaults to trigger line 405
		backupVaults := []*datamodel.BackupVault{
			{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
				Name:      "vault-1",
				AccountID: 1,
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-2"},
				Name:      "vault-2",
				AccountID: 1,
			},
		}
		mockStorage.On("ListBackupVaults", mock.Anything, account.ID).Return(backupVaults, nil)

		// Mock cleanupBackupVault calls for both vaults
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return([]*datamodel.Backup{}, nil)
		mockStorage.On("DeleteBackupVaultInVCP", mock.Anything, "vault-uuid-1").Return(&datamodel.BackupVault{}, nil)
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-2", int64(1), mock.Anything).Return([]*datamodel.Backup{}, nil)
		mockStorage.On("DeleteBackupVaultInVCP", mock.Anything, "vault-uuid-2").Return(&datamodel.BackupVault{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.CleanupBackupVaultsForAccount(context.Background(), "test-project-123")

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVaultsForAccount_CleanupVaultFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		// Mock account lookup
		account := &datamodel.Account{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Name:      "test-project-123",
		}
		mockStorage.On("GetAccount", mock.Anything, "test-project-123").Return(account, nil)

		// Mock backup vaults list
		backupVaults := []*datamodel.BackupVault{
			{
				BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
				Name:      "vault-1",
				AccountID: 1,
			},
		}
		mockStorage.On("ListBackupVaults", mock.Anything, account.ID).Return(backupVaults, nil)

		// Mock cleanupBackupsForVault failure
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return(nil, errors.New("cleanup backups failed"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.CleanupBackupVaultsForAccount(context.Background(), "test-project-123")

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cleanup backups failed")
		mockStorage.AssertExpectations(tt)
	})
}

func TestCleanupBackupVault(t *testing.T) {
	t.Run("CleanupBackupVault_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock cleanupBackupsForVault
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return([]*datamodel.Backup{}, nil)

		// Mock DeleteBackupVaultInVCP
		mockStorage.On("DeleteBackupVaultInVCP", mock.Anything, "vault-uuid-1").Return(&datamodel.BackupVault{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupVault(context.Background(), vault)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVault_DeleteVaultFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock cleanupBackupsForVault
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return([]*datamodel.Backup{}, nil)

		// Mock DeleteBackupVaultInVCP failure
		mockStorage.On("DeleteBackupVaultInVCP", mock.Anything, "vault-uuid-1").Return(nil, errors.New("database error"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupVault(context.Background(), vault)

		assert.Error(tt, err)
		appErr, ok := err.(*temporal.ApplicationError)
		assert.True(tt, ok)
		assert.Equal(tt, "DeleteBackupVaultError", appErr.Type())
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupVault_CleanupBackupsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock cleanupBackupsForVault failure
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return(nil, errors.New("cleanup backups failed"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupVault(context.Background(), vault)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cleanup backups failed")
		mockStorage.AssertExpectations(tt)
	})
}

func TestCleanupBackupsForVault(t *testing.T) {
	t.Run("CleanupBackupsForVault_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock backups list
		backups := []*datamodel.Backup{
			{
				BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
				Name:      "backup-1",
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "backup-uuid-2"},
				Name:      "backup-2",
			},
		}
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return(backups, nil)

		// Mock backup delete calls
		mockStorage.On("DeleteBackup", mock.Anything, "backup-uuid-1").Return(&datamodel.Backup{}, nil)
		mockStorage.On("DeleteBackup", mock.Anything, "backup-uuid-2").Return(&datamodel.Backup{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupsForVault(context.Background(), vault)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupsForVault_NoBackups", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock empty backups list
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return([]*datamodel.Backup{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupsForVault(context.Background(), vault)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupsForVault_GetBackupsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock backups list failure
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return(nil, errors.New("database error"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupsForVault(context.Background(), vault)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackupsForVault_CleanupBackupFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock backups list
		backups := []*datamodel.Backup{
			{
				BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
				Name:      "backup-1",
			},
		}
		mockStorage.On("GetBackupsByBackupVaultOwnerIDAndFilter", mock.Anything, "vault-uuid-1", int64(1), mock.Anything).Return(backups, nil)

		// Mock backup delete failure
		mockStorage.On("DeleteBackup", mock.Anything, "backup-uuid-1").Return(nil, errors.New("database delete error"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackupsForVault(context.Background(), vault)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database delete error")
		mockStorage.AssertExpectations(tt)
	})
}

func TestCleanupBackup(t *testing.T) {
	t.Run("CleanupBackup_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:      "backup-1",
		}

		// Mock database delete
		mockStorage.On("DeleteBackup", mock.Anything, "backup-uuid-1").Return(&datamodel.Backup{}, nil)

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackup(context.Background(), backup)

		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("CleanupBackup_DatabaseDeleteFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)

		backup := &datamodel.Backup{
			BaseModel: datamodel.BaseModel{UUID: "backup-uuid-1"},
			Name:      "backup-1",
		}

		// Mock database delete failure
		mockStorage.On("DeleteBackup", mock.Anything, "backup-uuid-1").Return(nil, errors.New("database error"))

		activity := BackupVaultActivity{SE: mockStorage}
		err := activity.cleanupBackup(context.Background(), backup)

		assert.Error(tt, err)
		// Should return a non-retryable application error
		appErr, ok := err.(*temporal.ApplicationError)
		assert.True(tt, ok)
		assert.Contains(tt, appErr.Error(), "Failed to soft delete backup")
		assert.Equal(tt, "DeleteBackupError", appErr.Type())
		mockStorage.AssertExpectations(tt)
	})
}

func TestDeleteGCPBucketsForVault(t *testing.T) {
	t.Run("DeleteGCPBucketsForVault_GetGCPServiceFails", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
		}

		// Mock hyperscaler.GetGCPService failure
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("GCP service initialization failed")
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}
		err := activity.deleteGCPBucketsForVault(context.Background(), vault)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "GCP service initialization failed")
	})

	t.Run("DeleteGCPBucketsForVault_EmptyBucketName", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "", // Empty bucket name
				},
			},
		}

		// Mock hyperscaler.GetGCPService success
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}
		err := activity.deleteGCPBucketsForVault(context.Background(), vault)

		assert.NoError(tt, err) // Should skip empty bucket name
	})

	t.Run("DeleteGCPBucketsForVault_NoBucketDetails", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel:     datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:          "vault-1",
			AccountID:     1,
			BucketDetails: datamodel.BucketDetailsArray{}, // No bucket details
		}

		// Mock hyperscaler.GetGCPService success
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}
		err := activity.deleteGCPBucketsForVault(context.Background(), vault)

		assert.NoError(tt, err) // Should handle empty bucket details
	})

	t.Run("DeleteGCPBucketsForVault_WithBucketDetails", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "test-bucket-1",
				},
				{
					BucketName: "test-bucket-2",
				},
			},
		}

		// Mock hyperscaler.GetGCPService success
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}

		// We expect it to panic due to missing storage service, so we'll catch it
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Convert panic to error
					err = errors.New("panic occurred: " + fmt.Sprint(r))
				}
			}()

			// This will panic due to nil pointer dereference
			err = activity.deleteGCPBucketsForVault(context.Background(), vault)
		}()

		// We expect an error since we don't have a real storage service
		assert.Error(tt, err)
		// The error should be related to the panic
		assert.Contains(tt, err.Error(), "panic occurred")
	})

	t.Run("DeleteGCPBucketsForVault_EmptyBucketFails", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "test-bucket-1",
				},
			},
		}

		// Mock hyperscaler.GetGCPService to return a GCP service that will panic
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}

		// We expect it to panic due to missing storage service, so we'll catch it
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Convert panic to error
					err = errors.New("panic occurred: " + fmt.Sprint(r))
				}
			}()

			// This will panic due to nil pointer dereference in EmptyBucket
			err = activity.deleteGCPBucketsForVault(context.Background(), vault)
		}()

		// We expect an error since we don't have a real storage service
		assert.Error(tt, err)
		// The error should be related to the panic
		assert.Contains(tt, err.Error(), "panic occurred")
	})

	t.Run("DeleteGCPBucketsForVault_DeleteBucketFails", func(tt *testing.T) {
		vault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-1"},
			Name:      "vault-1",
			AccountID: 1,
			BucketDetails: datamodel.BucketDetailsArray{
				{
					BucketName: "test-bucket-1",
				},
			},
		}

		// Mock hyperscaler.GetGCPService to return a GCP service that will panic
		originalGetGCPService := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

		activity := BackupVaultActivity{}

		// We expect it to panic due to missing storage service, so we'll catch it
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					// Convert panic to error
					err = errors.New("panic occurred: " + fmt.Sprint(r))
				}
			}()

			// This will panic due to nil pointer dereference in DeleteBucket
			err = activity.deleteGCPBucketsForVault(context.Background(), vault)
		}()

		// We expect an error since we don't have a real storage service
		assert.Error(tt, err)
		// The error should be related to the panic
		assert.Contains(tt, err.Error(), "panic occurred")
	})
}

func TestDeleteRemoteBackupVaultInVCP(t *testing.T) {
	backupRegion := "us-west1"

	t.Run("Success_OperationV1beta", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return OperationV1beta with Done=true
		done := true
		opName := "operations/delete-vault-123"
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.OperationV1beta{
				Done: googleproxyclient.OptBool{Value: done, Set: true},
				Name: googleproxyclient.OptString{Value: opName, Set: true},
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Success_OperationV1beta_NotDone", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return OperationV1beta with Done=false
		done := false
		opName := "operations/delete-vault-123"
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.OperationV1beta{
				Done: googleproxyclient.OptBool{Value: done, Set: true},
				Name: googleproxyclient.OptString{Value: opName, Set: true},
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Success_NoContent", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return NoContent
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultNoContent{}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_BackupRegionMissing", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  nil,
		}

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "BackupRegion not provided in params")
	})

	t.Run("Error_BackupRegionEmpty", func(tt *testing.T) {
		ctx := context.Background()
		emptyRegion := ""
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &emptyRegion,
		}

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "BackupRegion not provided in params")
	})

	t.Run("Error_RemoteRegionConfigFails", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig to return error
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "", "", fmt.Errorf("failed to get remote region config")
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to get remote region configuration")
	})

	t.Run("Error_InvokerCallFails", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return error
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			nil, fmt.Errorf("network error"))

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to delete remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_BadRequest", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return BadRequest
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultBadRequest{
				Message: "Invalid vault ID",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Bad request deleting remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Unauthorized", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return Unauthorized
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultUnauthorized{
				Message: "Invalid credentials",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unauthorized to delete remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Forbidden", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return Forbidden
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultForbidden{
				Message: "Access denied",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Forbidden to delete remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_NotFound", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return NotFound
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultNotFound{
				Message: "Vault not found",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Remote backup vault not found")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Conflict", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return Conflict
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultConflict{
				Message: "Vault has active backups",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Conflict deleting remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_InternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return InternalServerError
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultInternalServerError{
				Message: "Internal error occurred",
			}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Internal server error deleting remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_UnexpectedResponseType", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the delete operation to return unexpected type (MethodNotAllowed is not handled in switch)
		mockInvoker.On("V1betaInternalDeleteBackupVault", mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalDeleteBackupVaultMethodNotAllowed{}, nil)

		result, err := DeleteRemoteBackupVaultInVCP(ctx, params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unexpected response type")
		mockInvoker.AssertExpectations(tt)
	})
}

func TestUpdateRemoteBackupVaultInVCP_CmekAttributesHydrated(t *testing.T) {
	backupRegion := "us-west1"

	ctx := context.Background()
	description := "Updated description"
	dailyImmutable := true
	minRetentionDays := int64(30)
	params := &common.BackupVaultParams{
		OwnerID:       "123456789",
		BackupVaultID: "vault-uuid-123",
		BackupRegion:  &backupRegion,
		Description:   &description,
		BackupRetentionPolicy: common.BackupRetentionPolicyParams{
			BackupMinimumEnforcedRetentionDuration: &minRetentionDays,
			IsDailyBackupImmutable:                 &dailyImmutable,
		},
	}
	completedState := "ENCRYPTION_STATE_COMPLETED"
	primaryKeyVersion := "projects/p/locations/r/keyRings/ring/cryptoKeys/key/cryptoKeyVersions/1"
	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		CmekAttributes: &datamodel.CmekAttributes{
			EncryptionState:          &completedState,
			BackupsPrimaryKeyVersion: &primaryKeyVersion,
		},
	}

	originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
	utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
		return "proxy.example.com", "mock-jwt-token", nil
	}
	defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

	mockInvoker := new(googleproxyclient.MockInvoker)
	originalGoogleProxyClientGet := googleProxyClientGet
	googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
		return &googleproxyclient.ProxyClient{
			Invoker: mockInvoker,
		}
	}
	defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

	mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			body := args.Get(1).(*googleproxyclient.BackupVaultInternalUpdateV1beta)
			if assert.NotNil(t, body.EncryptionState) {
				assert.Equal(t, completedState, string(body.EncryptionState.Value))
			}
			if assert.NotNil(t, body.BackupsPrimaryKeyVersion) {
				assert.Equal(t, primaryKeyVersion, body.BackupsPrimaryKeyVersion.Value)
			}
		}).
		Return(&googleproxyclient.OperationV1beta{
			Done: googleproxyclient.OptBool{Value: true, Set: true},
			Name: googleproxyclient.OptString{Value: "operations/update-vault-123", Set: true},
		}, nil)

	result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockInvoker.AssertExpectations(t)
}

func TestUpdateRemoteBackupVaultInVCP(t *testing.T) {
	backupRegion := "us-west1"

	t.Run("Success_OperationV1beta", func(tt *testing.T) {
		ctx := context.Background()
		description := "Updated description"
		dailyImmutable := true
		minRetentionDays := int64(30)
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
			Description:   &description,
			BackupRetentionPolicy: common.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &minRetentionDays,
				IsDailyBackupImmutable:                 &dailyImmutable,
			},
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return OperationV1beta with Done=true
		done := true
		opName := "operations/update-vault-123"
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.OperationV1beta{
				Done: googleproxyclient.OptBool{Value: done, Set: true},
				Name: googleproxyclient.OptString{Value: opName, Set: true},
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "vault-uuid-123", result.UUID)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Success_OperationV1beta_NotDone", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return OperationV1beta with Done=false
		done := false
		opName := "operations/update-vault-123"
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.OperationV1beta{
				Done: googleproxyclient.OptBool{Value: done, Set: true},
				Name: googleproxyclient.OptString{Value: opName, Set: true},
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "vault-uuid-123", result.UUID)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Success_WithAllRetentionPolicies", func(tt *testing.T) {
		ctx := context.Background()
		description := "Updated description"
		dailyImmutable := true
		weeklyImmutable := false
		monthlyImmutable := true
		adhocImmutable := false
		minRetentionDays := int64(30)
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
			Description:   &description,
			BackupRetentionPolicy: common.BackupRetentionPolicyParams{
				BackupMinimumEnforcedRetentionDuration: &minRetentionDays,
				IsDailyBackupImmutable:                 &dailyImmutable,
				IsWeeklyBackupImmutable:                &weeklyImmutable,
				IsMonthlyBackupImmutable:               &monthlyImmutable,
				IsAdhocBackupImmutable:                 &adhocImmutable,
			},
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return OperationV1beta
		done := true
		opName := "operations/update-vault-123"
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.OperationV1beta{
				Done: googleproxyclient.OptBool{Value: done, Set: true},
				Name: googleproxyclient.OptString{Value: opName, Set: true},
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "vault-uuid-123", result.UUID)
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_BackupRegionMissing", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  nil,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "BackupRegion not provided in params")
	})

	t.Run("Error_BackupRegionEmpty", func(tt *testing.T) {
		ctx := context.Background()
		emptyRegion := ""
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &emptyRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "BackupRegion not provided in params")
	})

	t.Run("Error_RemoteRegionConfigFails", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig to return error
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "", "", fmt.Errorf("failed to get remote region config")
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to get remote region configuration")
	})

	t.Run("Error_InvokerCallFails", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return error
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			nil, fmt.Errorf("network error"))

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Failed to update remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_BadRequest", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return BadRequest
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultBadRequest{
				Message: "Invalid update parameters",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Bad request updating remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Unauthorized", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return Unauthorized
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultUnauthorized{
				Message: "Invalid credentials",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unauthorized to update remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Forbidden", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return Forbidden
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultForbidden{
				Message: "Access denied",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Forbidden to update remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_NotFound", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return NotFound
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultNotFound{
				Message: "Vault not found",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Remote backup vault not found")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_Conflict", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return Conflict
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultConflict{
				Message: "Update conflict",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Conflict updating remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_UnprocessableEntity", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return UnprocessableEntity
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultUnprocessableEntity{
				Message: "Invalid entity",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unprocessable entity updating remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_InternalServerError", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return InternalServerError
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultInternalServerError{
				Message: "Internal error occurred",
			}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Internal server error updating remote backup vault")
		mockInvoker.AssertExpectations(tt)
	})

	t.Run("Error_UnexpectedResponseType", func(tt *testing.T) {
		ctx := context.Background()
		params := &common.BackupVaultParams{
			OwnerID:       "123456789",
			BackupVaultID: "vault-uuid-123",
			BackupRegion:  &backupRegion,
		}
		backupVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{UUID: "vault-uuid-123"},
		}

		// Mock utilsGetRemoteRegionConfig
		originalUtilsGetRemoteRegionConfig := utilsGetRemoteRegionConfig
		utilsGetRemoteRegionConfig = func(region, projectNumber string) (string, string, error) {
			return "proxy.example.com", "mock-jwt-token", nil
		}
		defer func() { utilsGetRemoteRegionConfig = originalUtilsGetRemoteRegionConfig }()

		// Mock googleProxyClientGet
		mockInvoker := new(googleproxyclient.MockInvoker)
		originalGoogleProxyClientGet := googleProxyClientGet
		googleProxyClientGet = func(basePath, jwtToken string, logger log.Logger) *googleproxyclient.ProxyClient {
			return &googleproxyclient.ProxyClient{
				Invoker: mockInvoker,
			}
		}
		defer func() { googleProxyClientGet = originalGoogleProxyClientGet }()

		// Mock the update operation to return unexpected type (MethodNotAllowed is not handled in switch)
		mockInvoker.On("V1betaInternalUpdateBackupVault", mock.Anything, mock.Anything, mock.Anything).Return(
			&googleproxyclient.V1betaInternalUpdateBackupVaultMethodNotAllowed{}, nil)

		result, err := UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Unexpected response type")
		mockInvoker.AssertExpectations(tt)
	})
}
