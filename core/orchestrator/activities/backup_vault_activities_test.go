package activities

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestConvertsValidBackupVaultV1betaToDataModel(tt *testing.T) {
	tt.Run("ConvertsValidBackupVaultV1betaToDataModel", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		desc := "test-descriptopn"
		minEnforcedRetentionDuration := int64(30)
		dstBVname := "cross-region-vault"
		bv := &models.BackupVaultV1beta{
			ResourceID:      &reourceID,
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

		locationId := "us-central1"
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
		}

		result, err := convertToBackupVaultDataModel(bv, locationId)

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
			SourceRegionName:      &locationId,
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
		}

		result, err := _convertToBackupVaultDataModel(bv, locationId)

		assert.NoError(t, err)
		assert.Equal(t, expected, result)
	})
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

		convertToBackupVaultDataModel = func(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
			return nil, errors.New("conversion error")
		}

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() {
			cvpCreateClient = originalCreateClient
			convertToBackupVaultDataModel = _convertToBackupVaultDataModel
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
		Name: "test-vault",
	}
	state := "ERROR"
	stateDetails := "Failed due to timeout"

	mockStorage.On("UpdateBackupVaultState", ctx, backupVault, state, stateDetails).Return(backupVault, nil).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.NoError(t, err)
	mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, state, stateDetails)
}

func TestReturnsErrorWhenStateUpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()

	backupVault := &datamodel.BackupVault{
		Name: "test-vault",
	}
	state := "ERROR"
	stateDetails := "Failed due to timeout"

	mockStorage.On("UpdateBackupVaultState", ctx, backupVault, state, stateDetails).Return(nil, errors.New("update failed")).Once()

	activity := BackupVaultActivity{
		SE: mockStorage,
	}

	err := activity.UpdateBackupVaultStateInCaseOfError(ctx, backupVault, state, stateDetails)

	assert.Error(t, err)
	mockStorage.AssertCalled(t, "UpdateBackupVaultState", ctx, backupVault, state, stateDetails)
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
	originalGetGCPService := GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		GetGCPService = originalGetGCPService
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
					return nil
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
					// Should only be called for bucket-2
					assert.Equal(t, "bucket-2", bucketName)
					return nil
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
					t.Fatal("DeleteGCPBucket should not be called when there are no buckets")
					return nil
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
					if bucketName == "bucket-1" {
						return errors.New("failed to delete bucket-1")
					}
					t.Fatal("DeleteGCPBucket should not be called for bucket-2 when bucket-1 fails")
					return nil
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
				GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
					return &google.GcpServices{}, nil
				}
				DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
					if bucketName == "bucket-1" {
						return nil
					}
					if bucketName == "bucket-2" {
						return errors.New("failed to delete bucket-2")
					}
					t.Fatal("DeleteGCPBucket called with unexpected bucket name")
					return nil
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
	originalGetGCPService := GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		GetGCPService = originalGetGCPService
		DeleteGCPBucket = originalDeleteGCPBucket
	}()

	t.Run("GCP service initialization failure", func(t *testing.T) {
		// Setup function mocks to simulate GCP service initialization failure
		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
	originalGetGCPService := GetGCPService
	originalDeleteGCPBucket := DeleteGCPBucket
	defer func() {
		GetGCPService = originalGetGCPService
		DeleteGCPBucket = originalDeleteGCPBucket
	}()

	t.Run("backup vault with nil bucket details", func(t *testing.T) {
		// Setup function mocks
		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
			t.Fatal("DeleteGCPBucket should not be called when bucket details is nil")
			return nil
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
