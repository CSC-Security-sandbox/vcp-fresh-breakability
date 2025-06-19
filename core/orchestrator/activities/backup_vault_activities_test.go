package activities

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_CreateBackupVaultInVCP(tt *testing.T) {
	tt.Run("WhenSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		accId := 1
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		vcpBvParams := &datamodel.BackupVault{AccountID: int64(accId)}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1"}

		expectedBackupVault := &datamodel.BackupVault{Name: "test-vault", AccountID: int64(accId), RegionName: "us-central1"}
		mockStorage.On("CreateBackupVault", ctx, bvParams, vcpBvParams).Return(expectedBackupVault, nil)

		result, err := activity.CreateBackupVaultInVCP(ctx, bvParams, vcpBvParams, paramz)

		assert.NoError(t, err)
		assert.Equal(t, expectedBackupVault, result)
		mockStorage.AssertCalled(t, "CreateBackupVault", ctx, bvParams, vcpBvParams)
	})
	tt.Run("WhenError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &BackupVaultActivity{SE: mockStorage}
		accId := 1
		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		vcpBvParams := &datamodel.BackupVault{AccountID: int64(accId)}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1"}

		mockStorage.On("CreateBackupVault", ctx, bvParams, vcpBvParams).Return(nil, errors.New("creation failed"))

		result, err := activity.CreateBackupVaultInVCP(ctx, bvParams, vcpBvParams, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "creation failed")
		mockStorage.AssertCalled(t, "CreateBackupVault", ctx, bvParams, vcpBvParams)
	})
	tt.Run("CreateBackupVaultInVCP_InvalidParams", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		bvParams := &datamodel.BackupVault{}
		vcpBvParams := &datamodel.BackupVault{}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{}

		mockStorage.On("CreateBackupVault", ctx, bvParams, vcpBvParams).Return(nil, errors.New("invalid parameters"))

		result, err := activity.CreateBackupVaultInVCP(ctx, bvParams, vcpBvParams, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "invalid parameters")
		mockStorage.AssertCalled(t, "CreateBackupVault", ctx, bvParams, vcpBvParams)
	})
}

func Test_ConvertImmutableAttributesToBackupRetentionPolicy(tt *testing.T) {
	tt.Run("ReturnsNilWhenAttributesAreNil", func(t *testing.T) {
		var attrs *datamodel.ImmutableAttributes

		result := _convertImmutableAttributesToBackupRetentionPolicy(attrs)

		assert.Nil(t, result)
	})
	tt.Run("ConvertsValidImmutableAttributesToRetentionPolicy", func(t *testing.T) {
		mrd := int64(30)
		attrs := &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: &mrd,
			IsDailyBackupImmutable:                 true,
			IsWeeklyBackupImmutable:                false,
			IsMonthlyBackupImmutable:               true,
			IsAdhocBackupImmutable:                 false,
		}

		expected := &models.BackupRetentionPolicyV1beta{
			BackupMinimumEnforcedRetentionDays: &mrd,
			DailyBackupImmutable:               true,
			ManualBackupImmutable:              false,
			MonthlyBackupImmutable:             true,
			WeeklyBackupImmutable:              false,
		}

		result := _convertImmutableAttributesToBackupRetentionPolicy(attrs)

		assert.Equal(t, expected, result)
	})
	tt.Run("ConvertsEmptyImmutableAttributesToRetentionPolicy", func(t *testing.T) {
		attrs := &datamodel.ImmutableAttributes{}

		expected := &models.BackupRetentionPolicyV1beta{
			BackupMinimumEnforcedRetentionDays: nil,
			DailyBackupImmutable:               false,
			ManualBackupImmutable:              false,
			MonthlyBackupImmutable:             false,
			WeeklyBackupImmutable:              false,
		}

		result := _convertImmutableAttributesToBackupRetentionPolicy(attrs)

		assert.Equal(t, expected, result)
	})
}

func TestConvertsValidBackupVaultV1betaToDataModel(tt *testing.T) {
	tt.Run("ConvertsValidBackupVaultV1betaToDataModel", func(t *testing.T) {
		reourceID := "test-vault"
		backupRegion := "us-central1"
		bvType := "STANDARD"
		desc := "test-descriptopn"
		mrd := int64(30)
		dstBVname := "cross-region-vault"
		bv := &models.BackupVaultV1beta{
			ResourceID:      &reourceID,
			BackupRegion:    &backupRegion,
			BackupVaultType: &bvType,
			Description:     &desc,
			BackupRetentionPolicy: &models.BackupRetentionPolicyV1beta{
				BackupMinimumEnforcedRetentionDays: &mrd,
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
				BackupMinimumEnforcedRetentionDuration: &mrd,
				IsDailyBackupImmutable:                 true,
				IsWeeklyBackupImmutable:                false,
				IsMonthlyBackupImmutable:               true,
				IsAdhocBackupImmutable:                 false,
			},
			CrossRegionBackupVaultName: &dstBVname,
		}

		result, err := _convertToBackupVaultDataModel(bv, locationId)

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

func TestCreateBackupVault(tt *testing.T) {
	tt.Run("WhenSuccess", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		resourceID := "test-vault"
		backupRegion := "us-central1"
		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockVault := &models.BackupVaultV1beta{
			ResourceID:    &resourceID,
			BackupRegion:  &backupRegion,
			BackupVaultID: "uuid-123",
			CreatedAt:     strfmt.DateTime(time.Now()),
		}

		mockClient.On("V1betaCreateBackupVault", mock.Anything).Return(&backup_vault.V1betaCreateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: mockVault,
			},
		}, nil)

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := createBvToSde(ctx, bvParams, paramz)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
	tt.Run("WhenUtilsConvertJsonToModel", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		resourceID := "test-vault"
		backupRegion := "us-central1"
		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockVault := &models.BackupVaultV1beta{
			ResourceID:    &resourceID,
			BackupRegion:  &backupRegion,
			BackupVaultID: "uuid-123",
			CreatedAt:     strfmt.DateTime(time.Now()),
		}

		mockClient.On("V1betaCreateBackupVault", mock.Anything).Return(&backup_vault.V1betaCreateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: mockVault,
			},
		}, nil)

		defer func() {
			utilsConvertJsonToModel = utils.ConvertJsonToModel
		}()

		utilsConvertJsonToModel = func(data []byte, model interface{}) error {
			return errors.New("conversion error")
		}

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := createBvToSde(ctx, bvParams, paramz)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
	tt.Run("WhenConvertToBackupVaultDataModel", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		resourceID := "test-vault"
		backupRegion := "us-central1"
		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockVault := &models.BackupVaultV1beta{
			ResourceID:    &resourceID,
			BackupRegion:  &backupRegion,
			BackupVaultID: "uuid-123",
			CreatedAt:     strfmt.DateTime(time.Now()),
		}
		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		defer func() {
			convertToBackupVaultDataModel = _convertToBackupVaultDataModel
			cvpCreateClient = originalCreateClient
		}()

		mockClient.On("V1betaCreateBackupVault", mock.Anything).Return(&backup_vault.V1betaCreateBackupVaultAccepted{
			Payload: &models.OperationV1beta{
				Response: mockVault,
			},
		}, nil)

		convertToBackupVaultDataModel = func(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
			return nil, errors.New("conversion error")
		}

		result, err := createBvToSde(ctx, bvParams, paramz)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestCheckBackupVaultExistsInSDE(tt *testing.T) {
	tt.Run("ReturnsBackupVaultWhenExists", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		resourceID := "test-vault"
		backupRegion := "us-central1"
		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockVault := &models.BackupVaultV1beta{
			ResourceID:    &resourceID,
			BackupRegion:  &backupRegion,
			BackupVaultID: "uuid-123",
			CreatedAt:     strfmt.DateTime(time.Now()),
		}

		mockClient.On("V1betaListBackupVaults", mock.Anything).Return(&backup_vault.V1betaListBackupVaultsOK{
			Payload: &backup_vault.V1betaListBackupVaultsOKBody{
				BackupVaults: []*models.BackupVaultV1beta{mockVault},
			},
		}, nil)

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := checkBackupVaultExistsInSDE(ctx, bvParams, paramz)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test-vault", result.Name)
		mockClient.AssertCalled(t, "V1betaListBackupVaults", mock.Anything)
	})
	tt.Run("ReturnsErrorWhenBackupVaultListFails", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockClient.On("V1betaListBackupVaults", mock.Anything).Return(nil, errors.New("list failed"))

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := checkBackupVaultExistsInSDE(ctx, bvParams, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "list failed")
		mockClient.AssertCalled(t, "V1betaListBackupVaults", mock.Anything)
	})
	tt.Run("ReturnsErrorWhenBackupVaultNotFound", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)

		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "non-existent-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockClient.On("V1betaListBackupVaults", mock.Anything).Return(&backup_vault.V1betaListBackupVaultsOK{
			Payload: &backup_vault.V1betaListBackupVaultsOKBody{
				BackupVaults: []*models.BackupVaultV1beta{},
			},
		}, nil)

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := checkBackupVaultExistsInSDE(ctx, bvParams, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "not found")
		mockClient.AssertCalled(t, "V1betaListBackupVaults", mock.Anything)
	})
}

func TestCreateBackupVaultInSDE(tt *testing.T) {
	tt.Run("ReturnsExistingBackupVaultWhenAlreadyExists", func(t *testing.T) {
		rID := "test-vault"
		bRegion := "us-central1"
		mockClient := backup_vault.NewMockClientService(t)
		mockStorage := database.NewMockStorage(t)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		mockVault := &models.BackupVaultV1beta{
			ResourceID:    &rID,
			BackupRegion:  &bRegion,
			BackupVaultID: "uuid-123",
			CreatedAt:     strfmt.DateTime(time.Now()),
		}

		mockClient.On("V1betaListBackupVaults", mock.Anything).Return(&backup_vault.V1betaListBackupVaultsOK{
			Payload: &backup_vault.V1betaListBackupVaultsOKBody{
				BackupVaults: []*models.BackupVaultV1beta{mockVault},
			},
		}, nil)

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := activity.CreateBackupVaultInSDE(ctx, bvParams, paramz)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "test-vault", result.Name)
		mockClient.AssertCalled(t, "V1betaListBackupVaults", mock.Anything)
	})
	tt.Run("WhenCheckBackupVaultFailsWhileCreatingBackupVaultError", func(t *testing.T) {
		mockClient := backup_vault.NewMockClientService(t)
		mockStorage := database.NewMockStorage(t)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		bvParams := &datamodel.BackupVault{Name: "test-vault"}
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{LocationId: "us-central1", ProjectNumber: "12345", XCorrelationID: gcpgenserver.NewOptString("correlation-id")}

		checkBackupVaultExistsInSDE = func(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
			return nil, errors2.NewUserInputValidationErr("Backup vault SDE error")
		}

		cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
		originalCreateClient := cvpCreateClient
		defer func() { cvpCreateClient = originalCreateClient }()
		cvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := activity.CreateBackupVaultInSDE(ctx, bvParams, paramz)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
	tt.Run("WhenCreateBackupVaultSuccess", func(t *testing.T) {
		description := "test-description"
		name := "bvName"
		req := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID: "test-id",
			},
			Name:        "test-vault",
			RegionName:  "us-central1",
			Description: &description,
		}

		// Define mock response
		updatedTime := strfmt.DateTime(time.Now())
		ctx := context.Background()
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		checkBackupVaultExistsInSDE = func(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
			return nil, errors2.NewNotFoundErr("Backup vault not found in SDE", nil)
		}

		createBvToSde = func(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
			// Mock the response for creating a backup vault
			return &datamodel.BackupVault{
				BaseModel: datamodel.BaseModel{
					UUID:      "test-id",
					CreatedAt: time.Time(updatedTime),
					UpdatedAt: time.Time(updatedTime),
				},
				Name:                  name,
				Description:           &description,
				RegionName:            "us-central1",
				LifeCycleState:        "Available for use",
				LifeCycleStateDetails: "READY",
			}, nil
		}

		// Call the method under test
		bv, err := createBackupVaultInSDE(ctx, req, paramz)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, bv)
	})
	tt.Run("WhenCreateBackupVaultFails", func(t *testing.T) {
		description := "test-description"
		req := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID: "test-id",
			},
			Name:        "test-vault",
			RegionName:  "us-central1",
			Description: &description,
		}

		// Define mock response
		ctx := context.Background()
		paramz := gcpgenserver.V1betaCreateBackupVaultParams{
			LocationId:     "us-central1",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id"),
		}

		checkBackupVaultExistsInSDE = func(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
			return nil, errors2.NewNotFoundErr("Backup vault not found in SDE", nil)
		}

		createBvToSde = func(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
			// Mock the response for creating a backup vault
			return nil, errors.New("failed to create backup vault")
		}

		// Call the method under test
		bv, err := createBackupVaultInSDE(ctx, req, paramz)

		// Assertions
		assert.Error(t, err)
		assert.Nil(t, bv)
	})
}

func TestCheckBackupVaultExistsInVCP_ReturnsBackupVaultWhenExists(tt *testing.T) {
	tt.Run("WhenBackupVaultExists", func(t *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		vaultName := "test-vault"
		ownerID := "owner-123"
		expectedBackupVault := &datamodel.BackupVault{Name: vaultName, AccountID: 123}

		mockStorage.On("GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID).Return(expectedBackupVault, nil)

		result, err := activity.CheckBackupVaultExistsInVCP(ctx, vaultName, ownerID)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedBackupVault, result)
		mockStorage.AssertCalled(tt, "GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID)
	})
	tt.Run("WhenBackupVaultDoesNotExist", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		vaultName := "non-existent-vault"
		ownerID := "owner-123"

		mockStorage.On("GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID).Return(nil, errors.New("vault not found"))

		result, err := activity.CheckBackupVaultExistsInVCP(ctx, vaultName, ownerID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "vault not found")
		mockStorage.AssertCalled(tt, "GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID)
	})
	tt.Run("WhenStorageFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &BackupVaultActivity{SE: mockStorage}

		ctx := context.Background()
		vaultName := "test-vault"
		ownerID := "owner-123"

		mockStorage.On("GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID).Return(nil, errors.New("storage error"))

		result, err := activity.CheckBackupVaultExistsInVCP(ctx, vaultName, ownerID)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "storage error")
		mockStorage.AssertCalled(tt, "GetBackupVaultByNameAndOwnerID", ctx, vaultName, ownerID)
	})
}
