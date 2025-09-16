package activities_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func assertErrContainsOriginal(t *testing.T, err error, substring string) {
	t.Helper()
	var customErr *vsaerrors.CustomError
	if vsaerrors.As(err, &customErr) && customErr.Unwrap() != nil {
		assert.ErrorContains(t, customErr.Unwrap(), substring)
		return
	}
	assert.ErrorContains(t, err, substring)
}

func TestCreateBackup_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backup := &datamodel.Backup{Name: "test-backup"}

	mockStorage.On("CreateBackup", ctx, backup).Return(backup, nil)

	// Act
	result, err := activity.CreateBackup(ctx, backup)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, backup, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateBackup_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backup := &datamodel.Backup{Name: "test-backup"}

	mockStorage.On("CreateBackup", ctx, backup).Return(nil, errors.New("failed to create backup"))

	// Act
	result, err := activity.CreateBackup(ctx, backup)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to create backup")
	mockStorage.AssertExpectations(t)
}

func TestGetBackup_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backupUUID := "test-uuid"
	backupVaultUUID := "test-backup-vault-uuid"
	accountName := "test-account"
	backup := &datamodel.Backup{Name: "test-backup"}

	mockStorage.On("GetBackup", ctx, backupVaultUUID, backupUUID, accountName).Return(backup, nil)

	// Act
	result, err := activity.GetBackup(ctx, backupVaultUUID, backupUUID, accountName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, backup, result)
	mockStorage.AssertExpectations(t)
}

func TestGetBackup_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backupUUID := "test-uuid"
	backupVaultUUID := "test-backup-vault-uuid"
	accountName := "test-account"

	mockStorage.On("GetBackup", ctx, backupVaultUUID, backupUUID, accountName).Return(nil, errors.New("backup not found"))

	// Act
	result, err := activity.GetBackup(ctx, backupVaultUUID, backupUUID, accountName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "backup not found")
	mockStorage.AssertExpectations(t)
}

func TestDeleteBackup_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backupUUID := "test-uuid"
	backup := &datamodel.Backup{Name: "test-backup"}

	mockStorage.On("DeleteBackup", ctx, backupUUID).Return(backup, nil)

	// Act
	result, err := activity.DeleteBackup(ctx, backupUUID)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, backup, result)
	mockStorage.AssertExpectations(t)
}

func TestDeleteBackup_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backupUUID := "test-uuid"

	mockStorage.On("DeleteBackup", ctx, backupUUID).Return(nil, errors.New("failed to delete backup"))

	// Act
	result, err := activity.DeleteBackup(ctx, backupUUID)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete backup")
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupError_InvalidInput(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backup := &datamodel.Backup{}
	errorString := ""

	err := activity.UpdateBackupError(ctx, backup, errorString)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid input")
	mockStorage.AssertNotCalled(t, "UpdateBackupState")
}

func TestUpdateBackupError_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	backup := &datamodel.Backup{}
	errorString := "some error"

	mockStorage.On("UpdateBackupState", ctx, backup).Return(backup, nil)

	err := activity.UpdateBackupError(ctx, backup, errorString)

	assert.NoError(t, err)
	assert.Equal(t, models.LifeCycleStateError, backup.State)
	assert.Equal(t, "some error", backup.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestFinishBackup_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.Background()
	backup := &datamodel.Backup{}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)

	err := activity.FinishBackup(ctx, backup)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestFinishBackup_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.Background()
	backup := &datamodel.Backup{}

	mockStorage.On("FinishBackup", ctx, backup).Return(nil, errors.New("finish backup failed"))

	err := activity.FinishBackup(ctx, backup)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "finish backup failed")
	mockStorage.AssertExpectations(t)
}

func TestGetOrCreateObjectStore(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ct := oModels.CloudTarget{
		Name:      nillable.ToPointer("targetName"),
		Container: nillable.ToPointer("container"),
		UUID:      nillable.ToPointer("123e4567-e89b-12d3-a456-426614174000"),
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	node := &models.Node{}
	expectedResponse := &ontap_rest.CloudTarget{CloudTarget: ct}

	// Mock the CreateVolume method
	mockProvider.On("CloudTargetGet", mock.Anything).Return(expectedResponse, nil)

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, "container-name", "targetName")

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "targetName", result.Name)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorGetorCreate_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}
	node := &models.Node{}
	sourcePath := "source-path"
	destinationPath := "destination-path"
	expectedResponse := &ontap_rest.SnapmirrorRelationship{SnapmirrorRelationship: oModels.SnapmirrorRelationship{UUID: nillable.ToPointer(strfmt.UUID("smUUID")), Destination: &oModels.SnapmirrorEndpoint{UUID: nillable.ToPointer(strfmt.UUID("uuid"))}}}

	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      sourcePath,
		DestinationPath: destinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}
	mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(expectedResponse, nil)

	// Act
	result, err := activity.SnapmirrorGetOrCreate(ctx, node, SnapmirrorRelationshipParams)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse.Destination.UUID.String(), *result.DestinationUUID)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorGetorCreate_GetProviderByNode(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
	}()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider-error")
	}
	node := &models.Node{}
	sourcePath := "source-path"
	destinationPath := "destination-path"
	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      sourcePath,
		DestinationPath: destinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}
	// Act
	result, err := activity.SnapmirrorGetOrCreate(ctx, node, SnapmirrorRelationshipParams)

	// Assert
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider-error")
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorGetorCreate_CreateNew(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}
	node := &models.Node{}
	sourcePath := "source-path"
	destinationPath := "destination-path"
	expectedResponse := &ontap_rest.SnapmirrorRelationship{SnapmirrorRelationship: oModels.SnapmirrorRelationship{UUID: nillable.ToPointer(strfmt.UUID("smUUID")), Destination: &oModels.SnapmirrorEndpoint{UUID: nillable.ToPointer(strfmt.UUID("uuid"))}}}

	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      sourcePath,
		DestinationPath: destinationPath,
		SourceUUID:      nil,
		IsRestore:       false,
	}
	mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(nil, errors.New("not found"))
	mockProvider.On("SnapmirrorRelationshipCreate", SnapmirrorRelationshipParams, mock.Anything).Return(expectedResponse, nil)

	// Act
	result, err := activity.SnapmirrorGetOrCreate(ctx, node, SnapmirrorRelationshipParams)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse.UUID.String(), result.UUID)
	mockProvider.AssertExpectations(t)
}

func TestSnapshotCreate_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	volumeUUID := "volume-uuid"
	name := "snapshot-name"
	comment := "snapshot-comment"
	expectedResponse := &vsa.SnapshotProviderResponse{}

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: volumeUUID,
		Name:       name,
		Comment:    comment,
	}).Return(expectedResponse, nil)

	// Act
	result, err := activity.SnapshotCreate(ctx, node, volumeUUID, name, comment)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)
	mockProvider.AssertExpectations(t)
}

func TestSnapshotCreate_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	volumeUUID := "volume-uuid"
	name := "snapshot-name"
	comment := "snapshot-comment"

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: volumeUUID,
		Name:       name,
		Comment:    comment,
	}).Return(nil, errors.New("snapshot creation failed"))

	// Act
	result, err := activity.SnapshotCreate(ctx, node, volumeUUID, name, comment)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "snapshot creation failed")
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"
	expectedResponse := &ontap_rest.CloudTarget{CloudTarget: oModels.CloudTarget{
		Name:      nillable.ToPointer(objStoreName),
		Container: nillable.ToPointer(bucketName),
		UUID:      nillable.ToPointer("123e4567-e89b-12d3-a456-426614174000"),
	}}

	mockProvider.On("CloudTargetGet", &objStoreName).Return(expectedResponse, nil)

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, *expectedResponse.Name, result.Name)
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_GetProviderByNodeFailure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("get-povider-error")
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"
	// Act
	_, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get-povider-error")
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_CreateNew(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"
	expectedResponse := &ontap_rest.CloudTarget{CloudTarget: oModels.CloudTarget{Name: nillable.ToPointer(objStoreName), Container: nillable.ToPointer(bucketName), UUID: nillable.ToPointer("123e4567-e89b-12d3-a456-426614174000")}}

	mockProvider.On("CloudTargetGet", &objStoreName).Return(nil, errors.New("not found"))
	mockProvider.On("CloudTargetCreate", objStoreName, bucketName).Return(expectedResponse, nil)

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, *expectedResponse.Name, result.Name)
	assert.Equal(t, *expectedResponse.UUID, result.UUID)

	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"

	mockProvider.On("CloudTargetGet", &objStoreName).Return(nil, errors.New("not found"))
	mockProvider.On("CloudTargetCreate", objStoreName, bucketName).Return(nil, errors.New("creation failed"))

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get or create")
	mockProvider.AssertExpectations(t)
}

func TestSnapshotActivities(t *testing.T) {
	t.Run("SnapmirrorTransfer_WhenTransferSucceeds_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			token := "mock-token"
			return &token, nil
		}
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		mockProvider.On("SnapmirrorRelationshipTransferCreate", snapmirrorUUID, snapshotName, mock.Anything).Return(nil)

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransfer_WhenTransferFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			token := "mock-token"
			return &token, nil
		}
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		mockProvider.On("SnapmirrorRelationshipTransferCreate", snapmirrorUUID, snapshotName, mock.Anything).Return(errors.New("transfer failed"))

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "transfer failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransferPoll_WhenTransferSucceeds_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		state := "success"

		mockProvider.On("SnapmirrorRelationshipTransferGet", snapmirrorUUID, snapshotName).Return(&ontap_rest.SnapmirrorTransfer{SnapmirrorTransfer: oModels.SnapmirrorTransfer{State: &state}}, nil)
		status, err := activity.GetSnapmirrorTransferStatus(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.NoError(tt, err)
		assert.Equal(tt, state, status)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransferPoll_WhenTransferFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		state := "failed"

		mockProvider.On("SnapmirrorRelationshipTransferGet", snapmirrorUUID, snapshotName).Return(&ontap_rest.SnapmirrorTransfer{SnapmirrorTransfer: oModels.SnapmirrorTransfer{State: &state}}, fmt.Errorf("Snapmirror transfer failed with state: failed"))

		status, err := activity.GetSnapmirrorTransferStatus(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Equal(tt, state, status)
		assert.Contains(tt, err.Error(), "Snapmirror transfer failed with state: failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("DeleteSnapshot_WhenDeleteSucceeds_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"

		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(nil)

		err := activity.DeleteBackupSnapshot(context.Background(), node, snapshotUUID, volumeUUID)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("DeleteSnapshot_WhenDeleteFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"

		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(errors.New("delete failed"))

		err := activity.DeleteBackupSnapshot(context.Background(), node, snapshotUUID, volumeUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "delete failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("DeleteSnapshot_WhenSnapshotUUIDEmpty_ThenReturnError", func(tt *testing.T) {
		activity := activities.BackupActivity{}
		node := &models.Node{}
		snapshotUUID := ""
		volumeUUID := "volume-uuid"

		err := activity.DeleteBackupSnapshot(context.Background(), node, snapshotUUID, volumeUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "invalid input: snapshotUUID and volumeUUID cannot be empty")
	})

	t.Run("DeleteSnapshot_WhenVolumeUUIDEmpty_ThenReturnError", func(tt *testing.T) {
		activity := activities.BackupActivity{}
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := ""

		err := activity.DeleteBackupSnapshot(context.Background(), node, snapshotUUID, volumeUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "invalid input: snapshotUUID and volumeUUID cannot be empty")
	})

	t.Run("DeleteSnapshot_WhenBothUUIDsEmpty_ThenReturnError", func(tt *testing.T) {
		activity := activities.BackupActivity{}
		node := &models.Node{}
		snapshotUUID := ""
		volumeUUID := ""

		err := activity.DeleteBackupSnapshot(context.Background(), node, snapshotUUID, volumeUUID)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "invalid input: snapshotUUID and volumeUUID cannot be empty")
	})
	t.Run("SnapmirrorTransfer_WhenGetSmcLicenseFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "", errors.New("smc license error")
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			token := "mock-token"
			return &token, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get SMC license from cloud")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransfer_WhenGenerateTokenFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return nil, errors.New("token error")
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to generate SMC token for node")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransfer_WhenTokenIsNil_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			return nil, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SMC token is empty or nil")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransfer_WhenTokenIsEmpty_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			hyperscaler.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			empty := ""
			return &empty, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SMC token is empty or nil")
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetObjectStore_GetProviderByNodeFailure(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		bucketName := "test-bucket"
		mockProvider.On("CloudTargetGet", &bucketName).Return(&ontap_rest.CloudTarget{
			CloudTarget: oModels.CloudTarget{
				Name: nillable.ToPointer("test-container"),
				UUID: nillable.ToPointer("123e4567-e89b-12d3-a456-426614174000"),
			},
		}, nil)

		objectStore, err := activity.GetObjectStore(context.Background(), &models.Node{}, bucketName)
		assert.Nil(t, objectStore)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider error")
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		bucketName := "test-bucket"
		mockProvider.On("CloudTargetGet", &bucketName).Return(nil, errors.New("failed"))

		objectStore, err := activity.GetObjectStore(context.Background(), &models.Node{}, "test-bucket")
		assert.NotNil(t, err)
		assert.Nil(t, objectStore)
		assert.EqualError(t, err, "object store does not exist")
	})
}

func TestGetObjectStore(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		bucketName := "test-bucket"
		mockProvider.On("CloudTargetGet", &bucketName).Return(&ontap_rest.CloudTarget{
			CloudTarget: oModels.CloudTarget{
				Name: nillable.ToPointer("test-container"),
				UUID: nillable.ToPointer("123e4567-e89b-12d3-a456-426614174000"),
			},
		}, nil)

		objectStore, err := activity.GetObjectStore(context.Background(), &models.Node{}, bucketName)
		assert.Nil(t, err)
		assert.NotNil(t, objectStore)
		assert.Equal(t, "test-container", objectStore.Name)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		bucketName := "test-bucket"
		mockProvider.On("CloudTargetGet", &bucketName).Return(nil, errors.New("failed"))

		objectStore, err := activity.GetObjectStore(context.Background(), &models.Node{}, "test-bucket")
		assert.NotNil(t, err)
		assert.Nil(t, objectStore)
		assert.EqualError(t, err, "object store does not exist")
	})
}

func TestGetSnapmirror(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		sourcePath := "source-path"
		destinationPath := "destination-path"
		mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(&ontap_rest.SnapmirrorRelationship{
			SnapmirrorRelationship: oModels.SnapmirrorRelationship{
				UUID: nillable.ToPointer(strfmt.UUID("123e4567-e89b-12d3-a456-426614174000")),
			},
		}, nil)
		snapmirror, err := activity.GetSnapmirror(context.Background(), &models.Node{}, sourcePath, destinationPath)
		assert.Nil(t, err)
		assert.NotNil(t, snapmirror)
		assert.Equal(t, "123e4567-e89b-12d3-a456-426614174000", snapmirror.UUID)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		sourcePath := "source-path"
		destinationPath := "destination-path"
		mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(nil, errors.New("not found"))
		snapmirror, err := activity.GetSnapmirror(context.Background(), &models.Node{}, sourcePath, destinationPath)
		assert.NotNil(t, err)
		assert.Nil(t, snapmirror)
		assert.EqualError(t, err, "failed to get snapmirror relationship: not found")
	})
	t.Run("onGetProviderByNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		sourcePath := "source-path"
		destinationPath := "destination-path"
		snapmirror, err := activity.GetSnapmirror(context.Background(), &models.Node{}, sourcePath, destinationPath)
		assert.Error(t, err)
		assert.Nil(t, snapmirror)
		assert.EqualError(t, err, "provider error")
	})
}

func TestIsVolumeDeleted(t *testing.T) {
	t.Run("onSuccessWhenVolumeAvailable", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"
		store.On("GetVolume", ctx, volumeUUID).Return(&datamodel.Volume{}, nil)
		isDeleted, err := activity.IsVolumeDeleted(ctx, volumeUUID)
		assert.NoError(t, err)
		assert.False(t, isDeleted)
	})
	t.Run("onSuccessWhenVolumeDeleted", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"
		store.On("GetVolume", ctx, volumeUUID).Return(nil, utilerrors.NewNotFoundErr("volume", nil))
		isDeleted, err := activity.IsVolumeDeleted(ctx, volumeUUID)
		assert.NoError(t, err)
		assert.True(t, isDeleted)
	})
	t.Run("onDBFailure", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"
		store.On("GetVolume", ctx, volumeUUID).Return(nil, errors.New("failed to check volume deletion"))
		isDeleted, err := activity.IsVolumeDeleted(ctx, volumeUUID)
		assert.Error(t, err)
		assert.False(t, isDeleted)
		assert.EqualError(t, err, "failed to check volume deletion")
	})
}

func TestGetVolume(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"
		expectedVolume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				UUID: volumeUUID,
			},
		}
		store.On("GetVolume", ctx, volumeUUID).Return(expectedVolume, nil)

		volume, err := activity.GetVolume(ctx, volumeUUID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVolume, volume)
		store.AssertExpectations(t)
	})
	t.Run("onDBFailure", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"

		store.On("GetVolume", ctx, volumeUUID).Return(nil, errors.New("failed to get volume"))

		volume, err := activity.GetVolume(ctx, volumeUUID)

		assert.Error(t, err)
		assert.Nil(t, volume)
		assert.EqualError(t, err, "failed to get volume")
		store.AssertExpectations(t)
	})
}

func TestGetBackupVault(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		vaultUUID := "test-vault-uuid"
		expectedVault := &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID: vaultUUID,
			},
		}
		store.On("GetBackupVault", ctx, vaultUUID).Return(expectedVault, nil)

		vault, err := activity.GetBackupVault(ctx, vaultUUID)

		assert.NoError(t, err)
		assert.Equal(t, expectedVault, vault)
		store.AssertExpectations(t)
	})
	t.Run("onDBFailure", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		vaultUUID := "test-vault-uuid"

		store.On("GetBackupVault", ctx, vaultUUID).Return(nil, errors.New("failed to get backup vault"))

		vault, err := activity.GetBackupVault(ctx, vaultUUID)

		assert.Error(t, err)
		assert.Nil(t, vault)
		assert.EqualError(t, err, "failed to get backup vault")
		store.AssertExpectations(t)
	})
}

func TestGetBackupCountByVolumeUUID(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"
		expectedCount := int64(5)

		store.On("BackupCountByVolumeID", ctx, volumeUUID).Return(expectedCount, nil)

		count, err := activity.GetBackupCountByVolumeUUID(ctx, volumeUUID)

		assert.NoError(t, err)
		assert.Equal(t, expectedCount, count)
	})
	t.Run("onDBFailure", func(t *testing.T) {
		store := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: store}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volumeUUID := "test-volume-uuid"

		store.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(0), errors.New("failed to get backup count"))

		count, err := activity.GetBackupCountByVolumeUUID(ctx, volumeUUID)

		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get backup count")
		assert.Equal(t, int64(0), count)
	})
}

func TestDeleteSnapshotFromObjectStore(t *testing.T) {
	t.Run("onSuccessWithJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		jobUUID := "123e4567-e89b-12d3-a456-426614174000"
		mockProvider.On("SnapmirrorObjectStoreSnapshotDelete", objectStoreUUID, endpointUUID, snapshotUUID).Return(&vsa.OntapAsyncResponse{
			JobUUID: jobUUID,
		}, nil)
		job, err := activity.DeleteSnapshotFromObjectStore(ctx, node, objectStoreUUID, endpointUUID, snapshotUUID)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, jobUUID, job.JobUUID)
	})
	t.Run("onSuccessWithoutJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		mockProvider.On("SnapmirrorObjectStoreSnapshotDelete", objectStoreUUID, endpointUUID, snapshotUUID).Return(nil, nil)
		job, err := activity.DeleteSnapshotFromObjectStore(ctx, node, objectStoreUUID, endpointUUID, snapshotUUID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		mockProvider.On("SnapmirrorObjectStoreSnapshotDelete", objectStoreUUID, endpointUUID, snapshotUUID).Return(nil, errors.New("delete failed"))
		job, err := activity.DeleteSnapshotFromObjectStore(ctx, node, objectStoreUUID, endpointUUID, snapshotUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "delete failed")
	})
	t.Run("onGetProviderbyNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		job, err := activity.DeleteSnapshotFromObjectStore(ctx, node, objectStoreUUID, endpointUUID, snapshotUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "failed to get provider")
	})
}

func TestDeleteSnapmirror(t *testing.T) {
	t.Run("onSuccessWithJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		jobUUID := "123e4567-e89b-12d3-a456-426614174000"
		mockProvider.On("SnapmirrorRelationshipDelete", snapmirrorUUID).Return(&vsa.OntapAsyncResponse{
			JobUUID: jobUUID,
		}, nil)
		job, err := activity.DeleteSnapmirror(ctx, node, snapmirrorUUID)
		assert.Nil(t, err)
		assert.NotNil(t, job)
	})
	t.Run("onSuccessWithoutJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		mockProvider.On("SnapmirrorRelationshipDelete", snapmirrorUUID).Return(nil, nil)
		job, err := activity.DeleteSnapmirror(ctx, node, snapmirrorUUID)
		assert.Nil(t, err)
		assert.Nil(t, job)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		mockProvider.On("SnapmirrorRelationshipDelete", snapmirrorUUID).Return(nil, errors.New("delete failed"))
		job, err := activity.DeleteSnapmirror(ctx, node, snapmirrorUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "delete failed")
	})
	t.Run("onGetProviderbyNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		job, err := activity.DeleteSnapmirror(ctx, node, snapmirrorUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "failed to get provider")
	})
}

func TestDeleteCloudEndpoint(t *testing.T) {
	t.Run("onSuccessWithJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		jobUUID := "123e4567-e89b-12d3-a456-426614174000"
		mockProvider.On("SnapmirrorObjectStoreEndpointDelete", objectStoreUUID, endpointUUID).Return(&vsa.OntapAsyncResponse{
			JobUUID: jobUUID,
		}, nil)
		job, err := activity.DeleteCloudEndpoint(ctx, node, objectStoreUUID, endpointUUID)
		assert.Nil(t, err)
		assert.NotNil(t, job)
	})
	t.Run("onSuccessWithoutJob", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		mockProvider.On("SnapmirrorObjectStoreEndpointDelete", objectStoreUUID, endpointUUID).Return(nil, nil)
		job, err := activity.DeleteCloudEndpoint(ctx, node, objectStoreUUID, endpointUUID)
		assert.Nil(t, err)
		assert.Nil(t, job)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		mockProvider.On("SnapmirrorObjectStoreEndpointDelete", objectStoreUUID, endpointUUID).Return(nil, errors.New("delete failed"))
		job, err := activity.DeleteCloudEndpoint(ctx, node, objectStoreUUID, endpointUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "delete failed")
	})
	t.Run("onGetProviderByNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		endpointUUID := "endpoint-uuid"
		objectStoreUUID := "object-store-uuid"
		job, err := activity.DeleteCloudEndpoint(ctx, node, objectStoreUUID, endpointUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "failed to get provider")
	})
}

func TestDeleteSnapshotForBackup(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(nil)
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID, false)
		assert.Nil(t, err)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(errors.New("delete failed"))
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID, false)
		assert.Error(t, err)
		assert.EqualError(t, err, "delete failed")
	})
	t.Run("onGetProviderByNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID, false)
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get provider")
	})
}

func TestCreateSnapshotActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}
	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         node,
		SnapshotName: "test-backup", // Set snapshot name as it should be set by CreatingSnapshotActivity
	}

	snapshotResponse := &vsa.SnapshotProviderResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "snapshot-uuid",
		},
		SizeInBytes:        1024,
		LogicalSizeInBytes: 512,
	}

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: volume.VolumeAttributes.ExternalUUID,
		Name:       state.SnapshotName,
		Comment:    "VCP-Backup",
	}).Return(snapshotResponse, nil)

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, snapshotResponse, result.SnapshotResponse)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapshotActivity_GetProviderByNodeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}
	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "test-backup",
	}

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "provider error")
	mockStorage.AssertExpectations(t)
}

func TestCreatingSnapshotActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "accountUUID",
			ID:   2,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node: &models.Node{},
	}

	expectedSnapshotName := backup.Name
	expectedDbSnapshot := &datamodel.Snapshot{
		Name:               expectedSnapshotName,
		Description:        "VCP-Backup",
		VolumeID:           volume.ID,
		AccountID:          volume.AccountID,
		Volume:             volume,
		Account:            volume.Account,
		IsAppConsistent:    false,
		Type:               "backup-adhoc",
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	mockStorage.On("CreatingSnapshot", ctx, mock.MatchedBy(func(s *datamodel.Snapshot) bool {
		return s.Name == expectedSnapshotName &&
			s.VolumeID == volume.ID &&
			s.AccountID == volume.AccountID &&
			s.Description == "VCP-Backup" &&
			s.IsAppConsistent == false &&
			s.Type == "backup-adhoc"
	})).Return(expectedDbSnapshot, nil)

	// Act
	result, err := activity.CreatingSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedSnapshotName, result.SnapshotName)
	assert.Equal(t, expectedDbSnapshot, result.DbSnapshot)
	mockStorage.AssertExpectations(t)
}

func TestCreatingSnapshotActivity_DBFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "accountUUID",
			ID:   2,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node: &models.Node{},
	}

	mockStorage.On("CreatingSnapshot", ctx, mock.Anything).Return(nil, errors.New("database error"))

	// Act
	result, err := activity.CreatingSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.NotNil(t, result) // State is returned even on error
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestUpdateSnapshotActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbSnapshot := &datamodel.Snapshot{
		BaseModel:          datamodel.BaseModel{ID: 1},
		Name:               "test-snapshot",
		State:              models.LifeCycleStateCreating,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	snapshotResponse := &vsa.SnapshotProviderResponse{
		ProviderResponse:   vsa.ProviderResponse{ExternalUUID: "ext-uuid-123"},
		SizeInBytes:        int64(1024),
		LogicalSizeInBytes: int64(2048),
	}

	state := &activities.BackupActivitiesContext{
		DbSnapshot:       dbSnapshot,
		SnapshotResponse: snapshotResponse,
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: &datamodel.Backup{
				Name: "test-backup",
				Attributes: &datamodel.BackupAttributes{
					UseExistingSnapshot: false,
				},
			},
		},
	}

	expectedSnapshot := &datamodel.Snapshot{
		BaseModel:    datamodel.BaseModel{ID: 1},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateAvailableDetails,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID:           "ext-uuid-123",
			SizeInBytes:            int64(1024),
			LogicalSizeUsedInBytes: int64(2048),
		},
	}

	mockStorage.On("UpdateSnapshot", ctx, mock.MatchedBy(func(s *datamodel.Snapshot) bool {
		return s.State == models.LifeCycleStateREADY &&
			s.StateDetails == models.LifeCycleStateAvailableDetails &&
			s.SnapshotAttributes.ExternalUUID == "ext-uuid-123" &&
			s.SnapshotAttributes.SizeInBytes == int64(1024) &&
			s.SnapshotAttributes.LogicalSizeUsedInBytes == int64(2048)
	})).Return(expectedSnapshot, nil)

	// Act
	result, err := activity.UpdateSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, state, result)
	mockStorage.AssertExpectations(t)
}

func TestUpdateSnapshotActivity_NilDbSnapshot(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "test-backup",
		DbSnapshot:   nil, // Nil DbSnapshot
	}

	// Act
	result, err := activity.UpdateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database snapshot is nil")
	mockStorage.AssertExpectations(t)
}

func TestUpdateSnapshotActivity_DBFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
	}

	snapshotResponse := &vsa.SnapshotProviderResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "snapshot-uuid",
		},
		SizeInBytes:        1024,
		LogicalSizeInBytes: 512,
	}

	dbSnapshot := &datamodel.Snapshot{
		Name:               "test-backup",
		VolumeID:           volume.ID,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:             &models.Node{},
		SnapshotName:     "test-backup",
		SnapshotResponse: snapshotResponse,
		DbSnapshot:       dbSnapshot,
	}

	mockStorage.On("UpdateSnapshot", ctx, mock.Anything).Return(nil, errors.New("database update failed"))

	// Act
	result, err := activity.UpdateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestUpdateSnapshotActivity_WithNilSnapshotResponse(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbSnapshot := &datamodel.Snapshot{
		BaseModel:          datamodel.BaseModel{ID: 1},
		Name:               "test-snapshot",
		State:              models.LifeCycleStateCreating,
		SnapshotAttributes: &datamodel.SnapshotAttributes{},
	}

	state := &activities.BackupActivitiesContext{
		DbSnapshot:       dbSnapshot,
		SnapshotResponse: nil,
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: &datamodel.Backup{
				Name: "test-backup",
				Attributes: &datamodel.BackupAttributes{
					UseExistingSnapshot: false,
				},
			},
		},
	}

	mockStorage.On("UpdateSnapshot", ctx, mock.MatchedBy(func(s *datamodel.Snapshot) bool {
		return s.State == models.LifeCycleStateError &&
			s.StateDetails == models.LifeCycleStateCreationErrorDetails &&
			s.DeletedAt != nil &&
			s.DeletedAt.Valid == true
	})).Return(dbSnapshot, nil)

	// Act
	result, err := activity.UpdateSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, state, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateSnapshotActivity_OntapCreateSnapshotFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}
	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         node,
		SnapshotName: "test-backup",
	}

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: volume.VolumeAttributes.ExternalUUID,
		Name:       state.SnapshotName,
		Comment:    "VCP-Backup",
	}).Return(nil, errors.New("ONTAP snapshot creation failed"))

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "ONTAP snapshot creation failed")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapshotActivity_UpdateSnapshotAfterOntapFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		Account: &datamodel.Account{BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		}},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}
	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         node,
		SnapshotName: "test-backup",
	}

	// Mock ONTAP failure
	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: volume.VolumeAttributes.ExternalUUID,
		Name:       state.SnapshotName,
		Comment:    "VCP-Backup",
	}).Return(nil, errors.New("ONTAP snapshot creation failed"))

	// Act - First call should fail
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert - Should fail
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "ONTAP snapshot creation failed")

	// Now test that the state is not modified when ONTAP fails
	assert.Nil(t, state.SnapshotResponse)
	assert.Equal(t, "test-backup", state.SnapshotName)
	assert.Equal(t, backup, state.BackupWorkflowInit.Backup)
	assert.Equal(t, volume, state.BackupWorkflowInit.Volume)

	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestPrepareObjectStoreActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID: "subnet-1",
				BucketName:     "test-bucket",
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Act
	result, err := activity.PrepareObjectStoreActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-bucket", result.ObjStoreName)
	assert.Equal(t, "test-bucket", result.BucketName)
	assert.NotNil(t, result.BucketDetails)
	assert.Equal(t, "subnet-1", result.BucketDetails.VendorSubnetID)
}

func TestMarkBackupAvailable(t *testing.T) {
	t.Run("onSuccess", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		backup := &datamodel.Backup{}
		mockStorage.On("UpdateBackupState", ctx, backup).Return(backup, nil)
		err := activity.MarkBackupAvailable(ctx, backup)
		assert.Nil(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		backup := &datamodel.Backup{}
		expectedError := errors.New("update failed")
		mockStorage.On("UpdateBackupState", ctx, backup).Return(nil, expectedError)
		err := activity.MarkBackupAvailable(ctx, backup)
		assert.Error(t, err)
		assert.EqualError(t, err, "update failed")
		mockStorage.AssertExpectations(t)
	})
}

func TestPrepareObjectStoreActivity_GetObjStoreNameFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID: "subnet-1",
				BucketName:     "test-bucket",
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-2", // Different subnet ID
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Act
	result, err := activity.PrepareObjectStoreActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assertErrContainsOriginal(t, err, "no matching bucket details found")
}

func TestPrepareObjectStoreActivity_GetBucketDetailsFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID: "subnet-1",
				BucketName:     "", // Empty bucket name
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Act
	result, err := activity.PrepareObjectStoreActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assertErrContainsOriginal(t, err, "no matching bucket details found")
}

func TestGetOrCreateObjectStoreActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID:     "subnet-1",
				BucketName:         "test-bucket",
				ServiceAccountName: "test-service-account",
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
	}

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
		Node:         node,
		ObjStoreName: "test-bucket",
		BucketName:   "test-bucket",
		BucketDetails: &datamodel.BucketDetails{
			VendorSubnetID:     "subnet-1",
			BucketName:         "test-bucket",
			ServiceAccountName: "test-service-account",
		},
	}

	expectedObjStore := &ontap_rest.CloudTarget{
		CloudTarget: oModels.CloudTarget{
			Name: nillable.ToPointer("test-bucket"),
			UUID: nillable.ToPointer("objstore-uuid"),
		},
	}

	mockProvider.On("CloudTargetGet", &state.ObjStoreName).Return(expectedObjStore, nil)

	// Act
	result, err := activity.GetOrCreateObjectStoreActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-bucket", result.ObjStore.Name)
	assert.Equal(t, "test-bucket", result.BackupWorkflowInit.Backup.Attributes.BucketName)
	assert.Equal(t, "test-service-account", result.BackupWorkflowInit.Backup.Attributes.ServiceAccountName)
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStoreActivity_GetOrCreateObjectStoreFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID:     "subnet-1",
				BucketName:         "test-bucket",
				ServiceAccountName: "test-service-account",
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
	}

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup:      backup,
			BackupVault: backupVault,
			Volume:      volume,
		},
		Node:         node,
		ObjStoreName: "test-bucket",
		BucketName:   "test-bucket",
		BucketDetails: &datamodel.BucketDetails{
			VendorSubnetID:     "subnet-1",
			BucketName:         "test-bucket",
			ServiceAccountName: "test-service-account",
		},
	}

	// Act
	result, err := activity.GetOrCreateObjectStoreActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "provider error")
}

func TestPrepareSnapmirrorActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID: "subnet-1",
				BucketName:     "test-bucket",
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
		Svm: &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			Name: "test-svm",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Act
	result, err := activity.PrepareSnapmirrorActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-bucket:/objstore/volume-uuid", result.SmDestinationPath)
	assert.Equal(t, "test-svm:test-volume", result.SmSourcePath)
}

func TestPrepareSnapmirrorActivity_GetSmDestinationPathFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID: "backup-vault-uuid",
		},
		Name: "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				VendorSubnetID: "subnet-1",
				BucketName:     "", // Empty bucket name
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volume-uuid",
		},
		Name: "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-1",
		},
		Svm: &datamodel.Svm{
			BaseModel: datamodel.BaseModel{
				UUID: "svm-uuid",
			},
			Name: "test-svm",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
	}

	// Act
	result, err := activity.PrepareSnapmirrorActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assertErrContainsOriginal(t, err, "no matching bucket details found")
}

func TestCreateSnapmirrorRelationshipActivity_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node:              node,
		SmSourcePath:      "test-svm:test-volume",
		SmDestinationPath: "test-bucket:/objstore/volume-uuid",
	}

	expectedSnapmirror := &ontap_rest.SnapmirrorRelationship{
		SnapmirrorRelationship: oModels.SnapmirrorRelationship{
			UUID: nillable.ToPointer(strfmt.UUID("sm-uuid")),
			Destination: &oModels.SnapmirrorEndpoint{
				UUID: nillable.ToPointer(strfmt.UUID("dest-uuid")),
			},
		},
	}

	mockProvider.On("SnapmirrorRelationshipGet", state.SmDestinationPath, state.SmSourcePath).Return(nil, errors.New("not found"))
	mockProvider.On("SnapmirrorRelationshipCreate", mock.Anything, mock.Anything).Return(expectedSnapmirror, nil)

	// Act
	result, err := activity.CreateSnapmirrorRelationshipActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "sm-uuid", result.SnapmirrorRelationship.UUID)
	assert.Equal(t, "dest-uuid", *result.SnapmirrorRelationship.DestinationUUID)
	assert.Equal(t, "dest-uuid", result.BackupWorkflowInit.Backup.Attributes.EndpointUUID)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapmirrorRelationshipActivity_SnapmirrorGetOrCreateFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node:              node,
		SmSourcePath:      "test-svm:test-volume",
		SmDestinationPath: "test-bucket:/objstore/volume-uuid",
	}

	// Act
	result, err := activity.CreateSnapmirrorRelationshipActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "provider error")
}

func TestCreateSnapmirrorRelationshipActivity_WithNilDestinationUUID(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()
	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	node := &models.Node{}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node:              node,
		SmSourcePath:      "test-svm:test-volume",
		SmDestinationPath: "test-bucket:/objstore/volume-uuid",
	}

	expectedSnapmirror := &ontap_rest.SnapmirrorRelationship{
		SnapmirrorRelationship: oModels.SnapmirrorRelationship{
			UUID:        nillable.ToPointer(strfmt.UUID("sm-uuid")),
			Destination: &oModels.SnapmirrorEndpoint{}, // No UUID
		},
	}

	mockProvider.On("SnapmirrorRelationshipGet", state.SmDestinationPath, state.SmSourcePath).Return(nil, errors.New("not found"))
	mockProvider.On("SnapmirrorRelationshipCreate", mock.Anything, mock.Anything).Return(expectedSnapmirror, nil)

	// Act
	result, err := activity.CreateSnapmirrorRelationshipActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "sm-uuid", result.SnapmirrorRelationship.UUID)
	assert.Nil(t, result.SnapmirrorRelationship.DestinationUUID)
	assert.Empty(t, result.BackupWorkflowInit.Backup.Attributes.EndpointUUID)
	mockProvider.AssertExpectations(t)
}

func TestTransferSnapshotActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	// node := &models.Node{} // Unused variable

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node: &models.Node{},
		SnapmirrorRelationship: &commonparams.SnapmirrorRelationship{
			UUID: "sm-uuid",
		},
		SnapshotName: "test-snapshot",
	}

	mockProvider.On("SnapmirrorRelationshipTransferCreate", "sm-uuid", "test-snapshot", mock.Anything).Return(nil)

	// Act
	result, err := activity.TransferSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, state, result)
	mockProvider.AssertExpectations(t)
}

func TestTransferSnapshotActivity_SnapmirrorTransferFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	defer func() {
		hyperscaler.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
		return "mock-license", nil
	}
	activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
		token := "mock-token"
		return &token, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	// node := &models.Node{} // Unused variable

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node: &models.Node{},
		SnapmirrorRelationship: &commonparams.SnapmirrorRelationship{
			UUID: "sm-uuid",
		},
		SnapshotName: "test-snapshot",
	}

	mockProvider.On("SnapmirrorRelationshipTransferCreate", "sm-uuid", "test-snapshot", mock.Anything).Return(errors.New("transfer failed"))

	// Act
	result, err := activity.TransferSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "transfer failed")
	mockProvider.AssertExpectations(t)
}

func TestCheckTransferStatusActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	// node := &models.Node{} // Unused variable

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node: &models.Node{},
		SnapmirrorRelationship: &commonparams.SnapmirrorRelationship{
			UUID: "sm-uuid",
		},
		SnapshotName: "test-snapshot",
	}

	status := "success"
	mockProvider.On("SnapmirrorRelationshipTransferGet", "sm-uuid", "test-snapshot").Return(&ontap_rest.SnapmirrorTransfer{
		SnapmirrorTransfer: oModels.SnapmirrorTransfer{
			State: &status,
		},
	}, nil)

	// Act
	result, err := activity.CheckTransferStatusActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "success", result.TransferStatus)
	mockProvider.AssertExpectations(t)
}

func TestCheckTransferStatusActivity_GetSnapmirrorTransferStatusFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	// node := &models.Node{} // Unused variable

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
		Node: &models.Node{},
		SnapmirrorRelationship: &commonparams.SnapmirrorRelationship{
			UUID: "sm-uuid",
		},
		SnapshotName: "test-snapshot",
	}

	mockProvider.On("SnapmirrorRelationshipTransferGet", "sm-uuid", "test-snapshot").Return(nil, errors.New("status check failed"))

	// Act
	result, err := activity.CheckTransferStatusActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "status check failed")
	mockProvider.AssertExpectations(t)
}

func TestFinishBackupActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)

	// Act
	result, err := activity.FinishBackupActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, state, result)
	mockStorage.AssertExpectations(t)
}

func TestFinishBackupActivity_FinishBackupFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name:       "test-backup",
		Attributes: &datamodel.BackupAttributes{},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(nil, errors.New("finish backup failed"))

	// Act
	result, err := activity.FinishBackupActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "finish backup failed")
	mockStorage.AssertExpectations(t)
}

func TestGetAccountByName_Error(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	accountName := "test-account"

	mockStorage.On("GetAccount", ctx, accountName).Return(nil, errors.New("account not found"))

	// Act
	result, err := activity.GetAccountByName(ctx, accountName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "account not found")
	mockStorage.AssertExpectations(t)
}

func TestGetAccountByName_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	accountName := "test-account"
	expectedAccount := &datamodel.Account{
		BaseModel: datamodel.BaseModel{UUID: "account-uuid"},
		Name:      accountName,
	}

	mockStorage.On("GetAccount", ctx, accountName).Return(expectedAccount, nil)

	// Act
	result, err := activity.GetAccountByName(ctx, accountName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedAccount, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateSnapshotActivity_UseExistingSnapshot_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbSnapshot := &datamodel.Snapshot{
		Name:     "existing-snapshot",
		VolumeID: 1,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "snapshot-uuid",
		},
	}

	snapshotResponse := &vsa.SnapshotProviderResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "snapshot-uuid",
		},
		SizeInBytes:        2048,
		LogicalSizeInBytes: 1024,
	}

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: true,
			SnapshotName:        "existing-snapshot",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "existing-snapshot",
	}

	mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, "existing-snapshot", volume.AccountID, volume.ID).Return(dbSnapshot, nil)
	mockProvider.On("GetSnapshot", "snapshot-uuid", "volume-uuid").Return(snapshotResponse, nil)

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, snapshotResponse, result.SnapshotResponse)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapshotActivity_UseExistingSnapshot_EmptySnapshotName(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: true,
			SnapshotName:        "",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "", // Empty snapshot name
	}

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "snapshot name is empty in backup attributes")
	mockStorage.AssertExpectations(t)
}

func TestCreateSnapshotActivity_UseExistingSnapshot_GetSnapshotFromDBFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: true,
			SnapshotName:        "existing-snapshot",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "existing-snapshot",
	}

	mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, "existing-snapshot", volume.AccountID, volume.ID).Return(nil, errors.New("snapshot not found in database"))

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "snapshot not found in database")
	mockStorage.AssertExpectations(t)
}

func TestCreateSnapshotActivity_UseExistingSnapshot_GetSnapshotFromOntapFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	dbSnapshot := &datamodel.Snapshot{
		Name:     "existing-snapshot",
		VolumeID: 1,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			ExternalUUID: "snapshot-uuid",
		},
	}

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: true,
			SnapshotName:        "existing-snapshot",
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "existing-snapshot",
	}

	mockStorage.On("GetSnapshotByNameAndVolumeId", ctx, "existing-snapshot", volume.AccountID, volume.ID).Return(dbSnapshot, nil)
	mockProvider.On("GetSnapshot", "snapshot-uuid", "volume-uuid").Return(nil, errors.New("failed to get snapshot from ONTAP"))

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get snapshot from ONTAP")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapshotActivity_CreateNewSnapshot_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	snapshotResponse := &vsa.SnapshotProviderResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "new-snapshot-uuid",
		},
		SizeInBytes:        2048,
		LogicalSizeInBytes: 1024,
	}

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: false,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "test-backup",
	}

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: "volume-uuid",
		Name:       "test-backup",
		Comment:    "VCP-Backup",
	}).Return(snapshotResponse, nil)

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, snapshotResponse, result.SnapshotResponse)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCreateSnapshotActivity_CreateNewSnapshot_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		Name: "test-backup",
		Attributes: &datamodel.BackupAttributes{
			UseExistingSnapshot: false,
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			UUID: "volumeUUID",
			ID:   1,
		},
		AccountID: 2,
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid",
		},
	}

	state := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
		Node:         &models.Node{},
		SnapshotName: "test-backup",
	}

	mockProvider.On("CreateSnapshot", vsa.CreateSnapshotParams{
		VolumeUUID: "volume-uuid",
		Name:       "test-backup",
		Comment:    "VCP-Backup",
	}).Return(nil, errors.New("failed to create snapshot in ONTAP"))

	// Act
	result, err := activity.CreateSnapshotActivity(ctx, state)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to create snapshot in ONTAP")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestGetSnapshotFromObjectStore(t *testing.T) {
	t.Run("WhenProviderGetFails", func(tt *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider get failed")
		}

		ctx := context.Background()
		node := &models.Node{}

		result, err := activity.GetSnapshotFromObjectStore(ctx, node, "obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "provider get failed")
	})

	t.Run("WhenProviderGetSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		expectedSnapshot := &vsa.SmObjectStoreEndpointSnapshot{
			UUID: nillable.ToPointer(strfmt.UUID("snapshot-uuid")),
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("SnapmirrorObjectStoreSnapshotGet", "obj-uuid", "endpoint-uuid", "snapshot-uuid").Return(expectedSnapshot, nil)

		ctx := context.Background()
		node := &models.Node{}

		result, err := activity.GetSnapshotFromObjectStore(ctx, node, "obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.NoError(tt, err)
		assert.Equal(tt, expectedSnapshot, result)
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetObjectStoreSnapshotActivity(t *testing.T) {
	t.Run("WhenGetSnapshotFromObjectStoreFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("SnapmirrorObjectStoreSnapshotGet", "obj-uuid", "endpoint-uuid", "snapshot-uuid").Return(nil, errors.New("snapshot get failed"))

		ctx := context.Background()
		backupActivitiesContext := &activities.BackupActivitiesContext{
			Node: &models.Node{},
			ObjStore: &commonparams.CloudTarget{
				UUID: "obj-uuid",
			},
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup: &datamodel.Backup{
					Attributes: &datamodel.BackupAttributes{
						EndpointUUID: "endpoint-uuid",
						SnapshotID:   "snapshot-uuid",
					},
				},
			},
		}

		result, err := activity.GetObjectStoreSnapshotActivity(ctx, backupActivitiesContext)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "snapshot get failed")
	})

	t.Run("WhenGetSnapshotFromObjectStoreSucceedsWithLogicalSize", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		logicalSize := int64(1024)
		expectedSnapshot := &vsa.SmObjectStoreEndpointSnapshot{
			UUID:        nillable.ToPointer(strfmt.UUID("snapshot-uuid")),
			LogicalSize: &logicalSize,
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("SnapmirrorObjectStoreSnapshotGet", "obj-uuid", "endpoint-uuid", "snapshot-uuid").Return(expectedSnapshot, nil)

		ctx := context.Background()
		backupActivitiesContext := &activities.BackupActivitiesContext{
			Node: &models.Node{},
			ObjStore: &commonparams.CloudTarget{
				UUID: "obj-uuid",
			},
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup: &datamodel.Backup{
					Attributes: &datamodel.BackupAttributes{
						EndpointUUID: "endpoint-uuid",
						SnapshotID:   "snapshot-uuid",
					},
				},
			},
		}

		result, err := activity.GetObjectStoreSnapshotActivity(ctx, backupActivitiesContext)

		assert.NoError(tt, err)
		assert.Equal(tt, backupActivitiesContext, result)
		assert.Equal(tt, logicalSize, result.BackupWorkflowInit.Backup.SizeInBytes)
		assert.Equal(tt, expectedSnapshot, result.ObjStoreSnapshot)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetSnapshotFromObjectStoreSucceedsWithoutLogicalSize", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		expectedSnapshot := &vsa.SmObjectStoreEndpointSnapshot{
			UUID: nillable.ToPointer(strfmt.UUID("snapshot-uuid")),
			// LogicalSize is nil
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("SnapmirrorObjectStoreSnapshotGet", "obj-uuid", "endpoint-uuid", "snapshot-uuid").Return(expectedSnapshot, nil)

		ctx := context.Background()
		backupActivitiesContext := &activities.BackupActivitiesContext{
			Node: &models.Node{},
			ObjStore: &commonparams.CloudTarget{
				UUID: "obj-uuid",
			},
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup: &datamodel.Backup{
					Attributes: &datamodel.BackupAttributes{
						EndpointUUID: "endpoint-uuid",
						SnapshotID:   "snapshot-uuid",
					},
				},
			},
		}

		result, err := activity.GetObjectStoreSnapshotActivity(ctx, backupActivitiesContext)

		assert.NoError(tt, err)
		assert.Equal(tt, backupActivitiesContext, result)
		assert.Equal(tt, int64(0), result.BackupWorkflowInit.Backup.SizeInBytes)
		assert.Equal(tt, expectedSnapshot, result.ObjStoreSnapshot)
		mockProvider.AssertExpectations(tt)
	})
}

// TestIsSnapmirrorDeleted_ReturnsErrorWhenGetProviderFails tests error handling for provider lookup failure.
func TestIsSnapmirrorDeleted_ReturnsErrorWhenGetProviderFails(t *testing.T) {
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider lookup failed")
	}

	activity := activities.BackupActivity{}
	ctx := context.Background()
	node := &models.Node{}
	params := &commonparams.SnapmirrorRelationshipParams{
		DestinationPath: "/dest/path",
		SourcePath:      "/src/path",
	}
	deleted, err := activity.IsSnapmirrorDeleted(ctx, node, params)
	assert.False(t, deleted)
	assert.Error(t, err)
}

// TestIsSnapmirrorDeleted_ReturnsTrueWhenNotFound tests the case where the snapmirror is not found.
func TestIsSnapmirrorDeleted_ReturnsTrueWhenNotFound(t *testing.T) {
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	notFoundErr := utilerrors.NewNotFoundErr("SnapmirrorRelationship", nil)
	mockProvider.On("SnapmirrorRelationshipGet", "/dest/path", "/src/path").Return(nil, notFoundErr)

	activity := activities.BackupActivity{}
	ctx := context.Background()
	node := &models.Node{}
	params := &commonparams.SnapmirrorRelationshipParams{
		DestinationPath: "/dest/path",
		SourcePath:      "/src/path",
	}
	deleted, err := activity.IsSnapmirrorDeleted(ctx, node, params)
	assert.True(t, deleted)
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

// TestIsSnapmirrorDeleted_ReturnsErrorWhenOtherErrorOccurs tests error wrapping for non not-found errors.
func TestIsSnapmirrorDeleted_ReturnsErrorWhenOtherErrorOccurs(t *testing.T) {
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	mockProvider := new(vsa.MockProvider)
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	otherErr := errors.New("temporary error")
	mockProvider.On("SnapmirrorRelationshipGet", "/dest/path", "/src/path").Return(nil, otherErr)

	activity := activities.BackupActivity{}
	ctx := context.Background()
	node := &models.Node{}
	params := &commonparams.SnapmirrorRelationshipParams{
		DestinationPath: "/dest/path",
		SourcePath:      "/src/path",
	}
	deleted, err := activity.IsSnapmirrorDeleted(ctx, node, params)
	assert.False(t, deleted)
	assert.Error(t, err)
	mockProvider.AssertExpectations(t)
}

func TestGetObjectStoreEndpointInfo(t *testing.T) {
	t.Run("WhenProviderGetFails", func(tt *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider get failed")
		}

		ctx := context.Background()
		node := &models.Node{}

		result, err := activity.GetObjectStoreEndpointInfo(ctx, node, "obj-uuid", "endpoint-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "provider get failed")
	})

	t.Run("WhenProviderGetSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		expectedEndpointInfo := &vsa.SmObjectStoreEndpointt{
			UUID: nillable.ToPointer(strfmt.UUID("endpoint-uuid")),
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ObjectStoreEndpointInfoGet", "obj-uuid", "endpoint-uuid").Return(expectedEndpointInfo, nil)

		ctx := context.Background()
		node := &models.Node{}

		result, err := activity.GetObjectStoreEndpointInfo(ctx, node, "obj-uuid", "endpoint-uuid")

		assert.NoError(tt, err)
		assert.Equal(tt, expectedEndpointInfo, result)
		mockProvider.AssertExpectations(tt)
	})
}

func TestGetObjectStoreEndpointActivity(t *testing.T) {
	t.Run("WhenGetObjectStoreEndpointInfoFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ObjectStoreEndpointInfoGet", "obj-uuid", "endpoint-uuid").Return(nil, errors.New("endpoint info get failed"))

		ctx := context.Background()
		backupActivitiesContext := &activities.BackupActivitiesContext{
			Node: &models.Node{},
			ObjStore: &commonparams.CloudTarget{
				UUID: "obj-uuid",
			},
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup: &datamodel.Backup{
					Attributes: &datamodel.BackupAttributes{
						EndpointUUID: "endpoint-uuid",
					},
				},
			},
		}
		result, _ := activity.GetObjectStoreEndpointActivity(ctx, backupActivitiesContext)
		assert.Nil(tt, result)
	})

	t.Run("WhenGetObjectStoreEndpointInfoSucceeds", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		activity := activities.BackupActivity{SE: mockStorage}
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		expectedEndpointInfo := &vsa.SmObjectStoreEndpointt{
			UUID:        nillable.ToPointer(strfmt.UUID("endpoint-uuid")),
			LogicalSize: nillable.ToPointer(int64(1024)),
		}

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("ObjectStoreEndpointInfoGet", "obj-uuid", "endpoint-uuid").Return(expectedEndpointInfo, nil)

		ctx := context.Background()
		backupActivitiesContext := &activities.BackupActivitiesContext{
			Node: &models.Node{},
			ObjStore: &commonparams.CloudTarget{
				UUID: "obj-uuid",
			},
			BackupWorkflowInit: &activities.BackupWorkflowInput{
				Backup: &datamodel.Backup{
					Attributes: &datamodel.BackupAttributes{
						EndpointUUID: "endpoint-uuid",
					},
				},
			},
		}
		result, _ := activity.GetObjectStoreEndpointActivity(ctx, backupActivitiesContext)
		assert.Equal(tt, backupActivitiesContext, result)
	})
}

// Tests for CleanupOldAdhocBackupSnapshotsActivity

func TestCleanupOldAdhocBackupSnapshotsActivity_Success_MultipleSnapshots(t *testing.T) {
	// Test case 1: Successfully clean up older snapshots when multiple snapshots exist
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "volume-uuid-1",
		},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots - newest first (as returned by DB query)
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 3, UUID: "snapshot-uuid-3"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-3"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-older1", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older2", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-1"},
		},
	}

	// Mock database call to get snapshots
	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock successful deletion from database for older snapshots
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-2").
		Return(&datamodel.Snapshot{}, nil)
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").
		Return(&datamodel.Snapshot{}, nil)

	// Mock hyperscaler provider for ONTAP deletion
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock successful ONTAP deletion
	mockProvider.On("DeleteSnapshot", "snap-uuid-2", "volume-uuid-1").Return(nil)
	mockProvider.On("DeleteSnapshot", "snap-uuid-1", "volume-uuid-1").Return(nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_Success_SingleSnapshot(t *testing.T) {
	// Test case 2: No cleanup needed when only one snapshot exists
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create single snapshot
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-only", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-1"},
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	// No deletion calls should be made
}

func TestCleanupOldAdhocBackupSnapshotsActivity_Success_NoSnapshots(t *testing.T) {
	// Test case 3: No cleanup needed when no snapshots exist
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Return empty snapshot list
	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return([]*datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_OntapError_ContinueProcessing(t *testing.T) {
	// Test case 5: Handle ONTAP deletion error - should mark snapshot as error and continue
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-1"},
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock hyperscaler provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock ONTAP deletion failure
	ontapError := errors.New("ONTAP service unavailable")
	mockProvider.On("DeleteSnapshot", "snap-uuid-1", "volume-uuid-1").Return(ontapError)

	// Mock marking snapshot as error
	mockStorage.On("UpdateSnapshot", ctx, mock.MatchedBy(func(snapshot *datamodel.Snapshot) bool {
		return snapshot.UUID == "snapshot-uuid-1" &&
			snapshot.State == models.LifeCycleStateError &&
			snapshot.StateDetails == "Failed to delete from ONTAP: ONTAP service unavailable"
	})).Return(&datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err) // Should not fail the entire operation
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_SnapshotAttributesNil(t *testing.T) {
	// Test case 7: Handle snapshot with nil SnapshotAttributes
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots with nil SnapshotAttributes
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: nil, // Nil attributes
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock database deletion (should skip ONTAP deletion due to nil attributes)
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").
		Return(&datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_EmptyExternalUUID(t *testing.T) {
	// Test case 8: Handle snapshot with empty ExternalUUID
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots with empty ExternalUUID
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: ""}, // Empty external UUID
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock database deletion (should skip ONTAP deletion due to empty ExternalUUID)
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").
		Return(&datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_MarkSnapshotAsErrorFails(t *testing.T) {
	// Test case 9: Handle failure when marking snapshot as error
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-1"},
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock hyperscaler provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock ONTAP deletion failure
	ontapError := errors.New("ONTAP service unavailable")
	mockProvider.On("DeleteSnapshot", "snap-uuid-1", "volume-uuid-1").Return(ontapError)

	// Mock failure when marking snapshot as error
	updateError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, errors.New("database update failed"))
	mockStorage.On("UpdateSnapshot", ctx, mock.AnythingOfType("*datamodel.Snapshot")).Return(nil, updateError)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err) // Should still not fail the entire operation
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_Integration_FullWorkflow(t *testing.T) {
	// Integration test: Test the full cleanup workflow with mixed success and failure scenarios
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-1"},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create multiple test snapshots
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{ID: 5, UUID: "snapshot-uuid-5"},
			Name:      "backup-adhoc-latest", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-5"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 4, UUID: "snapshot-uuid-4"},
			Name:      "backup-adhoc-older1", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-4"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 3, UUID: "snapshot-uuid-3"},
			Name:      "backup-adhoc-older2", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-3"},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2, UUID: "snapshot-uuid-2"},
			Name:      "backup-adhoc-older3", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "snap-uuid-2"},
		},
		// Snapshot with nil attributes
		{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "snapshot-uuid-1"},
			Name:      "backup-adhoc-older4", Type: "backup-adhoc", VolumeID: 1, State: models.LifeCycleStateREADY,
			SnapshotAttributes: nil,
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock hyperscaler provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mixed ONTAP results
	mockProvider.On("DeleteSnapshot", "snap-uuid-4", "volume-uuid-1").Return(nil)                         // Success
	mockProvider.On("DeleteSnapshot", "snap-uuid-3", "volume-uuid-1").Return(errors.New("ONTAP timeout")) // Failure
	mockProvider.On("DeleteSnapshot", "snap-uuid-2", "volume-uuid-1").Return(nil)                         // Success
	// snap-uuid-1 won't be called due to nil attributes

	// Mixed database results
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-4").Return(&datamodel.Snapshot{}, nil) // Success
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-2").Return(&datamodel.Snapshot{}, nil) // Success
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").Return(&datamodel.Snapshot{}, nil) // Success (nil attributes case)

	// Mock marking snapshot as error for failed ONTAP deletion
	mockStorage.On("UpdateSnapshot", ctx, mock.MatchedBy(func(snapshot *datamodel.Snapshot) bool {
		return snapshot.UUID == "snapshot-uuid-3" &&
			snapshot.State == models.LifeCycleStateError
	})).Return(&datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err) // Should not fail despite partial failures
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_DatabaseDeletionError(t *testing.T) {
	// Test case: Handle database deletion error and mark snapshot as error
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1", ID: 1},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid-latest", CreatedAt: time.Now()},
			Name:      "latest-snapshot",
			VolumeID:  1,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "snap-uuid-latest",
			},
		},
		{
			BaseModel: datamodel.BaseModel{CreatedAt: time.Now().Add(-1 * time.Hour), UUID: "snapshot-uuid-1"},
			Name:      "old-snapshot-1",
			VolumeID:  1,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "snap-uuid-1",
			},
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock hyperscaler provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock successful ONTAP deletion
	mockProvider.On("DeleteSnapshot", "snap-uuid-1", "external-volume-uuid").Return(nil)

	// Mock database deletion failure
	dbError := errors.New("database connection error")
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").Return(nil, dbError)

	// Mock marking snapshot as error
	mockStorage.On("UpdateSnapshot", ctx, mock.MatchedBy(func(snapshot *datamodel.Snapshot) bool {
		return snapshot.UUID == "snapshot-uuid-1" &&
			snapshot.State == models.LifeCycleStateError &&
			strings.Contains(snapshot.StateDetails, "Failed to delete from database: database connection error")
	})).Return(&datamodel.Snapshot{}, nil)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err) // Should not fail the entire operation
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestCleanupOldAdhocBackupSnapshotsActivity_DatabaseDeletionError_MarkAsErrorFails(t *testing.T) {
	// Test case: Handle database deletion error when marking snapshot as error also fails
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)

	activity := activities.BackupActivity{SE: mockStorage}
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid-1", ID: 1},
		Name:      "test-volume",
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
		},
	}
	node := &models.Node{EndpointAddress: "test-node-address"}

	// Create test snapshots
	snapshots := []*datamodel.Snapshot{
		{
			BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid-latest", CreatedAt: time.Now()},
			Name:      "latest-snapshot",
			VolumeID:  1,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "snap-uuid-latest",
			},
		},
		{
			BaseModel: datamodel.BaseModel{CreatedAt: time.Now().Add(-1 * time.Hour), UUID: "snapshot-uuid-1"},
			Name:      "old-snapshot-1",
			VolumeID:  1,
			SnapshotAttributes: &datamodel.SnapshotAttributes{
				ExternalUUID: "snap-uuid-1",
			},
		},
	}

	mockStorage.On("GetSnapshotsByTypeAndVolumeID", ctx, "backup-adhoc", int64(1)).
		Return(snapshots, nil)

	// Mock hyperscaler provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	// Mock successful ONTAP deletion
	mockProvider.On("DeleteSnapshot", "snap-uuid-1", "external-volume-uuid").Return(nil)

	// Mock database deletion failure
	dbError := errors.New("database connection error")
	mockStorage.On("DeleteSnapshot", ctx, "snapshot-uuid-1").Return(nil, dbError)

	// Mock failure when marking snapshot as error
	updateError := errors.New("failed to update snapshot")
	mockStorage.On("UpdateSnapshot", ctx, mock.AnythingOfType("*datamodel.Snapshot")).Return(nil, updateError)

	// Execute the activity
	err := activity.CleanupOldAdhocBackupSnapshotsActivity(ctx, volume, node)

	// Assertions
	assert.NoError(t, err) // Should still not fail the entire operation
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshotForBackup_UseExistingSnapshot_SkipsDeletion(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	node := &models.Node{Name: "test-node"}
	snapshotUUID := "snapshot-uuid-123"
	volumeUUID := "volume-uuid-456"
	useExistingSnapshot := true

	// Mock the provider
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID, useExistingSnapshot)

	// Assert
	assert.NoError(t, err)
	// Ensure DeleteSnapshot was NOT called on the provider
	mockProvider.AssertNotCalled(t, "DeleteSnapshot", mock.Anything, mock.Anything)
}

func TestDeleteSnapshotForBackup_UseExistingSnapshot_GetProviderError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	node := &models.Node{Name: "test-node"}
	snapshotUUID := "snapshot-uuid-123"
	volumeUUID := "volume-uuid-456"
	useExistingSnapshot := true

	// Mock provider lookup failure
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	providerError := errors.New("provider lookup failed")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, providerError
	}

	// Act
	err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID, useExistingSnapshot)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider lookup failed")
}

func TestUpdateBackupSizeActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			LatestLogicalBackupSize: 0, // Initial value
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "test-volume-uuid", "test-backup-uuid").Return(nil)
	mockStorage.On("UpdateVolumeFields", ctx, "test-volume-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

	// Act
	result, err := activity.UpdateBackupSizeActivity(ctx, backupActivitiesContext)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, backupActivitiesContext, result)
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupSizeActivity_UpdateBackupFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			LatestLogicalBackupSize: 0, // Initial value
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(nil, errors.New("update backup failed"))

	// Act
	result, err := activity.UpdateBackupSizeActivity(ctx, backupActivitiesContext)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "update backup failed")
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupSizeActivity_UpdateBackupLatestLogicalBackupSizeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			LatestLogicalBackupSize: 0, // Initial value
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "test-volume-uuid", "test-backup-uuid").Return(errors.New("update latest logical backup size failed"))

	// Act
	result, err := activity.UpdateBackupSizeActivity(ctx, backupActivitiesContext)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "update latest logical backup size failed")
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupSizeActivity_UpdateVolumeFieldsFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 1024,
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			LatestLogicalBackupSize: 0, // Initial value
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)
	mockStorage.On("UpdateBackupLatestLogicalBackupSizeByVolume", ctx, "test-volume-uuid", "test-backup-uuid").Return(nil)
	mockStorage.On("UpdateVolumeFields", ctx, "test-volume-uuid", mock.AnythingOfType("map[string]interface {}")).Return(errors.New("update volume fields failed"))

	// Act
	result, err := activity.UpdateBackupSizeActivity(ctx, backupActivitiesContext)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "update volume fields failed")
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupSizeActivity_SkipsLatestLogicalBackupSizeUpdateWhenZero(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backup := &datamodel.Backup{
		BaseModel:               datamodel.BaseModel{UUID: "test-backup-uuid"},
		VolumeUUID:              "test-volume-uuid",
		LatestLogicalBackupSize: 0, // This should skip the UpdateBackupLatestLogicalBackupSizeByVolume call
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			LatestLogicalBackupSize: 0, // Initial value
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			Backup: backup,
			Volume: volume,
		},
	}

	mockStorage.On("FinishBackup", ctx, backup).Return(backup, nil)
	mockStorage.On("UpdateVolumeFields", ctx, "test-volume-uuid", mock.AnythingOfType("map[string]interface {}")).Return(nil)

	// Act
	result, err := activity.UpdateBackupSizeActivity(ctx, backupActivitiesContext)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, backupActivitiesContext, result)
	mockStorage.AssertExpectations(t)
	// Verify that UpdateBackupLatestLogicalBackupSizeByVolume was not called
	mockStorage.AssertNotCalled(t, "UpdateBackupLatestLogicalBackupSizeByVolume")
}

// Test HydrateSnapshotToCCFEActivity
func TestHydrateSnapshotToCCFEActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the hydration functions
	originalBatchHydrateCreatedSnapshots := commonparams.BatchHydrateCreatedSnapshots
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() {
		commonparams.BatchHydrateCreatedSnapshots = originalBatchHydrateCreatedSnapshots
		auth.GenerateCallbackToken = originalGenerateCallbackToken
	}()

	commonparams.BatchHydrateCreatedSnapshots = func(ctx context.Context, logger log.Logger, requests []models.Request, volumeName, region, projectId, token string) error {
		return nil
	}
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	backupVault := &datamodel.BackupVault{
		RegionName: "us-central1",
	}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{
			Name: "test-project",
		},
		Name: "test-volume",
	}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateAvailableDetails,
		Description:  "test description",
		Volume:       volume,
		Account:      volume.Account,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes: 1024,
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
		DbSnapshot: snapshot,
	}

	// Act
	err := activity.HydrateSnapshotToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, volume.Name, "us-central1", "test-project")

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotToCCFEActivity_NoSnapshot(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{},
		DbSnapshot:         nil,
	}

	// Act
	err := activity.HydrateSnapshotToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, "test-volume", "us-central1", "test-project")

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotToCCFEActivity_TokenGenerationFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock token generation failure
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "", errors.New("token generation failed")
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{},
		DbSnapshot:         &datamodel.Snapshot{},
	}

	// Act
	err := activity.HydrateSnapshotToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, "test-volume", "us-central1", "test-project")

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token generation failed")
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotToCCFEActivity_HydrationFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the hydration functions
	originalBatchHydrateCreatedSnapshots := commonparams.BatchHydrateCreatedSnapshots
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() {
		commonparams.BatchHydrateCreatedSnapshots = originalBatchHydrateCreatedSnapshots
		auth.GenerateCallbackToken = originalGenerateCallbackToken
	}()

	commonparams.BatchHydrateCreatedSnapshots = func(ctx context.Context, logger log.Logger, requests []models.Request, volumeName, region, projectId, token string) error {
		return errors.New("hydration failed")
	}
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	backupVault := &datamodel.BackupVault{
		RegionName: "us-central1",
	}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{
			Name: "test-project",
		},
		Name: "test-volume",
	}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateAvailableDetails,
		Description:  "test description",
		Volume:       volume,
		Account:      volume.Account,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes: 1024,
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
		DbSnapshot: snapshot,
	}

	// Act
	err := activity.HydrateSnapshotToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, volume.Name, "us-central1", "test-project")

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "hydration failed")
	mockStorage.AssertExpectations(t)
}

// Test convertSnapshotToGCPHydrateSnapshot
func TestConvertSnapshotToGCPHydrateSnapshot_WithAllFields(t *testing.T) {
	// Arrange
	volume := &datamodel.Volume{
		Name: "test-volume",
	}
	account := &datamodel.Account{
		Name: "test-account",
	}
	snapshot := datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateREADY,
		StateDetails: models.LifeCycleStateAvailableDetails,
		Description:  "test description",
		Volume:       volume,
		Account:      account,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes: 1024,
		},
	}

	// Act
	result := activities.ConvertSnapshotToGCPHydrateSnapshot(snapshot)

	// Assert
	assert.Equal(t, "test-snapshot", result.ResourceId)
	assert.Equal(t, "snapshot-uuid", result.SnapshotId)
	assert.Equal(t, models.LifeCycleStateREADY, result.State)
	assert.Equal(t, models.LifeCycleStateAvailableDetails, result.StateDetails)
	assert.Equal(t, "test description", result.Description)
	assert.Equal(t, int64(1024), result.UsedBytes)
	assert.Equal(t, snapshot.CreatedAt, result.CreateTime)
	assert.Equal(t, "test-volume", result.VolumeName)
	assert.Equal(t, "test-account", result.AccountName)
}

func TestConvertSnapshotToGCPHydrateSnapshot_WithMinimalFields(t *testing.T) {
	// Arrange
	volume := &datamodel.Volume{
		Name: "test-volume",
	}
	account := &datamodel.Account{
		Name: "test-account",
	}
	snapshot := datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:    "test-snapshot",
		State:   models.LifeCycleStateREADY,
		Volume:  volume,
		Account: account,
	}

	// Act
	result := activities.ConvertSnapshotToGCPHydrateSnapshot(snapshot)

	// Assert
	assert.Equal(t, "test-snapshot", result.ResourceId)
	assert.Equal(t, "snapshot-uuid", result.SnapshotId)
	assert.Equal(t, models.LifeCycleStateREADY, result.State)
	assert.Equal(t, "", result.StateDetails)
	assert.Equal(t, "", result.Description)
	assert.Equal(t, int64(0), result.UsedBytes)
	assert.Equal(t, snapshot.CreatedAt, result.CreateTime)
	assert.Equal(t, "test-volume", result.VolumeName)
	assert.Equal(t, "test-account", result.AccountName)
}

func TestConvertSnapshotToGCPHydrateSnapshot_WithNilSnapshotAttributes(t *testing.T) {
	// Arrange
	volume := &datamodel.Volume{
		Name: "test-volume",
	}
	account := &datamodel.Account{
		Name: "test-account",
	}
	snapshot := datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:               "test-snapshot",
		State:              models.LifeCycleStateREADY,
		StateDetails:       models.LifeCycleStateAvailableDetails,
		Description:        "test description",
		Volume:             volume,
		Account:            account,
		SnapshotAttributes: nil,
	}

	// Act
	result := activities.ConvertSnapshotToGCPHydrateSnapshot(snapshot)

	// Assert
	assert.Equal(t, "test-snapshot", result.ResourceId)
	assert.Equal(t, "snapshot-uuid", result.SnapshotId)
	assert.Equal(t, models.LifeCycleStateREADY, result.State)
	assert.Equal(t, models.LifeCycleStateAvailableDetails, result.StateDetails)
	assert.Equal(t, "test description", result.Description)
	assert.Equal(t, int64(0), result.UsedBytes)
	assert.Equal(t, snapshot.CreatedAt, result.CreateTime)
	assert.Equal(t, "test-volume", result.VolumeName)
	assert.Equal(t, "test-account", result.AccountName)
}

// Test HydrateSnapshotDeletionToCCFEActivity
func TestHydrateSnapshotDeletionToCCFEActivity_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the hydration functions
	originalBatchHydrateDeletedSnapshots := commonparams.BatchHydrateDeletedSnapshots
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() {
		commonparams.BatchHydrateDeletedSnapshots = originalBatchHydrateDeletedSnapshots
		auth.GenerateCallbackToken = originalGenerateCallbackToken
	}()

	commonparams.BatchHydrateDeletedSnapshots = func(ctx context.Context, logger log.Logger, requests []models.Request, volumeName, region, projectId, token string) error {
		return nil
	}
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	backupVault := &datamodel.BackupVault{
		RegionName: "us-central1",
	}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{
			Name: "test-project",
		},
		Name: "test-volume",
	}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateDeleted,
		StateDetails: models.LifeCycleStateDeletedDetails,
		Description:  "test description",
		Volume:       volume,
		Account:      volume.Account,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes: 1024,
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
		DbSnapshot: snapshot,
	}

	// Act
	err := activity.HydrateSnapshotDeletionToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, volume.Name, "us-central1", "test-project")

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotDeletionToCCFEActivity_NoSnapshot(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{},
		DbSnapshot:         nil,
	}

	// Act
	err := activity.HydrateSnapshotDeletionToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, "test-volume", "us-central1", "test-project")

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotDeletionToCCFEActivity_TokenGenerationFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock token generation failure
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() { auth.GenerateCallbackToken = originalGenerateCallbackToken }()
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "", errors.New("token generation failed")
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{},
		DbSnapshot:         &datamodel.Snapshot{},
	}

	// Act
	err := activity.HydrateSnapshotDeletionToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, "test-volume", "us-central1", "test-project")

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token generation failed")
	mockStorage.AssertExpectations(t)
}

func TestHydrateSnapshotDeletionToCCFEActivity_HydrationFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.BackupActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the hydration functions
	originalBatchHydrateDeletedSnapshots := commonparams.BatchHydrateDeletedSnapshots
	originalGenerateCallbackToken := auth.GenerateCallbackToken
	defer func() {
		commonparams.BatchHydrateDeletedSnapshots = originalBatchHydrateDeletedSnapshots
		auth.GenerateCallbackToken = originalGenerateCallbackToken
	}()

	commonparams.BatchHydrateDeletedSnapshots = func(ctx context.Context, logger log.Logger, requests []models.Request, volumeName, region, projectId, token string) error {
		return errors.New("deletion hydration failed")
	}
	auth.GenerateCallbackToken = func(ctx context.Context) (string, error) {
		return "test-token", nil
	}

	backupVault := &datamodel.BackupVault{
		RegionName: "us-central1",
	}
	volume := &datamodel.Volume{
		Account: &datamodel.Account{
			Name: "test-project",
		},
		Name: "test-volume",
	}
	snapshot := &datamodel.Snapshot{
		BaseModel: datamodel.BaseModel{
			UUID:      "snapshot-uuid",
			CreatedAt: time.Now(),
		},
		Name:         "test-snapshot",
		State:        models.LifeCycleStateDeleted,
		StateDetails: models.LifeCycleStateDeletedDetails,
		Description:  "test description",
		Volume:       volume,
		Account:      volume.Account,
		SnapshotAttributes: &datamodel.SnapshotAttributes{
			SizeInBytes: 1024,
		},
	}

	backupActivitiesContext := &activities.BackupActivitiesContext{
		BackupWorkflowInit: &activities.BackupWorkflowInput{
			BackupVault: backupVault,
			Volume:      volume,
		},
		DbSnapshot: snapshot,
	}

	// Act
	err := activity.HydrateSnapshotDeletionToCCFEActivity(ctx, backupActivitiesContext.DbSnapshot, volume.Name, "us-central1", "test-project")

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "deletion hydration failed")
	mockStorage.AssertExpectations(t)
}
