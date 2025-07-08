package activities_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	utilerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

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
	assert.Equal(t, errorString, backup.StateDetails)
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
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		activities.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	result, err := activity.SnapmirrorGetorCreate(ctx, node, SnapmirrorRelationshipParams)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse.Destination.UUID.String(), *result.DestinationUUID)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorGetorCreate_GetProviderByNode(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := activities.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		activities.GetProviderByNode = originalGetProviderByNode
	}()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	result, err := activity.SnapmirrorGetorCreate(ctx, node, SnapmirrorRelationshipParams)

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
	originalGetProviderByNode := activities.GetProviderByNode
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
	originalGenerateTokenForNode := activities.GenerateTokenForNode
	activity := activities.BackupActivity{SE: mockStorage}
	defer func() {
		activities.GetProviderByNode = originalGetProviderByNode
		activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
		activities.GenerateTokenForNode = originalGenerateTokenForNode
	}()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	result, err := activity.SnapmirrorGetorCreate(ctx, node, SnapmirrorRelationshipParams)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse.UUID.String(), result.UUID)
	mockProvider.AssertExpectations(t)
}

func TestSnapshotCreate_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"
	expectedResponse := &ontap_rest.CloudTarget{CloudTarget: oModels.CloudTarget{Name: nillable.ToPointer(objStoreName), Container: nillable.ToPointer(bucketName)}}

	mockProvider.On("CloudTargetGet", &objStoreName).Return(nil, errors.New("not found"))
	mockProvider.On("CloudTargetCreate", objStoreName, bucketName).Return(expectedResponse, nil)

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, *expectedResponse.Name, result.Name)
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			token := "mock-token"
			return &token, nil
		}
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		activities.GetSmcLicenseFromCloud = func(ctx context.Context) (string, error) {
			return "mock-license", nil
		}
		activities.GenerateTokenForNode = func(ctx context.Context, node *models.Node, clientSecret *string) (*string, error) {
			token := "mock-token"
			return &token, nil
		}
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		state := "success"

		mockProvider.On("SnapmirrorRelationshipTransferGet", snapmirrorUUID, snapshotName).Return(&ontap_rest.SnapmirrorTransfer{SnapmirrorTransfer: oModels.SnapmirrorTransfer{State: &state}}, nil)
		err := activity.SnapmirrorTransferPoll(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransferPoll_WhenTransferFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"
		state := "failed"

		mockProvider.On("SnapmirrorRelationshipTransferGet", snapmirrorUUID, snapshotName).Return(&ontap_rest.SnapmirrorTransfer{SnapmirrorTransfer: oModels.SnapmirrorTransfer{State: &state}}, fmt.Errorf("Snapmirror transfer failed with state: failed"))

		err := activity.SnapmirrorTransferPoll(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Snapmirror transfer failed with state: failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("DeleteSnapshot_WhenDeleteSucceeds_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	t.Run("SnapmirrorTransfer_WhenGetSmcLicenseFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		originalGetSmcLicenseFromCloud := activities.GetSmcLicenseFromCloud
		originalGenerateTokenForNode := activities.GenerateTokenForNode
		activity := activities.BackupActivity{}
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
			activities.GetSmcLicenseFromCloud = originalGetSmcLicenseFromCloud
			activities.GenerateTokenForNode = originalGenerateTokenForNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(nil)
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID)
		assert.Nil(t, err)
	})
	t.Run("onFailure", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		mockProvider.On("DeleteSnapshot", snapshotUUID, volumeUUID).Return(errors.New("delete failed"))
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID)
		assert.Error(t, err)
		assert.EqualError(t, err, "delete failed")
	})
	t.Run("onGetProviderByNodeFailure", func(t *testing.T) {
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		node := &models.Node{}
		snapshotUUID := "snapshot-uuid"
		volumeUUID := "volume-uuid"
		err := activity.DeleteSnapshotForBackup(ctx, node, snapshotUUID, volumeUUID)
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to get provider")
	})
}
