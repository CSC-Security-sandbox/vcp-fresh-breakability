package activities_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/mock"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"

	"github.com/stretchr/testify/assert"
	oModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
	backup := &datamodel.Backup{Name: "test-backup"}

	mockStorage.On("GetBackup", ctx, backupUUID).Return(backup, nil)

	// Act
	result, err := activity.GetBackup(ctx, backupUUID)

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

	mockStorage.On("GetBackup", ctx, backupUUID).Return(nil, errors.New("backup not found"))

	// Act
	result, err := activity.GetBackup(ctx, backupUUID)

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
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}
	ct := oModels.CloudTarget{
		Name:      nillable.ToPointer("targetName"),
		Container: nillable.ToPointer("container"),
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

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	sourcePath := "source-path"
	destinationPath := "destination-path"
	expectedResponse := &ontap_rest.SnapmirrorRelationship{SnapmirrorRelationship: oModels.SnapmirrorRelationship{UUID: nillable.ToPointer(strfmt.UUID("smUUID")), Destination: &oModels.SnapmirrorEndpoint{UUID: nillable.ToPointer(strfmt.UUID("uuid"))}}}

	mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(expectedResponse, nil)

	// Act
	result, err := activity.SnapmirrorGetorCreate(ctx, node, sourcePath, destinationPath)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse.Destination.UUID.String(), *result.DestinationUUID)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorGetorCreate_CreateNew(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	sourcePath := "source-path"
	destinationPath := "destination-path"
	expectedResponse := &ontap_rest.SnapmirrorRelationship{SnapmirrorRelationship: oModels.SnapmirrorRelationship{UUID: nillable.ToPointer(strfmt.UUID("smUUID")), Destination: &oModels.SnapmirrorEndpoint{UUID: nillable.ToPointer(strfmt.UUID("uuid"))}}}

	mockProvider.On("SnapmirrorRelationshipGet", destinationPath, sourcePath).Return(nil, errors.New("not found"))
	mockProvider.On("SnapmirrorRelationshipCreate", destinationPath, sourcePath).Return(expectedResponse, nil)

	// Act
	result, err := activity.SnapmirrorGetorCreate(ctx, node, sourcePath, destinationPath)

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

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
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

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
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

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}
	objStoreName := "test-objstore"
	bucketName := "test-bucket"
	expectedResponse := &ontap_rest.CloudTarget{CloudTarget: oModels.CloudTarget{Name: nillable.ToPointer(objStoreName), Container: nillable.ToPointer(bucketName)}}

	mockProvider.On("CloudTargetGet", &objStoreName).Return(expectedResponse, nil)

	// Act
	result, err := activity.GetOrCreateObjectStore(ctx, node, objStoreName, bucketName)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, *expectedResponse.Name, result.Name)
	mockProvider.AssertExpectations(t)
}

func TestGetOrCreateObjectStore_CreateNew(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := activities.GetProviderByNode

	activity := activities.BackupActivity{SE: mockStorage}
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
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

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
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
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		mockProvider.On("SnapmirrorRelationshipTransferCreate", snapmirrorUUID, snapshotName).Return(nil)

		err := activity.SnapmirrorTransfer(context.Background(), node, snapmirrorUUID, snapshotName)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("SnapmirrorTransfer_WhenTransferFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		activity := activities.BackupActivity{}
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() { activities.GetProviderByNode = originalGetProviderByNode }()
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
		}

		node := &models.Node{}
		snapmirrorUUID := "snapmirror-uuid"
		snapshotName := "snapshot-name"

		mockProvider.On("SnapmirrorRelationshipTransferCreate", snapmirrorUUID, snapshotName).Return(errors.New("transfer failed"))

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
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
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
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
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
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
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
		activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
			return mockProvider
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
}
