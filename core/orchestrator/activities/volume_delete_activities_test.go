package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: 10, UUID: volumeID}}

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(expectedVolume, nil)
	mockStorage.On("DeleteSnapshot", ctx, mock.Anything).Return(&datamodel.Snapshot{}, nil).Maybe()
	// Act
	err := activity.DeleteVolume(ctx, expectedVolume)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolume_Success_VolumeAlreadyDeleted(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedVolume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeID}}

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(nil, utilErrors.NewNotFoundErr("volume", nil))

	// Act
	err := activity.DeleteVolume(ctx, expectedVolume)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolume_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := "test-volume-id"
	expectedError := errors.New("volume not found")

	mockStorage.On("DeleteVolume", ctx, volumeID).Return(nil, expectedError)

	// Act
	err := activity.DeleteVolume(ctx, &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeID}})

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeExternalUUID := "uuid-123"
	volumeName := "test-volume"

	node := &models.Node{}

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volumeExternalUUID, volumeName).Return(nil)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volumeExternalUUID, volumeName, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeExternalUUID := "uuid-123"
	volumeName := "test-volume"

	node := &models.Node{}
	expectedError := errors.New("failed to delete volume in ONTAP")

	// Mock the DeleteVolume method
	mockProvider.On("DeleteVolume", volumeExternalUUID, volumeName).Return(expectedError)

	// Act
	err := activity.DeleteVolumeInONTAP(ctx, volumeExternalUUID, volumeName, node)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshotPolicyInONTAP_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{
				{
					DaysOfMonth:     []int{1, 15},
					DaysOfWeek:      []int{2},
					Hours:           []int{3},
					Minutes:         []int{0},
					SnapmirrorLabel: "label1",
					Count:           5,
				},
			},
		},
	}

	node := &models.Node{}

	// Mock the DeleteSnapshotPolicy method
	mockProvider.On("DeleteSnapshotPolicy", volume.SnapshotPolicy.Name).Return(nil)

	// Act
	err := activity.DeleteSnapshotPolicyInONTAP(ctx, volume.SnapshotPolicy.Name, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshotPolicyInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{
				{
					DaysOfMonth:     []int{1, 15},
					DaysOfWeek:      []int{2},
					Hours:           []int{3},
					Minutes:         []int{0},
					SnapmirrorLabel: "label1",
					Count:           5,
				},
			},
		},
	}

	node := &models.Node{}
	expectedError := errors.New("failed to delete snapshotPolicy in ONTAP")

	// Mock the DeleteSnapshotPolicy method
	mockProvider.On("DeleteSnapshotPolicy", volume.SnapshotPolicy.Name).Return(expectedError)

	// Act
	err := activity.DeleteSnapshotPolicyInONTAP(ctx, volume.SnapshotPolicy.Name, node)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPDeletesWhenBackupsExist(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "test-volume-uuid"
	node := &models.Node{}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockProvider.On("SnapmirrorRelationshipDelete", volumeUUID).Return(&vsa.OntapAsyncResponse{}, nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volumeUUID, node)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSkipsWhenNoBackupsExist(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "test-volume-uuid"
	node := &models.Node{}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(0), nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volumeUUID, node)

	assert.NoError(t, err)
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPFailsWhenBackupCountFails(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "test-volume-uuid"
	node := &models.Node{}
	expectedError := errors.New("failed to fetch backup count")

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(0), expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volumeUUID, node)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPFailsWhenDeleteFails(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeUUID := "test-volume-uuid"
	node := &models.Node{}
	expectedError := errors.New("failed to delete snapmirror relationship")

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockProvider.On("SnapmirrorRelationshipDelete", volumeUUID).Return(nil, expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volumeUUID, node)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteVolumeAssociatedSnapshots_NoSnapshotsFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := int64(123)

	mockStorage.On("GetSnapshotsByVolumeID", mock.Anything, volumeID).
		Return(nil, utilErrors.NewNotFoundErr("snapshot", nil))

	err := activity.DeleteVolumeAssociatedSnapshots(ctx, volumeID)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolumeAssociatedSnapshots_GetSnapshotsError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := int64(123)

	mockStorage.On("GetSnapshotsByVolumeID", mock.Anything, volumeID).
		Return(nil, errors.New("db error"))

	err := activity.DeleteVolumeAssociatedSnapshots(ctx, volumeID)
	assert.Error(t, err)
	assert.EqualError(t, err, "db error")
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolumeAssociatedSnapshots_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := int64(123)
	snapshots := []*datamodel.Snapshot{
		{BaseModel: datamodel.BaseModel{UUID: "snap-1"}, Name: "snap1"},
		{BaseModel: datamodel.BaseModel{UUID: "snap-2"}, Name: "snap2"},
	}

	mockStorage.On("GetSnapshotsByVolumeID", mock.Anything, volumeID).
		Return(snapshots, nil)
	mockStorage.On("DeleteSnapshot", ctx, "snap-1").Return(&datamodel.Snapshot{}, nil)
	mockStorage.On("DeleteSnapshot", ctx, "snap-2").Return(&datamodel.Snapshot{}, nil)

	err := activity.DeleteVolumeAssociatedSnapshots(ctx, volumeID)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteVolumeAssociatedSnapshots_DeleteSnapshotError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeID := int64(123)
	snapshots := []*datamodel.Snapshot{
		{BaseModel: datamodel.BaseModel{UUID: "snap-1"}, Name: "snap1"},
		{BaseModel: datamodel.BaseModel{UUID: "snap-2"}, Name: "snap2"},
	}

	mockStorage.On("GetSnapshotsByVolumeID", mock.Anything, volumeID).
		Return(snapshots, nil)
	mockStorage.On("DeleteSnapshot", ctx, "snap-1").Return(&datamodel.Snapshot{}, errors.New("delete error"))
	mockStorage.On("DeleteSnapshot", ctx, "snap-2").Return(&datamodel.Snapshot{}, nil)

	err := activity.DeleteVolumeAssociatedSnapshots(ctx, volumeID)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}
