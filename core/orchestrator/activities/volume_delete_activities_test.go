package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	oModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-123",
		},
	}
	node := &models.Node{}

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	// Mock snapmirror relationship
	mockSnapmirror := &ontap_rest.SnapmirrorRelationship{
		SnapmirrorRelationship: oModels.SnapmirrorRelationship{
			UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid-123")),
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)
	mockProvider.On("SnapmirrorRelationshipGet", "test-bucket:/objstore/test-volume-uuid", "test-svm:test-volume").Return(mockSnapmirror, nil)
	mockProvider.On("SnapmirrorRelationshipDelete", "snapmirror-uuid-123").Return(&vsa.OntapAsyncResponse{}, nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

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
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}}
	node := &models.Node{}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(0), nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

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
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: volumeUUID}}
	node := &models.Node{}
	expectedError := errors.New("failed to fetch backup count")

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(0), expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to fetch backup count")
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-123",
		},
	}
	node := &models.Node{}
	expectedError := errors.New("failed to delete snapmirror relationship")

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	// Mock snapmirror relationship
	mockSnapmirror := &ontap_rest.SnapmirrorRelationship{
		SnapmirrorRelationship: oModels.SnapmirrorRelationship{
			UUID: nillable.ToPointer(strfmt.UUID("snapmirror-uuid-123")),
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)
	mockProvider.On("SnapmirrorRelationshipGet", "test-bucket:/objstore/test-volume-uuid", "test-svm:test-volume").Return(mockSnapmirror, nil)
	mockProvider.On("SnapmirrorRelationshipDelete", "snapmirror-uuid-123").Return(nil, expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete snapmirror relationship")
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPFailsWhenVolumeAttributesIsNil(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: nil, // This should cause the error
	}
	node := &models.Node{}

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "volume test-volume has no volume attributes")
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSkipsWhenVolumeHasBackupsButNoDataProtection(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel:      datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: nil, // No data protection
	}
	node := &models.Node{}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSkipsWhenVolumeHasBackupsButEmptyBackupVaultID(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "", // Empty backup vault ID
		},
	}
	node := &models.Node{}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSkipsWhenSnapmirrorRelationshipNotFound(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-123",
		},
	}
	node := &models.Node{}

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)
	mockProvider.On("SnapmirrorRelationshipGet", "test-bucket:/objstore/test-volume-uuid", "test-svm:test-volume").Return(nil, utilErrors.NewNotFoundErr("snapmirror", nil))

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSuccessfullyDeletesSnapmirror(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-123",
		},
	}
	node := &models.Node{}

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	// Mock snapmirror relationship
	sourcePath := "test-svm:test-volume"
	destinationPath := "test-bucket:/objstore/test-volume-uuid"
	mockSnapmirror := &ontap_rest.SnapmirrorRelationship{
		SnapmirrorRelationship: oModels.SnapmirrorRelationship{
			UUID:        nillable.ToPointer(strfmt.UUID("snapmirror-uuid")),
			Source:      &oModels.SnapmirrorSourceEndpoint{Path: &sourcePath},
			Destination: &oModels.SnapmirrorEndpoint{Path: &destinationPath},
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)
	mockProvider.On("SnapmirrorRelationshipGet", "test-bucket:/objstore/test-volume-uuid", "test-svm:test-volume").Return(mockSnapmirror, nil)
	mockProvider.On("SnapmirrorRelationshipDelete", "snapmirror-uuid").Return(&vsa.OntapAsyncResponse{}, nil)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
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

func TestDeleteSnapshotPolicyInONTAP_WithNilNode(t *testing.T) {
	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	err := activity.DeleteSnapshotPolicyInONTAP(ctx, "policy1", nil)

	assert.NoError(t, err)
}

func TestDeleteSnapshotPolicyInONTAP_WithEmptyPolicyName(t *testing.T) {
	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	err := activity.DeleteSnapshotPolicyInONTAP(ctx, "", node)

	assert.NoError(t, err)
}

func TestDeleteSnapshotPolicyInONTAP_GetProviderByNodeFailure(t *testing.T) {
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	err := activity.DeleteSnapshotPolicyInONTAP(ctx, "policy1", node)

	assert.Error(t, err)
}

func TestDeleteVolumeInONTAP_GetProviderByNodeFailure(t *testing.T) {
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volumeExternalUUID := "uuid-123"
	volumeName := "test-volume"
	node := &models.Node{}

	err := activity.DeleteVolumeInONTAP(ctx, volumeExternalUUID, volumeName, node)

	assert.Error(t, err)
}

func TestSnapmirrorInONTAPSkipsWhenNodeIsNil(t *testing.T) {
	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"}}

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, nil)

	assert.NoError(t, err)
	assert.Nil(t, resp)
}

func TestSnapmirrorInONTAPSkipsWhenVolumeUUIDIsEmpty(t *testing.T) {
	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: ""}}
	node := &models.Node{}

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.Nil(t, resp)
}

func TestSnapmirrorInONTAPFailsWhenGetProviderByNodeFails(t *testing.T) {
	originalGetProviderByNode := GetProviderByNode
	defer func() { GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"}}
	node := &models.Node{}

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestSnapmirrorInONTAPFailsWhenGetBackupVaultFails(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
	}
	node := &models.Node{}
	expectedError := errors.New("failed to get backup vault")

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(nil, expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get backup vault")
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
}

func TestSnapmirrorInONTAPFailsWhenSnapmirrorRelationshipGetFails(t *testing.T) {
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
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: volumeUUID},
		Name:      "test-volume",
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		DataProtection: &datamodel.DataProtection{
			BackupVaultID: "backup-vault-123",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "test-subnet-123",
		},
	}
	node := &models.Node{}
	expectedError := errors.New("failed to get snapmirror relationship")

	// Mock backup vault
	mockBackupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "backup-vault-123"},
		Name:      "test-backup-vault",
		BucketDetails: []*datamodel.BucketDetails{
			{
				BucketName:     "test-bucket",
				VendorSubnetID: "test-subnet-123",
			},
		},
	}

	mockStorage.On("BackupCountByVolumeID", ctx, volumeUUID).Return(int64(1), nil)
	mockStorage.On("GetBackupVault", ctx, "backup-vault-123").Return(mockBackupVault, nil)
	mockProvider.On("SnapmirrorRelationshipGet", "test-bucket:/objstore/test-volume-uuid", "test-svm:test-volume").Return(nil, expectedError)

	resp, err := activity.DeleteSnapmirrorInONTAP(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get snapmirror relationship")
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}
