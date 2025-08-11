package activities

import (
	"context"
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	oModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"go.temporal.io/sdk/temporal"
)

func assertTemporalApplicationError(t *testing.T, err error, expectedMsg, expectedType string, expectedNonRetryable bool) {
	t.Helper()
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)

	var trackingID int
	var originalMsg string
	require.NoError(t, appErr.Details(&trackingID, &originalMsg))

	assert.Contains(t, originalMsg, expectedMsg)
	assert.Equal(t, expectedType, appErr.Type())
	assert.Equal(t, expectedNonRetryable, appErr.NonRetryable())
}

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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	assertTemporalApplicationError(t, err, "volume test-volume has no volume attributes", "CustomError", false)
	assert.Nil(t, resp)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestSnapmirrorInONTAPSkipsWhenVolumeHasBackupsButNoDataProtection(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &models.Node{}

	err := activity.DeleteSnapshotPolicyInONTAP(ctx, "policy1", node)

	assert.Error(t, err)
}

func TestDeleteIgroups_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Create test volume with block devices
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
						{
							HostGroupUUID: "hostgroup-uuid-2",
							HostQNs:       []string{"iqn.example.2"},
						},
					},
				},
				{
					Name: "block-device-2",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-3",
							HostQNs:       []string{"iqn.example.3"},
						},
					},
				},
			},
		},
		AccountID: 1,
		Svm:       &datamodel.Svm{Name: "test-svm"},
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	// Mock SE.GetAllVolumesForHG to return empty volumes (no usage)
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-2", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-3", int64(1)).Return([]*datamodel.Volume{}, nil)

	// Mock SE.GetHostGroup to return host groups
	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}
	hostgroup2 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-2"},
		Name:      "hostgroup-name-2",
		AccountID: 1,
	}
	hostgroup3 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-3"},
		Name:      "hostgroup-name-3",
		AccountID: 1,
	}
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-2", int64(1)).Return(hostgroup2, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-3", int64(1)).Return(hostgroup3, nil)

	// Mock provider.IgroupGet to return igroups
	igroup1 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("ontap-igroup-uuid-1"),
			Name: &hostgroup1.Name,
		},
	}
	igroup2 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("ontap-igroup-uuid-2"),
			Name: &hostgroup2.Name,
		},
	}
	igroup3 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("ontap-igroup-uuid-3"),
			Name: &hostgroup3.Name,
		},
	}
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(igroup1, nil)
	mockProvider.On("IgroupGet", &hostgroup2.Name, mock.Anything).Return(igroup2, nil)
	mockProvider.On("IgroupGet", &hostgroup3.Name, mock.Anything).Return(igroup3, nil)

	// Mock provider.IgroupDelete calls
	mockProvider.On("IgroupDelete", *igroup1.UUID).Return(nil)
	mockProvider.On("IgroupDelete", *igroup2.UUID).Return(nil)
	mockProvider.On("IgroupDelete", *igroup3.UUID).Return(nil)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_OneHostGroupInUse(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Create test volume with block devices
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
						{
							HostGroupUUID: "hostgroup-uuid-2",
							HostQNs:       []string{"iqn.example.2"},
						},
					},
				},
			},
		},
		AccountID: 1,
		Svm:       &datamodel.Svm{Name: "test-svm"},
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	// Mock SE.GetAllVolumesForHG to return:
	// - hostgroup-uuid-1: no other volumes (should be deleted)
	// - hostgroup-uuid-2: has another volume (should NOT be deleted)
	otherVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "other-volume-uuid"},
		Name:      "other-volume",
	}
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-2", int64(1)).Return([]*datamodel.Volume{volume, otherVolume}, nil)

	// Mock SE.GetHostGroup to return host groups - only for hostgroup-uuid-1 since hostgroup-uuid-2 won't be processed
	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)

	// Mock provider.IgroupGet to return igroups - only for hostgroup-uuid-1 since hostgroup-uuid-2 won't be processed
	igroup1 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("ontap-igroup-uuid-1"),
			Name: &hostgroup1.Name,
		},
	}
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(igroup1, nil)

	// Mock provider.IgroupDelete call - only for the unused hostgroup
	mockProvider.On("IgroupDelete", *igroup1.UUID).Return(nil)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_GetProviderByNodeFailure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
}

func TestDeleteIgroups_GetAllVolumesForHGFailure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	expectedError := errors.New("failed to get volumes for host group")
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return(nil, expectedError)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteIgroups_GetHostGroupFailure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	expectedError := errors.New("failed to get host group")
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(nil, expectedError)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteIgroups_IgroupGetNotFoundError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)

	// Mock IgroupGet to return not found error
	notFoundError := utilErrors.NewNotFoundErr("igroup", nil)
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(nil, notFoundError)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.NoError(t, err) // Should continue and not return error for not found
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_IgroupGetOtherError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	expectedError := errors.New("unexpected error getting igroup")
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(nil, expectedError)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_IgroupDeleteFailure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	igroup1 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("ontap-igroup-uuid-1"),
			Name: &hostgroup1.Name,
		},
	}

	expectedError := errors.New("failed to delete igroup")
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(igroup1, nil)
	mockProvider.On("IgroupDelete", *igroup1.UUID).Return(expectedError)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_IgroupWithNilUUID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	// Create igroup with nil UUID
	igroup1 := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nil,
			Name: &hostgroup1.Name,
		},
	}

	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(igroup1, nil)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.NoError(t, err) // Should continue when UUID is nil
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroups_IgroupWithNilIgroup(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := vsa.NewMockProvider(t)

	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &[]datamodel.BlockDevice{
				{
					Name: "block-device-1",
					HostGroupDetails: []datamodel.HostGroupDetail{
						{
							HostGroupUUID: "hostgroup-uuid-1",
							HostQNs:       []string{"iqn.example.1"},
						},
					},
				},
			},
		},
		AccountID: 1,
	}

	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "192.168.1.1",
	}

	hostgroup1 := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "hostgroup-uuid-1"},
		Name:      "hostgroup-name-1",
		AccountID: 1,
	}

	// Mock IgroupGet to return nil igroup
	mockStorage.On("GetAllVolumesForHG", ctx, "hostgroup-uuid-1", int64(1)).Return([]*datamodel.Volume{}, nil)
	mockStorage.On("GetHostGroup", ctx, "hostgroup-uuid-1", int64(1)).Return(hostgroup1, nil)
	mockProvider.On("IgroupGet", &hostgroup1.Name, mock.Anything).Return(nil, nil)

	err := activity.DeleteIgroups(ctx, volume, node)

	assert.NoError(t, err) // Should continue when igroup is nil
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteVolumeInONTAP_GetProviderByNodeFailure(t *testing.T) {
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("failed to get provider")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
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

func TestDeleteIgroupsFromBlockProperties_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	igroup := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("test-igroup-uuid"),
		},
	}

	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(igroup, nil)
	mockProvider.On("IgroupDelete", "test-igroup-uuid").Return(nil)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_GetProviderByNodeFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	expectedError := errors.New("provider not found")
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, expectedError
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
}

func TestDeleteIgroupsFromBlockProperties_GetAllVolumesForHGFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	expectedError := errors.New("database error")
	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return(nil, expectedError)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_HostGroupInUse(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	otherVolume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "other-volume-uuid"},
		AccountID: 1,
	}

	node := &models.Node{}

	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume, otherVolume}, nil)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_GetHostGroupFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	expectedError := errors.New("hostgroup not found")
	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(nil, expectedError)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_IgroupGetNotFoundError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	notFoundError := utilErrors.NewNotFoundErr("igroup", nil)
	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(nil, notFoundError)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_IgroupGetOtherError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	expectedError := errors.New("network error")
	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(nil, expectedError)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_IgroupDeleteFailure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	igroup := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nillable.GetStringPtr("test-igroup-uuid"),
		},
	}

	expectedError := errors.New("delete failed")
	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(igroup, nil)
	mockProvider.On("IgroupDelete", "test-igroup-uuid").Return(expectedError)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), expectedError.Error())
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_IgroupWithNilUUID(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	igroup := &ontap_rest.Igroup{
		Igroup: oModels.Igroup{
			UUID: nil, // Nil UUID
		},
	}

	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(igroup, nil)

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func TestDeleteIgroupsFromBlockProperties_IgroupWithNilIgroup(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := VolumeDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupDetails: []datamodel.HostGroupDetail{
					{HostGroupUUID: "test-hostgroup-uuid"},
				},
			},
		},
	}

	node := &models.Node{}

	hostgroupDB := &datamodel.HostGroup{
		BaseModel: datamodel.BaseModel{UUID: "test-hostgroup-uuid"},
		Name:      "test-hostgroup",
	}

	mockStorage.On("GetAllVolumesForHG", ctx, "test-hostgroup-uuid", int64(1)).Return([]*datamodel.Volume{volume}, nil)
	mockStorage.On("GetHostGroup", ctx, "test-hostgroup-uuid", int64(1)).Return(hostgroupDB, nil)
	mockProvider.On("IgroupGet", &hostgroupDB.Name, (*string)(nil)).Return(nil, nil) // Nil igroup

	// Act
	err := activity.DeleteIgroupsFromBlockProperties(ctx, volume, node)

	// Assert
	assert.NoError(t, err) // Should continue when igroup is nil
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}
