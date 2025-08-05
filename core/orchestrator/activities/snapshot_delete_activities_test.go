package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestDeleteSnapshot_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.SnapshotDeleteActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshotID := "test-snapshot-id"
	expectedSnapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}}

	mockStorage.On("DeleteSnapshot", ctx, snapshotID).Return(expectedSnapshot, nil)

	err := activity.DeleteSnapshot(ctx, expectedSnapshot)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestDeleteSnapshot_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.SnapshotDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshotID := "test-snapshot-id"
	expectedError := errors.New("snapshot not found")

	mockStorage.On("DeleteSnapshot", ctx, snapshotID).Return(nil, expectedError)

	err := activity.DeleteSnapshot(ctx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}})

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestDeleteSnapshotInONTAP_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.SnapshotDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshot := &datamodel.Snapshot{
		SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "uuid-123"},
		BaseModel: datamodel.BaseModel{
			UUID: "test-snapshot-id",
		},
		Volume: &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-123"},
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
			},
		},
		Name: "test-snapshot",
	}
	node := &models.Node{}

	// Mock the DeleteSnapshot method
	mockProvider.On("DeleteSnapshot", snapshot.SnapshotAttributes.ExternalUUID, snapshot.Volume.VolumeAttributes.ExternalUUID).Return(nil)

	err := activity.DeleteSnapshotInONTAP(ctx, snapshot, node)

	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshotInONTAP_Failure(t *testing.T) {
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.SnapshotDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshot := &datamodel.Snapshot{
		SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "uuid-123"},
		BaseModel: datamodel.BaseModel{
			UUID: "test-snapshot-id",
		},
		Volume: &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-123"},
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
			},
		},
		Name: "test-snapshot",
	}
	node := &models.Node{}
	expectedError := errors.New("failed to delete snapshot in ONTAP")

	// Mock the DeleteSnapshot method
	mockProvider.On("DeleteSnapshot", snapshot.SnapshotAttributes.ExternalUUID, snapshot.Volume.VolumeAttributes.ExternalUUID).Return(expectedError)

	err := activity.DeleteSnapshotInONTAP(ctx, snapshot, node)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestDeleteSnapshotInONTAP_GetproviderByNodeFailure(t *testing.T) {
	originalGetProviderByNode := hyperscaler.GetProviderByNode
	defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("failed to get provider by node")
	}

	activity := activities.SnapshotDeleteActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshot := &datamodel.Snapshot{
		SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "uuid-123"},
		BaseModel: datamodel.BaseModel{
			UUID: "test-snapshot-id",
		},
		Volume: &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "volume-uuid-123"},
			BaseModel: datamodel.BaseModel{
				UUID: "test-volume-id",
			},
		},
		Name: "test-snapshot",
	}
	node := &models.Node{}

	err := activity.DeleteSnapshotInONTAP(ctx, snapshot, node)

	assert.Error(t, err)
	assert.EqualError(t, err, "failed to get provider by node")
}

func TestUpdateDeleteSnapshotDetails_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.SnapshotDeleteActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: "test-snapshot-id"}}
	expectedError := errors.New("update failed")

	mockStorage.On("UpdateSnapshot", ctx, snapshot).Return(nil, expectedError)

	err := activity.UpdateDeleteSnapshotDetails(ctx, snapshot)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}
