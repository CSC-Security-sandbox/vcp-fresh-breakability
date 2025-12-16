package activities_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestUpdateClusterUpgradeJobStatusActivity_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterUpgradeActivity{
		SE: mockStorage,
	}

	ctx := context.Background()
	jobUUID := "test-job-uuid"
	status := "IN_PROGRESS"

	// Mock the upgrade job
	upgradeJob := &datamodel.ClusterUpgradeJob{
		BaseModel: datamodel.BaseModel{
			UUID:      jobUUID,
			UpdatedAt: time.Now().Add(-time.Hour),
		},
		ClusterID: "test-cluster-id",
		Status:    "PENDING",
	}

	mockStorage.On("GetClusterUpgradeJobByUUID", ctx, jobUUID).Return(upgradeJob, nil)
	mockStorage.On("UpdateClusterUpgradeJob", ctx, mock.AnythingOfType("*datamodel.ClusterUpgradeJob")).Return(nil)

	err := activity.UpdateClusterUpgradeJobStatusActivity(ctx, jobUUID, status, "")

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateClusterUpgradeJobStatusActivity_WithErrorMessage(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterUpgradeActivity{
		SE: mockStorage,
	}

	ctx := context.Background()
	jobUUID := "test-job-uuid"
	status := "FAILED"
	errorMessage := "Upgrade failed due to network issues"

	// Mock the upgrade job
	upgradeJob := &datamodel.ClusterUpgradeJob{
		BaseModel: datamodel.BaseModel{
			UUID:      jobUUID,
			UpdatedAt: time.Now().Add(-time.Hour),
		},
		ClusterID: "test-cluster-id",
		Status:    "IN_PROGRESS",
	}

	mockStorage.On("GetClusterUpgradeJobByUUID", ctx, jobUUID).Return(upgradeJob, nil)
	mockStorage.On("UpdateClusterUpgradeJob", ctx, mock.MatchedBy(func(job *datamodel.ClusterUpgradeJob) bool {
		return job.ErrorDetails != nil &&
			job.ErrorDetails.ErrorCode == "UPGRADE_FAILED" &&
			job.ErrorDetails.ErrorMessage == errorMessage &&
			job.ErrorDetails.ErrorType == "UPGRADE_ERROR" &&
			job.ErrorDetails.Retryable == true
	})).Return(nil)

	err := activity.UpdateClusterUpgradeJobStatusActivity(ctx, jobUUID, status, errorMessage)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateClusterUpgradeJobStatusActivity_CompletedStatus(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterUpgradeActivity{
		SE: mockStorage,
	}

	ctx := context.Background()
	jobUUID := "test-job-uuid"
	status := "COMPLETED"

	// Mock the upgrade job
	upgradeJob := &datamodel.ClusterUpgradeJob{
		BaseModel: datamodel.BaseModel{
			UUID:      jobUUID,
			UpdatedAt: time.Now().Add(-time.Hour),
		},
		ClusterID: "test-cluster-id",
		Status:    "IN_PROGRESS",
	}

	mockStorage.On("GetClusterUpgradeJobByUUID", ctx, jobUUID).Return(upgradeJob, nil)
	mockStorage.On("UpdateClusterUpgradeJob", ctx, mock.MatchedBy(func(job *datamodel.ClusterUpgradeJob) bool {
		return job.CompletedAt != nil && job.DeletedAt != nil
	})).Return(nil)

	err := activity.UpdateClusterUpgradeJobStatusActivity(ctx, jobUUID, status, "")

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateClusterUpgradeJobStatusActivity_JobNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterUpgradeActivity{
		SE: mockStorage,
	}

	ctx := context.Background()
	jobUUID := "non-existent-job"
	status := "IN_PROGRESS"

	mockStorage.On("GetClusterUpgradeJobByUUID", ctx, jobUUID).Return(nil, gorm.ErrRecordNotFound)

	err := activity.UpdateClusterUpgradeJobStatusActivity(ctx, jobUUID, status, "")

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateClusterUpgradeJobStatusActivity_UpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterUpgradeActivity{
		SE: mockStorage,
	}

	ctx := context.Background()
	jobUUID := "test-job-uuid"
	status := "IN_PROGRESS"

	// Mock the upgrade job
	upgradeJob := &datamodel.ClusterUpgradeJob{
		BaseModel: datamodel.BaseModel{
			UUID:      jobUUID,
			UpdatedAt: time.Now().Add(-time.Hour),
		},
		ClusterID: "test-cluster-id",
		Status:    "PENDING",
	}

	mockStorage.On("GetClusterUpgradeJobByUUID", ctx, jobUUID).Return(upgradeJob, nil)
	mockStorage.On("UpdateClusterUpgradeJob", ctx, mock.AnythingOfType("*datamodel.ClusterUpgradeJob")).Return(errors.New("update failed"))

	err := activity.UpdateClusterUpgradeJobStatusActivity(ctx, jobUUID, status, "")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update failed")
	mockStorage.AssertExpectations(t)
}

func TestClusterUpgradeActivity_Structure(t *testing.T) {
	t.Run("ActivityInitialization", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.ClusterUpgradeActivity{
			SE: mockStorage,
		}

		// Verify the activity struct is properly initialized
		assert.NotNil(t, activity.SE)
		assert.Equal(t, mockStorage, activity.SE)
	})
}

func TestUpgradeErrorDetails_Structure(t *testing.T) {
	t.Run("ErrorDetailsCreation", func(t *testing.T) {
		// Test error details structure validation
		errorDetails := &datamodel.UpgradeErrorDetails{
			ErrorCode:    "UPGRADE_FAILED",
			ErrorMessage: "Upgrade failed due to network issues",
			ErrorType:    "UPGRADE_ERROR",
			Retryable:    true,
		}

		assert.Equal(t, "UPGRADE_FAILED", errorDetails.ErrorCode)
		assert.Equal(t, "Upgrade failed due to network issues", errorDetails.ErrorMessage)
		assert.Equal(t, "UPGRADE_ERROR", errorDetails.ErrorType)
		assert.True(t, errorDetails.Retryable)
	})

	t.Run("ErrorDetailsEmpty", func(t *testing.T) {
		// Test empty error details
		errorDetails := &datamodel.UpgradeErrorDetails{}

		assert.Equal(t, "", errorDetails.ErrorCode)
		assert.Equal(t, "", errorDetails.ErrorMessage)
		assert.Equal(t, "", errorDetails.ErrorType)
		assert.False(t, errorDetails.Retryable)
	})
}

func TestClusterUpgradeJob_Structure(t *testing.T) {
	t.Run("JobCreation", func(t *testing.T) {
		// Test cluster upgrade job structure
		job := &datamodel.ClusterUpgradeJob{
			BaseModel: datamodel.BaseModel{
				UUID: "test-job-uuid",
			},
			ClusterID:          "test-cluster-id",
			PoolID:             "test-pool-id",
			TargetVersion:      "9.17.1",
			CurrentVersion:     "9.16.1",
			VSABuildImage:      "vsa-image:latest",
			MediatorBuildImage: "mediator-image:latest",
			Status:             "PENDING",
			ForceUpgrade:       true,
		}

		assert.Equal(t, "test-job-uuid", job.UUID)
		assert.Equal(t, "test-cluster-id", job.ClusterID)
		assert.Equal(t, "test-pool-id", job.PoolID)
		assert.Equal(t, "9.17.1", job.TargetVersion)
		assert.Equal(t, "9.16.1", job.CurrentVersion)
		assert.Equal(t, "vsa-image:latest", job.VSABuildImage)
		assert.Equal(t, "mediator-image:latest", job.MediatorBuildImage)
		assert.Equal(t, "PENDING", job.Status)
		assert.True(t, job.ForceUpgrade)
	})

	t.Run("JobWithErrorDetails", func(t *testing.T) {
		// Test cluster upgrade job with error details
		errorDetails := &datamodel.UpgradeErrorDetails{
			ErrorCode:    "UPGRADE_FAILED",
			ErrorMessage: "Network timeout",
			ErrorType:    "NETWORK_ERROR",
			Retryable:    true,
		}

		job := &datamodel.ClusterUpgradeJob{
			BaseModel: datamodel.BaseModel{
				UUID: "test-job-uuid",
			},
			ClusterID:    "test-cluster-id",
			Status:       "FAILED",
			ErrorDetails: errorDetails,
		}

		assert.Equal(t, "test-job-uuid", job.UUID)
		assert.Equal(t, "test-cluster-id", job.ClusterID)
		assert.Equal(t, "FAILED", job.Status)
		assert.NotNil(t, job.ErrorDetails)
		assert.Equal(t, "UPGRADE_FAILED", job.ErrorDetails.ErrorCode)
		assert.Equal(t, "Network timeout", job.ErrorDetails.ErrorMessage)
		assert.Equal(t, "NETWORK_ERROR", job.ErrorDetails.ErrorType)
		assert.True(t, job.ErrorDetails.Retryable)
	})
}

func TestStatusValidation(t *testing.T) {
	t.Run("ValidStatuses", func(t *testing.T) {
		validStatuses := []string{"PENDING", "IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED"}

		for _, status := range validStatuses {
			assert.NotEmpty(t, status)
			assert.True(t, len(status) > 0)
		}
	})

	t.Run("StatusComparison", func(t *testing.T) {
		status1 := "IN_PROGRESS"
		status2 := "COMPLETED"

		assert.NotEqual(t, status1, status2)
		assert.Equal(t, "IN_PROGRESS", status1)
		assert.Equal(t, "COMPLETED", status2)
	})
}

func TestCreateClusterPeer(t *testing.T) {
	ctx := context.Background()
	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "127.0.0.1",
	}

	t.Run("Success_NoExistingPeer", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		expectedClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:    "new-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:      (*log.Secret)(stringPtr("test-passphrase")),
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(expectedClusterPeer, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "new-peer-uuid", result.UUID)
		assert.NotNil(t, result.Passphrase)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_ExistingPeerAvailable_Reuse", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "available",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "existing-peer-uuid", result.UUID)
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_ExistingPeerPending_ReturnsError", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "pending",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		// Check that the error is a VCPError with ErrClusterPeerNotAvailable
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(t, vsaerrors.ErrClusterPeerNotAvailable, customErr.TrackingID)
		}
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_ExistingPeerPartial_Reuse", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    vsa.ClusterPeerAvailabilityStatePartial,
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "existing-peer-uuid", result.UUID)
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_ExistingPeerNotAvailable_DeleteAndCreate", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "unavailable",
		}

		newClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:    "new-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:      (*log.Secret)(stringPtr("test-passphrase")),
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)
		mockProvider.On("DeleteClusterPeer", "existing-peer-uuid").Return(nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(newClusterPeer, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "new-peer-uuid", result.UUID)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_DeleteExistingPeerFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "unavailable",
		}

		deleteError := errors.New("failed to delete cluster peer")

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)
		mockProvider.On("DeleteClusterPeer", "existing-peer-uuid").Return(deleteError)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		// Check that the error is a VCPError with ErrDeletingClusterPeer
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(t, vsaerrors.ErrDeletingClusterPeer, customErr.TrackingID)
			if customErr.Unwrap() != nil {
				assert.Contains(t, customErr.Unwrap().Error(), "failed to delete cluster peer")
			}
		} else {
			assert.Contains(t, err.Error(), "failed to delete cluster peer")
		}
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_ListClusterPeersFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		listError := errors.New("failed to list cluster peers")
		mockProvider.On("ListClusterPeers").Return(nil, listError)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_CreateClusterPeerFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		createError := errors.New("failed to create cluster peer")

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(nil, createError)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_GetProviderByNodeFails", func(t *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		providerError := errors.New("failed to get provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, providerError
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestAcceptClusterPeer(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := activities.ClusterPeerActivity{
		SE: mockStorage,
	}
	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "127.0.0.1",
	}

	t.Run("Success_NoExistingPeer", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		expectedClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:    "new-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("AcceptClusterPeer", mock.Anything).Return(expectedClusterPeer, nil)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "new-peer-uuid", result.UUID)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_ExistingPeerAvailable_Reuse", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "available",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "existing-peer-uuid", result.UUID)
		mockProvider.AssertNotCalled(t, "AcceptClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_ExistingPeerPending_ReturnsError", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "pending",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		// Check that the error is a VCPError with ErrClusterPeerNotAvailable
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(t, vsaerrors.ErrClusterPeerNotAvailable, customErr.TrackingID)
		}
		mockProvider.AssertNotCalled(t, "AcceptClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Success_ExistingPeerNotAvailable_DeleteAndAccept", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "unavailable",
		}

		newClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:    "new-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)
		mockProvider.On("DeleteClusterPeer", "existing-peer-uuid").Return(nil)
		mockProvider.On("AcceptClusterPeer", mock.Anything).Return(newClusterPeer, nil)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "new-peer-uuid", result.UUID)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_DeleteExistingPeerFails_ReturnsError", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    "unavailable",
		}

		deleteError := errors.New("failed to delete cluster peer")

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)
		mockProvider.On("DeleteClusterPeer", "existing-peer-uuid").Return(deleteError)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		// Check that the error is a VCPError with ErrDeletingClusterPeer
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Equal(t, vsaerrors.ErrDeletingClusterPeer, customErr.TrackingID)
			if customErr.Unwrap() != nil {
				assert.Contains(t, customErr.Unwrap().Error(), "failed to delete cluster peer")
			}
		} else {
			assert.Contains(t, err.Error(), "failed to delete cluster peer")
		}
		mockProvider.AssertNotCalled(t, "AcceptClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_ListClusterPeersFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		listError := errors.New("failed to list cluster peers")
		mockProvider.On("ListClusterPeers").Return(nil, listError)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockProvider.AssertNotCalled(t, "AcceptClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_AcceptClusterPeerFails", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		acceptError := errors.New("failed to accept cluster peer")

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{}, nil)
		mockProvider.On("AcceptClusterPeer", mock.Anything).Return(nil, acceptError)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_GetProviderByNodeFails", func(t *testing.T) {
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		providerError := errors.New("failed to get provider")
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, providerError
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("Success_ExistingPeerPartial_Reuse", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		passphrase := "test-passphrase"
		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:    &passphrase,
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Availability:    vsa.ClusterPeerAvailabilityStatePartial,
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activity.AcceptClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "existing-peer-uuid", result.UUID)
		mockProvider.AssertNotCalled(t, "AcceptClusterPeer")
		mockProvider.AssertExpectations(t)
	})
}

func TestAreIPsMatching_Indirect(t *testing.T) {
	// Test areIPsMatching indirectly through CreateClusterPeer and AcceptClusterPeer
	// by testing scenarios where IPs match and don't match
	ctx := context.Background()
	node := &models.Node{
		Name:            "test-node",
		EndpointAddress: "127.0.0.1",
	}

	t.Run("MatchingIPs_ReusesExistingPeer", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.2", "10.1.1.1"}, // Different order
			Availability:    "available",
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "existing-peer-uuid", result.UUID)
		mockProvider.AssertNotCalled(t, "CreateClusterPeer")
		mockProvider.AssertExpectations(t)
	})

	t.Run("NonMatchingIPs_CreatesNewPeer", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		params := &common.ClusterPeerParams{
			PeerName:      "test-peer",
			PeerAddresses: []string{"10.1.1.1", "10.1.1.2"},
		}

		existingPeer := &vsa.ClusterPeer{
			ExternalUUID:    "existing-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.3", "10.1.1.4"}, // Different IPs
			Availability:    "available",
		}

		newClusterPeer := &vsa.ClusterPeer{
			ExternalUUID:    "new-peer-uuid",
			PeerClusterName: "test-peer",
			PeerAddresses:   []string{"10.1.1.1", "10.1.1.2"},
			Passphrase:      (*log.Secret)(stringPtr("test-passphrase")),
		}

		mockProvider.On("ListClusterPeers").Return([]*vsa.ClusterPeer{existingPeer}, nil)
		mockProvider.On("CreateClusterPeer", mock.Anything).Return(newClusterPeer, nil)

		result, err := activities.CreateClusterPeer(ctx, params, node)

		assert.NoError(t, err)
		assert.Equal(t, "new-peer-uuid", result.UUID)
		mockProvider.AssertExpectations(t)
	})
}

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}
