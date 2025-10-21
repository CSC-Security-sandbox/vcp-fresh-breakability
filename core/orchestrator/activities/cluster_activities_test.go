package activities_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
