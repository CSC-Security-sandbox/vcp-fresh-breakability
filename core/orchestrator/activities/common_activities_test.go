package activities

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"gorm.io/gorm"
)

func TestUpdateJobStatus_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := CommonActivities{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		State:        models.JobStateSuccess,
		TrackingID:   1003,
		ErrorDetails: []byte("Requested Resource is not found"),
	}

	mockStorage.On("UpdateJob", ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails).Return(nil)

	// Act
	err := activity.UpdateJobStatus(ctx, job)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateJobStatus(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := CommonActivities{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "test-job-uuid",
		},
		State:        models.JobStateSuccess,
		TrackingID:   1003,
		ErrorDetails: []byte("Requested Resource is not found"),
	}

	mockStorage.On("UpdateJob", ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails).Return(nil)

	// Act
	err := activity.UpdateJobStatus(ctx, job)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetNode(t *testing.T) {
	t.Run("GetNode_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)
		expectedNode := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-node",
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(expectedNode, nil)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.NoError(tt, err)
		assert.Equal(tt, expectedNode, node)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_Error", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nil, gorm.ErrInvalidDB)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.Error(tt, err)
		assert.Nil(tt, node)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_NotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := CommonActivities{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return([]*datamodel.Node{}, nil)

		node, err := activity.GetNode(ctx, poolId)

		// Assert
		assert.Error(tt, err)
		assert.Equal(tt, "no node found for the pool", err.Error())
		assert.Nil(tt, node)
		mockStorage.AssertExpectations(tt)
	})
}
