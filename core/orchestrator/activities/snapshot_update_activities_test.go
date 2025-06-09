package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestUpdateSnapshot_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.SnapshotUpdateActivity{
		SE: mockStorage,
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshotID := "test-snapshot-id"
	expectedSnapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}}

	mockStorage.On("UpdateSnapshot", ctx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}}).Return(expectedSnapshot, nil)

	err := activity.UpdateSnapshot(ctx, expectedSnapshot)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateSnapshot_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.SnapshotUpdateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	snapshotID := "test-snapshot-id"
	expectedError := errors.New("snapshot not found")

	mockStorage.On("UpdateSnapshot", ctx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}}).Return(nil, expectedError)

	err := activity.UpdateSnapshot(ctx, &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: snapshotID}})

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}
