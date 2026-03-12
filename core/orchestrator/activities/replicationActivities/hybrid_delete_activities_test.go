package replicationActivities

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestHybridCreateJobForHybridDeleteVolume(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("WhenEventIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := HybridDeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: nil,
		}

		job, err := activity.CreateJobForHybridDeleteVolume(ctx, result, "DELETE_VOLUME")

		assert.Error(tt, err)
		assert.Nil(tt, job)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "replication model is nil")
	})

	t.Run("WhenReplicationModelIsNil", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := HybridDeleteVolumeReplicationActivity{SE: mockStorage}

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					ReplicationModel: nil,
				},
			},
		}

		job, err := activity.CreateJobForHybridDeleteVolume(ctx, result, "DELETE_VOLUME")

		assert.Error(tt, err)
		assert.Nil(tt, job)
		var customErr *vsaerrors.CustomError
		assert.True(tt, vsaerrors.As(err, &customErr))
		assert.Contains(tt, customErr.OriginalErr.Error(), "replication model is nil")
	})

	t.Run("WhenCreateJobFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		dstProjectNumber := "123456"

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					AccountID: 1,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dest-vol-uuid",
						},
					},
				},
			},
			DstProjectNumber: &dstProjectNumber,
		}

		expectedError := errors.New("failed to create job")
		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == "DELETE_VOLUME" &&
				job.State == string(models.JobsStateNEW) &&
				job.ResourceName == fmt.Sprintf("projects/%s/locations/%s/volumes/%s", dstProjectNumber, "us-central1", "dest-vol-uuid")
		})).Return(nil, expectedError)

		job, err := activity.CreateJobForHybridDeleteVolume(ctx, result, "DELETE_VOLUME")

		assert.Error(tt, err)
		assert.Nil(tt, job)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		dstProjectNumber := "123456"

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					AccountID: 1,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-central1",
							DestinationVolumeUUID: "dest-vol-uuid",
						},
					},
				},
			},
			DstProjectNumber: &dstProjectNumber,
		}

		createdJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-uuid-123",
			},
			Type:  "DELETE_VOLUME",
			State: string(models.JobsStateNEW),
		}

		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == "DELETE_VOLUME" &&
				job.State == string(models.JobsStateNEW) &&
				job.AccountID.Int64 == int64(1) &&
				job.AccountID.Valid == true &&
				job.ResourceName == fmt.Sprintf("projects/%s/locations/%s/volumes/%s", dstProjectNumber, "us-central1", "dest-vol-uuid") &&
				job.JobAttributes != nil &&
				job.JobAttributes.ResourceUUID == "dest-vol-uuid"
		})).Return(createdJob, nil)

		job, err := activity.CreateJobForHybridDeleteVolume(ctx, result, "DELETE_VOLUME")

		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid-123", job.UUID)
		assert.Equal(tt, "DELETE_VOLUME", job.Type)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSuccessfulWithDifferentJobType", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := HybridDeleteVolumeReplicationActivity{SE: mockStorage}
		dstProjectNumber := "789012"

		result := &replication.DeleteReplicationResult{
			Event: &replication.DeleteReplicationEvent{
				CommonReplicationEventParams: replication.CommonReplicationEventParams{
					AccountID: 2,
					ReplicationModel: &datamodel.VolumeReplication{
						ReplicationAttributes: &datamodel.ReplicationDetails{
							DestinationLocation:   "us-east1",
							DestinationVolumeUUID: "another-vol-uuid",
						},
					},
				},
			},
			DstProjectNumber: &dstProjectNumber,
		}

		createdJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-uuid-456",
			},
			Type:  "FORCE_DELETE_VOLUME",
			State: string(models.JobsStateNEW),
		}

		mockStorage.On("CreateJob", ctx, mock.MatchedBy(func(job *datamodel.Job) bool {
			return job.Type == "FORCE_DELETE_VOLUME" &&
				job.State == string(models.JobsStateNEW) &&
				job.AccountID.Int64 == int64(2)
		})).Return(createdJob, nil)

		job, err := activity.CreateJobForHybridDeleteVolume(ctx, result, "FORCE_DELETE_VOLUME")

		assert.NoError(tt, err)
		assert.NotNil(tt, job)
		assert.Equal(tt, "job-uuid-456", job.UUID)
		assert.Equal(tt, "FORCE_DELETE_VOLUME", job.Type)
		mockStorage.AssertExpectations(tt)
	})
}
