package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestInternalDescribePool(t *testing.T) {
	t.Run("WhenErrorGetPoolByName", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetPoolByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribePoolParams{
			PoolName:      "test-pool",
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		_, err := handler.V1betaInternalDescribePool(context.Background(), params)
		assert.Error(tt, err)
		assert.Equal(tt, "some error", err.Error())
	})
	t.Run("WhenPoolNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetPoolByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("pool", nil))
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribePoolParams{
			PoolName:      "test-pool",
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		expectedResponse := &gcpgenserver.V1betaInternalDescribePoolNotFound{
			Code:    404,
			Message: "Pool not found",
		}
		resp, err := handler.V1betaInternalDescribePool(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		pool := &models.Pool{
			Name: "test-pool",
			BaseModel: models.BaseModel{
				UUID: "test-uuid",
			},
			VendorSubNetID: "test-vendor-subnet-id",
			ServiceLevel:   "test-service-level",
			QosType:        "test-qos-type",
			SizeInBytes:    1000,
			PoolAttributes: &models.PoolAttributes{
				SecondaryZone: "test-secondary-zone",
			},
			ClusterAttributes: &models.ClusterAttributes{
				ExternalName:     "test-external-name",
				InterClusterLifs: []string{"10.0.0.1", "10.0.0.2"},
			},
			CustomPerformanceParams: &models.CustomPerformanceParams{
				Enabled: true,
			},
		}

		mockOrchestrator.EXPECT().GetPoolByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribePoolParams{
			PoolName:      "test-pool",
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		expectedResponse := &gcpgenserver.PoolInternalV1beta{
			Network:                  pool.VendorSubNetID,
			PoolId:                   gcpgenserver.NewOptString(pool.UUID),
			ResourceId:               pool.Name,
			ServiceLevel:             gcpgenserver.PoolInternalV1betaServiceLevel(pool.ServiceLevel),
			QosType:                  gcpgenserver.NewOptNilString(pool.QosType),
			SizeInBytes:              float64(pool.SizeInBytes),
			AllocatedBytes:           gcpgenserver.NewOptNilFloat64(pool.PoolAttributes.AllocatedBytes),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps),
			AvailableThroughputMibps: gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps - pool.UtilizedThroughputMibps),
			NumberOfVolumes:          gcpgenserver.NewOptNilInt32(int32(pool.PoolAttributes.NumberOfVolumes)),
			StoragePoolState: gcpgenserver.OptPoolInternalV1betaStoragePoolState{
				Value: gcpgenserver.PoolInternalV1betaStoragePoolState(pool.State),
			},
			StoragePoolStateDetails:  gcpgenserver.NewOptString(pool.StateDetails),
			CreatedAt:                gcpgenserver.NewOptDateTime(pool.CreatedAt),
			UpdatedAt:                gcpgenserver.NewOptDateTime(pool.UpdatedAt),
			StateDetails:             gcpgenserver.NewOptString(pool.StateDetails),
			Description:              gcpgenserver.NewOptNilString(pool.Description),
			Zone:                     gcpgenserver.NewOptString(pool.Zone),
			AllowAutoTiering:         gcpgenserver.NewOptNilBool(pool.AllowAutoTiering),
			SecondaryZone:            gcpgenserver.NewOptString(pool.PoolAttributes.SecondaryZone),
			CustomPerformanceEnabled: gcpgenserver.NewOptBool(pool.CustomPerformanceParams.Enabled),
			InterclusterLifs:         pool.ClusterAttributes.InterClusterLifs,
			ClusterName:              gcpgenserver.NewOptString(pool.ClusterAttributes.ExternalName),
			TotalIops:                gcpgenserver.NewOptNilFloat64(float64(pool.CustomPerformanceParams.Iops)),
		}
		resp, err := handler.V1betaInternalDescribePool(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
}

func TestInternalCreateVolumeReplication(t *testing.T) {
	t.Run("WhenEndpointNotDst", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		reqParams := &gcpgenserver.VolumeReplicationCreateInternalV1beta{
			EndpointType: "src",
		}
		params := gcpgenserver.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		expectedResponse := &gcpgenserver.V1betaInternalCreateVolumeReplicationBadRequest{
			Code:    400,
			Message: "Incorrect endpoint type",
		}
		resp, err := handler.V1betaInternalCreateVolumeReplication(context.Background(), reqParams, params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
	t.Run("WhenCreateVolumeReplicationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		reqParams := &gcpgenserver.VolumeReplicationCreateInternalV1beta{
			EndpointType: "dst",
		}
		params := gcpgenserver.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		mockOrchestrator.EXPECT().CreateVolumeReplicationInternal(mock.Anything, mock.Anything).Return(nil, nil, errors.New("some error"))
		_, err := handler.V1betaInternalCreateVolumeReplication(context.Background(), reqParams, params)
		assert.Error(tt, err)
		assert.Equal(tt, "some error", err.Error())
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		reqParams := &gcpgenserver.VolumeReplicationCreateInternalV1beta{
			EndpointType: "dst",
		}
		params := gcpgenserver.V1betaInternalCreateVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		volumeReplication := &models.VolumeReplication{
			BaseModel: models.BaseModel{
				UUID: "uuid-1",
			},
			Name:        "test-replication",
			Description: "Test replication",
			Uri:         "test-uri",
			RemoteUri:   "test-remote-uri",
			ReplicationAttributes: &models.ReplicationDetails{
				EndpointType:               "dst",
				ReplicationType:            "test-replication-type",
				ReplicationSchedule:        "test-schedule",
				SourceVolumeUUID:           "test-source-volume-uuid",
				SourceRegion:               "test-source-region",
				SourceHostName:             "test-source-host",
				SourceReplicationUUID:      "test-source-replication-uuid",
				SourceSvmName:              "test-source-svm",
				SourceVolumeName:           "test-source-volume",
				DestinationVolumeUUID:      "test-destination-volume-uuid",
				DestinationRegion:          "test-destination-region",
				DestinationHostName:        "test-destination-host",
				DestinationReplicationUUID: "test-destination-replication-uuid",
				DestinationSvmName:         "test-destination-svm",
				DestinationVolumeName:      "test-destination-volume",
			},
		}
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "test-job-uuid",
				CreatedAt: time.Now(),
			},
			WorkflowID: "test-workflow-id",
			Type:       "job-type-create-volume-replication",
			State:      "job-state-processing",
		}
		expectedResponse := &gcpgenserver.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: gcpgenserver.NewOptString(volumeReplication.UUID),
			EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointType(volumeReplication.ReplicationAttributes.EndpointType),
			RemoteRegion:          volumeReplication.ReplicationAttributes.SourceRegion,
			SourceHostName:        volumeReplication.ReplicationAttributes.SourceHostName,
			SourceServerName:      volumeReplication.ReplicationAttributes.SourceSvmName,
			SourceVolumeName:      volumeReplication.ReplicationAttributes.SourceVolumeName,
			SourceVolumeUuid:      gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourceVolumeUUID),
			SourcePoolUuid:        gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.SourcePoolUUID),
			DestinationHostName:   volumeReplication.ReplicationAttributes.DestinationHostName,
			DestinationServerName: volumeReplication.ReplicationAttributes.DestinationSvmName,
			DestinationVolumeName: volumeReplication.ReplicationAttributes.DestinationVolumeName,
			DestinationVolumeUuid: gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationVolumeUUID),
			DestinationPoolUuid:   gcpgenserver.NewOptString(volumeReplication.ReplicationAttributes.DestinationPoolUUID),
			ReplicationType: gcpgenserver.OptVolumeReplicationInternalV1betaReplicationType{
				Value: gcpgenserver.VolumeReplicationInternalV1betaReplicationType(volumeReplication.ReplicationAttributes.ReplicationType),
				Set:   true,
			},
			Jobs: []gcpgenserver.JobV1beta{
				{
					JobId:    gcpgenserver.NewOptString(job.UUID),
					Created:  gcpgenserver.NewOptDateTime(job.CreatedAt),
					WorkerId: gcpgenserver.NewOptString(job.WorkflowID),
				},
			},
		}
		mockOrchestrator.EXPECT().CreateVolumeReplicationInternal(mock.Anything, mock.Anything).Return(volumeReplication, job, nil)
		actualResp, err := handler.V1betaInternalCreateVolumeReplication(context.Background(), reqParams, params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, actualResp)
	})
}

func TestV1betaInternalGetReplicationJobs(t *testing.T) {
	t.Run("ReturnsInternalServerErrorWhenGetReplicationJobsFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetReplicationJobs(mock.Anything, "test-project", "test-pool").Return(nil, errors.New("some error"))
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalGetReplicationJobsParams{
			ProjectNumber: "test-project",
			PoolId:        "test-pool",
			LocationId:    "test-location",
		}
		resp, err := handler.V1betaInternalGetReplicationJobs(context.Background(), params)
		assert.Error(tt, err)
		assert.Equal(tt, "some error", err.Error())
		assert.IsType(tt, &gcpgenserver.V1betaInternalGetReplicationJobsInternalServerError{}, resp)
	})
	t.Run("ReturnsEmptyListWhenNoReplicationJobsExist", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetReplicationJobs(mock.Anything, "test-project", "test-pool").Return([]*models.Job{}, nil)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalGetReplicationJobsParams{
			ProjectNumber: "test-project",
			PoolId:        "test-pool",
			LocationId:    "test-location",
		}
		resp, err := handler.V1betaInternalGetReplicationJobs(context.Background(), params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalGetReplicationJobsOK{}, resp)
		assert.Empty(tt, resp.(*gcpgenserver.V1betaInternalGetReplicationJobsOK).Jobs)
	})
	t.Run("ReturnsReplicationJobsWhenTheyExist", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		jobs := []*models.Job{
			{
				BaseModel: models.BaseModel{
					UUID:      "job-uuid-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				CorrelationID: "correlation-id-1",
				State:         "COMPLETED",
				Type:          "REPLICATION",
				ScheduledAt:   time.Now(),
			},
			{
				BaseModel: models.BaseModel{
					UUID:      "job-uuid-2",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				CorrelationID: "correlation-id-2",
				State:         "FAILED",
				Type:          "REPLICATION",

				ScheduledAt: time.Now(),
			},
		}

		mockOrchestrator.EXPECT().GetReplicationJobs(mock.Anything, "test-project", "test-pool").Return(jobs, nil)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalGetReplicationJobsParams{
			ProjectNumber: "test-project",
			PoolId:        "test-pool",
			LocationId:    "test-location",
		}
		resp, err := handler.V1betaInternalGetReplicationJobs(context.Background(), params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalGetReplicationJobsOK{}, resp)
		assert.Len(tt, resp.(*gcpgenserver.V1betaInternalGetReplicationJobsOK).Jobs, 2)
		assert.Equal(tt, "job-uuid-1", resp.(*gcpgenserver.V1betaInternalGetReplicationJobsOK).Jobs[0].JobUuid.Value)
		assert.Equal(tt, "job-uuid-2", resp.(*gcpgenserver.V1betaInternalGetReplicationJobsOK).Jobs[1].JobUuid.Value)
	})
}

func TestV1betaGetMultipleReplicationsInternal(t *testing.T) {
	t.Run("WhenGetReplicationsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-1", "replication-2"},
		}
		mockOrchestrator.EXPECT().GetMultipleReplicationsInternal(context.Background(), mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		resp, err := handler.V1betaGetMultipleReplicationsInternal(context.Background(), req, params)
		if intErr, ok := resp.(*gcpgenserver.V1betaGetMultipleReplicationsInternalInternalServerError); ok {
			assert.Equal(tt, 500, int(intErr.Code))
		}

		assert.Error(tt, err)
		assert.Equal(tt, "some error", err.Error())
	})
	t.Run("WhenGetReplicationReturnsNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-1", "replication-2"},
		}
		mockOrchestrator.EXPECT().GetMultipleReplicationsInternal(context.Background(), mock.Anything, mock.Anything).Return(nil, errors.NewNotFoundErr("replication", nil))
		resp, err := handler.V1betaGetMultipleReplicationsInternal(context.Background(), req, params)
		if notFoundResp, ok := resp.(*gcpgenserver.V1betaGetMultipleReplicationsInternalNotFound); ok {
			assert.Equal(tt, 404, int(notFoundResp.Code))
		}
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
	})
	t.Run("WhenGetMultipleReplicationsSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		defer func() {
			convertToVolumeReplicationsInternalV1Beta = _convertToVolumeReplicationsInternalV1Beta
		}()

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsInternalParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.ReplicationIDListV1beta{
			ReplicationUUIDs: []string{"replication-1", "replication-2"},
		}
		replications := []*datamodel.VolumeReplication{
			{
				Name: "replication-1",
			},
			{
				Name: "replication-2",
			},
		}
		expectedResponse := &gcpgenserver.V1betaGetMultipleReplicationsInternalOK{
			Replications: []gcpgenserver.VolumeReplicationInternalV1beta{
				gcpgenserver.VolumeReplicationInternalV1beta{
					Name:         gcpgenserver.NewOptString("replication-1"),
					EndpointType: gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
				},
				gcpgenserver.VolumeReplicationInternalV1beta{
					Name:         gcpgenserver.NewOptString("replication-2"),
					EndpointType: gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
				},
			},
		}

		convertToVolumeReplicationsInternalV1Beta = func(reps []*datamodel.VolumeReplication) []gcpgenserver.VolumeReplicationInternalV1beta {
			var internalReps []gcpgenserver.VolumeReplicationInternalV1beta
			for _, rep := range reps {
				internalReps = append(internalReps, gcpgenserver.VolumeReplicationInternalV1beta{
					Name:         gcpgenserver.NewOptString(rep.Name),
					EndpointType: gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
				})
			}
			return internalReps
		}

		mockOrchestrator.EXPECT().GetMultipleReplicationsInternal(mock.Anything, mock.Anything, mock.Anything).Return(replications, nil)

		resp, err := handler.V1betaGetMultipleReplicationsInternal(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
}
