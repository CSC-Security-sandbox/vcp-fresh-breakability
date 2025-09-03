package api

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
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
			PoolUUID:      gcpgenserver.NewOptString("test-pool"),
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
			PoolUUID:      gcpgenserver.NewOptString("test-pool"),
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
			PoolUUID:      gcpgenserver.NewOptString("test-pool"),
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

func TestBetaInternalmountVolumeReplication(t *testing.T) {
	t.Run("ReturnsInternalServerErrorWhenPerformMountCheckFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		mockOrchestrator.On("PerformMountCheck", mock.Anything, "volume-replication-id", "project-number").
			Return(nil, errors.New("mount check failed"))

		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaInternalMountVolumeReplicationParams{
			VolumeReplicationId: "volume-replication-id",
			ProjectNumber:       "project-number",
		}
		result, _ := handler.V1betaInternalMountVolumeReplication(context.Background(), params)
		assert.IsType(tt, &gcpgenserver.V1betaInternalMountVolumeReplicationInternalServerError{Code: 500, Message: "mount check failed"}, result)
		mockOrchestrator.AssertExpectations(tt)
	})
	t.Run("ReturnsVolumeReplicationInternalWhenMountCheckSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		mockJob := &models.Job{
			BaseModel: models.BaseModel{
				UUID:      "job-uuid",
				CreatedAt: time.Now(),
			},
			WorkflowID: "worker-id",
			State:      "completed",
		}
		mockOrchestrator.On("PerformMountCheck", mock.Anything, "volume-replication-id", "project-number").
			Return(mockJob, nil)

		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaInternalMountVolumeReplicationParams{
			VolumeReplicationId: "volume-replication-id",
			ProjectNumber:       "project-number",
		}
		result, err := handler.V1betaInternalMountVolumeReplication(context.Background(), params)

		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.InternalJobV1beta{}, result)
		volumeReplication := result.(*gcpgenserver.InternalJobV1beta)
		assert.Equal(tt, "job-uuid", volumeReplication.JobUuid.Value)
		mockOrchestrator.AssertExpectations(tt)
	})
}

func TestV1betaInternalResumeVolumeReplication(t *testing.T) {
	t.Run("WhenResumeVolumeReplicationInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			ForceResume:   gcpgenserver.OptBool{Set: true, Value: false},
		}
		expectedResponse := &gcpgenserver.V1betaInternalResumeVolumeReplicationInternalServerError{
			Code:    500,
			Message: "Internal server error while resuming replication",
		}
		mockOrchestrator.EXPECT().ResumeReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("some error"))
		resp, err := handler.V1betaInternalResumeVolumeReplication(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
	t.Run("WhenResumeVolumeReplicationBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			ForceResume:   gcpgenserver.OptBool{Set: true, Value: false},
		}
		expectedResponse := &gcpgenserver.V1betaInternalResumeVolumeReplicationBadRequest{
			Code:    400,
			Message: "Invalid request parameters",
		}
		mockOrchestrator.EXPECT().ResumeReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewUserInputValidationErr("Invalid request parameters"))
		resp, err := handler.V1betaInternalResumeVolumeReplication(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
	t.Run("WhenResumeVolumeReplicationNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			ForceResume:   gcpgenserver.OptBool{Set: true, Value: false},
		}
		expectedResponse := &gcpgenserver.V1betaInternalResumeVolumeReplicationNotFound{
			Code:    404,
			Message: "Volume replication not found",
		}
		mockOrchestrator.EXPECT().ResumeReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundErr("Volume replication not found", nil))
		resp, err := handler.V1betaInternalResumeVolumeReplication(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
	t.Run("WhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaInternalResumeVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			ForceResume:         gcpgenserver.OptBool{Set: true, Value: false},
			VolumeReplicationId: "test-replication-id",
		}
		replication := &models.VolumeReplication{
			Name: "test-replication",
			BaseModel: models.BaseModel{
				UUID: "replication-uuid",
			},
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationReplicationUUID: "destination-replication-uuid",
			},
		}
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "job-uuid",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			WorkflowID: "job-workflow-id",
		}
		expectedResponse := convertToInternalV1betaVolumeReplication(replication, job)
		mockOrchestrator.EXPECT().ResumeReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(replication, job, nil)
		resp, err := handler.V1betaInternalResumeVolumeReplication(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
}

func TestV1betaInternalDeleteVolumeReplicationRow(t *testing.T) {
	t.Run("ReturnsInternalServerErrorWhenDeleteFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().ReleaseVolumeReplication(ctx, mock.Anything).Return(nil, nil, errors.New("delete error"))
		resp, err := handler.V1betaInternalReleaseVolumeReplication(ctx, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalReleaseVolumeReplicationInternalServerError{}, resp)
		assert.Equal(tt, float64(500), resp.(*gcpgenserver.V1betaInternalReleaseVolumeReplicationInternalServerError).Code)
	})
	t.Run("ReturnsNotFoundErrorWhenDeleteFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().ReleaseVolumeReplication(ctx, mock.Anything).Return(nil, nil, errors.NewNotFoundErr("Volume replication not found", nil))
		resp, err := handler.V1betaInternalReleaseVolumeReplication(ctx, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalReleaseVolumeReplicationBadRequest{}, resp)
		assert.Equal(tt, float64(404), resp.(*gcpgenserver.V1betaInternalReleaseVolumeReplicationBadRequest).Code)
	})
	t.Run("ReturnsOKWhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalReleaseVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
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
		expectedOperationName := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)
		expectedResponse := &gcpgenserver.OperationV1beta{
			Name: gcpgenserver.NewOptString(expectedOperationName),
			Done: gcpgenserver.NewOptBool(true),
		}
		mockOrchestrator.EXPECT().ReleaseVolumeReplication(ctx, mock.Anything).Return(volumeReplication, job, nil)

		resp, err := handler.V1betaInternalReleaseVolumeReplication(ctx, params)
		operationResp := resp.(*gcpgenserver.OperationV1beta)

		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse.Name.Value, operationResp.Name.Value)
		assert.Equal(tt, expectedResponse.Done.Value, operationResp.Done.Value)
	})
}

func TestV1betaInternalDeleteVolumeReplication(t *testing.T) {
	t.Run("ReturnsInternalServerErrorWhenDeleteFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalDeleteVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteReplicationInternal(ctx, params.VolumeReplicationId).Return(nil, nil, errors.New("delete error"))
		resp, err := handler.V1betaInternalDeleteVolumeReplication(ctx, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalDeleteVolumeReplicationInternalServerError{}, resp)
		assert.Equal(tt, float64(500), resp.(*gcpgenserver.V1betaInternalDeleteVolumeReplicationInternalServerError).Code)
	})
	t.Run("WhenVolumeReplicationNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalDeleteVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteReplicationInternal(ctx, params.VolumeReplicationId).Return(nil, nil, errors.NewNotFoundErr("Volume replication not found", nil))
		resp, err := handler.V1betaInternalDeleteVolumeReplication(ctx, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalDeleteVolumeReplicationBadRequest{}, resp)
		assert.Equal(tt, float64(404), resp.(*gcpgenserver.V1betaInternalDeleteVolumeReplicationBadRequest).Code)
	})
	t.Run("ReturnsOKWhenSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		ctx := context.Background()
		params := gcpgenserver.V1betaInternalDeleteVolumeReplicationParams{
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
			VolumeReplicationId: "test-replication-id",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
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
		mockOrchestrator.EXPECT().DeleteReplicationInternal(ctx, mock.Anything).Return(volumeReplication, job, nil)
		resp, err := handler.V1betaInternalDeleteVolumeReplication(ctx, params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})
}

func TestV1betaInternalDeleteVolumeSnapshot(t *testing.T) {
	params := gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
		VolumeId:      "test-volume",
	}
	t.Run("ReturnsBadRequestOnInvalidLocation", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		resp, err := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest{}, resp)

		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest).Message)
	})
	t.Run("ReturnsNotFoundWhenSnapshotNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteSnapmirrorSnapshots(mock.Anything, mock.Anything).Return("", errors.NewNotFoundErr("snapshot", nil))
		resp, _ := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(404), resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest).Code)
	})
	t.Run("ReturnsBadRequestOnUserInputValidationErr", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteSnapmirrorSnapshots(mock.Anything, mock.Anything).Return("", errors.NewUserInputValidationErr("bad input"))
		resp, _ := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest).Code)
	})
	t.Run("ReturnsConflictOnConflictErr", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteSnapmirrorSnapshots(mock.Anything, mock.Anything).Return("", errors.NewConflictErr("conflict"))
		resp, _ := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(409), resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotConflict).Code)
	})
	t.Run("ReturnsInternalServerErrorOnOtherError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteSnapmirrorSnapshots(mock.Anything, mock.Anything).Return("", errors.New("internal error"))
		resp, _ := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(500), resp.(*gcpgenserver.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError).Code)
	})
	t.Run("ReturnsInternalServerErrorOnOtherError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		operationID := "/v1beta/projects/" + params.ProjectNumber + "/locations/" + params.LocationId + "/operations/" + "op-id"
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		mockOrchestrator.EXPECT().DeleteSnapmirrorSnapshots(mock.Anything, mock.Anything).Return("op-id", nil)
		resp, err := handler.V1betaInternalDeleteVolumeSnapmirrorSnapshot(context.Background(), params)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, operationID, resp.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}

func TestV1betaInternalStopVolumeReplication(t *testing.T) {
	t.Run("WhenVolumeReplicationNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaInternalStopVolumeReplicationParams{
			VolumeReplicationId: "test-replication-id",
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
		}
		req := &gcpgenserver.V1betaInternalStopVolumeReplicationReq{
			Force: gcpgenserver.OptBool{Set: true, Value: false},
		}

		mockOrchestrator.EXPECT().StopReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundErr("Volume replication not found", nil))

		resp, err := handler.V1betaInternalStopVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.Equal(tt, float64(404), resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationNotFound).Code)
		assert.Equal(tt, "Volume replication not found", resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationNotFound).Message)
	})

	t.Run("WhenInvalidRequestParameters", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaInternalStopVolumeReplicationParams{
			VolumeReplicationId: "test-replication-id",
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
		}
		req := &gcpgenserver.V1betaInternalStopVolumeReplicationReq{
			Force: gcpgenserver.OptBool{Set: true, Value: false},
		}

		mockOrchestrator.EXPECT().StopReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.NewUserInputValidationErr("Invalid request parameters"))

		resp, err := handler.V1betaInternalStopVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid request parameters", resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationBadRequest).Message)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaInternalStopVolumeReplicationParams{
			VolumeReplicationId: "test-replication-id",
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
		}
		req := &gcpgenserver.V1betaInternalStopVolumeReplicationReq{
			Force: gcpgenserver.OptBool{Set: true, Value: false},
		}

		mockOrchestrator.EXPECT().StopReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, errors.New("Internal server error"))

		resp, err := handler.V1betaInternalStopVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.Equal(tt, float64(500), resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationInternalServerError).Code)
		assert.Equal(tt, "Internal server error while resuming replication", resp.(*gcpgenserver.V1betaInternalStopVolumeReplicationInternalServerError).Message)
	})
	t.Run("WhenStopReplicationSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaInternalStopVolumeReplicationParams{
			VolumeReplicationId: "test-replication-id",
			ProjectNumber:       "test-project",
			LocationId:          "test-location",
		}
		req := &gcpgenserver.V1betaInternalStopVolumeReplicationReq{
			Force: gcpgenserver.OptBool{Set: true, Value: true},
		}

		replication := &models.VolumeReplication{
			Name: "test-replication",
			BaseModel: models.BaseModel{
				UUID: "replication-uuid",
			},
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationReplicationUUID: "destination-replication-uuid",
			},
		}
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "job-uuid",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			WorkflowID: "job-workflow-id",
		}

		mockOrchestrator.EXPECT().StopReplicationInternal(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(replication, job, nil)

		resp, err := handler.V1betaInternalStopVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.Equal(tt, convertToInternalV1betaVolumeReplication(replication, job), resp)
	})
}

func TestV1betaUpdateVolumeReplicationInternal(t *testing.T) {
	t.Run("WhenUpdateSuccess", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.VolumeReplicationUpdateInternalV1beta{
			Description: gcpgenserver.NewOptNilString("desc"),
		}
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationParams{
			VolumeReplicationId: "rep-uuid",
			ProjectNumber:       "proj-1",
			XCorrelationID:      gcpgenserver.OptString{Value: "corr-id", Set: true},
			LocationId:          "loc-1",
		}
		volumeReplication := &models.VolumeReplication{
			BaseModel:             models.BaseModel{UUID: "rep-uuid"},
			ReplicationAttributes: &models.ReplicationDetails{EndpointType: "dst"},
		}
		job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-uuid"}}
		mockOrchestrator.EXPECT().UpdateVolumeReplicationInternal(mock.Anything, mock.Anything).Return(volumeReplication, job, nil)

		resp, err := handler.V1betaInternalUpdateVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
	})

	t.Run("WhenNotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.VolumeReplicationUpdateInternalV1beta{}
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationParams{
			VolumeReplicationId: "rep-uuid",
			ProjectNumber:       "proj-1",
			XCorrelationID:      gcpgenserver.OptString{Value: "corr-id", Set: true},
			LocationId:          "loc-1",
		}
		mockOrchestrator.EXPECT().UpdateVolumeReplicationInternal(mock.Anything, mock.Anything).Return(nil, nil, errors.NewNotFoundErr("replication", nil))

		resp, err := handler.V1betaInternalUpdateVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalUpdateVolumeReplicationBadRequest{}, resp)
		assert.Equal(tt, 404, int(resp.(*gcpgenserver.V1betaInternalUpdateVolumeReplicationBadRequest).Code))
	})

	t.Run("WhenInternalError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		req := &gcpgenserver.VolumeReplicationUpdateInternalV1beta{}
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationParams{
			VolumeReplicationId: "rep-uuid",
			ProjectNumber:       "proj-1",
			XCorrelationID:      gcpgenserver.OptString{Value: "corr-id", Set: true},
			LocationId:          "loc-1",
		}
		mockOrchestrator.EXPECT().UpdateVolumeReplicationInternal(mock.Anything, mock.Anything).Return(nil, nil, errors.New("some error"))

		resp, err := handler.V1betaInternalUpdateVolumeReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalUpdateVolumeReplicationInternalServerError{}, resp)
		assert.Equal(tt, "some error", resp.(*gcpgenserver.V1betaInternalUpdateVolumeReplicationInternalServerError).Message)
	})
}

func TestV1betaInternalDescribeVolume(t *testing.T) {
	t.Run("WhenVolumeNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(nil, errors.NewNotFoundErr("volume", nil))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		expectedResponse := &gcpgenserver.V1betaInternalDescribeVolumeNotFound{
			Code:    404,
			Message: "Volume not found",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)
		assert.Equal(tt, expectedResponse, resp)
	})

	t.Run("WhenGetVolumeReturnsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(nil, errors.New("database error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.Error(tt, err)
		assert.Equal(tt, "database error", err.Error())

		// Should return Internal Server Error
		serverError, ok := resp.(*gcpgenserver.V1betaInternalDescribeVolumeInternalServerError)
		assert.True(tt, ok, "Response should be V1betaInternalDescribeVolumeInternalServerError")
		assert.Equal(tt, float64(500), serverError.Code)
		assert.Equal(tt, "Internal server error", serverError.Message)
	})

	t.Run("WhenJsonMarshalFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Mock volume data
		volume := &models.Volume{
			BaseModel: models.BaseModel{
				UUID: "vol-123",
			},
			DisplayName:    "test-volume",
			AccountName:    "test-account",
			PoolID:         "pool-123",
			PoolName:       "test-pool",
			CreationToken:  "creation-token",
			LifeCycleState: "READY",
			QuotaInBytes:   1073741824,
			SvmName:        "test-svm",
			ProtocolTypes:  []string{"NFSV3"},
			EncryptionType: "SOFTWARE_ENCRYPTION",
			Region:         "us-central1",
		}

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(volume, nil)

		// Restore original functions after tests
		defer func() {
			jsonMarshal = json.Marshal
		}()

		// Mock jsonMarshal to return error
		jsonMarshal = func(v interface{}) ([]byte, error) {
			return nil, errors.New("marshal error")
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)

		// Should return Internal Server Error with marshal error message
		serverError, ok := resp.(*gcpgenserver.V1betaInternalDescribeVolumeInternalServerError)
		assert.True(tt, ok, "Response should be V1betaInternalDescribeVolumeInternalServerError")
		assert.Equal(tt, float64(500), serverError.Code)
		assert.Equal(tt, "marshal error", serverError.Message)
	})

	t.Run("WhenJsonUnmarshalFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Mock volume data
		volume := &models.Volume{
			BaseModel: models.BaseModel{
				UUID: "vol-123",
			},
			DisplayName:    "test-volume",
			AccountName:    "test-account",
			PoolID:         "pool-123",
			PoolName:       "test-pool",
			CreationToken:  "creation-token",
			LifeCycleState: "READY",
			QuotaInBytes:   1073741824,
			SvmName:        "test-svm",
			ProtocolTypes:  []string{"NFSV3"},
			EncryptionType: "SOFTWARE_ENCRYPTION",
			Region:         "us-central1",
		}

		// Restore original functions after tests
		defer func() {
			jsonMarshal = json.Marshal
			jsonUnmarshal = json.Unmarshal
		}()

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(volume, nil)
		jsonUnmarshal = func(data []byte, v interface{}) error {
			return errors.New("unmarshal error")
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)

		// Should return Internal Server Error with unmarshal error message
		serverError, ok := resp.(*gcpgenserver.V1betaInternalDescribeVolumeInternalServerError)
		assert.True(tt, ok, "Response should be V1betaInternalDescribeVolumeInternalServerError")
		assert.Equal(tt, float64(500), serverError.Code)
		assert.Equal(tt, "unmarshal error", serverError.Message)
	})

	t.Run("WhenSuccessfulVolumeDescribe", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Mock volume data with SvmName
		volume := &models.Volume{
			BaseModel: models.BaseModel{
				UUID: "vol-123",
			},
			DisplayName:    "test-volume",
			AccountName:    "test-account",
			PoolID:         "pool-123",
			PoolName:       "test-pool",
			CreationToken:  "creation-token",
			LifeCycleState: "READY",
			QuotaInBytes:   1073741824,
			SvmName:        "test-svm",
			ProtocolTypes:  []string{"NFSV3"},
			EncryptionType: "SOFTWARE_ENCRYPTION",
			Region:         "us-central1",
		}

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(volume, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)

		// Verify response type
		successResp, ok := resp.(*gcpgenserver.InternalVolumeV1beta)
		assert.True(tt, ok, "Response should be *InternalVolumeV1beta")
		assert.NotNil(tt, successResp)

		// Verify SvmName is set
		assert.True(tt, successResp.SvmName.Set)
		assert.Equal(tt, "test-svm", successResp.SvmName.Value)
	})

	t.Run("WhenSuccessfulVolumeDescribeWithoutSvmName", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		// Mock volume data without SvmName
		volume := &models.Volume{
			BaseModel: models.BaseModel{
				UUID: "vol-123",
			},
			DisplayName:    "test-volume",
			AccountName:    "test-account",
			PoolID:         "pool-123",
			PoolName:       "test-pool",
			CreationToken:  "creation-token",
			LifeCycleState: "READY",
			QuotaInBytes:   1073741824,
			SvmName:        "", // Empty SvmName
			ProtocolTypes:  []string{"NFSV3"},
			EncryptionType: "SOFTWARE_ENCRYPTION",
			Region:         "us-central1",
		}

		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-123", true).Return(volume, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaInternalDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-123",
		}

		resp, err := handler.V1betaInternalDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)

		// Verify response type
		successResp, ok := resp.(*gcpgenserver.InternalVolumeV1beta)
		assert.True(tt, ok, "Response should be *InternalVolumeV1beta")
		assert.NotNil(tt, successResp)

		// Verify SvmName is empty when volume SvmName is empty
		assert.True(tt, successResp.SvmName.Set)
		assert.Equal(tt, "", successResp.SvmName.Value)
	})
}

// TestInternalVolumeV1beta_ResourceId_ValidationChange tests the validation change
// for InternalVolume_v1beta resourceId from the old pattern ^[a-zA-Z][a-zA-Z0-9_]{0,62}$
// to the new pattern ^[a-z]([a-z0-9-_]{0,61}[a-z0-9])?$
func TestInternalVolumeV1beta_ResourceId_ValidationChange(t *testing.T) {
	t.Run("ValidResourceIds_ShouldPass", func(t *testing.T) {
		validResourceIds := []string{
			"a",             // Single lowercase letter
			"ab",            // Two characters
			"a-b",           // Hyphen allowed
			"a_b",           // Underscore allowed
			"volume1",       // Common format
			"my-volume-123", // Mixed with hyphen
			"test_vol_1",    // Mixed with underscore
		}

		for _, resourceId := range validResourceIds {
			volume := gcpgenserver.InternalVolumeV1beta{
				ResourceId: gcpgenserver.NewOptString(resourceId),
			}
			err := volume.Validate()
			assert.NoError(t, err, "ResourceId '%s' should be valid", resourceId)
		}
	})

	t.Run("InvalidResourceIds_ShouldFail", func(t *testing.T) {
		invalidResourceIds := []string{
			"",        // Empty string
			"A",       // Uppercase not allowed
			"Volume1", // Starts with uppercase
			"1volume", // Starts with number
			"-volume", // Starts with hyphen
			"_volume", // Starts with underscore
			"volume-", // Ends with hyphen
			"volume_", // Ends with underscore
			"vol#ume", // Special characters not allowed
			"vol ume", // Space not allowed
		}

		for _, resourceId := range invalidResourceIds {
			volume := gcpgenserver.InternalVolumeV1beta{
				ResourceId: gcpgenserver.NewOptString(resourceId),
			}
			err := volume.Validate()
			assert.Error(t, err, "ResourceId '%s' should be invalid", resourceId)
		}
	})
}

func TestV1betaInternalUpdateVolumeReplicationAttributes(t *testing.T) {
	t.Run("WhenNotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		notFoundErr := errors.NewNotFoundErr("volume replication", nil)
		mockOrchestrator.EXPECT().UpdateVolumeReplicationAttributes(mock.Anything, mock.AnythingOfType("models.UpdateVolumeReplicationAttributesParams")).Return(nil, notFoundErr)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		req := &gcpgenserver.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: gcpgenserver.NewOptString("replication-123"),
			EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
		}
		
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-123",
		}
		
		response, err := handler.V1betaInternalUpdateVolumeReplicationAttributes(context.Background(), req, params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesBadRequest{}, response)
		badRequestResp := response.(*gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesBadRequest)
		assert.Equal(tt, float64(400), badRequestResp.Code)
		assert.Contains(tt, badRequestResp.Message, "volume replication")
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		internalErr := errors.New("database connection failed")
		mockOrchestrator.EXPECT().UpdateVolumeReplicationAttributes(mock.Anything, mock.AnythingOfType("models.UpdateVolumeReplicationAttributesParams")).Return(nil, internalErr)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		req := &gcpgenserver.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: gcpgenserver.NewOptString("replication-456"),
			EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeSrc,
		}
		
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesParams{
			ProjectNumber:       "project-456",
			LocationId:          "europe-west1",
			VolumeReplicationId: "replication-456",
		}
		
		response, err := handler.V1betaInternalUpdateVolumeReplicationAttributes(context.Background(), req, params)
		
		assert.Error(tt, err)
		assert.Equal(tt, "database connection failed", err.Error())
		assert.IsType(tt, &gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesInternalServerError{}, response)
		internalServerResp := response.(*gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesInternalServerError)
		assert.Equal(tt, float64(500), internalServerResp.Code)
		assert.Equal(tt, "Internal server error", internalServerResp.Message)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		job := &models.Job{
			BaseModel: models.BaseModel{
				UUID: "job-789",
			},
			State: models.JobsStateDONE,
		}
		mockOrchestrator.EXPECT().UpdateVolumeReplicationAttributes(mock.Anything, mock.AnythingOfType("models.UpdateVolumeReplicationAttributesParams")).Return(job, nil)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		req := &gcpgenserver.VolumeReplicationInternalV1beta{
			VolumeReplicationUuid: gcpgenserver.NewOptString("replication-789"),
			EndpointType:          gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst,
			SourceHostName:        "source-host",
			SourceServerName:      "source-svm",
			SourceVolumeName:      "source-volume",
		}
		
		params := gcpgenserver.V1betaInternalUpdateVolumeReplicationAttributesParams{
			ProjectNumber:       "project-789",
			LocationId:          "asia-southeast1",
			VolumeReplicationId: "replication-789",
		}
		
		response, err := handler.V1betaInternalUpdateVolumeReplicationAttributes(context.Background(), req, params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.OperationV1beta{}, response)
		operationResp := response.(*gcpgenserver.OperationV1beta)
		expectedOpName := fmt.Sprintf("/v1beta/projects/%s/locations/%s/operations/%s", params.ProjectNumber, params.LocationId, job.UUID)
		assert.Equal(tt, expectedOpName, operationResp.Name.Value)
		assert.True(tt, operationResp.Done.Value)
	})
}

func TestV1betaInternalReverseVolumeReplication(t *testing.T) {
	t.Run("WhenNotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		notFoundErr := errors.NewNotFoundErr("volume replication", nil)
		mockOrchestrator.EXPECT().ReverseReplicationInternal(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, notFoundErr)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		params := gcpgenserver.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "project-123",
			LocationId:          "us-central1",
			VolumeReplicationId: "replication-123",
			XCorrelationID:      gcpgenserver.NewOptString("corr-123"),
		}
		
		response, err := handler.V1betaInternalReverseVolumeReplication(context.Background(), params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalReverseVolumeReplicationNotFound{}, response)
		notFoundResp := response.(*gcpgenserver.V1betaInternalReverseVolumeReplicationNotFound)
		assert.Equal(tt, float64(404), notFoundResp.Code)
		assert.Equal(tt, "Volume replication not found", notFoundResp.Message)
	})

	t.Run("WhenUserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		validationErr := errors.NewUserInputValidationErr("Invalid replication state for reverse operation")
		mockOrchestrator.EXPECT().ReverseReplicationInternal(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, validationErr)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		params := gcpgenserver.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "project-456",
			LocationId:          "europe-west1",
			VolumeReplicationId: "replication-456",
			XCorrelationID:      gcpgenserver.NewOptString("corr-456"),
		}
		
		response, err := handler.V1betaInternalReverseVolumeReplication(context.Background(), params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalReverseVolumeReplicationBadRequest{}, response)
		badRequestResp := response.(*gcpgenserver.V1betaInternalReverseVolumeReplicationBadRequest)
		assert.Equal(tt, float64(400), badRequestResp.Code)
		assert.Equal(tt, "Invalid request parameters", badRequestResp.Message)
	})

	t.Run("WhenInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		internalErr := errors.New("database connection failed")
		mockOrchestrator.EXPECT().ReverseReplicationInternal(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil, internalErr)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		params := gcpgenserver.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "project-789",
			LocationId:          "asia-southeast1",
			VolumeReplicationId: "replication-789",
			XCorrelationID:      gcpgenserver.NewOptString("corr-789"),
		}
		
		response, err := handler.V1betaInternalReverseVolumeReplication(context.Background(), params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.V1betaInternalReverseVolumeReplicationInternalServerError{}, response)
		internalServerResp := response.(*gcpgenserver.V1betaInternalReverseVolumeReplicationInternalServerError)
		assert.Equal(tt, float64(500), internalServerResp.Code)
		assert.Equal(tt, "Internal server error while reversing replication", internalServerResp.Message)
	})

	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		volumeReplication := &models.VolumeReplication{
			BaseModel: models.BaseModel{
				UUID: "replication-success",
			},
			Name:  "test-replication",
			State: "reversed",
			ReplicationAttributes: &models.ReplicationDetails{
				EndpointType:            "dst",
				SourceRegion:            "us-central1",
				SourceHostName:          "source-host",
				SourceSvmName:           "source-svm",
				SourceVolumeName:        "source-volume",
				SourceVolumeUUID:        "src-vol-uuid",
				SourcePoolUUID:          "src-pool-uuid",
				DestinationHostName:     "dest-host",
				DestinationSvmName:      "dest-svm",
				DestinationVolumeName:   "dest-volume",
				DestinationVolumeUUID:   "dest-vol-uuid",
				DestinationPoolUUID:     "dest-pool-uuid",
				ReplicationType:         "async",
			},
		}
		
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-success",
			},
			State: "done",
		}
		
		mockOrchestrator.EXPECT().ReverseReplicationInternal(mock.Anything, mock.Anything, mock.Anything).Return(volumeReplication, job, nil)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		params := gcpgenserver.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "project-success",
			LocationId:          "us-west1",
			VolumeReplicationId: "replication-success",
			XCorrelationID:      gcpgenserver.NewOptString("corr-success"),
		}
		
		response, err := handler.V1betaInternalReverseVolumeReplication(context.Background(), params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.VolumeReplicationInternalV1beta{}, response)
		volumeReplResp := response.(*gcpgenserver.VolumeReplicationInternalV1beta)
		assert.Equal(tt, "replication-success", volumeReplResp.VolumeReplicationUuid.Value)
		assert.Equal(tt, gcpgenserver.VolumeReplicationInternalV1betaEndpointTypeDst, volumeReplResp.EndpointType)
		assert.Equal(tt, "source-host", volumeReplResp.SourceHostName)
		mockOrchestrator.AssertExpectations(tt)
	})

	t.Run("WhenEmptyCorrelationId", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		volumeReplication := &models.VolumeReplication{
			BaseModel: models.BaseModel{
				UUID: "replication-no-corr",
			},
			Name:  "test-replication-no-corr",
			State: "reversed",
			ReplicationAttributes: &models.ReplicationDetails{
				EndpointType:            "dst",
				SourceRegion:            "us-central1",
				SourceHostName:          "source-host-no-corr",
				SourceSvmName:           "source-svm-no-corr",
				SourceVolumeName:        "source-volume-no-corr",
				SourceVolumeUUID:        "src-vol-uuid-no-corr",
				SourcePoolUUID:          "src-pool-uuid-no-corr",
				DestinationHostName:     "dest-host-no-corr",
				DestinationSvmName:      "dest-svm-no-corr",
				DestinationVolumeName:   "dest-volume-no-corr",
				DestinationVolumeUUID:   "dest-vol-uuid-no-corr",
				DestinationPoolUUID:     "dest-pool-uuid-no-corr",
				ReplicationType:         "async",
			},
		}
		
		job := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID: "job-no-corr",
			},
			State: "done",
		}
		
		mockOrchestrator.EXPECT().ReverseReplicationInternal(mock.Anything, "replication-no-corr", "project-no-corr").Return(volumeReplication, job, nil)
		
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		
		params := gcpgenserver.V1betaInternalReverseVolumeReplicationParams{
			ProjectNumber:       "project-no-corr",
			LocationId:          "us-east1",
			VolumeReplicationId: "replication-no-corr",
			// No XCorrelationID provided
		}
		
		response, err := handler.V1betaInternalReverseVolumeReplication(context.Background(), params)
		
		assert.NoError(tt, err)
		assert.IsType(tt, &gcpgenserver.VolumeReplicationInternalV1beta{}, response)
		volumeReplResp := response.(*gcpgenserver.VolumeReplicationInternalV1beta)
		assert.NotNil(tt, volumeReplResp)
	})
}
