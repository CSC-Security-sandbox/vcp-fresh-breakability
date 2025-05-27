package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
			AllocatedBytes:           gcpgenserver.NewOptNilFloat64(pool.AllocatedBytes),
			TotalThroughputMibps:     gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps),
			AvailableThroughputMibps: gcpgenserver.NewOptNilFloat64(pool.TotalThroughputMibps - pool.UtilizedThroughputMibps),
			NumberOfVolumes:          gcpgenserver.NewOptNilInt32(int32(pool.NumberOfVolumes)),
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
