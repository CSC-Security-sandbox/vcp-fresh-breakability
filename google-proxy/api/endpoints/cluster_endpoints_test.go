package api

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestV1BetaInternalAcceptClusterPeer(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaInternalAcceptClusterPeerParams{
			LocationId:    "invalid-location-id",
			ProjectNumber: "project-number",
		}
		time1 := time.Now()
		req := &gcpgenserver.ClusterPeerV1{
			PeerAddresses:   []string{"10.20.0.0"},
			PeerClusterName: "peer-cluster",
			ExpiryTime:      gcpgenserver.NewOptNilDateTime(time1),
			Passphrase:      "passphrase",
			PoolUUID:        "pool-uuid",
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaInternalAcceptClusterPeer(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaInternalAcceptClusterPeerBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaInternalAcceptClusterPeerBadRequest).Message)
	})
	t.Run("WhenOrchestratorFailsToAcceptClusterPeer", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaInternalAcceptClusterPeerParams{
			LocationId:    "valid-location-id",
			ProjectNumber: "project-number",
		}
		time1 := time.Now()
		req := &gcpgenserver.ClusterPeerV1{
			PeerAddresses:   []string{"10.20.0.0"},
			PeerClusterName: "peer-cluster",
			ExpiryTime:      gcpgenserver.NewOptNilDateTime(time1),
			Passphrase:      "passphrase",
			PoolUUID:        "pool-uuid",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().AcceptClusterPeer(mock.Anything, mock.Anything, req.PoolUUID).Return(nil, nil, errors.New("Some error"))

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaInternalAcceptClusterPeer(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
	})
	t.Run("WhenAllInputsAreValid", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaInternalAcceptClusterPeerParams{
			LocationId:    "valid-location-id",
			ProjectNumber: "project-number",
		}
		time1 := time.Now()
		req := &gcpgenserver.ClusterPeerV1{
			PeerAddresses:   []string{"10.20.0.0"},
			PeerClusterName: "peer-cluster",
			ExpiryTime:      gcpgenserver.NewOptNilDateTime(time1),
			Passphrase:      "passphrase",
			PoolUUID:        "pool-uuid",
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}
		mockOrchestrator.EXPECT().AcceptClusterPeer(mock.Anything, mock.Anything, req.PoolUUID).Return(&common.ClusterPeerParams{
			PeerAddresses: []string{"10.20.0.0"},
			PeerName:      "peer-cluster",
		}, &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      "job-uuid",
				CreatedAt: time1,
			},
			WorkflowID: "workflow-id",
		}, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaInternalAcceptClusterPeer(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		res := result.(*gcpgenserver.ClusterPeerV1)
		assert.Equal(tt, "peer-cluster", res.PeerClusterName)
		assert.Equal(tt, "pool-uuid", res.PoolUUID)
		assert.Len(tt, res.Jobs, 1)
		assert.Equal(tt, "job-uuid", res.Jobs[0].JobId.Value)
	})
}
