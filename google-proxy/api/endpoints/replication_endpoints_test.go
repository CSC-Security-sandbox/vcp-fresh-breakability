package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaGetMultipleReplications(t *testing.T) {
	t.Run("WhenReplicationURIsAreEmpty", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{},
		}

		errorMessage := "Replication URIs cannot be empty"
		errorCode := float64(400)

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenAccountNameDoesNotMatch", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/stargate/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "replicationURIs projectNumber in body does not match projectNumber in parameter"
		errorCode := float64(400)

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenLocationDoesNotMatch", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "replicationURIs locationId in body does not match locationId in parameter"
		errorCode := float64(400)

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenVolumeDoesNotMatch", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource/replications/replication-name-6"},
		}

		errorMessage := "replicationURIs volumeId in body does not match volumeResourceId in parameter"
		errorCode := float64(400)

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenGetMultipleReplicationsReturnsError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return(nil, errors.New("Error retrieving replications from VCP"))

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Code)
		assert.Equal(tt, "Error retrieving replications from VCP", result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Message)
	})
	t.Run("WhenRelicationsFoundInVCP", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}
		expResp := []gcpgenserver.ReplicationV1beta{
			{
				ReplicationId: gcpgenserver.NewOptString("replication-id-1"),
				ResourceId:    gcpgenserver.NewOptString("resource-id-1"),
				MirrorState:   gcpgenserver.NewOptReplicationV1betaMirrorState(gcpgenserver.ReplicationV1betaMirrorStateMIRRORED),
			},
		}

		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return(expResp, nil)

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, 1, len(result.(*gcpgenserver.V1betaGetMultipleReplicationsOK).Replications))
	})
	t.Run("WhenGetMultipleReplicationsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &replications.V1betaGetMultipleReplicationsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithUnauthorized", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &replications.V1betaGetMultipleReplicationsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsUnauthorized).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithForbidden", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &replications.V1betaGetMultipleReplicationsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsForbidden).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsForbidden).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithNotFound", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "NotFound error"
		errorCode := float64(404)
		mockError := &replications.V1betaGetMultipleReplicationsNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsNotFound).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsNotFound).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithTooManyRequests", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "TooManyRequests error"
		errorCode := float64(429)
		mockError := &replications.V1betaGetMultipleReplicationsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsTooManyRequests).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsTooManyRequests).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithDefault", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &replications.V1betaGetMultipleReplicationsDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleReplicationsResponseIsNil", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}

		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, nil)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Code)
		assert.Equal(tt, "unknown error during the get multiple replications", result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleReplicationsSucceeds", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"projects/project-number/locations/location-id/volumes/volume-resource-id/replications/replication-name-6"},
		}
		clusterLocation := "cluster-location"
		description := "description"
		destination := models.ReplicationVolumeInformationV1beta{
			VolumeName: "volume-name",
			VolumeID:   "volume-id",
		}
		mockResponse := &replications.V1betaGetMultipleReplicationsOK{
			Payload: &replications.V1betaGetMultipleReplicationsOKBody{
				Replications: []*models.ReplicationV1beta{
					{
						ClusterLocation: &clusterLocation,
						ReplicationID:   "replication-id-1",
						Description:     &description,
						Destination:     &destination,
					},
				},
			},
		}
		mockOrchestrator.EXPECT().GetMultipleReplications(mock.Anything, mock.Anything).Return([]gcpgenserver.ReplicationV1beta{}, nil)
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpgenserver.V1betaGetMultipleReplicationsOK)
		assert.True(tt, ok)
		assert.Equal(tt, "replication-id-1", successResult.Replications[0].ReplicationId.Value)
	})
}

func TestV1betaGetReplicationCount(t *testing.T) {
	t.Run("WhenGetReplicationCountSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		mockOrchestrator.On("GetReplicationCount", mock.Anything, "project-number").Return(int64(5), nil)

		params := gcpgenserver.V1betaGetReplicationCountParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}

		result, err := handler.V1betaGetReplicationCount(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, 5, result.(*gcpgenserver.V1betaGetReplicationCountOK).ReplicationCount)
	})

	t.Run("WhenGetReplicationCountFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		mockOrchestrator.On("GetReplicationCount", mock.Anything, "project-number").Return(int64(0), assert.AnError)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		params := gcpgenserver.V1betaGetReplicationCountParams{
			ProjectNumber: "project-number",
			LocationId:    "location-id",
		}

		result, err := handler.V1betaGetReplicationCount(context.Background(), params)

		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestV1betaCreateReplication(t *testing.T) {
	t.Run("WhenCRRNotEnabled", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			crrEnabled = env.GetBool("CRR_ENABLED", true)
		}()
		crrEnabled = false
		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}
		result, _ := handler.V1betaCreateReplication(context.Background(), req, params)
		assert.Equal(tt, float64(403), result.(*gcpgenserver.V1betaCreateReplicationForbidden).Code)
	})
	t.Run("WhenCreateReplicationSucceedsWithNoJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertModelToVCPVolumeReplication = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		mockOrchestrator.On("CreateVolumeReplication", mock.Anything, mock.Anything).Return(&models2.VolumeReplication{}, "job-uuid", nil)

		result, err := handler.V1betaCreateReplication(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenCreateReplicationSucceedsWithJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertModelToVCPVolumeReplication = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		repResponse := &models2.VolumeReplication{
			State: models2.LifeCycleStateCreating,
		}

		replicationParams := &common.CreateVolumeReplicationParams{
			AccountName:      params.ProjectNumber,
			Region:           "location-id",
			Name:             req.ResourceId,
			SourceVolumeName: params.VolumeResourceId,
			CorrelationId:    params.XCorrelationID.Value,
		}

		replicationParams.Body = req
		if req.Description.IsSet() {
			replicationParams.Description, _ = req.Description.Get()
		}

		mockOrchestrator.On("CreateVolumeReplication", context.Background(), replicationParams).Return(repResponse, "job-uuid", nil)

		result, err := handler.V1betaCreateReplication(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}

		result, _ := handler.V1betaCreateReplication(context.Background(), req, params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreateReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaCreateReplicationBadRequest).Message)
	})
	t.Run("WhenCreateReplicationFailsWithBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("CreateVolumeReplication", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("Invalid input"))

		result, err := handler.V1betaCreateReplication(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaCreateReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid input", result.(*gcpgenserver.V1betaCreateReplicationBadRequest).Message)
	})
	t.Run("WhenCreateReplicationFailsWithSomeOtherError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		params := gcpgenserver.V1betaCreateReplicationParams{
			ProjectNumber:    "project-number",
			LocationId:       "location-id",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		req := &gcpgenserver.ReplicationCreateV1beta{
			ResourceId:  "resource-id",
			Description: gcpgenserver.NewOptString("description"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("CreateVolumeReplication", mock.Anything, mock.Anything).Return(nil, "", errors.New("some error"))

		result, _ := handler.V1betaCreateReplication(context.Background(), req, params)

		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaCreateReplicationInternalServerError).Code)
		assert.Equal(tt, "some error", result.(*gcpgenserver.V1betaCreateReplicationInternalServerError).Message)
	})
}

func TestV1betaResumeReplication(t *testing.T) {
	t.Run("WhenCRRNotEnabled", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			crrEnabled = env.GetBool("CRR_ENABLED", true)
		}()
		crrEnabled = false
		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		result, _ := handler.V1betaResumeReplication(context.Background(), params)
		assert.Equal(tt, float64(403), result.(*gcpgenserver.V1betaResumeReplicationForbidden).Code)
	})
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		resp, _ := handler.V1betaResumeReplication(context.Background(), params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaResumeReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", resp.(*gcpgenserver.V1betaResumeReplicationBadRequest).Message)
	})
	t.Run("WhenResumeReplicationFailsWithBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("ResumeReplication", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("Invalid input"))

		result, err := handler.V1betaResumeReplication(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaResumeReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid input", result.(*gcpgenserver.V1betaResumeReplicationBadRequest).Message)
	})
	t.Run("WhenResumeReplicationFailsWithSomeOtherError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("ResumeReplication", mock.Anything, mock.Anything).Return(nil, "", errors.New("some error"))

		result, err := handler.V1betaResumeReplication(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaResumeReplicationInternalServerError).Code)
		assert.Equal(tt, "some error", result.(*gcpgenserver.V1betaResumeReplicationInternalServerError).Message)
	})
	t.Run("WhenResumeReplicationSucceedsWithNoJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertResumeModelToVCPVolumeReplicationV1beta = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		mockOrchestrator.On("ResumeReplication", mock.Anything, mock.Anything).Return(&models2.VolumeReplication{}, "job-uuid", nil)

		result, err := handler.V1betaResumeReplication(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenResumeReplicationSucceedsWithJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaResumeReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertResumeModelToVCPVolumeReplicationV1beta = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		repResponse := &models2.VolumeReplication{
			State: models2.LifeCycleStateUpdating,
		}

		mockOrchestrator.On("ResumeReplication", mock.Anything, mock.Anything).Return(repResponse, "job-uuid", nil)

		result, err := handler.V1betaResumeReplication(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}

func TestV1betaStopReplication(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaStopReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "invalid-location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationStopV1beta{}

		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{Code: 400, Message: "Invalid location ID"}
		}

		result, _ := handler.V1betaStopReplication(context.Background(), req, params)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaStopReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", result.(*gcpgenserver.V1betaStopReplicationBadRequest).Message)
	})

	t.Run("WhenStopReplicationFailsWithBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		params := gcpgenserver.V1betaStopReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationStopV1beta{}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("StopReplication", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("Invalid input"))

		result, err := handler.V1betaStopReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(400), result.(*gcpgenserver.V1betaStopReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid input", result.(*gcpgenserver.V1betaStopReplicationBadRequest).Message)
	})
	t.Run("WhenStopReplicationFailsWithInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		params := gcpgenserver.V1betaStopReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		req := &gcpgenserver.ReplicationStopV1beta{}

		mockOrchestrator.On("StopReplication", mock.Anything, mock.Anything).Return(nil, "", errors.New("internal error"))

		result, err := handler.V1betaStopReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaStopReplicationInternalServerError).Code)
		assert.Equal(tt, "internal error", result.(*gcpgenserver.V1betaStopReplicationInternalServerError).Message)
	})
	t.Run("WhenStopReplicationSucceeds", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		params := gcpgenserver.V1betaStopReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationStopV1beta{}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertResumeModelToVCPVolumeReplicationV1beta = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}
		mockOrchestrator.On("StopReplication", mock.Anything, mock.Anything).Return(&models2.VolumeReplication{}, "job-uuid", nil)

		result, err := handler.V1betaStopReplication(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", result.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}

// google-proxy/api/endpoints/replication_endpoints_test.go

func TestV1betaDeleteReplication(t *testing.T) {
	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()
		req := gcpgenserver.ReplicationDeleteV1beta{
			CleanupResourcesJobId: gcpgenserver.NewOptString("123"),
		}
		params := gcpgenserver.V1betaDeleteReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location ID",
			}
		}

		resp, _ := handler.V1betaDeleteReplication(context.Background(), &req, params)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaDeleteReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid location ID", resp.(*gcpgenserver.V1betaDeleteReplicationBadRequest).Message)
	})
	t.Run("WhenDeleteReplicationFailsWithBadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := gcpgenserver.ReplicationDeleteV1beta{
			CleanupResourcesJobId: gcpgenserver.NewOptString("123"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("DeleteReplication", mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("Invalid input"))

		resp, err := handler.V1betaDeleteReplication(context.Background(), &req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(400), resp.(*gcpgenserver.V1betaDeleteReplicationBadRequest).Code)
		assert.Equal(tt, "Invalid input", resp.(*gcpgenserver.V1betaDeleteReplicationBadRequest).Message)
	})
	t.Run("WhenDeleteReplicationFailsWithSomeOtherError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := gcpgenserver.ReplicationDeleteV1beta{
			CleanupResourcesJobId: gcpgenserver.NewOptString("123"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator.On("DeleteReplication", mock.Anything, mock.Anything).Return(nil, "", errors.New("some error"))

		resp, err := handler.V1betaDeleteReplication(context.Background(), &req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, float64(500), resp.(*gcpgenserver.V1betaDeleteReplicationInternalServerError).Code)
		assert.Equal(tt, "some error", resp.(*gcpgenserver.V1betaDeleteReplicationInternalServerError).Message)
	})

	t.Run("WhenResumeReplicationSucceedsWithNoJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := gcpgenserver.ReplicationDeleteV1beta{
			CleanupResourcesJobId: gcpgenserver.NewOptString("123"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertResumeModelToVCPVolumeReplicationV1beta = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		mockOrchestrator.On("DeleteReplication", mock.Anything, mock.Anything).Return(&models2.VolumeReplication{}, "job-uuid", nil)

		resp, err := handler.V1betaDeleteReplication(context.Background(), &req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", resp.(*gcpgenserver.OperationV1beta).Name.Value)
	})
	t.Run("WhenResumeReplicationSucceedsWithJob", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		defer func() {
			parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
		}()

		params := gcpgenserver.V1betaDeleteReplicationParams{
			ProjectNumber:         "project-number",
			LocationId:            "location-id",
			VolumeResourceId:      "volume-resource-id",
			ReplicationResourceId: "replication-resource-id",
			XCorrelationID:        gcpgenserver.NewOptString("X-Correlation-ID"),
		}

		req := gcpgenserver.ReplicationDeleteV1beta{
			CleanupResourcesJobId: gcpgenserver.NewOptString("123"),
		}
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}
		convertResumeModelToVCPVolumeReplicationV1beta = func(volumeReplication *models2.VolumeReplication) *gcpgenserver.ReplicationV1beta {
			return &gcpgenserver.ReplicationV1beta{}
		}

		repResponse := &models2.VolumeReplication{
			State: models2.LifeCycleStateDeleting,
		}

		mockOrchestrator.On("DeleteReplication", mock.Anything, mock.Anything).Return(repResponse, "job-uuid", nil)

		resp, err := handler.V1betaDeleteReplication(context.Background(), &req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", resp.(*gcpgenserver.OperationV1beta).Name.Value)
	})
}
