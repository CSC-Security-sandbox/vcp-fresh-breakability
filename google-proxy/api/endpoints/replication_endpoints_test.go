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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaGetMultipleReplications(t *testing.T) {
	t.Run("WhenGetMultipleReplicationsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
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
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsBadRequest).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithUnauthorized", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &replications.V1betaGetMultipleReplicationsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsUnauthorized).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithForbidden", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &replications.V1betaGetMultipleReplicationsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsForbidden).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsForbidden).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithNotFound", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		errorMessage := "NotFound error"
		errorCode := float64(404)
		mockError := &replications.V1betaGetMultipleReplicationsNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsNotFound).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsNotFound).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithTooManyRequests", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		errorMessage := "TooManyRequests error"
		errorCode := float64(429)
		mockError := &replications.V1betaGetMultipleReplicationsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsTooManyRequests).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsTooManyRequests).Message)
	})
	t.Run("WhenGetMultipleReplicationsFailsWithDefault", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &replications.V1betaGetMultipleReplicationsDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleReplicationsResponseIsNil", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
		}

		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(nil, nil)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleReplications(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Code)
		assert.Equal(tt, "unknown error during the get multiple replications", result.(*gcpgenserver.V1betaGetMultipleReplicationsInternalServerError).Message)
	})
	t.Run("WhenGetMultipleReplicationsSucceeds", func(tt *testing.T) {
		mockClient := replications.NewMockClientService(tt)

		params := gcpgenserver.V1betaGetMultipleReplicationsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpgenserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpgenserver.ReplicationURIListV1beta{
			ReplicationUris: []string{"uri1", "uri2"},
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
		mockClient.EXPECT().V1betaGetMultipleReplications(mock.Anything).Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Replications: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
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

		result, err := handler.V1betaCreateReplication(context.Background(), req, params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, float64(500), result.(*gcpgenserver.V1betaCreateReplicationInternalServerError).Code)
		assert.Equal(tt, "some error", result.(*gcpgenserver.V1betaCreateReplicationInternalServerError).Message)
	})
}
