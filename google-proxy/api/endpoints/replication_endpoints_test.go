package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/replications"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
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
