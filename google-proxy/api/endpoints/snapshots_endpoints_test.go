package api

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/snapshots"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestHandler_V1betaGetMultipleSnapshots(t *testing.T) {
	t.Run("WhenGetMultipleSnapshotsFailsWithBadRequest", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &snapshots.V1betaGetMultipleSnapshotsBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsBadRequest).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithUnauthorized", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &snapshots.V1betaGetMultipleSnapshotsUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsUnauthorized).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithForbidden", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &snapshots.V1betaGetMultipleSnapshotsForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsForbidden).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsForbidden).Message)
	})

	t.Run("WhenGetMultipleSnapshotsFailsWithTooManyRequests", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		errorMessage := "Too many requests"
		errorCode := float64(429)
		mockError := &snapshots.V1betaGetMultipleSnapshotsTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpserver.V1betaGetMultipleSnapshotsTooManyRequests).Code)
		assert.Equal(tt, errorMessage, result.(*gcpserver.V1betaGetMultipleSnapshotsTooManyRequests).Message)
	})

	t.Run("WhenGetMultipleReplicationsSucceeds", func(tt *testing.T) {
		mockClient := snapshots.NewMockClientService(tt)

		params := gcpserver.V1betaGetMultipleSnapshotsParams{
			LocationId:       "location-id",
			ProjectNumber:    "project-number",
			VolumeResourceId: "volume-resource-id",
			XCorrelationID:   gcpserver.NewOptString("X-Correlation-ID"),
		}
		req := &gcpserver.SnapshotIdListV1beta{
			SnapshotUuids: []string{"uri1", "uri2"},
		}

		description := "description"
		mockResponse := &snapshots.V1betaGetMultipleSnapshotsOK{
			Payload: &snapshots.V1betaGetMultipleSnapshotsOKBody{
				Snapshots: []*models.SnapshotV1beta{
					{
						SnapshotID:  "snapshot-id-1",
						Description: &description,
						VolumeID:    "vol-id",
					},
				},
			},
		}
		mockClient.EXPECT().V1betaGetMultipleSnapshots(mock.Anything).Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{Snapshots: mockClient}
		originalClient := createClient
		defer func() {
			createClient = originalClient
		}()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		result, err := handler.V1betaGetMultipleSnapshots(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		successResult, ok := result.(*gcpserver.V1betaGetMultipleSnapshotsOK)
		assert.True(tt, ok)
		assert.Equal(tt, "snapshot-id-1", successResult.Snapshots[0].SnapshotId.Value)
	})
}

func Test_convertToSnapshotsV1Beta(t *testing.T) {
	t.Run("WhenConvertToSnapshotsV1BetaSucceeds", func(tt *testing.T) {
		snapshotV1betas := &models.SnapshotV1beta{
			SnapshotID:  "snapshot-id-1",
			Description: nillable.ToPointer("description"),
			VolumeID:    "vol-id",
		}

		result := convertToSnapshotsV1Beta(snapshotV1betas)

		assert.Equal(t, result.SnapshotId.Value, snapshotV1betas.SnapshotID)
		assert.Equal(t, result.Description.Value, *snapshotV1betas.Description)
		assert.Equal(t, result.VolumeId.Value, snapshotV1betas.VolumeID)
	})
}
