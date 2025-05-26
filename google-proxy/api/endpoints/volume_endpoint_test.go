package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/volumes"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestPrepareCreateVolumeParams(t *testing.T) {
	t.Run("ValidInputWithBlockProperties", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols: []gcpgenserver.ProtocolsV1beta{
					gcpgenserver.ProtocolsV1betaISCSI,
				},
				BlockProperties: gcpgenserver.NewOptBlockPropertiesV1beta(
					gcpgenserver.BlockPropertiesV1beta{
						OsType: gcpgenserver.NewOptBlockPropertiesV1betaOsType("LINUX"),
					},
				),
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:   "test-project",
			Region:        "test-region",
			Name:          "test-volume",
			VendorID:      "/projects/test-project/locations/test-location/volumes/test-volume",
			CreationToken: "test-token",
			PoolID:        "test-pool",
			QuotaInBytes:  1024,
			BlockProperties: &models.BlockProperties{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
}

func TestV1betaGetMultipleVolumes(t *testing.T) {
	t.Run("WhenGetMultipleVolumesFailsWithBadRequest", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &volumes.V1betaGetMultipleVolumesBadRequest{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesBadRequest).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesBadRequest).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsWithUnprocessableEntity", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &volumes.V1betaGetMultipleVolumesUnprocessableEntity{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesUnprocessableEntity).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesUnprocessableEntity).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsUnauthorized", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &volumes.V1betaGetMultipleVolumesUnauthorized{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesUnauthorized).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesUnauthorized).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsForbidden", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "BadRequest error"
		errorCode := float64(400)
		mockError := &volumes.V1betaGetMultipleVolumesForbidden{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesForbidden).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesForbidden).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsNotFound", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "NotFound error"
		errorCode := float64(404)
		mockError := &volumes.V1betaGetMultipleVolumesNotFound{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesNotFound).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesNotFound).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsTooManyRequests", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &volumes.V1betaGetMultipleVolumesTooManyRequests{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesTooManyRequests).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesTooManyRequests).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsDefault", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "InternalServerError error"
		errorCode := float64(500)
		mockError := &volumes.V1betaGetMultipleVolumesDefault{
			Payload: &cvpmodels.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesInternalServerError).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesInternalServerError).Message)
	})
	t.Run("WhenGetMultipleVolumesFailsInternalServerError", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		errorMessage := "unknown error during get multiple volumes operation"
		errorCode := float64(500)
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(nil, nil)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, errorCode, result.(*gcpgenserver.V1betaGetMultipleVolumesInternalServerError).Code)
		assert.Equal(tt, errorMessage, result.(*gcpgenserver.V1betaGetMultipleVolumesInternalServerError).Message)
	})
	t.Run("WhenGetMultipleVolumesNoVolumesFromCVP", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		res := &volumes.V1betaGetMultipleVolumesOK{}
		res.Payload = &volumes.V1betaGetMultipleVolumesOKBody{
			Volumes: make([]*cvpmodels.VolumeV1beta, 0),
		}
		res.Payload.Volumes = append(res.Payload.Volumes, &cvpmodels.VolumeV1beta{
			ResourceID:    nillable.GetStringPtr("test-volume"),
			CreationToken: nillable.GetStringPtr("test-token"),
			PoolID:        nillable.GetStringPtr("test-pool"),
			QuotaInBytes:  nillable.GetFloat64Ptr(1024),
		})
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(res, nil)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.Nil(tt, err)
		assert.Len(tt, result.(*gcpgenserver.V1betaGetMultipleVolumesOK).Volumes, 1)
	})
	t.Run("WhenGetMultipleVolumesNoVolumesFromCVPANDVCP", func(tt *testing.T) {
		mockClient := volumes.NewMockClientService(tt)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		res := &volumes.V1betaGetMultipleVolumesOK{}
		res.Payload = &volumes.V1betaGetMultipleVolumesOKBody{
			Volumes: make([]*cvpmodels.VolumeV1beta, 0),
		}
		res.Payload.Volumes = append(res.Payload.Volumes, &cvpmodels.VolumeV1beta{
			ResourceID:    nillable.GetStringPtr("test-volume"),
			CreationToken: nillable.GetStringPtr("test-token"),
			PoolID:        nillable.GetStringPtr("test-pool"),
			QuotaInBytes:  nillable.GetFloat64Ptr(1024),
		})
		mockClient.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(res, nil)
		cvpClient := &cvpapi.Cvp{Volumes: mockClient}
		originalClient := createCVPClient
		defer func() {
			createCVPClient = originalClient
		}()
		createCVPClient = func(logger slogger.Logger, JWT string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		var vcpVolumes = make([]*models.Volume, 0)
		vcpVolumes = append(vcpVolumes, &models.Volume{
			CreationToken: "test-token",
			PoolID:        "test-pool",
			QuotaInBytes:  1024,
		})

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(vcpVolumes, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.Nil(tt, err)
		assert.Len(tt, result.(*gcpgenserver.V1betaGetMultipleVolumesOK).Volumes, 2)
	})
}
