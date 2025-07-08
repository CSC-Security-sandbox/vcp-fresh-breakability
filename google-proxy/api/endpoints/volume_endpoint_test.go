package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/volumes"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
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
				BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(
					gcpgenserver.BackupConfigV1beta{
						BackupPolicyId:         gcpgenserver.NewOptNilString("backup-policy-id"),
						BackupVaultId:          gcpgenserver.NewOptNilString("backup-vault-id"),
						ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(true),
					},
				),
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Name:             "test-volume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/test-volume",
			CreationToken:    "test-token",
			PoolID:           "test-pool",
			QuotaInBytes:     1024,
			IsDataProtection: true,
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
	t.Run("ValidInputWithBlockPropertiesForSnaphotRestore", func(tt *testing.T) {
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
				BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(
					gcpgenserver.BackupConfigV1beta{
						BackupPolicyId:         gcpgenserver.NewOptNilString("backup-policy-id"),
						BackupVaultId:          gcpgenserver.NewOptNilString("backup-vault-id"),
						ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(true),
					},
				),
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
			SnapshotId: gcpgenserver.NewOptString("test-snapshot-id"),
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Name:             "test-volume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/test-volume",
			CreationToken:    "test-token",
			PoolID:           "test-pool",
			QuotaInBytes:     1024,
			IsDataProtection: true,
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
			},
			SnapshotID: "test-snapshot-id",
		}
		result, err := prepareCreateVolumeParams(req, params, region)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("WhenTieringPolicyIsEnabled", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true
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
				BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(
					gcpgenserver.BackupConfigV1beta{
						BackupPolicyId:         gcpgenserver.NewOptNilString("backup-policy-id"),
						BackupVaultId:          gcpgenserver.NewOptNilString("backup-vault-id"),
						ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(true),
					},
				),
				TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
					gcpgenserver.TieringPolicyV1beta{
						TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
						CoolingThresholdDays: gcpgenserver.OptNilInt32{
							Value: 30,
							Set:   true,
						},
					},
				),
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Name:             "test-volume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/test-volume",
			CreationToken:    "test-token",
			PoolID:           "test-pool",
			QuotaInBytes:     1024,
			IsDataProtection: true,
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
			},
			TieringPolicy: &common.TieringPolicy{
				CoolAccess:                true,
				CoolnessPeriod:            30,
				CoolAccessTieringPolicy:   "auto",
				CoolAccessRetrievalPolicy: "default",
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("WhenTieringPolicyIsPaused", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true
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
				BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(
					gcpgenserver.BackupConfigV1beta{
						BackupPolicyId:         gcpgenserver.NewOptNilString("backup-policy-id"),
						BackupVaultId:          gcpgenserver.NewOptNilString("backup-vault-id"),
						ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(true),
					},
				),
				TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
					gcpgenserver.TieringPolicyV1beta{
						TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("PAUSED"),
					},
				),
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Name:             "test-volume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/test-volume",
			CreationToken:    "test-token",
			PoolID:           "test-pool",
			QuotaInBytes:     1024,
			IsDataProtection: true,
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
			},
			TieringPolicy: &common.TieringPolicy{
				CoolAccess:              false,
				CoolAccessTieringPolicy: "none",
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
		schedule := models.Schedule{
			Name:   "test-schedule",
			Months: []int{1, 2, 3},
		}
		snapshotPolicySchedule := &models.SnapshotPolicySchedule{
			Count:    1,
			Schedule: &schedule,
		}
		vcpVolumes = append(vcpVolumes, &models.Volume{
			CreationToken: "test-token",
			PoolID:        "test-pool",
			QuotaInBytes:  1024,
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
				BackupChainBytes:       nillable.GetInt64Ptr(10199181),
			},
			SnapshotPolicy: &models.SnapshotPolicy{
				Name:      "test-snapshot-policy",
				IsEnabled: true,
				Schedules: []*models.SnapshotPolicySchedule{snapshotPolicySchedule},
			},
		})

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(vcpVolumes, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.Nil(tt, err)
		assert.Len(tt, result.(*gcpgenserver.V1betaGetMultipleVolumesOK).Volumes, 2)
	})
}

func TestConvertVolumeV1betaCVPToModel(t *testing.T) {
	t.Run("ConvertVolumeV1betaCVPToModelWithFlexCacheParams", func(tt *testing.T) {
		backupConfig := &cvpmodels.BackupConfigV1beta{
			BackupChainBytes: nillable.GetInt64Ptr(10199181),
			BackupPolicyID:   nillable.GetStringPtr("backup-policy-id"),
			BackupVaultID:    nillable.GetStringPtr("backup-vault-id"),
		}

		cachePrepopulate := &cvpmodels.FlexCachePrePopulateV1beta{
			ExcludePathList: []string{"/exclude1", "/exclude2"},
			PathList:        []string{"/path1", "/path2"},
			Recursion:       nillable.GetBoolPtr(true),
		}

		cacheConfig := &cvpmodels.FlexCacheConfigV1beta{
			AtimeScrubEnabled:       nillable.GetBoolPtr(true),
			AtimeScrubMinutes:       nillable.GetInt16Ptr(30),
			CifsChangeNotifyEnabled: nillable.GetBoolPtr(true),
			PrePopulate:             cachePrepopulate,
			WritebackEnabled:        nillable.GetBoolPtr(true),
		}

		timeNowStrfmt := strfmt.DateTime(time.Now())

		cachePrams := &cvpmodels.FlexCacheV1beta{
			CacheConfig:          cacheConfig,
			Command:              "test-command",
			CommandExpiryTime:    &timeNowStrfmt,
			EnableGlobalFileLock: nillable.GetBoolPtr(true),
			Passphrase:           nillable.GetStringPtr("test-passphrase"),
			PeerClusterName:      "alderan",
			PeerIPAddresses:      []string{"10.0.0.1", "10.0.0.2"},
			PeerSvmName:          "peer-svm",
			PeerVolumeName:       "peer-volume",
		}

		input := &cvpmodels.VolumeV1beta{
			ActiveDirectoryConfigID:     nillable.GetStringPtr("ad-config-id"),
			BackupConfig:                backupConfig,
			CacheParameters:             cachePrams,
			ColdTierSizeGib:             nillable.GetFloat64Ptr(10.5),
			Created:                     strfmt.DateTime(time.Now()),
			CreationToken:               nillable.GetStringPtr("test-token"),
			DedicatedCapacity:           nillable.GetBoolPtr(true),
			Deleted:                     &timeNowStrfmt,
			Description:                 nillable.GetStringPtr("test description"),
			ExportPolicy:                nil,
			InReplication:               nillable.GetBoolPtr(false),
			IsDataProtection:            nillable.GetBoolPtr(true),
			IsOnPremMigration:           nillable.GetBoolPtr(false),
			KerberosEnabled:             nillable.GetBoolPtr(true),
			KmsConfigID:                 nillable.GetStringPtr("kms-config-id"),
			KmsConfigResourceID:         nillable.GetStringPtr("kms-resource-id"),
			Labels:                      map[string]string{"env": "test", "team": "avatar"},
			LargeCapacity:               nillable.GetBoolPtr(false),
			LargeVolumeConstituentCount: nillable.GetInt32Ptr(5),
			LdapEnabled:                 nillable.GetBoolPtr(true),
			MountPoints:                 nil,
			MultipleEndpoints:           nillable.GetBoolPtr(true),
			Network:                     "network-id",
			PoolID:                      nillable.GetStringPtr("pool-id"),
			PoolResourceID:              nillable.GetStringPtr("pool-resource-id"),
			Protocols:                   []cvpmodels.ProtocolsV1beta{cvpmodels.ProtocolsV1betaNFSV3},
			QuotaInBytes:                nillable.GetFloat64Ptr(2048),
			ResourceID:                  nillable.GetStringPtr("resource-id"),
			RestrictedActions:           []string{"action1", "action2"},
			SecondaryZone:               nillable.GetStringPtr("secondary-zone"),
			SecurityStyle:               "unix",
			ServiceLevel:                cvpmodels.ServiceLevelV1betaNameFLEX,
			SmbSettings:                 []string{"smb1", "smb2"},
			SnapReserve:                 nillable.GetFloat64Ptr(100),
			SnapshotDirectory:           nillable.GetBoolPtr(true),
			SnapshotPolicy:              nil,
			ThroughputMibps:             nillable.GetFloat64Ptr(150),
			TieringPolicy:               nil,
			UnixPermissions:             nillable.GetStringPtr("755"),
			UsedBytes:                   nillable.GetFloat64Ptr(1024),
			VolumeID:                    "vol-123",
			VolumeState:                 "active",
			VolumeStateDetails:          "in use",
			Zone:                        "us-central1",
		}

		res := _convertVolumeV1betaCVPToModel(input)

		assert.Equal(tt, "ad-config-id", res.ActiveDirectoryConfigId.Value)
		assert.Equal(tt, "test-token", res.CreationToken.Value)
		assert.Equal(tt, "test description", res.Description.Value)
		assert.Equal(tt, "pool-id", res.PoolId.Value)
		assert.Equal(tt, "pool-resource-id", res.PoolResourceId.Value)
		assert.Equal(tt, "resource-id", res.ResourceId)
		assert.Equal(tt, "vol-123", res.VolumeId.Value)
		assert.Equal(tt, gcpgenserver.NewOptVolumeV1betaServiceLevel(gcpgenserver.VolumeV1betaServiceLevelFLEX), res.ServiceLevel)
		assert.Equal(tt, "us-central1", res.Zone.Value)
		assert.Equal(tt, "test-passphrase", res.CacheParameters.Value.Passphrase.Value)
		assert.Equal(tt, "peer-svm", res.CacheParameters.Value.PeerSvmName.Value)
		assert.Equal(tt, "peer-volume", res.CacheParameters.Value.PeerVolumeName.Value)
		assert.Equal(tt, "test-command", res.CacheParameters.Value.Command.Value)
		assert.Equal(tt, "alderan", res.CacheParameters.Value.PeerClusterName.Value)
		assert.Equal(tt, "test-passphrase", res.CacheParameters.Value.Passphrase.Value)
		assert.Equal(tt, "network-id", res.Network.Value)
		assert.Equal(tt, "pool-id", res.PoolId.Value)
		assert.Equal(tt, "pool-resource-id", res.PoolResourceId.Value)

		assert.Equal(tt, int64(10199181), res.BackupConfig.Value.BackupChainBytes.Value)
		assert.Equal(tt, "backup-policy-id", res.BackupConfig.Value.BackupPolicyId.Value)
		assert.Equal(tt, "backup-vault-id", res.BackupConfig.Value.BackupVaultId.Value)
	})
}

func TestConvertFromSnapshotPolicyV2(t *testing.T) {
	t.Run("NilInput_ReturnsNil", func(tt *testing.T) {
		result, err := convertFromSnapshotPolicyV2(nil)
		assert.NoError(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("MonthlySchedule", func(tt *testing.T) {
		pol := &gcpgenserver.SnapshotPolicyV1beta{
			Enabled: gcpgenserver.NewOptNilBool(true),
			MonthlySchedule: gcpgenserver.NewOptMonthlyScheduleV1beta(
				gcpgenserver.MonthlyScheduleV1beta{
					SnapshotsToKeep: gcpgenserver.NewOptFloat64(5),
					DaysOfMonth:     gcpgenserver.NewOptString("1,15"),
					Hour:            gcpgenserver.NewOptFloat64(2),
					Minute:          gcpgenserver.NewOptFloat64(30),
				},
			),
		}
		result, err := convertFromSnapshotPolicyV2(pol)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.True(tt, result.IsEnabled)
		assert.Len(tt, result.Schedules, 1)
		sched := result.Schedules[0]
		assert.Equal(tt, int64(5), sched.Count)
		assert.Equal(tt, "monthly", sched.SnapmirrorLabel)
		assert.Equal(tt, []int{1, 15}, sched.Schedule.DaysOfMonth)
		assert.Equal(tt, []int{2}, sched.Schedule.Hours)
		assert.Equal(tt, []int{30}, sched.Schedule.Minutes)
	})

	t.Run("WeeklySchedule", func(tt *testing.T) {
		pol := &gcpgenserver.SnapshotPolicyV1beta{
			Enabled: gcpgenserver.NewOptNilBool(true),
			WeeklySchedule: gcpgenserver.NewOptWeeklyScheduleV1beta(
				gcpgenserver.WeeklyScheduleV1beta{
					SnapshotsToKeep: gcpgenserver.NewOptFloat64(3),
					Day:             gcpgenserver.NewOptString("Monday,Tuesday"),
					Hour:            gcpgenserver.NewOptFloat64(5),
					Minute:          gcpgenserver.NewOptFloat64(10),
				},
			),
		}
		result, err := convertFromSnapshotPolicyV2(pol)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.True(tt, result.IsEnabled)
		assert.Len(tt, result.Schedules, 1)
		sched := result.Schedules[0]
		assert.Equal(tt, int64(3), sched.Count)
		assert.Equal(tt, "weekly", sched.SnapmirrorLabel)
		assert.Equal(tt, []int{1, 2}, sched.Schedule.DaysOfWeek)
		assert.Equal(tt, []int{5}, sched.Schedule.Hours)
		assert.Equal(tt, []int{10}, sched.Schedule.Minutes)
	})

	t.Run("DailySchedule", func(tt *testing.T) {
		pol := &gcpgenserver.SnapshotPolicyV1beta{
			Enabled: gcpgenserver.NewOptNilBool(true),
			DailySchedule: gcpgenserver.NewOptDailyScheduleV1beta(
				gcpgenserver.DailyScheduleV1beta{
					SnapshotsToKeep: gcpgenserver.NewOptFloat64(2),
					Hour:            gcpgenserver.NewOptFloat64(7),
					Minute:          gcpgenserver.NewOptFloat64(45),
				},
			),
		}
		result, err := convertFromSnapshotPolicyV2(pol)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.True(tt, result.IsEnabled)
		assert.Len(tt, result.Schedules, 1)
		sched := result.Schedules[0]
		assert.Equal(tt, int64(2), sched.Count)
		assert.Equal(tt, "daily", sched.SnapmirrorLabel)
		assert.Equal(tt, []int{7}, sched.Schedule.Hours)
		assert.Equal(tt, []int{45}, sched.Schedule.Minutes)
	})

	t.Run("HourlySchedule", func(tt *testing.T) {
		pol := &gcpgenserver.SnapshotPolicyV1beta{
			Enabled: gcpgenserver.NewOptNilBool(true),
			HourlySchedule: gcpgenserver.NewOptHourlyScheduleV1beta(
				gcpgenserver.HourlyScheduleV1beta{
					SnapshotsToKeep: gcpgenserver.NewOptFloat64(1),
					Minute:          gcpgenserver.NewOptFloat64(15),
				},
			),
		}
		result, err := convertFromSnapshotPolicyV2(pol)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.True(tt, result.IsEnabled)
		assert.Len(tt, result.Schedules, 1)
		sched := result.Schedules[0]
		assert.Equal(tt, int64(1), sched.Count)
		assert.Equal(tt, "hourly", sched.SnapmirrorLabel)
		assert.Equal(tt, []int{15}, sched.Schedule.Minutes)
	})

	t.Run("WeeklySchedule_InvalidDay_ReturnsError", func(tt *testing.T) {
		pol := &gcpgenserver.SnapshotPolicyV1beta{
			Enabled: gcpgenserver.NewOptNilBool(true),
			WeeklySchedule: gcpgenserver.NewOptWeeklyScheduleV1beta(
				gcpgenserver.WeeklyScheduleV1beta{
					SnapshotsToKeep: gcpgenserver.NewOptFloat64(3),
					Day:             gcpgenserver.NewOptString("Funday"),
					Hour:            gcpgenserver.NewOptFloat64(5),
					Minute:          gcpgenserver.NewOptFloat64(10),
				},
			),
		}
		result, err := convertFromSnapshotPolicyV2(pol)
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}

func TestV1betaUpdateVolume(t *testing.T) {
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	mockParseAndValidateRegionAndZone := func(region string) (string, string, *gcpgenserver.Error) {
		return "test-region", "test-location", nil
	}
	utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone
	defer func() { utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

	t.Run("ValidUpdateVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
	})

	t.Run("UserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{}
		prepareUpdateVolumeParams = func(req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams, region string) (*common.UpdateVolumeParams, error) {
			return nil, errors.NewUserInputValidationErr("invalid input")
		}
		defer func() { prepareUpdateVolumeParams = _prepareUpdateVolumeParams }()

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaUpdateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Contains(tt, badReq.Message, "invalid input")
	})

	t.Run("InternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{}
		prepareUpdateVolumeParams = func(req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams, region string) (*common.UpdateVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareUpdateVolumeParams = _prepareUpdateVolumeParams }()

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.Error(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "unexpected error")
	})

	t.Run("BadRequest", func(tt *testing.T) {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		defer func() { utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{}
		prepareUpdateVolumeParams = func(req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams, region string) (*common.UpdateVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareUpdateVolumeParams = _prepareUpdateVolumeParams }()

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "LocationID represents neither a region nor a zone")
	})

	t.Run("WhenOrchestratorValidationThrowsAnError_Return400BadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
		}

		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("An error occurred"))

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorThrowsAnError_ReturnError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
		}

		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.New("An error occurred"))

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.Error(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenLifeCycleStateUpdating_ThenReturnDoneAsFalse", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "UPDATING",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
		assert.False(tt, op.Done.Value)
	})

	t.Run("TieringPolicy ENABLED with feature enabled", func(tt *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = true
		defer func() { autoTieringEnabled = currentATState }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction:           gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
					CoolingThresholdDays: gcpgenserver.OptNilInt32{Value: 30, Set: true},
				},
			),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
	})

	t.Run("TieringPolicy PAUSED with feature enabled", func(tt *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = true
		defer func() { autoTieringEnabled = currentATState }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("PAUSED"),
				},
			),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
	})

	t.Run("TieringPolicy set with feature disabled", func(tt *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = false
		defer func() { autoTieringEnabled = currentATState }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(2048),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
				},
			),
		}
		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaUpdateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Contains(tt, badReq.Message, "Auto-Tiering feature is currently not enabled.")
	})
}

func TestPrepareUpdateVolumeParams(t *testing.T) {
	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "proj",
		LocationId:    "loc",
		VolumeId:      "vol",
	}
	region := "region"

	t.Run("WhenAllFieldsSet_ThenFieldsAreMapped", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(1234),
			Description:  gcpgenserver.NewOptNilString("desc"),
			Protocols:    []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			BlockProperties: gcpgenserver.NewOptBlockPropertiesV1beta(
				gcpgenserver.BlockPropertiesV1beta{
					OsType: gcpgenserver.NewOptBlockPropertiesV1betaOsType("LINUX"),
				},
			),
			BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(
				gcpgenserver.BackupConfigV1beta{
					BackupVaultId:          gcpgenserver.NewOptNilString("backup-vault-id"),
					BackupPolicyId:         gcpgenserver.NewOptNilString("backup-policy-id"),
					BackupChainBytes:       gcpgenserver.NewOptNilInt64(10199181),
					ScheduledBackupEnabled: gcpgenserver.NewOptNilBool(true),
				}),
			Labels: gcpgenserver.NewOptVolumeUpdateV1betaLabels(map[string]string{"k": "v"}),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.Equal(t, "pool", out.PoolID)
		assert.Equal(t, int64(1234), out.QuotaInBytes)
		assert.Equal(t, "desc", out.Description)
		assert.Equal(t, []string{"ISCSI"}, out.Protocols)
		assert.NotNil(t, out.BlockProperties)
		assert.Equal(t, "LINUX", out.BlockProperties.OSType)
		assert.Equal(t, map[string]string{"k": "v"}, out.Labels)
	})

	t.Run("WhenOptionalFieldsNotSet_ThenDefaultsAreUsed", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.Equal(t, "", out.PoolID)
		assert.Equal(t, int64(0), out.QuotaInBytes)
		assert.Equal(t, "", out.Description)
		assert.Empty(t, out.Protocols)
		assert.Nil(t, out.BlockProperties)
		assert.Nil(t, out.Labels)
	})

	t.Run("WhenProtocolsIsOtherThanISCSII_ThenThrowError", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			Protocols: []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaNFSV3},
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err, "only ISCSI protocol is supported")
		assert.Nil(t, out)
	})

	t.Run("WhenBlockPropertiesSetWithoutOsType_ThenBlockPropertiesIsNil", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BlockProperties: gcpgenserver.NewOptBlockPropertiesV1beta(gcpgenserver.BlockPropertiesV1beta{}),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, out.BlockProperties)
	})

	t.Run("WhenLabelsContainEmptyKey_ThenLabelIsSkipped", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			Labels: gcpgenserver.NewOptVolumeUpdateV1betaLabels(map[string]string{"": "v", "k": "v2"}),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.EqualError(t, err, "Labels cannot have empty keys")
		assert.Nil(t, out)
	})

	t.Run("WhenProtocolMarshalTextFails_ThenErrorIsReturned", func(t *testing.T) {
		badProtocol := gcpgenserver.ProtocolsV1beta(rune(255)) // assuming this is invalid
		req := &gcpgenserver.VolumeUpdateV1beta{
			Protocols: []gcpgenserver.ProtocolsV1beta{badProtocol},
		}
		_, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
	})

	t.Run("WhenSnapshotPolicySet_ThenFieldsAreMapped", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			SnapshotPolicy: gcpgenserver.NewOptSnapshotPolicyV1beta(
				gcpgenserver.SnapshotPolicyV1beta{
					Enabled: gcpgenserver.NewOptNilBool(true),
					MonthlySchedule: gcpgenserver.NewOptMonthlyScheduleV1beta(
						gcpgenserver.MonthlyScheduleV1beta{
							SnapshotsToKeep: gcpgenserver.NewOptFloat64(2),
							DaysOfMonth:     gcpgenserver.NewOptString("1,15"),
							Hour:            gcpgenserver.NewOptFloat64(2),
							Minute:          gcpgenserver.NewOptFloat64(30),
						},
					),
				},
			),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, out.SnapshotPolicy)
		assert.True(t, out.SnapshotPolicy.IsEnabled)
		if len(out.SnapshotPolicy.Schedules) > 0 {
			assert.Equal(t, int64(2), out.SnapshotPolicy.Schedules[0].Count)
			assert.Equal(t, "monthly", out.SnapshotPolicy.Schedules[0].SnapmirrorLabel)
			assert.Equal(t, []int{1, 15}, out.SnapshotPolicy.Schedules[0].Schedule.DaysOfMonth)
		}
	})

	t.Run("WhenSnapshotPolicySetWithInvalidWeeklyDay_ThenError", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			SnapshotPolicy: gcpgenserver.NewOptSnapshotPolicyV1beta(
				gcpgenserver.SnapshotPolicyV1beta{
					Enabled: gcpgenserver.NewOptNilBool(true),
					WeeklySchedule: gcpgenserver.NewOptWeeklyScheduleV1beta(
						gcpgenserver.WeeklyScheduleV1beta{
							SnapshotsToKeep: gcpgenserver.NewOptFloat64(2),
							Day:             gcpgenserver.NewOptString("Funday"),
							Hour:            gcpgenserver.NewOptFloat64(2),
							Minute:          gcpgenserver.NewOptFloat64(30),
						},
					),
				},
			),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.Nil(t, out)
	})
	t.Run("TieringPolicy ENABLED with feature enabled", func(t *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = true
		defer func() { autoTieringEnabled = currentATState }()

		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(1234),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction:           gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
					CoolingThresholdDays: gcpgenserver.OptNilInt32{Value: 30, Set: true},
				},
			),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, param.TieringPolicy)
		assert.True(t, param.TieringPolicy.CoolAccess)
		assert.Equal(t, int32(30), param.TieringPolicy.CoolnessPeriod)
	})

	t.Run("TieringPolicy PAUSED with feature enabled", func(t *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = true
		defer func() { autoTieringEnabled = currentATState }()

		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(1234),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("PAUSED"),
				},
			),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, param.TieringPolicy)
		assert.False(t, param.TieringPolicy.CoolAccess)
	})

	t.Run("TieringPolicy set with feature disabled", func(t *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = false
		defer func() { autoTieringEnabled = currentATState }()

		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(1234),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
				},
			),
		}
		_, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled.")
	})

	t.Run("TieringPolicy not set", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(1234),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.Nil(t, param.TieringPolicy)
	})
}

func TestV1betaGetVolumeCount(t *testing.T) {
	t.Run("ValidVolumeCount", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetVolumeCountParams{
			ProjectNumber: "test-project",
		}

		expectedCount := 5
		mockOrchestrator.EXPECT().GetVolumeCount(mock.Anything, params.ProjectNumber).Return(int64(expectedCount), nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaGetVolumeCount(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedCount, result.(*gcpgenserver.V1betaGetVolumeCountOK).VolumeCount)
	})

	t.Run("ErrorGettingVolumeCount", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetVolumeCountParams{
			ProjectNumber: "test-project",
		}

		mockError := errors.New("failed to get volume count")
		mockOrchestrator.EXPECT().GetVolumeCount(mock.Anything, params.ProjectNumber).Return(0, mockError)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaGetVolumeCount(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
	})
}

func TestV1betaListVolumes(t *testing.T) {
	t.Run("SuccessfulListVolumes", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListVolumesParams{
			ProjectNumber: "test-project",
		}

		expectedVolumes := []*models.Volume{
			{
				CreationToken: "test-token-1",
				PoolID:        "test-pool-1",
				QuotaInBytes:  1024,
			},
			{
				CreationToken: "test-token-2",
				PoolID:        "test-pool-2",
				QuotaInBytes:  2048,
			},
		}

		mockOrchestrator.EXPECT().ListVolumes(mock.Anything, params.ProjectNumber).Return(expectedVolumes, nil)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaListVolumes(context.Background(), params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result.(*gcpgenserver.V1betaListVolumesOK).Volumes, len(expectedVolumes))
	})

	t.Run("ErrorListingVolumes", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaListVolumesParams{
			ProjectNumber: "test-project",
		}

		mockError := errors.New("failed to list volumes")
		mockOrchestrator.EXPECT().ListVolumes(mock.Anything, params.ProjectNumber).Return(nil, mockError)

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		result, err := handler.V1betaListVolumes(context.Background(), params)

		assert.Error(tt, err)
		assert.NotNil(tt, result)
	})
}

func TestConvertDaysOfWeekToIntArray(t *testing.T) {
	t.Run("ReturnsSundayByDefaultWhenEmpty", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("")
		assert.NoError(tt, err)
		assert.Equal(tt, []int{0}, result)
	})

	t.Run("ReturnsCorrectIntsForFullNames", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("Monday,Tuesday,Wednesday")
		assert.NoError(tt, err)
		assert.Equal(tt, []int{1, 2, 3}, result)
	})

	t.Run("ReturnsCorrectIntsForShortNames", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("Mon,Tue,Wed")
		assert.NoError(tt, err)
		assert.Equal(tt, []int{1, 2, 3}, result)
	})

	t.Run("ReturnsErrorForInvalidDay", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("Funday")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("ReturnsErrorForDuplicateDay", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("Monday,Monday")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("TrimsSpacesAndIsCaseInsensitive", func(tt *testing.T) {
		result, err := convertDaysOfWeekToIntArray("  tuesday ,  WEDNESDAY ")
		assert.NoError(tt, err)
		assert.Equal(tt, []int{2, 3}, result)
	})
}

func TestConvertDaysOfWeekFromIntArray(t *testing.T) {
	t.Run("ReturnsCorrectStringForValidInts", func(tt *testing.T) {
		result := convertDaysOfWeekFromIntArray([]int{1, 2, 3})
		assert.Equal(tt, "Monday,Tuesday,Wednesday", result)
	})

	t.Run("ReturnsSundayForEmptyInput", func(tt *testing.T) {
		result := convertDaysOfWeekFromIntArray([]int{})
		assert.Equal(tt, "Sunday", result)
	})

	t.Run("IgnoresInvalidInts", func(tt *testing.T) {
		result := convertDaysOfWeekFromIntArray([]int{-1, 0, 6, 7})
		assert.Equal(tt, "Sunday,Saturday", result)
	})

	t.Run("HandlesAllWeekdays", func(tt *testing.T) {
		result := convertDaysOfWeekFromIntArray([]int{0, 1, 2, 3, 4, 5, 6})
		assert.Equal(tt, "Sunday,Monday,Tuesday,Wednesday,Thursday,Friday,Saturday", result)
	})
}

func TestConvertToSnapshotPolicyV2(t *testing.T) {
	t.Run("NilInput_ReturnsNil", func(tt *testing.T) {
		result := convertToSnapshotPolicyV2(nil)
		assert.Nil(tt, result)
	})

	t.Run("EmptySchedules_ReturnsEnabledWithNoSchedules", func(tt *testing.T) {
		pol := &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{},
		}
		result := convertToSnapshotPolicyV2(pol)
		assert.NotNil(tt, result)
		assert.True(tt, result.Enabled.Value)
	})

	t.Run("MonthlySchedule", func(tt *testing.T) {
		pol := &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Count:           5,
					SnapmirrorLabel: "monthly",
					Schedule: &models.Schedule{
						DaysOfMonth: []int{1, 15},
						Hours:       []int{2},
						Minutes:     []int{30},
					},
				},
			},
		}
		result := convertToSnapshotPolicyV2(pol)
		assert.NotNil(tt, result)
		assert.True(tt, result.Enabled.Value)
		assert.True(tt, result.MonthlySchedule.IsSet())
		assert.Equal(tt, "1,15", result.MonthlySchedule.Value.DaysOfMonth.Value)
		assert.Equal(tt, float64(2), result.MonthlySchedule.Value.Hour.Value)
		assert.Equal(tt, float64(30), result.MonthlySchedule.Value.Minute.Value)
		assert.Equal(tt, float64(5), result.MonthlySchedule.Value.SnapshotsToKeep.Value)
	})

	t.Run("WeeklySchedule", func(tt *testing.T) {
		pol := &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Count:           3,
					SnapmirrorLabel: "weekly",
					Schedule: &models.Schedule{
						DaysOfWeek: []int{1, 2},
						Hours:      []int{5},
						Minutes:    []int{10},
					},
				},
			},
		}
		result := convertToSnapshotPolicyV2(pol)
		assert.NotNil(tt, result)
		assert.True(tt, result.WeeklySchedule.IsSet())
		assert.Contains(tt, result.WeeklySchedule.Value.Day.Value, "Monday")
		assert.Contains(tt, result.WeeklySchedule.Value.Day.Value, "Tuesday")
		assert.Equal(tt, float64(5), result.WeeklySchedule.Value.Hour.Value)
		assert.Equal(tt, float64(10), result.WeeklySchedule.Value.Minute.Value)
		assert.Equal(tt, float64(3), result.WeeklySchedule.Value.SnapshotsToKeep.Value)
	})

	t.Run("DailySchedule", func(tt *testing.T) {
		pol := &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Count:           2,
					SnapmirrorLabel: "daily",
					Schedule: &models.Schedule{
						Hours:   []int{7},
						Minutes: []int{45},
					},
				},
			},
		}
		result := convertToSnapshotPolicyV2(pol)
		assert.NotNil(tt, result)
		assert.True(tt, result.DailySchedule.IsSet())
		assert.Equal(tt, float64(7), result.DailySchedule.Value.Hour.Value)
		assert.Equal(tt, float64(45), result.DailySchedule.Value.Minute.Value)
		assert.Equal(tt, float64(2), result.DailySchedule.Value.SnapshotsToKeep.Value)
	})

	t.Run("HourlySchedule", func(tt *testing.T) {
		pol := &models.SnapshotPolicy{
			IsEnabled: true,
			Schedules: []*models.SnapshotPolicySchedule{
				{
					Count:           1,
					SnapmirrorLabel: "hourly",
					Schedule: &models.Schedule{
						Minutes: []int{15},
					},
				},
			},
		}
		result := convertToSnapshotPolicyV2(pol)
		assert.NotNil(tt, result)
		assert.True(tt, result.HourlySchedule.IsSet())
		assert.Equal(tt, float64(15), result.HourlySchedule.Value.Minute.Value)
		assert.Equal(tt, float64(1), result.HourlySchedule.Value.SnapshotsToKeep.Value)
	})
}

func TestV1betaCreateVolume(t *testing.T) {
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	mockParseAndValidateRegionAndZone := func(region string) (string, string, *gcpgenserver.Error) {
		return "test-region", "test-location", nil
	}
	utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone
	defer func() { utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

	t.Run("ValidCreateVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "CREATING",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
	})

	t.Run("UserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.VolumeCreateV1beta{}
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string) (*common.CreateVolumeParams, error) {
			return nil, errors.NewUserInputValidationErr("invalid input")
		}
		defer func() { prepareCreateVolumeParams = _prepareCreateVolumeParams }()

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaCreateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Contains(tt, badReq.Message, "invalid input")
	})

	t.Run("InternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.VolumeCreateV1beta{}
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string) (*common.CreateVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareCreateVolumeParams = _prepareCreateVolumeParams }()

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.Error(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaCreateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "unexpected error")
	})

	t.Run("BadRequest_InvalidLocation", func(tt *testing.T) {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		defer func() { utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.VolumeCreateV1beta{}
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string) (*common.CreateVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareCreateVolumeParams = _prepareCreateVolumeParams }()

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaCreateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "LocationID represents neither a region nor a zone")
	})

	t.Run("WhenOrchestratorValidationThrowsAnError_Return400BadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}

		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("An error occurred"))

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaCreateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorThrowsAnError_ReturnError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}

		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.New("An error occurred"))

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.Error(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaCreateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenLifeCycleStateCreating_ThenReturnDoneAsFalse", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "CREATING",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
		assert.False(tt, op.Done.Value)
	})

	t.Run("WhenLifeCycleStateCreating_ThenReturnDoneAsFalse", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "ERROR",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
		assert.True(tt, op.Done.Value)
	})

	t.Run("ValidCreateVolumeWithTieringPolicy", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "test-volume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
					gcpgenserver.TieringPolicyV1beta{
						TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
						CoolingThresholdDays: gcpgenserver.OptNilInt32{
							Value: 30,
							Set:   true,
						},
					},
				),
			},
			VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "CREATING",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
		assert.False(tt, op.Done.Value)
	})
}

func TestConvertModelToVCPVolume(t *testing.T) {
	t.Run("AllFieldsSet", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:   "token",
			PoolID:          "pool",
			QuotaInBytes:    1234,
			BlockProperties: &models.BlockProperties{OSType: "LINUX"},
			ProtocolTypes:   []string{"ISCSI"},
			LifeCycleState:  "READY",
			IPAddress:       "10.72.177.17",
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)
		assert.Equal(t, "token", out.CreationToken.Value)
		assert.Equal(t, "LINUX", string(out.BlockProperties.Value.OsType.Value))
		assert.Equal(t, "ISCSI", string(out.Protocols[0]))
	})
}

func TestPrepareCreateVolumeParams_WithAutoTieringFeatureDisabled(t *testing.T) {
	// Save and restore the original value
	currentATState := autoTieringEnabled
	defer func() { autoTieringEnabled = currentATState }()
	autoTieringEnabled = false

	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "test-volume",
			CreationToken: gcpgenserver.NewOptString("test-token"),
			PoolId:        gcpgenserver.NewNilString("test-pool"),
			QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
			Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
					CoolingThresholdDays: gcpgenserver.OptNilInt32{
						Value: 30,
						Set:   true,
					},
				},
			),
		},
		VolumeType: gcpgenserver.NewOptVolumeCreateV1betaVolumeType("SECONDARY"),
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"

	_, err := _prepareCreateVolumeParams(req, params, region)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled.")
}
