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
						BackupChainBytes:       gcpgenserver.NewOptNilInt64(10199181),
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
			BlockProperties: &models.BlockProperties{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
				BackupChainBytes:       nillable.GetInt64Ptr(10199181),
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
			DataProtection: &models.DataProtection{
				ScheduledBackupEnabled: nillable.GetBoolPtr(true),
				BackupVaultID:          "backup-vault-id",
				BackupPolicyId:         "backup-policy-id",
				BackupChainBytes:       nillable.GetInt64Ptr(10199181),
				PolicyEnforced:         nillable.GetBoolPtr(true),
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
		assert.Nil(t, out.BlockProperties)
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
}
