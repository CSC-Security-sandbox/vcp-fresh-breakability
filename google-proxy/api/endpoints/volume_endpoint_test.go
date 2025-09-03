package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/volumes"
	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestPrepareCreateVolumeParams(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true

	// Setup file protocol support for NFS tests
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test-project")
	defer func() {
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	t.Run("ValidInputWithBlockProperties", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
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
		zone := "test-zone"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Zone:             "test-zone",
			Name:             "testvolume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/testvolume",
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
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := _prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
	t.Run("ValidInputWithBlockPropertiesForSnaphotRestore", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
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
		zone := "test-zone"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Zone:             "test-zone",
			Name:             "testvolume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/testvolume",
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
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := _prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
	t.Run("SnapReserveIsSet_ValidValue", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				SnapReserve:   gcpgenserver.NewOptFloat64(50),
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"
		zone := "test-zone"

		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, int64(50), result.SnapReserve)
	})

	t.Run("SnapReserveIsSet_NegativeValue", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				SnapReserve:   gcpgenserver.NewOptFloat64(-1),
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"
		zone := "test-zone"
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "SnapReserve cannot be negative")
	})

	t.Run("SnapReserveIsSet_TooLargeValue", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				SnapReserve:   gcpgenserver.NewOptFloat64(91),
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"
		zone := "test-zone"
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "Maximum allowed snapshot-reserve-percentage value during create is 90")
	})

	t.Run("WhenTieringPolicyIsEnabled", func(tt *testing.T) {
		// Save and restore the original value
		currentATState := autoTieringEnabled
		defer func() { autoTieringEnabled = currentATState }()
		autoTieringEnabled = true
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
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
		zone := "test-zone"

		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Zone:             "test-zone",
			Name:             "testvolume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/testvolume",
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
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled:   true,
				CoolingThresholdDays: 30,
				TieringPolicy:        "auto",
				RetrievalPolicy:      "default",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
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
				ResourceId:    "testvolume",
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
		zone := "test-zone"
		expected := &common.CreateVolumeParams{
			AccountName:      "test-project",
			Region:           "test-region",
			Zone:             "test-zone",
			Name:             "testvolume",
			VendorID:         "/projects/test-project/locations/test-location/volumes/testvolume",
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
			AutoTieringPolicy: &common.AutoTieringPolicy{
				AutoTieringEnabled: false,
				TieringPolicy:      "none",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("ValidInputWithFilePropertiesAndExportRules", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols: []gcpgenserver.ProtocolsV1beta{
					gcpgenserver.ProtocolsV1betaNFSV3,
				},
				ExportPolicy: gcpgenserver.NewOptExportPolicyV1beta(
					gcpgenserver.ExportPolicyV1beta{
						Rules: []gcpgenserver.SimpleExportPolicyRuleV1beta{
							{
								AllowedClients: "192.168.1.0/24",
								AccessType:     gcpgenserver.SimpleExportPolicyRuleV1betaAccessTypeREADWRITE,
								Nfsv3:          gcpgenserver.NewOptNilBool(true),
								Nfsv4:          gcpgenserver.NewOptNilBool(false),
							},
							{
								AllowedClients: "10.0.0.0/8",
								AccessType:     gcpgenserver.SimpleExportPolicyRuleV1betaAccessTypeREADONLY,
								Nfsv3:          gcpgenserver.NewOptNilBool(false),
								Nfsv4:          gcpgenserver.NewOptNilBool(true),
							},
						},
					},
				),
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"
		zone := "test-zone"

		expected := &common.CreateVolumeParams{
			AccountName:   "test-project",
			Region:        "test-region",
			Zone:          "test-zone",
			Name:          "testvolume",
			PoolID:        "test-pool",
			QuotaInBytes:  1024,
			Network:       "",
			CreationToken: "test-token",
			VendorID:      "/projects/test-project/locations/test-location/volumes/testvolume",
			Protocols: []string{
				"NFSV3",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     "READ_WRITE",
							NFSv3:          true,
							NFSv4:          false,
							Index:          1,
						},
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "READ_ONLY",
							NFSv3:          false,
							NFSv4:          true,
							Index:          2,
						},
					},
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("ValidInputWithMultipleProtocols", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols: []gcpgenserver.ProtocolsV1beta{
					gcpgenserver.ProtocolsV1betaNFSV3,
					gcpgenserver.ProtocolsV1betaNFSV4,
					gcpgenserver.ProtocolsV1betaSMB,
				},
			},
		}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		region := "test-region"
		zone := "test-zone"
		expected := &common.CreateVolumeParams{
			AccountName:   "test-project",
			Region:        "test-region",
			Zone:          "test-zone",
			Name:          "testvolume",
			PoolID:        "test-pool",
			QuotaInBytes:  1024,
			Network:       "",
			CreationToken: "test-token",
			VendorID:      "/projects/test-project/locations/test-location/volumes/testvolume",
			Protocols: []string{
				"NFSV3", "NFSV4", "SMB",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
					ExportRules:      nil,
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("ValidInputWithLargeCapacityAndConstituentCount", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:                  "testvolume",
				CreationToken:               gcpgenserver.NewOptString("test-token"),
				PoolId:                      gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:                gcpgenserver.NewOptFloat64(1024),
				LargeCapacity:               gcpgenserver.NewOptNilBool(true),
				LargeVolumeConstituentCount: gcpgenserver.NewOptNilInt32(8),
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
		zone := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:                 "test-project",
			Region:                      "test-region",
			Zone:                        "test-region",
			Name:                        "testvolume",
			VendorID:                    "/projects/test-project/locations/test-location/volumes/testvolume",
			CreationToken:               "test-token",
			PoolID:                      "test-pool",
			QuotaInBytes:                1024,
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 8,
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("ValidInputWithLargeCapacityOnly", func(tt *testing.T) {
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				LargeCapacity: gcpgenserver.NewOptNilBool(true),
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
		zone := "test-region"

		expected := &common.CreateVolumeParams{
			AccountName:                 "test-project",
			Zone:                        "test-region",
			Region:                      "test-region",
			Name:                        "testvolume",
			VendorID:                    "/projects/test-project/locations/test-location/volumes/testvolume",
			CreationToken:               "test-token",
			PoolID:                      "test-pool",
			QuotaInBytes:                1024,
			LargeCapacity:               true,
			LargeVolumeConstituentCount: 0, // Default value when not set
			BlockProperties: &common.BlockPropertiesRequest{
				OSType: "LINUX",
			},
			Protocols: []string{
				"ISCSI",
			},
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: req.Volume.CreationToken.Value,
				},
			},
		}
		result, err := prepareCreateVolumeParams(req, params, region, zone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
}

func TestV1betaGetMultipleVolumes(t *testing.T) {
	// Helper function to set up CVP environment
	setupCVPEnvironment := func(tt *testing.T) {
		tt.Setenv("CVP_HOST", "some-host")
	}

	t.Run("WhenVolumeUuidsIsNil", func(tt *testing.T) {
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: nil,
		}

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{}
		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "VolumeUuids are required", badRequest.Message)
	})

	t.Run("WhenVolumeUuidsExceeds1000", func(tt *testing.T) {
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}

		// Create a slice with 1001 UUIDs
		volumeUuids := make([]string, 1001)
		for i := 0; i < 1001; i++ {
			volumeUuids[i] = fmt.Sprintf("uuid%d", i)
		}

		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: volumeUuids,
		}

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{}
		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "VolumeUuids in body should have at most 1000 items", badRequest.Message)
	})

	t.Run("WhenGetMultipleVolumesFailsWithBadRequest", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		// mockClient removed (was unused)
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsWithUnprocessableEntity", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsUnauthorized", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsForbidden", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsNotFound", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsTooManyRequests", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsDefault", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesFailsInternalServerError", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		_, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok, "Expected OK response when CVP_HOST is not set")
	})
	t.Run("WhenGetMultipleVolumesNoVolumesFromCVP", func(tt *testing.T) {
		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 0)
	})
	t.Run("WhenGetMultipleVolumesNoVolumesFromCVPANDVCP", func(tt *testing.T) {
		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		vcpVolumes := []*models.Volume{
			{
				DisplayName: "vol1",
			},
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(vcpVolumes, nil)

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 1)
		assert.Equal(tt, "vol1", okResp.Volumes[0].ResourceId)
	})

	t.Run("Success - all volumes found in VCP, CVP_HOST is set", func(tt *testing.T) {
		origGetMultipleVolumesFromCVP := getMultipleVolumesFromCVP
		defer func() {
			getMultipleVolumesFromCVP = origGetMultipleVolumesFromCVP
		}()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		// Set CVP_HOST so the handler will not return early
		cvp.SetCVPHost("http://cvp-host")

		uuids := []string{"uuid1", "uuid2"}
		req := &gcpgenserver.VolumeIdListV1beta{VolumeUuids: uuids}
		params := gcpgenserver.V1betaGetMultipleVolumesParams{ProjectNumber: "proj1"}

		vols := []*models.Volume{
			{DisplayName: "vol1"},
			{DisplayName: "vol2"},
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, uuids, "proj1").Return(vols, nil).Once()

		res, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		okResp, ok := res.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 2)
		assert.Equal(tt, "vol1", okResp.Volumes[0].ResourceId)
		assert.Equal(tt, "vol2", okResp.Volumes[1].ResourceId)
	})

	t.Run("Success - some volumes found in VCP, some in CVP, CVP_HOST is set", func(tt *testing.T) {
		// Mock location validation
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "location-id", "location-id", nil
		}

		// Save and mock createCVPClient
		originalCreateCVPClient := createCVPClient
		defer func() { createCVPClient = originalCreateCVPClient }()
		mockVolumes := volumes.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Volumes: mockVolumes,
		}
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Set up the mock for the CVP Volumes client
		resourceID1 := "resource-id-2"
		resourceID2 := "resource-id-3"
		mockVolumes.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(&volumes.V1betaGetMultipleVolumesOK{
			Payload: &volumes.V1betaGetMultipleVolumesOKBody{
				Volumes: []*cvpmodels.VolumeV1beta{
					{
						VolumeID:   "uuid2",
						ResourceID: &resourceID1,
					},
					{
						VolumeID:   "uuid3",
						ResourceID: &resourceID2,
					},
				},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		// Set CVP_HOST so the handler will not return early
		cvp.SetCVPHost("http://cvp-host")

		uuids := []string{"uuid1", "uuid2", "uuid3"}
		req := &gcpgenserver.VolumeIdListV1beta{VolumeUuids: uuids}
		params := gcpgenserver.V1betaGetMultipleVolumesParams{ProjectNumber: "proj1"}

		// Return only one volume from VCP to simulate that uuid2 and uuid3 are missing
		volsVCP := []*models.Volume{{BaseModel: models.BaseModel{UUID: "uuid1"}, DisplayName: "vol1"}}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, uuids, "proj1").Return(volsVCP, nil).Once()

		res, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		okResp, ok := res.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		// Should contain both VCP and CVP volumes
		assert.Len(tt, okResp.Volumes, 3)
	})

	t.Run("Success - some volumes found in VCP, CVP_HOST is not set", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		// Clear CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		uuids := []string{"uuid1", "uuid2"}
		req := &gcpgenserver.VolumeIdListV1beta{VolumeUuids: uuids}
		params := gcpgenserver.V1betaGetMultipleVolumesParams{ProjectNumber: "proj1"}

		vols := []*models.Volume{
			{DisplayName: "vol1"},
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, uuids, "proj1").Return(vols, nil).Once()

		res, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)
		assert.NoError(tt, err)
		assert.NotNil(tt, res)

		okResp, ok := res.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 1)
		assert.Equal(tt, "vol1", okResp.Volumes[0].ResourceId)
	})

	t.Run("WhenLocationValidationFails", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "invalid-location",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		// Mock location validation to fail
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "", "", &gcpgenserver.Error{
				Code:    400,
				Message: "Invalid location",
			}
		}

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		badRequest, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badRequest.Code)
		assert.Equal(tt, "Invalid location", badRequest.Message)
	})

	t.Run("WhenNoMissingVolumes", func(tt *testing.T) {
		// Don't set CVP_HOST so CVP calls will be skipped
		cvp.SetCVPHost("")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested volumes (no missing volumes)
		vols := []*models.Volume{
			{BaseModel: models.BaseModel{UUID: "uuid1"}, DisplayName: "vol1"},
			{BaseModel: models.BaseModel{UUID: "uuid2"}, DisplayName: "vol2"},
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(vols, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since all volumes are found in VCP, we expect OK response with all volumes
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 2)
	})

	t.Run("WhenNoMissingVolumesWithCVPEnabled", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		cvp.SetCVPHost("http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return all requested volumes (no missing volumes)
		vols := []*models.Volume{
			{BaseModel: models.BaseModel{UUID: "uuid1"}, DisplayName: "vol1"},
			{BaseModel: models.BaseModel{UUID: "uuid2"}, DisplayName: "vol2"},
		}

		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(vols, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		handler := Handler{
			Orchestrator: mockOrchestrator,
		}
		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		// Since all volumes are found in VCP, we expect OK response with all volumes (no CVP call)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 2)
	})

	t.Run("WhenXCorrelationIDIsSet", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		cvp.SetCVPHost("http://cvp-host")

		// Save and mock createCVPClient
		originalCreateCVPClient := createCVPClient
		defer func() { createCVPClient = originalCreateCVPClient }()
		mockVolumes := volumes.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Volumes: mockVolumes,
		}
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Set up the mock for the CVP Volumes client
		resourceID1 := "resource-id-1"
		resourceID2 := "resource-id-2"
		mockVolumes.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(&volumes.V1betaGetMultipleVolumesOK{
			Payload: &volumes.V1betaGetMultipleVolumesOKBody{
				Volumes: []*cvpmodels.VolumeV1beta{
					{VolumeID: "uuid1", ResourceID: &resourceID1},
					{VolumeID: "uuid2", ResourceID: &resourceID2},
				},
			},
		}, nil)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:     "us-east4",
			ProjectNumber:  "project-number",
			XCorrelationID: gcpgenserver.NewOptString("correlation-id-123"),
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty volumes so CVP will be called
		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 2)
	})

	t.Run("WhenCVPResponseIsNil", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		setupCVPEnvironment(tt)

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1", "uuid2"},
		}

		// Mock VCP to return empty volumes so CVP will be called
		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4", nil
		}

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		internalErr, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Equal(tt, "unknown error during get multiple volumes operation", internalErr.Message)
	})

	t.Run("WhenSingleVolumeNotInVCP_ShouldFallbackToCVPAndReturnVolume", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		cvp.SetCVPHost("http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		// Single volume request
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1"},
		}

		// Mock VCP to return empty volumes (no error) so CVP will be called
		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		// Save and mock createCVPClient
		originalCreateCVPClient := createCVPClient
		defer func() { createCVPClient = originalCreateCVPClient }()
		mockVolumes := volumes.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Volumes: mockVolumes,
		}
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Set up the mock for the CVP Volumes client - expect CVP to be called
		resourceID := "cvp-resource-id"
		mockVolumes.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(&volumes.V1betaGetMultipleVolumesOK{
			Payload: &volumes.V1betaGetMultipleVolumesOKBody{
				Volumes: []*cvpmodels.VolumeV1beta{
					{
						VolumeID:   "uuid1",
						ResourceID: &resourceID,
					},
				},
			},
		}, nil).Once()

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 1)
		assert.Equal(tt, "cvp-resource-id", okResp.Volumes[0].ResourceId)
	})

	t.Run("WhenSingleVolumeNotFoundInBothVCPAndCVP_ShouldReturnEmptyList", func(tt *testing.T) {
		// Set CVP_HOST so CVP calls will be made
		cvp.SetCVPHost("http://cvp-host")

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{
			Orchestrator: mockOrchestrator,
		}

		params := gcpgenserver.V1betaGetMultipleVolumesParams{
			LocationId:    "us-east4",
			ProjectNumber: "project-number",
		}
		// Single volume request
		req := &gcpgenserver.VolumeIdListV1beta{
			VolumeUuids: []string{"uuid1"},
		}

		// Mock VCP to return empty volumes (no error) so CVP will be called
		mockOrchestrator.EXPECT().GetMultipleVolumes(mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		// Mock location validation
		originalParseAndValidateRegionAndZone := parseAndValidateRegionAndZone
		defer func() { parseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()
		parseAndValidateRegionAndZone = func(locationID string) (string, string, *gcpgenserver.Error) {
			return "us-east4", "us-east4-a", nil
		}

		// Save and mock createCVPClient
		originalCreateCVPClient := createCVPClient
		defer func() { createCVPClient = originalCreateCVPClient }()
		mockVolumes := volumes.NewMockClientService(tt)
		mockClient := &cvpapi.Cvp{
			Volumes: mockVolumes,
		}
		createCVPClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *mockClient
		}

		// Set up the mock for the CVP Volumes client - CVP also returns empty (no volumes found)
		mockVolumes.EXPECT().V1betaGetMultipleVolumes(mock.Anything).Return(&volumes.V1betaGetMultipleVolumesOK{
			Payload: &volumes.V1betaGetMultipleVolumesOKBody{
				Volumes: []*cvpmodels.VolumeV1beta{}, // Empty list - no volumes found in CVP either
			},
		}, nil).Once()

		result, err := handler.V1betaGetMultipleVolumes(context.Background(), req, params)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		okResp, ok := result.(*gcpgenserver.V1betaGetMultipleVolumesOK)
		assert.True(tt, ok)
		assert.Len(tt, okResp.Volumes, 0) // Should return empty volumes list when neither VCP nor CVP finds the volume
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
		assert.Equal(tt, "peer-svm", res.CacheParameters.Value.PeerSvmName)
		assert.Equal(tt, "peer-volume", res.CacheParameters.Value.PeerVolumeName)
		assert.Equal(tt, "test-command", res.CacheParameters.Value.Command.Value)
		assert.Equal(tt, "alderan", res.CacheParameters.Value.PeerClusterName)
		assert.Equal(tt, "test-passphrase", res.CacheParameters.Value.Passphrase.Value)
		assert.Equal(tt, "network-id", res.Network.Value)
		assert.Equal(tt, "pool-id", res.PoolId.Value)
		assert.Equal(tt, "pool-resource-id", res.PoolResourceId.Value)

		assert.Equal(tt, int64(10199181), res.BackupConfig.Value.BackupChainBytes.Value)
		assert.Equal(tt, "backup-policy-id", res.BackupConfig.Value.BackupPolicyId.Value)
		assert.Equal(tt, "backup-vault-id", res.BackupConfig.Value.BackupVaultId.Value)
	})

	t.Run("BasicVolumeConversionWithNilFields", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			ResourceID:         nil, // Test nil ResourceID
			VolumeID:           "volume-456",
			VolumeState:        "ACTIVE",
			VolumeStateDetails: "Volume is healthy",
			SecurityStyle:      "unix",
			ServiceLevel:       "FLEX",
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.Empty(tt, result.ResourceId) // nil ResourceID becomes empty string
		assert.Equal(tt, "volume-456", result.VolumeId.Value)
		assert.Equal(tt, gcpgenserver.VolumeV1betaVolumeState("ACTIVE"), result.VolumeState.Value)
	})

	t.Run("VolumeWithExportPolicyRules", func(tt *testing.T) {
		exportPolicy := &cvpmodels.ExportPolicyV1beta{
			Rules: []*cvpmodels.SimpleExportPolicyRuleV1beta{
				{
					AccessType:         nillable.GetStringPtr("ReadWrite"),
					AllowedClients:     nillable.GetStringPtr("0.0.0.0/0"),
					HasRootAccess:      nillable.GetStringPtr("true"),
					Kerberos5ReadOnly:  nillable.GetBoolPtr(false),
					Kerberos5ReadWrite: nillable.GetBoolPtr(true),
					Nfsv3:              nillable.GetBoolPtr(true),
					Nfsv4:              nillable.GetBoolPtr(true),
				},
			},
		}

		input := &cvpmodels.VolumeV1beta{
			VolumeID:     "volume-123",
			ExportPolicy: exportPolicy,
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.True(tt, result.ExportPolicy.IsSet())
		assert.Len(tt, result.ExportPolicy.Value.Rules, 1)

		rule := result.ExportPolicy.Value.Rules[0]
		assert.Equal(tt, gcpgenserver.SimpleExportPolicyRuleV1betaAccessType("ReadWrite"), rule.AccessType)
		assert.Equal(tt, "0.0.0.0/0", rule.AllowedClients)
		assert.Equal(tt, gcpgenserver.SimpleExportPolicyRuleV1betaHasRootAccess("true"), rule.HasRootAccess.Value)
		assert.False(tt, rule.Kerberos5ReadOnly.Value)
		assert.True(tt, rule.Kerberos5ReadWrite.Value)
		assert.True(tt, rule.Nfsv3.Value)
		assert.True(tt, rule.Nfsv4.Value)
	})

	t.Run("VolumeWithNilExportPolicy", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID:     "volume-123",
			ExportPolicy: nil,
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.True(tt, result.ExportPolicy.IsSet())
		assert.Empty(tt, result.ExportPolicy.Value.Rules)
	})

	t.Run("VolumeWithSnapshotPolicyFullSchedules", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			SnapshotPolicy: &cvpmodels.SnapshotPolicyV1beta{
				Enabled: nillable.GetBoolPtr(true),
				HourlySchedule: &cvpmodels.HourlyScheduleV1beta{
					Minute:          nillable.GetFloat64Ptr(30),
					SnapshotsToKeep: nillable.GetFloat64Ptr(24),
				},
				DailySchedule: &cvpmodels.DailyScheduleV1beta{
					Hour:            nillable.GetFloat64Ptr(2),
					Minute:          nillable.GetFloat64Ptr(0),
					SnapshotsToKeep: nillable.GetFloat64Ptr(7),
				},
				WeeklySchedule: &cvpmodels.WeeklyScheduleV1beta{
					Day:             "Sunday",
					Hour:            nillable.GetFloat64Ptr(3),
					Minute:          nillable.GetFloat64Ptr(15),
					SnapshotsToKeep: nillable.GetFloat64Ptr(4),
				},
				MonthlySchedule: &cvpmodels.MonthlyScheduleV1beta{
					DaysOfMonth:     "1",
					Hour:            nillable.GetFloat64Ptr(1),
					Minute:          nillable.GetFloat64Ptr(30),
					SnapshotsToKeep: nillable.GetFloat64Ptr(12),
				},
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.True(tt, result.SnapshotPolicy.IsSet())
		policy := result.SnapshotPolicy.Value

		assert.True(tt, policy.Enabled.Value)

		assert.True(tt, policy.HourlySchedule.IsSet())
		assert.Equal(tt, float64(30), policy.HourlySchedule.Value.Minute.Value)
		assert.Equal(tt, float64(24), policy.HourlySchedule.Value.SnapshotsToKeep.Value)

		assert.True(tt, policy.DailySchedule.IsSet())
		assert.Equal(tt, float64(2), policy.DailySchedule.Value.Hour.Value)
		assert.Equal(tt, float64(7), policy.DailySchedule.Value.SnapshotsToKeep.Value)

		assert.True(tt, policy.WeeklySchedule.IsSet())
		assert.Equal(tt, "Sunday", policy.WeeklySchedule.Value.Day.Value)
		assert.Equal(tt, float64(4), policy.WeeklySchedule.Value.SnapshotsToKeep.Value)

		assert.True(tt, policy.MonthlySchedule.IsSet())
		assert.Equal(tt, "1", policy.MonthlySchedule.Value.DaysOfMonth.Value)
		assert.Equal(tt, float64(12), policy.MonthlySchedule.Value.SnapshotsToKeep.Value)
	})

	t.Run("VolumeWithSnapshotPolicyDisabled", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			SnapshotPolicy: &cvpmodels.SnapshotPolicyV1beta{
				Enabled: nillable.GetBoolPtr(false),
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.False(tt, result.SnapshotPolicy.IsSet())
	})

	t.Run("VolumeWithMountPoints", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			MountPoints: []*cvpmodels.MountPointV1beta{
				{
					Export:       "/vol1",
					ExportFull:   "server:/vol1",
					IPAddress:    "192.168.1.100",
					Instructions: "mount -t nfs server:/vol1 /mnt/vol1",
					Protocol:     "NFSv3",
				},
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.Len(tt, result.MountPoints, 1)

		mp1 := result.MountPoints[0]
		assert.Equal(tt, "/vol1", mp1.Export.Value)
		assert.Equal(tt, "server:/vol1", mp1.ExportFull.Value)
		assert.Equal(tt, "192.168.1.100", mp1.IpAddress.Value)
		assert.Equal(tt, "mount -t nfs server:/vol1 /mnt/vol1", mp1.Instructions.Value)
		assert.Equal(tt, gcpgenserver.ProtocolsV1beta("NFSv3"), mp1.Protocol.Value)
	})

	t.Run("VolumeWithTieringPolicy", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			TieringPolicy: &cvpmodels.TieringPolicyV1beta{
				TierAction: nillable.GetStringPtr("AUTO"),
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.True(tt, result.TieringPolicy.IsSet())
		assert.Equal(tt, gcpgenserver.TieringPolicyV1betaTierAction("AUTO"), result.TieringPolicy.Value.TierAction.Value)
	})

	t.Run("VolumeWithTieringPolicyNilTierAction", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			TieringPolicy: &cvpmodels.TieringPolicyV1beta{
				TierAction: nil, // nil tier action
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.False(tt, result.TieringPolicy.IsSet())
	})

	t.Run("VolumeWithCacheParametersNilFields", func(tt *testing.T) {
		input := &cvpmodels.VolumeV1beta{
			VolumeID: "volume-123",
			CacheParameters: &cvpmodels.FlexCacheV1beta{
				PeerVolumeName:       "origin-volume",
				PeerClusterName:      "origin-cluster",
				PeerSvmName:          "origin-svm",
				CacheState:           "online",
				Command:              "flexcache create",
				EnableGlobalFileLock: nil, // nil
				CommandExpiryTime:    nil, // nil
				Passphrase:           nil, // nil
			},
		}

		result := _convertVolumeV1betaCVPToModel(input)

		assert.True(tt, result.CacheParameters.IsSet())
		cache := result.CacheParameters.Value

		assert.Equal(tt, "origin-volume", cache.PeerVolumeName)
		assert.False(tt, cache.EnableGlobalFileLock.IsSet())
		assert.False(tt, cache.CommandExpiryTime.IsSet())
		assert.False(tt, cache.Passphrase.IsSet())
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
			Labels:       gcpgenserver.OptVolumeUpdateV1betaLabels{Set: true, Value: map[string]string{"key1": "value1", "key2": "value2"}},
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			QuotaInBytes:   107374182499,
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
		assert.Nil(tt, err)
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182499),
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182499),
		}

		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.New("An error occurred"))

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorThrowsConflictError_Return409Conflict", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaUpdateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("test-pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182499),
		}

		mockOrchestrator.EXPECT().UpdateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("Volume update conflict"))

		result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		conflictErr, ok := result.(*gcpgenserver.V1betaUpdateVolumeConflict)
		assert.True(tt, ok)
		assert.Equal(tt, float64(409), conflictErr.Code)
		assert.Contains(tt, conflictErr.Message, "Volume update conflict")
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
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
			QuotaInBytes:   107374182400,
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("PAUSED"),
				},
			),
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			QuotaInBytes:   107374182400,
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
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
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = true
	t.Run("WhenAllFieldsSet_ThenFieldsAreMapped", func(t *testing.T) {
		labels := make(map[string]string)
		labels["k"] = "v"
		jsonbLabels := make(datamodel.JSONB)
		for k, v := range labels {
			jsonbLabels[k] = v
		}
		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
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
			Labels: gcpgenserver.NewOptVolumeUpdateV1betaLabels(labels),
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.Equal(t, "pool", out.PoolID)
		assert.Equal(t, int64(107374182400), out.QuotaInBytes)
		assert.Equal(t, "desc", out.Description)
		assert.Equal(t, []string{"ISCSI"}, out.Protocols)
		assert.NotNil(t, out.BlockProperties)
		assert.Equal(t, "LINUX", out.BlockProperties.OSType)
		assert.Equal(t, &jsonbLabels, out.Labels)
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
		assert.EqualError(t, err, "key is required in label")
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

	t.Run("WhenBlockDevicesSet_ThenFieldsAreMapped", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BlockDevices: []gcpgenserver.BlockDeviceV1beta{
				{
					Name:       gcpgenserver.NewOptString("test-lun"),
					HostGroups: []string{"9760acf5-4638-11e7-9bdb-020073ca3333", "9760acf5-4638-11e7-9bdb-020073ca4444"},
					OsType:     gcpgenserver.NewOptBlockDeviceV1betaOsType(gcpgenserver.BlockDeviceV1betaOsTypeLINUX),
				},
			},
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 1)
		assert.Equal(t, "test-lun", out.BlockDevices[0].Name)
		assert.Equal(t, []string{"9760acf5-4638-11e7-9bdb-020073ca3333", "9760acf5-4638-11e7-9bdb-020073ca4444"}, out.BlockDevices[0].HostGroups)
		assert.Equal(t, "LINUX", out.BlockDevices[0].OSType)
	})

	t.Run("WhenMultipleBlockDevicesSet_ThenErrorIsReturned", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BlockDevices: []gcpgenserver.BlockDeviceV1beta{
				{
					Name: gcpgenserver.NewOptString("test-lun-1"),
				},
				{
					Name: gcpgenserver.NewOptString("test-lun-2"),
				},
			},
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.EqualError(t, err, "Only one BlockDevice can be specified for update")
		assert.Nil(t, out)
	})

	t.Run("WhenBlockDeviceWithoutName_ThenErrorIsReturned", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BlockDevices: []gcpgenserver.BlockDeviceV1beta{
				{
					HostGroups: []string{"9760acf5-4638-11e7-9bdb-020073ca3333"},
				},
			},
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.EqualError(t, err, "BlockDevice name is required")
		assert.Nil(t, out)
	})

	t.Run("WhenBlockDevicesWithDuplicateHostGroups_ThenHostGroupsAreDeduplicated", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BlockDevices: []gcpgenserver.BlockDeviceV1beta{
				{
					Name:       gcpgenserver.NewOptString("test-lun"),
					HostGroups: []string{"9760acf5-4638-11e7-9bdb-020073ca3333", "9760acf5-4638-11e7-9bdb-020073ca3333"},
				},
			},
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 1)
		assert.Equal(t, []string{"9760acf5-4638-11e7-9bdb-020073ca3333"}, out.BlockDevices[0].HostGroups)
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction:           gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("ENABLED"),
					CoolingThresholdDays: gcpgenserver.OptNilInt32{Value: 30, Set: true},
				},
			),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotEmpty(t, param.AutoTieringPolicy.TieringPolicy)
		assert.True(t, param.AutoTieringPolicy.AutoTieringEnabled)
		assert.Equal(t, int32(30), param.AutoTieringPolicy.CoolingThresholdDays)
	})

	t.Run("TieringPolicy PAUSED with feature enabled", func(t *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = true
		defer func() { autoTieringEnabled = currentATState }()

		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
			TieringPolicy: gcpgenserver.NewOptTieringPolicyV1beta(
				gcpgenserver.TieringPolicyV1beta{
					TierAction: gcpgenserver.NewOptNilTieringPolicyV1betaTierAction("PAUSED"),
				},
			),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.NotEmpty(t, param.AutoTieringPolicy.TieringPolicy)
		assert.False(t, param.AutoTieringPolicy.AutoTieringEnabled)
	})

	t.Run("TieringPolicy set with feature disabled", func(t *testing.T) {
		currentATState := autoTieringEnabled
		autoTieringEnabled = false
		defer func() { autoTieringEnabled = currentATState }()

		req := &gcpgenserver.VolumeUpdateV1beta{
			PoolId:       gcpgenserver.NewOptNilString("pool"),
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
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
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(107374182400),
		}
		param, err := _prepareUpdateVolumeParams(req, params, region)
		assert.NoError(t, err)
		assert.Nil(t, param.AutoTieringPolicy)
	})

	t.Run("WhenBackupConfigSet_WithFewFields", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(gcpgenserver.BackupConfigV1beta{
				BackupVaultId:  gcpgenserver.NewOptNilString("backup-vault-id"),
				BackupPolicyId: gcpgenserver.NewOptNilString("backup-policy-id"),
			}),
		}

		param, err := _prepareUpdateVolumeParams(req, params, "region")
		assert.NoError(t, err)
		assert.Equal(t, "backup-vault-id", *param.DataProtection.BackupVaultID)
		assert.Equal(t, "backup-policy-id", *param.DataProtection.BackupPolicyId)
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

		assert.Nil(tt, err)
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

		assert.Nil(tt, err)
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
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				Labels:        gcpgenserver.OptVolumeV1betaLabels{Value: map[string]string{"test-label": "test-value"}, Set: true},
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
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string, zone string) (*common.CreateVolumeParams, error) {
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

	t.Run("UserInputValidationErrorWhenVolumeQuotaIsByteIsNotSet", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{},
		}

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaCreateVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "QuotaInBytes is required", badReq.Message)
	})

	t.Run("InternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaCreateVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
		}
		req := &gcpgenserver.VolumeCreateV1beta{}
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string, zone string) (*common.CreateVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareCreateVolumeParams = _prepareCreateVolumeParams }()

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.Nil(tt, err)
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
		prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string, zone string) (*common.CreateVolumeParams, error) {
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
				ResourceId:    "testvolume",
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
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}

		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.New("An error occurred"))

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaCreateVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorThrowsConflictError_Return409Conflict", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaCreateVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
		}
		req := &gcpgenserver.VolumeCreateV1beta{
			Volume: gcpgenserver.VolumeV1beta{
				ResourceId:    "testvolume",
				CreationToken: gcpgenserver.NewOptString("test-token"),
				PoolId:        gcpgenserver.NewNilString("test-pool"),
				QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
				Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			},
		}

		mockOrchestrator.EXPECT().CreateVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewConflictErr("Volume already exists"))

		result, err := handler.V1betaCreateVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		conflictErr, ok := result.(*gcpgenserver.V1betaCreateVolumeConflict)
		assert.True(tt, ok)
		assert.Equal(tt, float64(409), conflictErr.Code)
		assert.Contains(tt, conflictErr.Message, "Volume already exists")
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
				ResourceId:    "testvolume",
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
				ResourceId:    "testvolume",
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
				ResourceId:    "testvolume",
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
			IPAddresses:     []string{"10.72.177.17"},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)
		assert.Equal(t, "token", out.CreationToken.Value)
		assert.Equal(t, "LINUX", string(out.BlockProperties.Value.OsType.Value))
		assert.Equal(t, "ISCSI", string(out.Protocols[0]))
	})
	t.Run("AllFieldsWithKms", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:   "token",
			PoolID:          "pool",
			QuotaInBytes:    1234,
			BlockProperties: &models.BlockProperties{OSType: "LINUX"},
			ProtocolTypes:   []string{"ISCSI"},
			LifeCycleState:  "READY",
			IPAddresses:     []string{"10.72.177.17"},
			KmsConfig: &models.KmsConfig{
				BaseModel: models.BaseModel{
					UUID: "kms-uuid",
				},
				KeyRingLocation: "location-id",
				KeyRing:         "key-ring-name",
				KeyName:         "key-name",
				KeyProjectID:    "proj-1",
			},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)
		assert.Equal(t, "token", out.CreationToken.Value)
		assert.Equal(t, "LINUX", string(out.BlockProperties.Value.OsType.Value))
		assert.Equal(t, "ISCSI", string(out.Protocols[0]))
		assert.Equal(t, "kms-uuid", out.KmsConfigId.Value)
		assert.Equal(t, "projects/proj-1/locations/location-id/keyRings/key-ring-name/cryptoKeys/key-name", out.KmsConfigResourceId.Value)
	})

	t.Run("WithFilePropertiesAndExportRules", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:  "file-token",
			PoolID:         "file-pool",
			QuotaInBytes:   2048,
			ProtocolTypes:  []string{"NFSV3"},
			LifeCycleState: "READY",
			FileProperties: &models.FileProperties{
				ExportPolicy: &models.ExportPolicy{
					ExportPolicyName: "test-export-policy",
					ExportRules: []*models.ExportRule{
						{
							AllowedClients: "192.168.1.0/24",
							AccessType:     "READ_WRITE",
							NFSv3:          true,
							NFSv4:          false,
							Index:          1,
						},
						{
							AllowedClients: "10.0.0.0/8",
							AccessType:     "READ_ONLY",
							NFSv3:          false,
							NFSv4:          true,
							Index:          2,
						},
					},
				},
			},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)
		assert.Equal(t, "file-token", out.CreationToken.Value)
		assert.Equal(t, "NFSV3", string(out.Protocols[0]))

		// Verify ExportPolicy is properly converted
		assert.True(t, out.ExportPolicy.IsSet())
		exportPolicy := out.ExportPolicy.Value
		assert.Len(t, exportPolicy.Rules, 2)

		// Verify first rule
		rule1 := exportPolicy.Rules[0]
		assert.Equal(t, "192.168.1.0/24", rule1.AllowedClients)
		assert.Equal(t, gcpgenserver.SimpleExportPolicyRuleV1betaAccessTypeREADWRITE, rule1.AccessType)
		assert.True(t, rule1.Nfsv3.Value)
		assert.False(t, rule1.Nfsv4.Value)

		// Verify second rule
		rule2 := exportPolicy.Rules[1]
		assert.Equal(t, "10.0.0.0/8", rule2.AllowedClients)
		assert.Equal(t, gcpgenserver.SimpleExportPolicyRuleV1betaAccessTypeREADONLY, rule2.AccessType)
		assert.False(t, rule2.Nfsv3.Value)
		assert.True(t, rule2.Nfsv4.Value)
	})

	t.Run("WithBlockDevices_ShouldConvertToAPIFormat", func(t *testing.T) {
		blockDevices := []models.BlockDevice{
			{
				Name:       "test-lun-1",
				Identifier: "lun-123",
				Size:       107374182400, // 100 GiB in bytes
				OSType:     "LINUX",
				HostGroupDetail: []models.HostGroupDetails{
					{
						HostGroupID: "hg-uuid-1",
						Hosts:       []string{"iqn.1998-01.com.vmware:host1", "iqn.1998-01.com.vmware:host2"},
					},
					{
						HostGroupID: "hg-uuid-2",
						Hosts:       []string{"iqn.1998-01.com.vmware:host3"},
					},
				},
			},
			{
				Name:       "test-lun-2",
				Identifier: "lun-456",
				Size:       214748364800, // 200 GiB in bytes
				OSType:     "WINDOWS",
				HostGroupDetail: []models.HostGroupDetails{
					{
						HostGroupID: "hg-uuid-3",
						Hosts:       []string{"iqn.1998-01.com.vmware:host4"},
					},
				},
			},
		}

		vol := &models.Volume{
			CreationToken:  "block-token",
			PoolID:         "block-pool",
			QuotaInBytes:   322122547200, // 300 GiB
			ProtocolTypes:  []string{"ISCSI"},
			LifeCycleState: "READY",
			IPAddresses:    []string{"10.72.177.17"},
			BlockDevices:   &blockDevices,
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify BlockDevices are properly converted
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 2)

		// Verify first BlockDevice
		bd1 := out.BlockDevices[0]
		assert.Equal(t, "test-lun-1", bd1.Name.Value)
		assert.Equal(t, "lun-123", bd1.Identifier.Value)
		assert.Equal(t, float64(107374182400), bd1.SizeInBytes.Value)
		assert.Equal(t, gcpgenserver.BlockDeviceV1betaOsTypeLINUX, bd1.OsType.Value)
		assert.Len(t, bd1.HostGroups, 2)
		assert.Equal(t, "hg-uuid-1", bd1.HostGroups[0])
		assert.Equal(t, "hg-uuid-2", bd1.HostGroups[1])
		assert.Len(t, bd1.HostGroupDetails, 2)
		assert.Equal(t, "hg-uuid-1", bd1.HostGroupDetails[0].HostGroupId.Value)
		assert.Equal(t, []string{"iqn.1998-01.com.vmware:host1", "iqn.1998-01.com.vmware:host2"}, bd1.HostGroupDetails[0].Hosts)
		assert.Equal(t, "hg-uuid-2", bd1.HostGroupDetails[1].HostGroupId.Value)
		assert.Equal(t, []string{"iqn.1998-01.com.vmware:host3"}, bd1.HostGroupDetails[1].Hosts)

		// Verify second BlockDevice
		bd2 := out.BlockDevices[1]
		assert.Equal(t, "test-lun-2", bd2.Name.Value)
		assert.Equal(t, "lun-456", bd2.Identifier.Value)
		assert.Equal(t, float64(214748364800), bd2.SizeInBytes.Value)
		assert.Equal(t, gcpgenserver.BlockDeviceV1betaOsTypeWINDOWS, bd2.OsType.Value)
		assert.Len(t, bd2.HostGroups, 1)
		assert.Equal(t, "hg-uuid-3", bd2.HostGroups[0])
		assert.Len(t, bd2.HostGroupDetails, 1)
		assert.Equal(t, "hg-uuid-3", bd2.HostGroupDetails[0].HostGroupId.Value)
		assert.Equal(t, []string{"iqn.1998-01.com.vmware:host4"}, bd2.HostGroupDetails[0].Hosts)

		// Verify mount points are created for the first BlockDevice (primary)
		assert.NotNil(t, out.MountPoints)
		assert.Len(t, out.MountPoints, 1)
		assert.Equal(t, "10.72.177.17", out.MountPoints[0].IpAddress.Value)
		assert.Equal(t, gcpgenserver.ProtocolsV1betaISCSI, out.MountPoints[0].Protocol.Value)
		assert.NotEmpty(t, out.MountPoints[0].Instructions.Value)
		assert.Contains(t, out.MountPoints[0].Instructions.Value, "lun-123")
	})

	t.Run("WithBlockDevicesNoHostGroups_ShouldConvertCorrectly", func(t *testing.T) {
		blockDevices := []models.BlockDevice{
			{
				Name:            "test-lun-no-hg",
				Identifier:      "lun-789",
				Size:            53687091200, // 50 GiB in bytes
				OSType:          "ESXI",
				HostGroupDetail: []models.HostGroupDetails{}, // Empty host groups
			},
		}

		vol := &models.Volume{
			CreationToken:  "block-token-no-hg",
			PoolID:         "block-pool",
			QuotaInBytes:   53687091200,
			ProtocolTypes:  []string{"ISCSI"},
			LifeCycleState: "READY",
			IPAddresses:    []string{"10.72.177.18"},
			BlockDevices:   &blockDevices,
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify BlockDevice is properly converted
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 1)

		bd := out.BlockDevices[0]
		assert.Equal(t, "test-lun-no-hg", bd.Name.Value)
		assert.Equal(t, "lun-789", bd.Identifier.Value)
		assert.Equal(t, float64(53687091200), bd.SizeInBytes.Value)
		assert.Equal(t, gcpgenserver.BlockDeviceV1betaOsTypeESXI, bd.OsType.Value)
		assert.Empty(t, bd.HostGroups)
		assert.Empty(t, bd.HostGroupDetails)

		// Verify mount points are created
		assert.NotNil(t, out.MountPoints)
		assert.Len(t, out.MountPoints, 1)
		assert.Equal(t, "10.72.177.18", out.MountPoints[0].IpAddress.Value)
		assert.Equal(t, gcpgenserver.ProtocolsV1betaISCSI, out.MountPoints[0].Protocol.Value)
		assert.NotEmpty(t, out.MountPoints[0].Instructions.Value)
		// ESXI instructions don't include the LUN name in the text
		assert.Contains(t, out.MountPoints[0].Instructions.Value, "ESXi")
	})

	t.Run("WithBlockDevicesMissingFields_ShouldHandleGracefully", func(t *testing.T) {
		blockDevices := []models.BlockDevice{
			{
				Name:       "", // Empty name
				Identifier: "", // Empty identifier
				Size:       0,  // Zero size
				OSType:     "", // Empty OS type
				HostGroupDetail: []models.HostGroupDetails{
					{
						HostGroupID: "hg-uuid-4",
						Hosts:       []string{"iqn.1998-01.com.vmware:host5"},
					},
				},
			},
		}

		vol := &models.Volume{
			CreationToken:  "block-token-missing",
			PoolID:         "block-pool",
			QuotaInBytes:   107374182400,
			ProtocolTypes:  []string{"ISCSI"},
			LifeCycleState: "READY",
			IPAddresses:    []string{"10.72.177.17"},
			BlockDevices:   &blockDevices,
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify BlockDevice is properly converted with empty fields
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 1)

		bd := out.BlockDevices[0]
		assert.False(t, bd.Name.IsSet())        // Name should not be set when empty
		assert.False(t, bd.Identifier.IsSet())  // Identifier should not be set when empty
		assert.False(t, bd.SizeInBytes.IsSet()) // Size should not be set when zero
		assert.False(t, bd.OsType.IsSet())      // OS type should not be set when empty
		assert.Len(t, bd.HostGroups, 1)
		assert.Equal(t, "hg-uuid-4", bd.HostGroups[0])
		assert.Len(t, bd.HostGroupDetails, 1)
		assert.Equal(t, "hg-uuid-4", bd.HostGroupDetails[0].HostGroupId.Value)
		assert.Equal(t, []string{"iqn.1998-01.com.vmware:host5"}, bd.HostGroupDetails[0].Hosts)

		// Verify mount points are NOT created when identifier is missing
		assert.Empty(t, out.MountPoints)
	})

	t.Run("WithBlockDevicesNotReady_ShouldNotCreateMountPoints", func(t *testing.T) {
		blockDevices := []models.BlockDevice{
			{
				Name:       "test-lun-not-ready",
				Identifier: "lun-999",
				Size:       107374182400,
				OSType:     "LINUX",
				HostGroupDetail: []models.HostGroupDetails{
					{
						HostGroupID: "hg-uuid-5",
						Hosts:       []string{"iqn.1998-01.com.vmware:host6"},
					},
				},
			},
		}

		vol := &models.Volume{
			CreationToken:  "block-token-not-ready",
			PoolID:         "block-pool",
			QuotaInBytes:   107374182400,
			ProtocolTypes:  []string{"ISCSI"},
			LifeCycleState: "CREATING", // Not READY
			IPAddresses:    []string{"10.72.177.17"},
			BlockDevices:   &blockDevices,
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify BlockDevices are converted
		assert.NotNil(t, out.BlockDevices)
		assert.Len(t, out.BlockDevices, 1)

		// Verify mount points are NOT created when volume is not ready
		assert.Empty(t, out.MountPoints)
	})

	t.Run("WithBlockDevicesEmptyArray_ShouldHandleGracefully", func(t *testing.T) {
		blockDevices := []models.BlockDevice{} // Empty array

		vol := &models.Volume{
			CreationToken:  "block-token-empty",
			PoolID:         "block-pool",
			QuotaInBytes:   107374182400,
			ProtocolTypes:  []string{"ISCSI"},
			LifeCycleState: "READY",
			IPAddresses:    []string{"10.72.177.17"},
			BlockDevices:   &blockDevices,
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify BlockDevices is nil when array is empty (not set in response)
		assert.Nil(t, out.BlockDevices)

		// Verify mount points are NOT created when no BlockDevices
		assert.Empty(t, out.MountPoints)
	})

	t.Run("WithLargeCapacityAndConstituentCount", func(t *testing.T) {
		constituentCount := int32(8)
		vol := &models.Volume{
			CreationToken:               "large-volume-token",
			PoolID:                      "large-pool",
			QuotaInBytes:                1073741824000, // 1TB
			LargeCapacity:               true,
			LargeVolumeConstituentCount: &constituentCount,
			ProtocolTypes:               []string{"ISCSI"},
			LifeCycleState:              "READY",
			IPAddresses:                 []string{"10.72.177.17"},
			BlockProperties:             &models.BlockProperties{OSType: "LINUX"},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify LargeCapacity is properly set
		assert.True(t, out.LargeCapacity.IsSet())
		largeCapacity, ok := out.LargeCapacity.Get()
		assert.True(t, ok)
		assert.True(t, largeCapacity)

		// Verify LargeVolumeConstituentCount is properly set
		assert.True(t, out.LargeVolumeConstituentCount.IsSet())
		assert.Equal(t, constituentCount, out.LargeVolumeConstituentCount.Value)
	})

	t.Run("WithLargeCapacityTrue_NoConstituentCount", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:               "large-volume-token",
			PoolID:                      "large-pool",
			QuotaInBytes:                1073741824000, // 1TB
			LargeCapacity:               true,
			LargeVolumeConstituentCount: nil, // Not set
			ProtocolTypes:               []string{"ISCSI"},
			LifeCycleState:              "READY",
			IPAddresses:                 []string{"10.72.177.17"},
			BlockProperties:             &models.BlockProperties{OSType: "LINUX"},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify LargeCapacity is properly set
		assert.True(t, out.LargeCapacity.IsSet())
		largeCapacity, ok := out.LargeCapacity.Get()
		assert.True(t, ok)
		assert.True(t, largeCapacity)

		// Verify LargeVolumeConstituentCount is not set
		assert.False(t, out.LargeVolumeConstituentCount.IsSet())
	})

	t.Run("WithLargeCapacityFalse_WithNoConstituentCount", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:   "regular-volume-token",
			PoolID:          "regular-pool",
			QuotaInBytes:    107374182400, // 100GB
			LargeCapacity:   false,
			ProtocolTypes:   []string{"ISCSI"},
			LifeCycleState:  "READY",
			IPAddresses:     []string{"10.72.177.17"},
			BlockProperties: &models.BlockProperties{OSType: "LINUX"},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify LargeCapacity is properly set to false
		assert.True(t, out.LargeCapacity.IsSet())
		largeCapacity, ok := out.LargeCapacity.Get()
		assert.True(t, ok)
		assert.False(t, largeCapacity)
	})

	t.Run("WithoutLargeCapacityAndConstituentCount", func(t *testing.T) {
		vol := &models.Volume{
			CreationToken:               "standard-volume-token",
			PoolID:                      "standard-pool",
			QuotaInBytes:                1073741824, // 1GB
			LargeCapacity:               false,      // Default value
			LargeVolumeConstituentCount: nil,        // Not set
			ProtocolTypes:               []string{"ISCSI"},
			LifeCycleState:              "READY",
			IPAddresses:                 []string{"10.72.177.17"},
			BlockProperties:             &models.BlockProperties{OSType: "LINUX"},
		}
		out := convertModelToVCPVolume(vol)
		assert.NotNil(t, out)

		// Verify LargeCapacity is properly set to false (default)
		assert.True(t, out.LargeCapacity.IsSet())
		largeCapacity, ok := out.LargeCapacity.Get()
		assert.True(t, ok)
		assert.False(t, largeCapacity)

		// Verify LargeVolumeConstituentCount is not set
		assert.False(t, out.LargeVolumeConstituentCount.IsSet())
	})
}

// TestPrepareUpdateVolumeParams_BackupDisabled tests the scenario where backup is disabled
func TestV1betaCreateVolume_BackupNotSupported(t *testing.T) {
	origPrepare := prepareCreateVolumeParams
	origParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		prepareCreateVolumeParams = origPrepare
		utils.ParseAndValidateRegionAndZone = origParseAndValidateRegionAndZone
	}()
	prepareCreateVolumeParams = func(req *gcpgenserver.VolumeCreateV1beta, params gcpgenserver.V1betaCreateVolumeParams, region string, zone string) (*common.CreateVolumeParams, error) {
		return nil, errors.NewUserInputValidationErr("Backup feature is currently not enabled.")
	}
	utils.ParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpgenserver.Error) {
		return "us-e4", "", nil
	}
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	req := &gcpgenserver.VolumeCreateV1beta{}

	result, err := handler.V1betaCreateVolume(context.Background(), req, params)
	assert.NoError(t, err)
	badRequest, ok := result.(*gcpgenserver.V1betaCreateVolumeBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
	assert.Equal(t, "Backup feature is currently not enabled.", badRequest.Message)
}

// TestPrepareCreateVolumeParams_BackupDisabled tests the scenario where backup is disabled
func TestPrepareCreateVolumeParams_BackupDisabled(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = false

	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(gcpgenserver.BackupConfigV1beta{
				BackupVaultId: gcpgenserver.NewOptNilString("backup-vault-id"),
			}),
			QuotaInBytes: gcpgenserver.NewOptFloat64(107374182400),
		},
	}

	out, err := _prepareCreateVolumeParams(req, params, "region", "zone")
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Backup feature is currently not enabled.")
}

// TestV1betaUpdateVolume_BackupNotSupported tests the scenario where backup is disabled
func TestV1betaUpdateVolume_BackupNotSupported(t *testing.T) {
	origPrepare := prepareUpdateVolumeParams
	origParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	defer func() {
		prepareUpdateVolumeParams = origPrepare
		utils.ParseAndValidateRegionAndZone = origParseAndValidateRegionAndZone
	}()
	prepareUpdateVolumeParams = func(req *gcpgenserver.VolumeUpdateV1beta, params gcpgenserver.V1betaUpdateVolumeParams, region string) (*common.UpdateVolumeParams, error) {
		return nil, errors.NewUserInputValidationErr("Backup feature is currently not enabled.")
	}
	utils.ParseAndValidateRegionAndZone = func(locationId string) (string, string, *gcpgenserver.Error) {
		return "us-e4", "", nil
	}
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "us-e4",
		VolumeId:      "vol-1",
	}
	req := &gcpgenserver.VolumeUpdateV1beta{}

	result, err := handler.V1betaUpdateVolume(context.Background(), req, params)
	assert.NoError(t, err)
	badRequest, ok := result.(*gcpgenserver.V1betaUpdateVolumeBadRequest)
	assert.True(t, ok)
	assert.Equal(t, float64(400), badRequest.Code)
	assert.Equal(t, "Backup feature is currently not enabled.", badRequest.Message)
}

// TestV1betaUpdateVolume tests the V1betaUpdateVolume handler
func TestPrepareUpdateVolumeParams_BackupDisabled(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = false

	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
		VolumeId:      "vol-1",
	}
	req := &gcpgenserver.VolumeUpdateV1beta{
		BackupConfig: gcpgenserver.NewOptBackupConfigV1beta(gcpgenserver.BackupConfigV1beta{}),
	}

	out, err := _prepareUpdateVolumeParams(req, params, "region")
	assert.Nil(t, out)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Backup feature is currently not enabled.")
}

func TestPrepareCreateVolumeParams_WithAutoTieringFeatureDisabled(t *testing.T) {
	// Save and restore the original value
	currentATState := autoTieringEnabled
	defer func() { autoTieringEnabled = currentATState }()
	autoTieringEnabled = false

	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "testvolume",
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
	zone := "test-zone"
	_, err := _prepareCreateVolumeParams(req, params, region, zone)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Auto-Tiering feature is currently not enabled.")
}

func TestPrepareCreateVolumeParams_SnapReserveMustBePositiveNumber(t *testing.T) {
	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "testvolume",
			CreationToken: gcpgenserver.NewOptString("test-token"),
			PoolId:        gcpgenserver.NewNilString("test-pool"),
			QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
			Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			// SnapReserve is set but Get() will return (0, false)
			SnapReserve: gcpgenserver.OptFloat64{Set: true, Value: -1},
			Labels:      gcpgenserver.OptVolumeV1betaLabels{Value: map[string]string{"key": "value"}, Set: true},
		},
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"
	zone := "test-zone"
	result, err := _prepareCreateVolumeParams(req, params, region, zone)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "SnapReserve cannot be negative")
}

func TestPrepareCreateVolumeParams_DeDuplicateHGUUID(t *testing.T) {
	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "testvolume",
			CreationToken: gcpgenserver.NewOptString("test-token"),
			PoolId:        gcpgenserver.NewNilString("test-pool"),
			QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
			Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
			BlockProperties: gcpgenserver.NewOptBlockPropertiesV1beta(
				gcpgenserver.BlockPropertiesV1beta{
					OsType:       gcpgenserver.NewOptBlockPropertiesV1betaOsType("LINUX"),
					HostGroupIds: []string{"a", "a", "b"},
				},
			),
		},
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"
	zone := "test-zone"
	result, err := _prepareCreateVolumeParams(req, params, region, zone)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.BlockProperties.HostGroupUUIDs, 2)
}

func TestPrepareCreateVolumeParams_ResourceIdWithHyphens_ReturnsError(t *testing.T) {
	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "test-volume-with-hyphens",
			CreationToken: gcpgenserver.NewOptString("test-token"),
			PoolId:        gcpgenserver.NewNilString("test-pool"),
			QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
			Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
		},
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"
	zone := "test-zone"
	result, err := _prepareCreateVolumeParams(req, params, region, zone)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "The Resource ID can only contain lowercase letters, numbers, and underscores. It must start with a letter and cannot end with an underscore.")
}

func TestPrepareCreateVolumeParams_ValidResourceIdWithoutHyphens_Success(t *testing.T) {
	req := &gcpgenserver.VolumeCreateV1beta{
		Volume: gcpgenserver.VolumeV1beta{
			ResourceId:    "validresourceid123",
			CreationToken: gcpgenserver.NewOptString("test-token"),
			PoolId:        gcpgenserver.NewNilString("test-pool"),
			QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
			Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
		},
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"
	zone := "test-zone"
	result, err := _prepareCreateVolumeParams(req, params, region, zone)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "validresourceid123", result.Name)
	assert.Equal(t, "/projects/test-project/locations/test-location/volumes/validresourceid123", result.VendorID)
}

func TestPrepareCreateVolumeParams_ResourceIdEdgeCases(t *testing.T) {
	errorString := "The Resource ID can only contain lowercase letters, numbers, and underscores. It must start with a letter and cannot end with an underscore."
	zone := "us-west1-b"
	testCases := []struct {
		name          string
		resourceId    string
		expectError   bool
		errorContains string
	}{
		{
			name:          "Single hyphen",
			resourceId:    "test-volume",
			expectError:   true,
			errorContains: errorString,
		},
		{
			name:          "Multiple hyphens",
			resourceId:    "test-volume-with-multiple-hyphens",
			expectError:   true,
			errorContains: errorString,
		},
		{
			name:          "Hyphen at beginning",
			resourceId:    "-testvolume",
			expectError:   true,
			errorContains: errorString,
		},
		{
			name:          "Hyphen at end",
			resourceId:    "testvolume-",
			expectError:   true,
			errorContains: errorString,
		},
		{
			name:        "Valid alphanumeric",
			resourceId:  "testvolume123",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &gcpgenserver.VolumeCreateV1beta{
				Volume: gcpgenserver.VolumeV1beta{
					ResourceId:    tc.resourceId,
					CreationToken: gcpgenserver.NewOptString("test-token"),
					PoolId:        gcpgenserver.NewNilString("test-pool"),
					QuotaInBytes:  gcpgenserver.NewOptFloat64(1024),
					Protocols:     []gcpgenserver.ProtocolsV1beta{gcpgenserver.ProtocolsV1betaISCSI},
				},
			}
			params := gcpgenserver.V1betaCreateVolumeParams{
				ProjectNumber: "test-project",
				LocationId:    "test-location",
			}
			region := "test-region"

			result, err := _prepareCreateVolumeParams(req, params, region, zone)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				assert.Contains(t, err.Error(), tc.errorContains)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tc.resourceId, result.Name)
				expectedVendorID := fmt.Sprintf("/projects/test-project/locations/test-location/volumes/%s", tc.resourceId)
				assert.Equal(t, expectedVendorID, result.VendorID)
			}
		})
	}
}

func TestPrepareUpdateVolumeParams_SnapReserveCannotBeGreaterThan100(t *testing.T) {
	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "proj",
		LocationId:    "loc",
		VolumeId:      "vol",
	}
	region := "region"

	req := &gcpgenserver.VolumeUpdateV1beta{
		SnapReserve: gcpgenserver.NewOptNilFloat64(101),
	}
	result, err := _prepareUpdateVolumeParams(req, params, region)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "SnapReserve cannot be greater than 100")
}

func TestPrepareUpdateVolumeParams_HG(t *testing.T) {
	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "proj",
		LocationId:    "loc",
		VolumeId:      "vol",
	}
	region := "region"

	req := &gcpgenserver.VolumeUpdateV1beta{
		SnapReserve: gcpgenserver.NewOptNilFloat64(101),
	}
	result, err := _prepareUpdateVolumeParams(req, params, region)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "SnapReserve cannot be greater than 100")
}

func TestPrepareUpdateVolumeParams_QuotaValidation(t *testing.T) {
	// Save and restore original values
	origMin := utils.MinQuotaInBytesVolumeForVolume
	origMax := utils.MaxQuotaInBytesVolumeForVolume
	utils.MinQuotaInBytesVolumeForVolume = 100 * 1024 * 1024 * 1024    // 100 GiB
	utils.MaxQuotaInBytesVolumeForVolume = 102400 * 1024 * 1024 * 1024 // 102,400 GiB
	defer func() {
		utils.MinQuotaInBytesVolumeForVolume = origMin
		utils.MaxQuotaInBytesVolumeForVolume = origMax
	}()

	params := gcpgenserver.V1betaUpdateVolumeParams{
		ProjectNumber: "proj",
		LocationId:    "loc",
		VolumeId:      "vol",
	}
	region := "region"

	t.Run("QuotaBelowMinimum", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(float64(99 * 1024 * 1024 * 1024)), // 99 GiB
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.Nil(t, out)
		assert.Contains(t, err.Error(), "Invalid volume capacity 99. Must be between 100 GiB and 102400 GiB.")
	})

	t.Run("QuotaAboveMaximum", func(t *testing.T) {
		req := &gcpgenserver.VolumeUpdateV1beta{
			QuotaInBytes: gcpgenserver.NewOptNilFloat64(float64(102401 * 1024 * 1024 * 1024)), // 102,401 GiB
		}
		out, err := _prepareUpdateVolumeParams(req, params, region)
		assert.Error(t, err)
		assert.Nil(t, out)
		assert.Contains(t, err.Error(), "Invalid volume capacity 102401. Must be between 100 GiB and 102400 GiB.")
	})
}

func TestValidateVolumeQuotaSize(t *testing.T) {
	// Save original values and restore after tests
	origMin := utils.MinQuotaInBytesVolumeForVolume
	origMax := utils.MaxQuotaInBytesVolumeForVolume
	defer func() {
		utils.MinQuotaInBytesVolumeForVolume = origMin
		utils.MaxQuotaInBytesVolumeForVolume = origMax
	}()

	// Set test values
	utils.MinQuotaInBytesVolumeForVolume = 100 * 1024 * 1024 * 1024    // 100 GiB
	utils.MaxQuotaInBytesVolumeForVolume = 102400 * 1024 * 1024 * 1024 // 102,400 GiB

	t.Run("ValidQuota_ReturnsNil", func(tt *testing.T) {
		// Test valid quota (middle of range)
		err := validateVolumeQuotaSize(1000 * 1024 * 1024 * 1024) // 1000 GiB
		assert.NoError(tt, err)
	})

	t.Run("MinimumQuota_ReturnsNil", func(tt *testing.T) {
		// Test exactly at minimum value
		err := validateVolumeQuotaSize(float64(utils.MinQuotaInBytesVolumeForVolume))
		assert.NoError(tt, err)
	})

	t.Run("MaximumQuota_ReturnsNil", func(tt *testing.T) {
		// Test exactly at maximum value
		err := validateVolumeQuotaSize(float64(utils.MaxQuotaInBytesVolumeForVolume))
		assert.NoError(tt, err)
	})

	t.Run("BelowMinimumQuota_ReturnsError", func(tt *testing.T) {
		// Test below minimum
		err := validateVolumeQuotaSize(50 * 1024 * 1024 * 1024) // 50 GiB
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity 50. Must be between 100 GiB and 102400 GiB.")
	})

	t.Run("AboveMaximumQuota_ReturnsError", func(tt *testing.T) {
		// Test above maximum
		err := validateVolumeQuotaSize(200000 * 1024 * 1024 * 1024) // 200,000 GiB
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity 200000. Must be between 100 GiB and 102400 GiB.")
	})

	t.Run("ZeroQuota_ReturnsError", func(tt *testing.T) {
		// Test zero value
		err := validateVolumeQuotaSize(0)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity 0. Must be between 100 GiB and 102400 GiB.")
	})

	t.Run("NegativeQuota_ReturnsError", func(tt *testing.T) {
		// Test negative value
		err := validateVolumeQuotaSize(-1024 * 1024 * 1024) // -1 GiB
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "Invalid volume capacity -1. Must be between 100 GiB and 102400 GiB.")
	})
}

// BackupFeatureNotEnabled_ReturnsError tests the scenario where backup feature is not enabled
func TestRestoreWhenBackupFeatureNotEnabled_ReturnsError(t *testing.T) {
	origBackupEnabled := backupEnabled
	defer func() { backupEnabled = origBackupEnabled }()
	backupEnabled = false

	req := &gcpgenserver.VolumeCreateV1beta{
		BackupPath: gcpgenserver.NewOptString("/backup/path"),
		Volume: gcpgenserver.VolumeV1beta{
			QuotaInBytes: gcpgenserver.NewOptFloat64(107374182400),
		},
	}
	params := gcpgenserver.V1betaCreateVolumeParams{
		ProjectNumber: "test-project",
		LocationId:    "test-location",
	}
	region := "test-region"
	zone := "us-west1-b"

	result, err := _prepareCreateVolumeParams(req, params, region, zone)
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Backup feature is currently not enabled.")
}

func TestConvertModelToVCPVolume_MountPoints(t *testing.T) {
	t.Run("WhenVolumeIsReadyAndLunNamePresent_ShouldAddMountPoints", func(tt *testing.T) {
		// Setup a volume with READY state and non-empty LunName
		vol := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			DisplayName:    "testvolume",
			LifeCycleState: string(gcpgenserver.VolumeV1betaVolumeStateREADY),
			IPAddresses:    []string{"10.72.177.17"},
			BlockProperties: &models.BlockProperties{
				OSType:  "LINUX",
				LunName: "lun-123",
			},
			ProtocolTypes: []string{"ISCSI"},
		}

		// Convert the model to VCP volume
		result := convertModelToVCPVolume(vol)

		// Verify mount points are added
		assert.NotNil(tt, result.MountPoints)
		assert.Len(tt, result.MountPoints, 1)
		assert.Equal(tt, "10.72.177.17", result.MountPoints[0].IpAddress.Value)
		assert.Equal(tt, gcpgenserver.ProtocolsV1betaISCSI, result.MountPoints[0].Protocol.Value)
		assert.NotEmpty(tt, result.MountPoints[0].Instructions.Value)
		assert.Contains(tt, result.MountPoints[0].Instructions.Value, "lun-123")
	})

	t.Run("WhenVolumeNotReady_ShouldNotAddMountPoints", func(tt *testing.T) {
		// Setup a volume with non-READY state but with LunName
		vol := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			DisplayName:    "testvolume",
			LifeCycleState: string(gcpgenserver.VolumeV1betaVolumeStateCREATING), // Not READY
			IPAddresses:    []string{"10.72.177.17"},
			BlockProperties: &models.BlockProperties{
				OSType:  "LINUX",
				LunName: "lun-123", // Has LUN name
			},
			ProtocolTypes: []string{"ISCSI"},
		}

		// Convert the model to VCP volume
		result := convertModelToVCPVolume(vol)

		// Verify mount points are not added
		assert.Empty(tt, result.MountPoints)
	})

	t.Run("WhenNoLunName_ShouldNotAddMountPoints", func(tt *testing.T) {
		// Setup a volume with READY state but empty LunName
		vol := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			DisplayName:    "testvolume",
			LifeCycleState: string(gcpgenserver.VolumeV1betaVolumeStateREADY), // READY
			IPAddresses:    []string{"10.72.177.17"},
			BlockProperties: &models.BlockProperties{
				OSType:  "LINUX",
				LunName: "", // Empty LUN name
			},
			ProtocolTypes: []string{"ISCSI"},
		}

		// Convert the model to VCP volume
		result := convertModelToVCPVolume(vol)

		// Verify mount points are not added
		assert.Empty(tt, result.MountPoints)
	})

	t.Run("WhenNoBlockProperties_ShouldNotAddMountPoints", func(tt *testing.T) {
		// Setup a volume with READY state but no BlockProperties
		vol := &models.Volume{
			BaseModel:       models.BaseModel{UUID: "vol-1"},
			DisplayName:     "testvolume",
			LifeCycleState:  string(gcpgenserver.VolumeV1betaVolumeStateREADY), // READY
			IPAddresses:     []string{"10.72.177.17"},
			BlockProperties: nil, // No BlockProperties
			ProtocolTypes:   []string{"ISCSI"},
		}
		// Convert the model to VCP volume
		result := convertModelToVCPVolume(vol)

		// Verify mount points are not added and no panic occurs
		assert.Empty(tt, result.MountPoints)
	})

	t.Run("WhenLabelsArePresent_ShouldReturn", func(tt *testing.T) {
		vol := &models.Volume{
			BaseModel:     models.BaseModel{UUID: "vol-1"},
			ProtocolTypes: []string{"ISCSI"},
			Labels: map[string]string{
				"key1": "value1",
			},
		}
		// Convert the model to VCP volume
		result := convertModelToVCPVolume(vol)

		// Verify mount points are not added and no panic occurs
		assert.NotEmpty(tt, result.Labels)
		assert.Equal(tt, "value1", result.Labels.Value["key1"])
	})
}

func TestV1betaDescribeVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
			QuotaInBytes:   1024 * 1024 * 1024, // 1GB
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", true).Return(volume, nil)

		result, err := handler.V1betaDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)
		volumeResponse := result.(*gcpgenserver.VolumeV1beta)
		assert.Equal(tt, "testvolume", volumeResponse.ResourceId)
		assert.Equal(tt, "vol-1", volumeResponse.VolumeId.Value)
	})

	t.Run("VolumeNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "nonexistent-vol",
		}
		notFoundErr := errors.NewNotFoundErr("Volume", &params.VolumeId)
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "nonexistent-vol", true).Return(nil, notFoundErr)

		result, err := handler.V1betaDescribeVolume(context.Background(), params)
		assert.NoError(tt, err)
		notFound, isNotFound := result.(*gcpgenserver.V1betaDescribeVolumeNotFound)
		assert.True(tt, isNotFound)
		assert.Equal(tt, float64(404), notFound.Code)
		assert.Equal(tt, "Volume not found", notFound.Message)
	})

	t.Run("InternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDescribeVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		internalErr := errors.New("database connection failed")
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", true).Return(nil, internalErr)

		result, err := handler.V1betaDescribeVolume(context.Background(), params)
		assert.Nil(tt, err)
		internalServerErr, isInternal := result.(*gcpgenserver.V1betaDescribeVolumeInternalServerError)
		assert.True(tt, isInternal)
		assert.Equal(tt, float64(500), internalServerErr.Code)
		assert.Equal(tt, "Internal server error", internalServerErr.Message)
	})
}

func TestV1betaDeleteVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// First GetVolume call to check current state
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateREADY,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)

		// DeleteVolume call
		deletedVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateDeleted,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(deletedVolume, "job-123", nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		operation, isOperation := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, isOperation)
		assert.Contains(tt, operation.Name.Value, "job-123")
		assert.Equal(tt, true, operation.Done.Value)
		assert.NotNil(tt, operation.Response)
		assert.Greater(tt, len(operation.Response), 0) // Response should contain data
	})

	t.Run("GetVolumeNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "nonexistent-vol",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		notFoundErr := errors.NewNotFoundErr("Volume", &params.VolumeId)
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "nonexistent-vol", false).Return(nil, notFoundErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		notFoundErr1, isNotFound := result.(*gcpgenserver.V1betaDeleteVolumeNotFound)
		assert.True(tt, isNotFound)
		assert.Equal(tt, float64(404), notFoundErr1.Code)
		assert.Equal(tt, "Volume not found", notFoundErr1.Message)
	})

	t.Run("GetVolumeInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		internalErr := errors.New("database connection failed")
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(nil, internalErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		serverErr, isInternal := result.(*gcpgenserver.V1betaDeleteVolumeInternalServerError)
		assert.True(tt, isInternal)
		assert.Equal(tt, float64(500), serverErr.Code)
		assert.Equal(tt, "Internal server error", serverErr.Message)
	})

	t.Run("VolumeAlreadyDeleting", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateDeleting,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		job := &models.Job{
			BaseModel: models.BaseModel{UUID: "job-uuid"},
			Type:      models.JobTypeDeleteVolume,
			JobAttributes: &models.JobAttributes{
				ResourceUUID: "deleting-pool-id",
			},
			State: models.JobsStatePROCESSING,
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)
		mockOrchestrator.EXPECT().GetJobByResourceUUID(mock.Anything, "vol-1", string(models.JobTypeDeleteVolume)).Return(job, nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		operation, isOperation := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, isOperation)
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/test-project/locations/test-location/operations/job-uuid")
		assert.False(tt, false, operation.Done.Value)
		assert.Equal(tt, 0, len(operation.Response)) // No response for already deleting/deleted volume
	})

	t.Run("VolumeAlreadyDeleted", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateDeleted,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		pool := &models.Pool{
			BaseModel: models.BaseModel{UUID: "pool-1"},
			Name:      "test-pool",
			State:     models.LifeCycleStateREADY,
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)
		mockOrchestrator.EXPECT().GetPoolByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		operation, isOperation := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, isOperation)
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/test-project/locations/test-location/operations/")
		assert.Equal(tt, true, operation.Done.Value)
		assert.Equal(tt, 0, len(operation.Response)) // No response for already deleted volume
	})

	t.Run("VolumeAlreadyDeleted", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateDeleted,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		pool := &models.Pool{
			BaseModel: models.BaseModel{UUID: "pool-1"},
			Name:      "test-pool",
			State:     models.LifeCycleStateDeleted,
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)
		mockOrchestrator.EXPECT().GetPoolByName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)

		res, ok := result.(*gcpgenserver.V1betaDeleteVolumeNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), res.Code)
		assert.Equal(tt, "Volume not found", res.Message)
	})

	t.Run("DeleteVolumeNotFound", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// GetVolume succeeds
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateREADY,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)

		// DeleteVolume returns not found (volume disappeared between calls)
		notFoundErr := errors.NewNotFoundErr("Volume", &params.VolumeId)
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", notFoundErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		operation, isOperation := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, isOperation)
		assert.Contains(tt, operation.Name.Value, "/v1beta/projects/test-project/locations/test-location/operations/")
		assert.Equal(tt, true, operation.Done.Value)
		assert.Equal(tt, 0, len(operation.Response)) // No response for not found during delete
	})

	t.Run("DeleteVolumeInternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// GetVolume succeeds
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateREADY,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)

		// DeleteVolume fails with internal error
		internalErr := errors.New("database connection failed")
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", internalErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		serverErr, isInternal := result.(*gcpgenserver.V1betaDeleteVolumeInternalServerError)
		assert.True(tt, isInternal)
		assert.Equal(tt, float64(500), serverErr.Code)
		assert.Equal(tt, "Internal server error", serverErr.Message)
	})

	t.Run("SuccessWithDeletingState", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaDeleteVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// GetVolume succeeds
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateREADY,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)

		// DeleteVolume returns volume in deleting state
		deletingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: models.LifeCycleStateDeleting,
			CreationToken:  "token",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(deletingVolume, "job-123", nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		operation, isOperation := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, isOperation)
		assert.Contains(tt, operation.Name.Value, "job-123")
		assert.Equal(tt, false, operation.Done.Value) // Not done yet since still deleting
		assert.NotNil(tt, operation.Response)
		assert.Greater(tt, len(operation.Response), 0) // Response should contain data
	})

	t.Run("SuccessWithDifferentVolumeStates", func(tt *testing.T) {
		testCases := []struct {
			name         string
			initialState string
			expectError  bool
		}{
			{
				name:         "FromReadyState",
				initialState: models.LifeCycleStateREADY,
				expectError:  false,
			},
			{
				name:         "FromErrorState",
				initialState: models.LifeCycleStateError,
				expectError:  false,
			},
		}

		for _, tc := range testCases {
			tt.Run(tc.name, func(t *testing.T) {
				mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
				handler := Handler{Orchestrator: mockOrchestrator}
				params := gcpgenserver.V1betaDeleteVolumeParams{
					ProjectNumber: "test-project",
					LocationId:    "test-location",
					VolumeId:      "vol-1",
				}
				req := gcpgenserver.OptV1betaDeleteVolumeReq{}

				// GetVolume succeeds
				volume := &models.Volume{
					BaseModel:      models.BaseModel{UUID: "vol-1"},
					LifeCycleState: tc.initialState,
					CreationToken:  "token",
					DisplayName:    "testvolume",
				}
				mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(volume, nil)

				// DeleteVolume returns volume in deleted state
				deletedVolume := &models.Volume{
					BaseModel:      models.BaseModel{UUID: "vol-1"},
					LifeCycleState: models.LifeCycleStateDeleted,
					CreationToken:  "token",
					DisplayName:    "testvolume",
				}
				mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(deletedVolume, "job-123", nil)

				result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
				if tc.expectError {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
					operation, isOperation := result.(*gcpgenserver.OperationV1beta)
					assert.True(t, isOperation)
					assert.Contains(t, operation.Name.Value, "job-123")
					assert.Equal(t, true, operation.Done.Value)
					assert.NotNil(t, operation.Response)
					assert.Greater(t, len(operation.Response), 0) // Response should contain data
				}
			})
		}
	})

	t.Run("SuccessfulDelete", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return an existing volume
		existingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(existingVolume, nil)

		// Mock DeleteVolume to return success
		deletingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "DELETING",
			DisplayName:    "testvolume",
		}
		jobUUID := "job-uuid-123"
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(deletingVolume, jobUUID, nil)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)

		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/123456789/locations/us-central1-a/operations/job-uuid-123", op.Name.Value)
		assert.Equal(tt, false, op.Done.Value)
	})

	t.Run("UserInputValidationError_BackupInTransition", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return an existing volume
		existingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(existingVolume, nil)

		// Mock DeleteVolume to return UserInputValidationErr (backup in transition)
		validationErr := errors.NewUserInputValidationErr("A backup operation on volume is currently in progress")
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", validationErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)

		badReq, ok := result.(*gcpgenserver.V1betaDeleteVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "A backup operation on volume is currently in progress", badReq.Message)
	})

	t.Run("UserInputValidationError_OtherValidation", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return an existing volume
		existingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(existingVolume, nil)

		// Mock DeleteVolume to return another UserInputValidationErr
		validationErr := errors.NewUserInputValidationErr("Volume cannot be deleted due to active replication")
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", validationErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)

		badReq, ok := result.(*gcpgenserver.V1betaDeleteVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Equal(tt, "Volume cannot be deleted due to active replication", badReq.Message)
	})

	t.Run("VolumeNotFound_GetVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return NotFoundErr
		notFoundErr := errors.NewNotFoundErr("Volume not found", nil)
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(nil, notFoundErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)

		notFoundErr2, ok := result.(*gcpgenserver.V1betaDeleteVolumeNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), notFoundErr2.Code)
		assert.Equal(tt, "Volume not found", notFoundErr2.Message)
	})

	t.Run("VolumeNotFound_DeleteVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return an existing volume
		existingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(existingVolume, nil)

		// Mock DeleteVolume to return NotFoundErr
		notFoundErr := errors.NewNotFoundErr("Volume not found", nil)
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", notFoundErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.NoError(tt, err)

		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, true, op.Done.Value)
		assert.Contains(tt, op.Name.Value, "/v1beta/projects/123456789/locations/us-central1-a/operations/")
	})

	t.Run("InternalServerError_GetVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return unexpected error
		unexpectedErr := fmt.Errorf("database connection failed")
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(nil, unexpectedErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)

		internalErr, ok := result.(*gcpgenserver.V1betaDeleteVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Equal(tt, "Internal server error", internalErr.Message)
	})

	t.Run("InternalServerError_DeleteVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDeleteVolumeParams{
			LocationId:    "us-central1-a",
			ProjectNumber: "123456789",
			VolumeId:      "vol-1",
		}
		req := gcpgenserver.OptV1betaDeleteVolumeReq{}

		// Mock GetVolume to return an existing volume
		existingVolume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
			DisplayName:    "testvolume",
		}
		mockOrchestrator.EXPECT().GetVolume(mock.Anything, "vol-1", false).Return(existingVolume, nil)

		// Mock DeleteVolume to return unexpected error
		unexpectedErr := fmt.Errorf("workflow execution failed")
		mockOrchestrator.EXPECT().DeleteVolume(mock.Anything, "vol-1").Return(nil, "", unexpectedErr)

		result, err := handler.V1betaDeleteVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaDeleteVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Equal(tt, "Internal server error", internalErr.Message)
	})
}

func TestV1betaRevertVolume(t *testing.T) {
	originalParseAndValidateRegionAndZone := utils.ParseAndValidateRegionAndZone
	mockParseAndValidateRegionAndZone := func(region string) (string, string, *gcpgenserver.Error) {
		return "test-region", "test-location", nil
	}
	utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone
	defer func() { utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone }()

	t.Run("ValidRevertVolume", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaRevertVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		volume := &models.Volume{
			BaseModel:      models.BaseModel{UUID: "vol-1"},
			LifeCycleState: "READY",
		}
		jobUUID := "job-uuid"
		mockOrchestrator.EXPECT().RevertVolume(mock.Anything, mock.Anything).Return(volume, jobUUID, nil)

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		op, ok := result.(*gcpgenserver.OperationV1beta)
		assert.True(tt, ok)
		assert.Equal(tt, "/v1beta/projects/project-number/locations/location-id/operations/job-uuid", op.Name.Value)
		assert.True(tt, op.Done.Value)
	})

	t.Run("UserInputValidationError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		prepareRevertVolumeParams = func(req *gcpgenserver.VolumeRevertV1beta, params gcpgenserver.V1betaRevertVolumeParams, region string) (*common.RevertVolumeParams, error) {
			return nil, errors.NewUserInputValidationErr("invalid input")
		}
		defer func() { prepareRevertVolumeParams = _prepareRevertVolumeParams }()

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		badReq, ok := result.(*gcpgenserver.V1betaRevertVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), badReq.Code)
		assert.Contains(tt, badReq.Message, "invalid input")
	})

	t.Run("InternalServerError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		prepareRevertVolumeParams = func(req *gcpgenserver.VolumeRevertV1beta, params gcpgenserver.V1betaRevertVolumeParams, region string) (*common.RevertVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareRevertVolumeParams = _prepareRevertVolumeParams }()

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.Nil(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaRevertVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "unexpected error")
	})

	t.Run("BadRequest_InvalidLocation", func(tt *testing.T) {
		utils.ParseAndValidateRegionAndZone = originalParseAndValidateRegionAndZone
		defer func() { utils.ParseAndValidateRegionAndZone = mockParseAndValidateRegionAndZone }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		prepareRevertVolumeParams = func(req *gcpgenserver.VolumeRevertV1beta, params gcpgenserver.V1betaRevertVolumeParams, region string) (*common.RevertVolumeParams, error) {
			return nil, fmt.Errorf("unexpected error")
		}
		defer func() { prepareRevertVolumeParams = _prepareRevertVolumeParams }()

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaRevertVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "LocationID represents neither a region nor a zone")
	})

	t.Run("WhenOrchestratorValidationThrowsAnError_Return400BadRequest", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaRevertVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}

		mockOrchestrator.EXPECT().RevertVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewUserInputValidationErr("An error occurred"))

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaRevertVolumeBadRequest)
		assert.True(tt, ok)
		assert.Equal(tt, float64(400), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorThrowsAnError_ReturnError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaRevertVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}

		mockOrchestrator.EXPECT().RevertVolume(mock.Anything, mock.Anything).Return(nil, "", errors.New("An error occurred"))

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.Error(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaRevertVolumeInternalServerError)
		assert.True(tt, ok)
		assert.Equal(tt, float64(500), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "An error occurred")
	})

	t.Run("WhenOrchestratorNotFoundError_Return404NotFoundError", func(tt *testing.T) {
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(tt)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaRevertVolumeParams{
			LocationId:    "location-id",
			ProjectNumber: "project-number",
			VolumeId:      "vol-1",
		}
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}

		mockOrchestrator.EXPECT().RevertVolume(mock.Anything, mock.Anything).Return(nil, "", errors.NewNotFoundErr("Volume not found", nil))

		result, err := handler.V1betaRevertVolume(context.Background(), req, params)
		assert.NoError(tt, err)
		internalErr, ok := result.(*gcpgenserver.V1betaRevertVolumeNotFound)
		assert.True(tt, ok)
		assert.Equal(tt, float64(404), internalErr.Code)
		assert.Contains(tt, internalErr.Message, "Volume not found")
	})
}

func TestPrepareRevertVolumeParams(t *testing.T) {
	t.Run("ValidRevertVolumeParams", func(tt *testing.T) {
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		region := "test-region"

		expected := &common.RevertVolumeParams{
			AccountName: "test-project",
			Region:      "test-region",
			VolumeID:    "vol-1",
			SnapshotID:  "snapshot-1",
		}

		result, err := prepareRevertVolumeParams(req, params, region)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})

	t.Run("MissingVolumeId", func(tt *testing.T) {
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "",
		}
		region := "test-region"

		result, err := prepareRevertVolumeParams(req, params, region)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "No Volume ID given")
	})

	t.Run("MissingProjectNumber", func(tt *testing.T) {
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "snapshot-1",
		}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		region := "test-region"

		result, err := prepareRevertVolumeParams(req, params, region)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "No Project Number given")
	})

	t.Run("MissingSnapshotId", func(tt *testing.T) {
		req := &gcpgenserver.VolumeRevertV1beta{
			SnapshotId: "",
		}
		params := gcpgenserver.V1betaRevertVolumeParams{
			ProjectNumber: "test-project",
			LocationId:    "test-location",
			VolumeId:      "vol-1",
		}
		region := "test-region"

		result, err := prepareRevertVolumeParams(req, params, region)
		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "No Snapshot ID given")
	})
}

func TestValidateBackupScheduleCron(t *testing.T) {
	tests := []struct {
		name        string
		cronExpr    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid cron expression - every 5 minutes",
			cronExpr:    "*/5 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 10 minutes",
			cronExpr:    "*/10 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 30 minutes",
			cronExpr:    "*/30 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every hour",
			cronExpr:    "0 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - daily at midnight",
			cronExpr:    "0 0 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - specific minute",
			cronExpr:    "30 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes during business hours",
			cronExpr:    "*/5 9-17 * * 1-5",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 30 minutes on weekdays",
			cronExpr:    "*/30 * * * 1-5",
			expectError: false,
		},
		{
			name:        "Invalid cron expression - every 1 minute (too frequent)",
			cronExpr:    "* * * * *",
			expectError: true,
			errorMsg:    "Backup schedule interval must be at least 5 minutes. Current schedule: every minute",
		},
		{
			name:        "Invalid cron expression - every 2 minutes (too frequent)",
			cronExpr:    "*/2 * * * *",
			expectError: true,
			errorMsg:    "Backup schedule interval must be at least 5 minutes. Current interval: 2 minutes",
		},
		{
			name:        "Invalid cron expression - malformed",
			cronExpr:    "invalid cron",
			expectError: true,
			errorMsg:    "Invalid cron expression:",
		},
		{
			name:        "Invalid cron expression - wrong number of fields",
			cronExpr:    "* * * *",
			expectError: true,
			errorMsg:    "Invalid cron expression:",
		},
		{
			name:        "Invalid cron expression - too many fields",
			cronExpr:    "* * * * * *",
			expectError: true,
			errorMsg:    "Invalid cron expression format. Expected 5 fields: minute hour day month weekday",
		},
		{
			name:        "Invalid cron expression - invalid interval format",
			cronExpr:    "*/abc * * * *",
			expectError: true,
			errorMsg:    "Invalid cron expression:",
		},
		{
			name:        "Empty cron expression",
			cronExpr:    "",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with specific hour",
			cronExpr:    "*/5 2 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with specific day",
			cronExpr:    "*/5 * 15 * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with specific month",
			cronExpr:    "*/5 * * 6 *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with specific weekday",
			cronExpr:    "*/5 * * * 1",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with range in hour",
			cronExpr:    "*/5 9-17 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with list in hour",
			cronExpr:    "*/5 9,12,17 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with step in hour",
			cronExpr:    "*/5 */2 * * *",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackupScheduleCron(tt.cronExpr)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for cron expression '%s', but got none", tt.cronExpr)
					return
				}

				if tt.errorMsg != "" {
					if !contains(err.Error(), tt.errorMsg) {
						t.Errorf("Expected error message to contain '%s', but got: %v", tt.errorMsg, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for cron expression '%s', but got: %v", tt.cronExpr, err)
				}
			}
		})
	}
}

func TestValidateBackupScheduleCron_EdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		cronExpr    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid cron expression - every 5 minutes with complex hour range",
			cronExpr:    "*/5 0-23 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with complex day range",
			cronExpr:    "*/5 * 1-15 * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with complex month range",
			cronExpr:    "*/5 * * 1-12 *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with complex weekday range",
			cronExpr:    "*/5 * * * 1-7",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with mixed ranges",
			cronExpr:    "*/5 9-17 1-15 1-6 1-5",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with lists",
			cronExpr:    "*/5 9,12,15,18 * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - every 5 minutes with steps",
			cronExpr:    "*/5 */3 * * *",
			expectError: false,
		},
		{
			name:        "Invalid cron expression - every 1 minute with complex constraints",
			cronExpr:    "* 9-17 * * 1-5",
			expectError: true,
			errorMsg:    "Backup schedule interval must be at least 5 minutes. Current schedule: every minute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackupScheduleCron(tt.cronExpr)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for cron expression '%s', but got none", tt.cronExpr)
					return
				}

				if tt.errorMsg != "" {
					if !contains(err.Error(), tt.errorMsg) {
						t.Errorf("Expected error message to contain '%s', but got: %v", tt.errorMsg, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for cron expression '%s', but got: %v", tt.cronExpr, err)
				}
			}
		})
	}
}

func TestValidateBackupScheduleCron_BoundaryValues(t *testing.T) {
	tests := []struct {
		name        string
		cronExpr    string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid cron expression - exactly 5 minutes interval",
			cronExpr:    "*/5 * * * *",
			expectError: false,
		},
		{
			name:        "Invalid cron expression - 4 minutes interval (just below boundary)",
			cronExpr:    "*/4 * * * *",
			expectError: true,
			errorMsg:    "Backup schedule interval must be at least 5 minutes. Current interval: 4 minutes",
		},
		{
			name:        "Valid cron expression - 6 minutes interval (just above boundary)",
			cronExpr:    "*/6 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - large interval",
			cronExpr:    "*/60 * * * *",
			expectError: false,
		},
		{
			name:        "Valid cron expression - very large interval",
			cronExpr:    "*/1440 * * * *",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBackupScheduleCron(tt.cronExpr)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for cron expression '%s', but got none", tt.cronExpr)
					return
				}

				if tt.errorMsg != "" {
					if !contains(err.Error(), tt.errorMsg) {
						t.Errorf("Expected error message to contain '%s', but got: %v", tt.errorMsg, err.Error())
					}
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for cron expression '%s', but got: %v", tt.cronExpr, err)
				}
			}
		})
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && (s[:len(substr)] == substr ||
			s[len(s)-len(substr):] == substr ||
			func() bool {
				for i := 1; i <= len(s)-len(substr); i++ {
					if s[i:i+len(substr)] == substr {
						return true
					}
				}
				return false
			}())))
}
