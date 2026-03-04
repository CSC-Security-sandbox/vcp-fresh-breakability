package gcp

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

func TestConvertDatastoreVolumeToModel_InheritsDirectoryFields(t *testing.T) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "volume-name",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acct-uuid"},
			Name:      "acct",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-central1-a",
				LdapEnabled: true,
			},
			ActiveDirectory: &datamodel.ActiveDirectory{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
				AdName:    "ad-name",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:     "token",
			Protocols:         []string{"NFSV4"},
			VendorSubnetID:    "network",
			IsDataProtection:  false,
			SnapReserve:       0,
			SnapshotDirectory: true,
			KerberosEnabled:   true,
			LdapEnabled:       false,
		},
	}

	result := convertDatastoreVolumeToModel(volume, nil)

	assert.True(t, result.KerberosEnabled)
	assert.True(t, result.LdapEnabled)
	assert.Equal(t, "ad-uuid", result.ActiveDirectoryConfigId)
	assert.Equal(t, "projects/acct/locations/us-central1/activeDirectories/ad-name", result.ActiveDirectoryResourceId)
}

func TestConvertDatastoreVolumeToModel_ActiveDirectoryResourceIdParseErrorFallback(t *testing.T) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "volume-name",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acct-uuid"},
			Name:      "acct",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "invalid-zone-format", // forces ParseRegionAndZone error
				LdapEnabled: true,
			},
			ActiveDirectory: &datamodel.ActiveDirectory{
				BaseModel: datamodel.BaseModel{UUID: "ad-uuid"},
				AdName:    "ad-name",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:     "token",
			Protocols:         []string{"NFSV4"},
			VendorSubnetID:    "network",
			IsDataProtection:  false,
			SnapReserve:       0,
			SnapshotDirectory: true,
			KerberosEnabled:   true,
			LdapEnabled:       false,
		},
	}

	result := convertDatastoreVolumeToModel(volume, nil)

	assert.True(t, result.KerberosEnabled)
	assert.True(t, result.LdapEnabled)
	assert.Equal(t, "ad-uuid", result.ActiveDirectoryConfigId)
	// Falls back to raw AD name when region parsing fails
	assert.Equal(t, "ad-name", result.ActiveDirectoryResourceId)
}

func TestBuildFilePropertiesFromParams_DefaultUnixPermissions(t *testing.T) {
	params := &models.FileProperties{
		ExportPolicy:  &models.ExportPolicy{ExportPolicyName: "policy"},
		SecurityStyle: UnixSecurityStyle,
	}

	result := buildFilePropertiesFromParams(params, "ct")

	assert.NotNil(t, result)
	assert.Equal(t, DefaultUnixPermissionsOctal, result.UnixPermissions)
}

func TestBuildFilePropertiesFromParams_CustomUnixPermissions(t *testing.T) {
	params := &models.FileProperties{
		ExportPolicy:    &models.ExportPolicy{ExportPolicyName: "policy"},
		SecurityStyle:   UnixSecurityStyle,
		UnixPermissions: "0755",
	}

	result := buildFilePropertiesFromParams(params, "ct")

	assert.NotNil(t, result)
	assert.Equal(t, "0755", result.UnixPermissions)
}

func TestConvertDatastoreVolumeToModel_PreservesUnixPermissions(t *testing.T) {
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
		Name:      "volume-name",
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "acct-uuid"},
			Name:      "acct",
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "pool",
			PoolAttributes: &datamodel.PoolAttributes{
				PrimaryZone: "us-central1-a",
			},
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:     "token",
			Protocols:         []string{"NFSv4"},
			VendorSubnetID:    "network",
			SnapReserve:       0,
			SnapshotDirectory: true,
			FileProperties: &datamodel.FileProperties{
				JunctionPath:    "/junction",
				UnixPermissions: "0750",
			},
		},
	}

	result := convertDatastoreVolumeToModel(volume, nil)

	if assert.NotNil(t, result.FileProperties) {
		assert.Equal(t, "0750", result.FileProperties.UnixPermissions)
	}
}

func TestConvertDatastoreVolumeToModel_VolumePerformanceGroupId(t *testing.T) {
	minimalVolume := func(pool *datamodel.Pool, vpg *datamodel.VolumePerformanceGroup, vpgID sql.NullInt64) *datamodel.Volume {
		vol := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{UUID: "vol-uuid"},
			Name:      "vol-name",
			Account:   &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "acct-uuid"}, Name: "acct"},
			Pool:      pool,
			VolumeAttributes: &datamodel.VolumeAttributes{
				CreationToken:     "token",
				Protocols:         []string{"NFSV3"},
				VendorSubnetID:    "network",
				IsDataProtection:  false,
				SnapReserve:       0,
				SnapshotDirectory: true,
			},
		}
		if pool != nil && pool.PoolAttributes == nil {
			vol.Pool.PoolAttributes = &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"}
		}
		vol.VolumePerformanceGroupID = vpgID
		vol.VolumePerformanceGroup = vpg
		return vol
	}

	t.Run("ManualPool_NonAutogenVPG_ReturnsOnlyVPGUUID", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "pool",
			QosType:        utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "vpg-uuid-123"},
			IsAutoGen:       false,
			ThroughputMibps: 100,
			Iops:            500,
		}
		volume := minimalVolume(pool, vpg, sql.NullInt64{Int64: 1, Valid: true})
		result := convertDatastoreVolumeToModel(volume, nil)
		require.NotNil(t, result)
		assert.Equal(t, "vpg-uuid-123", result.VolumePerformanceGroupId)
		assert.Nil(t, result.ThroughputMibps)
		assert.Nil(t, result.Iops)
	})

	t.Run("ManualPool_AutogenVPG_ReturnsThroughputIopsNoUUID", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "pool",
			QosType:        utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: "autogen-vpg-uuid"},
			IsAutoGen:       true,
			ThroughputMibps: 50,
			Iops:            200,
		}
		volume := minimalVolume(pool, vpg, sql.NullInt64{Int64: 1, Valid: true})
		result := convertDatastoreVolumeToModel(volume, nil)
		require.NotNil(t, result)
		assert.Equal(t, "", result.VolumePerformanceGroupId)
		require.NotNil(t, result.ThroughputMibps)
		assert.Equal(t, int64(50), *result.ThroughputMibps)
		require.NotNil(t, result.Iops)
		assert.Equal(t, int64(200), *result.Iops)
	})

	t.Run("AutoPool_NoVPG_ReturnsNone", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "pool",
			QosType:        utils.QosTypeAuto,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
		}
		volume := minimalVolume(pool, nil, sql.NullInt64{Valid: false})
		result := convertDatastoreVolumeToModel(volume, nil)
		require.NotNil(t, result)
		assert.Equal(t, "", result.VolumePerformanceGroupId)
		assert.Nil(t, result.ThroughputMibps)
		assert.Nil(t, result.Iops)
	})

	// Volumes in manual pools are always expected to have a VPG (invariant). Manual pool with no VPG is an invalid state
	// (something has gone wrong); conversion does not set any of the three fields.
	t.Run("ManualPool_NoVPG_InvalidState_ReturnsNone", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "pool",
			QosType:        utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
		}
		volume := minimalVolume(pool, nil, sql.NullInt64{Valid: false})
		result := convertDatastoreVolumeToModel(volume, nil)
		require.NotNil(t, result)
		assert.Equal(t, "", result.VolumePerformanceGroupId)
		assert.Nil(t, result.ThroughputMibps)
		assert.Nil(t, result.Iops)
	})

	t.Run("ManualPool_NonAutogenVPG_EmptyUUID_ReturnsNoUUID", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "pool",
			QosType:        utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{PrimaryZone: "us-central1-a"},
		}
		vpg := &datamodel.VolumePerformanceGroup{
			BaseModel:       datamodel.BaseModel{UUID: ""},
			IsAutoGen:       false,
			ThroughputMibps: 100,
			Iops:            500,
		}
		volume := minimalVolume(pool, vpg, sql.NullInt64{Int64: 1, Valid: true})
		result := convertDatastoreVolumeToModel(volume, nil)
		require.NotNil(t, result)
		assert.Equal(t, "", result.VolumePerformanceGroupId)
		assert.Nil(t, result.ThroughputMibps)
		assert.Nil(t, result.Iops)
	})
}
