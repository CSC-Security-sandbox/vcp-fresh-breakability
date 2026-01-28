package orchestrator

import (
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"testing"
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
