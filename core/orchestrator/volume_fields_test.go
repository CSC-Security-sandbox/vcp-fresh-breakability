package orchestrator

import (
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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
