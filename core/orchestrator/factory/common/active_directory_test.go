package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestConvertDatastoreActiveDirectoryToModel_Success(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:        1,
			UUID:      "test-uuid",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:         "test-ad",
		Username:       "testuser",
		CredentialPath: "projects/test/secrets/ad-cred/testuser",
		Domain:         "example.com",
		DNS:            "8.8.8.8",
		NetBIOS:        "EXAMPLE",
		State:          "READY",
		StateDetails:   "Ready",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test",
			Site:               "Default-Site",
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege:         {"sec-user"},
				utils.ActiveDirectoryGroupBuiltInBackupOperators: {"backup-user"},
				utils.ActiveDirectoryGroupBuiltInAdministrators:  {"admin-user"},
			},
			KdcIP:                      "1.2.3.4",
			KdcHostname:                "kdc.example.com",
			AesEncryption:              true,
			EncryptDCConnections:       true,
			LdapSigning:                false,
			AllowLocalNFSUsersWithLdap: true,
			Description:                "Test Description",
		},
	}

	result := ConvertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, int64(1), result.ID)
	assert.Equal(t, "test-uuid", result.UUID)
	assert.Equal(t, now, result.CreatedAt)
	assert.Equal(t, now, result.UpdatedAt)
	assert.Equal(t, "test-ad", result.AdName)
	assert.Equal(t, "testuser", result.Username)
	assert.Equal(t, log.PasswordMask, result.Password)
	assert.Equal(t, "example.com", result.Domain)
	assert.Equal(t, "8.8.8.8", result.DNS)
	assert.Equal(t, "EXAMPLE", result.NetBIOS)
	assert.Equal(t, "READY", result.State)
	assert.Equal(t, "Ready", result.StateDetails)

	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test", result.ActiveDirectoryAttributes.OrganizationalUnit)
	assert.Equal(t, "Default-Site", result.ActiveDirectoryAttributes.Site)
	assert.Equal(t, []string{"sec-user"}, result.ActiveDirectoryAttributes.SecurityOperators)
	assert.Equal(t, []string{"backup-user"}, result.ActiveDirectoryAttributes.BackupOperators)
	assert.Equal(t, []string{"admin-user"}, result.ActiveDirectoryAttributes.Administrators)
	assert.Equal(t, "1.2.3.4", result.ActiveDirectoryAttributes.KdcIP)
	assert.Equal(t, "kdc.example.com", result.ActiveDirectoryAttributes.KdcHostname)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.AesEncryption)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.EncryptDCConnections)
	assert.Equal(t, false, result.ActiveDirectoryAttributes.LdapSigning)
	assert.Equal(t, true, result.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap)
	assert.Equal(t, "Test Description", result.ActiveDirectoryAttributes.Description)
}

func TestConvertDatastoreActiveDirectoryToModel_NilInput(t *testing.T) {
	result := ConvertDatastoreActiveDirectoryToModel(nil)
	assert.Nil(t, result)
}

func TestConvertDatastoreActiveDirectoryToModel_NilAttributes(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			ID:        2,
			UUID:      "test-uuid-2",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:                    "test-ad-2",
		Username:                  "testuser2",
		Domain:                    "example2.com",
		DNS:                       "8.8.8.9",
		NetBIOS:                   "EXAMPLE2",
		State:                     "CREATING",
		StateDetails:              "Creating",
		ActiveDirectoryAttributes: nil,
	}

	result := ConvertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.Equal(t, int64(2), result.ID)
	assert.Equal(t, "test-uuid-2", result.UUID)
	assert.Equal(t, now, result.CreatedAt)
	assert.Equal(t, now, result.UpdatedAt)
	assert.Equal(t, "test-ad-2", result.AdName)
	assert.Equal(t, "testuser2", result.Username)
	assert.Equal(t, log.PasswordMask, result.Password)
	assert.Equal(t, "example2.com", result.Domain)
	assert.Equal(t, "8.8.8.9", result.DNS)
	assert.Equal(t, "EXAMPLE2", result.NetBIOS)
	assert.Equal(t, "CREATING", result.State)
	assert.Equal(t, "Creating", result.StateDetails)
	assert.Nil(t, result.ActiveDirectoryAttributes)
}

func TestConvertDatastoreActiveDirectoryToModel_EmptyAdUsers(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid-3",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:   "test-ad-3",
		Username: "testuser3",
		Domain:   "example3.com",
		State:    "READY",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test3",
			AdUsers:            map[string][]string{},
		},
	}

	result := ConvertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, "OU=Test3", result.ActiveDirectoryAttributes.OrganizationalUnit)
	// When AdUsers map doesn't have the keys, accessing them should return nil
	assert.Nil(t, result.ActiveDirectoryAttributes.SecurityOperators)
	assert.Nil(t, result.ActiveDirectoryAttributes.BackupOperators)
	assert.Nil(t, result.ActiveDirectoryAttributes.Administrators)
}

func TestConvertDatastoreActiveDirectoryToModel_PartialAdUsers(t *testing.T) {
	now := time.Now()
	ad := &datamodel.ActiveDirectory{
		BaseModel: datamodel.BaseModel{
			UUID:      "test-uuid-4",
			CreatedAt: now,
			UpdatedAt: now,
		},
		AdName:   "test-ad-4",
		Username: "testuser4",
		Domain:   "example4.com",
		State:    "READY",
		ActiveDirectoryAttributes: &datamodel.ActiveDirectoryAttributes{
			OrganizationalUnit: "OU=Test4",
			AdUsers: map[string][]string{
				utils.ActiveDirectorySeSecurityPrivilege: {"sec-user-1", "sec-user-2"},
				// Missing BackupOperators and Administrators
			},
			KdcIP:         "1.2.3.5",
			AesEncryption: false,
		},
	}

	result := ConvertDatastoreActiveDirectoryToModel(ad)

	assert.NotNil(t, result)
	assert.NotNil(t, result.ActiveDirectoryAttributes)
	assert.Equal(t, []string{"sec-user-1", "sec-user-2"}, result.ActiveDirectoryAttributes.SecurityOperators)
	assert.Nil(t, result.ActiveDirectoryAttributes.BackupOperators)
	assert.Nil(t, result.ActiveDirectoryAttributes.Administrators)
	assert.Equal(t, "1.2.3.5", result.ActiveDirectoryAttributes.KdcIP)
	assert.Equal(t, false, result.ActiveDirectoryAttributes.AesEncryption)
}
