package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// ConvertDatastoreActiveDirectoryToModel converts datamodel.ActiveDirectory to models.ActiveDirectory
func ConvertDatastoreActiveDirectoryToModel(ad *datamodel.ActiveDirectory) *models.ActiveDirectory {
	if ad == nil {
		return nil
	}

	model := &models.ActiveDirectory{
		BaseModel: models.BaseModel{
			ID:        ad.ID,
			UUID:      ad.UUID,
			CreatedAt: ad.CreatedAt,
			UpdatedAt: ad.UpdatedAt,
		},
		AdName:       ad.AdName,
		Username:     ad.Username,
		Password:     log.PasswordMask,
		Domain:       ad.Domain,
		DNS:          ad.DNS,
		NetBIOS:      ad.NetBIOS,
		State:        ad.State,
		StateDetails: ad.StateDetails,
	}

	// Convert ActiveDirectoryAttributes if available
	if ad.ActiveDirectoryAttributes != nil {
		model.ActiveDirectoryAttributes = &models.ActiveDirectoryAttributes{
			OrganizationalUnit:         ad.ActiveDirectoryAttributes.OrganizationalUnit,
			Site:                       ad.ActiveDirectoryAttributes.Site,
			SecurityOperators:          ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectorySeSecurityPrivilege],
			BackupOperators:            ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInBackupOperators],
			Administrators:             ad.ActiveDirectoryAttributes.AdUsers[utils.ActiveDirectoryGroupBuiltInAdministrators],
			KdcIP:                      ad.ActiveDirectoryAttributes.KdcIP,
			KdcHostname:                ad.ActiveDirectoryAttributes.KdcHostname,
			AesEncryption:              ad.ActiveDirectoryAttributes.AesEncryption,
			EncryptDCConnections:       ad.ActiveDirectoryAttributes.EncryptDCConnections,
			LdapSigning:                ad.ActiveDirectoryAttributes.LdapSigning,
			AllowLocalNFSUsersWithLdap: ad.ActiveDirectoryAttributes.AllowLocalNFSUsersWithLdap,
			Description:                ad.ActiveDirectoryAttributes.Description,
		}
	}

	return model
}
