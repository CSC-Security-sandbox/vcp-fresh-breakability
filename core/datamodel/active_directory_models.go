package datamodel

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type ActiveDirectory struct {
	BaseModel
	AdName                    string                     `gorm:"column:name"`
	Username                  string                     `gorm:"column:username"`
	CredentialPath            string                     `gorm:"column:credential_path"`
	Domain                    string                     `gorm:"column:domain"`
	DNS                       string                     `gorm:"column:dns"`
	NetBIOS                   string                     `gorm:"column:netbios"`
	State                     string                     `gorm:"column:state"`
	StateDetails              string                     `gorm:"column:state_details"`
	AccountId                 int64                      `gorm:"column:account_id"`
	ActiveDirectoryAttributes *ActiveDirectoryAttributes `gorm:"column:active_directory_attributes;type:jsonb"`
}

type ActiveDirectoryAttributes struct {
	PrimaryAD                     bool                `json:"primary_ad"`
	ManagedAD                     bool                `json:"managed_ad"`
	OrganizationalUnit            string              `json:"organizational_unit"`
	Site                          string              `json:"site"`
	SvmIds                        []string            `json:"svm_ids"`
	AdUsers                       map[string][]string `json:"ad_users"`
	KdcIP                         string              `json:"kdc_ip"`
	UserDN                        string              `json:"user_dn"`
	GroupDN                       string              `json:"group_dn"`
	GroupMembershipFilter         string              `json:"group_membership_filter"`
	AesEncryption                 bool                `json:"aes_encryption"`
	EncryptDCConnections          bool                `json:"encrypt_dc_connections"`
	ServerRootCaCertificate       string              `json:"server_root_ca_certificate"`
	LdapSigning                   bool                `json:"ldap_signing"`
	AllowLocalNFSUsersWithLdap    bool                `json:"allow_local_nfs_users_with_ldap"`
	LdapOverTLS                   bool                `json:"ldap_over_tls"`
	PreferredServersForLdapClient string              `json:"preferred_servers_for_ldap_client"`
	Description                   string              `json:"description"`
}

func (ad *ActiveDirectoryAttributes) Scan(value interface{}) error {
	if value == nil {
		*ad = ActiveDirectoryAttributes{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(bytes, ad)
}

func (rd ActiveDirectoryAttributes) Value() (driver.Value, error) {
	return json.Marshal(rd)
}
