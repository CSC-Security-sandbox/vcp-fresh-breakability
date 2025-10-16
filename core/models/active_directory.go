package models

type ActiveDirectory struct {
	BaseModel
	AdName                    string
	Username                  string
	Password                  string
	Domain                    string
	DNS                       string
	NetBIOS                   string
	State                     string
	StateDetails              string
	ActiveDirectoryAttributes *ActiveDirectoryAttributes
}

type ActiveDirectoryAttributes struct {
	OrganizationalUnit         string
	Site                       string
	SecurityOperators          []string
	BackupOperators            []string
	Administrators             []string
	KdcIP                      string
	KdcHostname                string
	AesEncryption              bool
	EncryptDCConnections       bool
	LdapSigning                bool
	AllowLocalNFSUsersWithLdap bool
	Description                string
}
