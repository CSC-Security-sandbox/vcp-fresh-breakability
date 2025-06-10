package models

// Volume describes a volume in the cloud volume model
type Volume struct {
	BaseModel
	AccountName           string
	PoolID                string
	PoolName              string
	VendorID              string
	VendorSubnetID        string
	ProtocolTypes         []string
	Region                string
	CreationToken         string
	DisplayName           string
	Description           string
	LifeCycleState        string
	LifeCycleStateDetails string
	LifeCycleTrackingID   int32
	QuotaInBytes          uint64
	IsDataProtection      bool
	BlockProperties       *BlockProperties
	IPAddress             string
	DataProtection        *DataProtection
}

type BlockProperties struct {
	OSType         string
	HostGroupUUIDs []string
}

type DataProtection struct {
	ScheduledBackupEnabled *bool
	BackupVaultID          string
	BackupPolicyId         string
	BackupChainBytes       *int64
	PolicyEnforced         *bool
}
