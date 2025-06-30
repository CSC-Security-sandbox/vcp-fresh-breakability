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
	SnapshotPolicy        *SnapshotPolicy
	IPAddress             string
	DataProtection        *DataProtection
	Zone                  string
	UsedBytes             uint64
	EncryptionType        string
}

type BlockProperties struct {
	OSType          string
	HostGroupDetail []HostGroupDetails
	LunSerialNumber string
}

type HostGroupDetails struct {
	HostGroupID string
	Hosts       []string
}

type DataProtection struct {
	ScheduledBackupEnabled *bool
	BackupVaultID          string
	BackupPolicyId         string
	BackupChainBytes       *int64
}
