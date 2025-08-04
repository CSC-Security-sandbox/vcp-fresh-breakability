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
	IPAddresses           []string
	DataProtection        *DataProtection
	Zone                  string
	UsedBytes             uint64
	EncryptionType        string
	SnapReserve           int64
	AutoTieringPolicy     *AutoTieringPolicy
	Labels                map[string]string
	FileProperties        *FileProperties
	SvmName               string
	KmsConfig             *KmsConfig
}

// AutoTieringPolicy describes the auto tiering policy for a volume
type AutoTieringPolicy struct {
	AutoTieringEnabled   bool
	CoolingThresholdDays int32
	TieringPolicy        string
}

type BlockProperties struct {
	OSType          string
	HostGroupDetail []HostGroupDetails
	LunName         string
	LunSerialNumber string
}

type FileProperties struct {
	JunctionPath string
	ExportPolicy *ExportPolicy
}

type ExportPolicy struct {
	ExportPolicyName string
	ExportRules      []*ExportRule
}

type ExportRule struct {
	AllowedClients      string
	AnonymousUser       string
	Index               int
	ChownMode           string
	AccessType          string
	CIFS                bool
	NFSv3               bool
	NFSv4               bool
	S3                  bool
	UnixReadOnly        bool
	UnixReadWrite       bool
	Kerberos5ReadOnly   bool
	Kerberos5ReadWrite  bool
	Kerberos5iReadOnly  bool
	Kerberos5iReadWrite bool
	Kerberos5pReadOnly  bool
	Kerberos5pReadWrite bool
	Superuser           bool
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

type UpdateDataProtection struct {
	ScheduledBackupEnabled *bool
	BackupVaultID          *string
	BackupPolicyId         *string
	BackupChainBytes       *int64
}
