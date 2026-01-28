package models

// Volume describes a volume in the cloud volume model
type Volume struct {
	BaseModel
	AccountName                 string
	PoolID                      string
	PoolName                    string
	VendorID                    string
	VendorSubnetID              string
	ProtocolTypes               []string
	Region                      string
	CreationToken               string
	DisplayName                 string
	Description                 string
	LargeCapacity               bool
	LargeVolumeConstituentCount *int32
	LifeCycleState              string
	LifeCycleStateDetails       string
	LifeCycleTrackingID         int32
	QuotaInBytes                uint64
	IsDataProtection            bool
	Mounted                     bool
	BlockProperties             *BlockProperties
	BlockDevices                *[]BlockDevice
	SnapshotPolicy              *SnapshotPolicy
	IPAddresses                 []string
	DataProtection              *DataProtection
	Zone                        string
	UsedBytes                   uint64
	EncryptionType              string
	SnapReserve                 int64
	SnapshotDirectory           bool
	KerberosEnabled             bool
	LdapEnabled                 bool
	ActiveDirectoryConfigId     string
	ActiveDirectoryResourceId   string
	AutoTieringPolicy           *AutoTieringPolicy
	Labels                      map[string]string
	FileProperties              *FileProperties
	SvmName                     string
	KmsConfig                   *KmsConfig
	CacheParameters             *CacheParameters
	CloneSharedBytes            uint64
	HotTierSizeGib              uint64
	ColdTierSizeGib             uint64
	CloneParentInfo             *CloneParentInfo
	ThroughputMibps             *int64
	Iops                        *int64
}

// AutoTieringPolicy describes the auto tiering policy for a volume
type AutoTieringPolicy struct {
	AutoTieringEnabled       bool
	CoolingThresholdDays     int32
	TieringPolicy            string
	HotTierBypassModeEnabled bool
	CloudWriteModeEnabled    *bool
}

type BlockProperties struct {
	OSType          string
	HostGroupDetail []HostGroupDetails
	LunName         string
	LunSerialNumber string
}

// BlockDevice describes a block device within a volume
type BlockDevice struct {
	Name            string
	HostGroupDetail []HostGroupDetails
	Identifier      string
	Size            uint64
	OSType          string
}

type FileProperties struct {
	JunctionPath     string
	ExportPolicy     *ExportPolicy
	Fqdn             string
	SMBShareSettings []string
	SecurityStyle    string
	UnixPermissions  string
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
	AllSquash           *bool
	AnonUid             *int64
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
	KmsGrant               *string
}

type UpdateDataProtection struct {
	ScheduledBackupEnabled *bool
	BackupVaultID          *string
	BackupPolicyId         *string
	BackupChainBytes       *int64
	KmsGrant               *string
}

type CloneParentInfo struct {
	ParentVolumeId   *string
	ParentSnapshotId *string
}
