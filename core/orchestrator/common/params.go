package common

import (
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const (
	// HTTP Status Codes for SDE Error Handling
	HTTPStatusBadRequest            = 400
	HTTPStatusUnauthorized          = 401
	HTTPStatusForbidden             = 403
	HTTPStatusNotFound              = 404
	HTTPStatusConflict              = 409
	HTTPStatusUnprocessableEntity   = 422
	HTTPStatusTooManyRequests       = 429
	HTTPStatusInternalServerError   = 500
	HttpStatusNotImplemented      = 501
)

// CreatePoolParams describes parameters supplied to CreatingPool
type CreatePoolParams struct {
	AccountName             string
	Region                  string
	Name                    string
	Description             string
	VendorID                string
	ServiceLevel            string
	QosType                 string
	Tags                    string
	SizeInBytes             uint64
	AllowAutoTiering        bool
	HotTierSizeInBytes      uint64
	EnableHotTierAutoResize bool
	PrimaryZone             string
	VendorSubNetID          string
	SecondaryZone           string
	IsRegionalHA            bool
	HostUUID                string
	CustomPerformanceParams *CustomPerformanceParams
	KmsConfigId             string
	KmsConfigResourceID     string
	KmsConfig               *models.KmsConfig
	Labels                  *datamodel.JSONB
	LargeCapacity           bool
	ActiveDirectoryId       string
	ActiveDirectory         *models.ActiveDirectory
	LdapEnabled             bool
	Mode                    string
	XCorrelationID          string
	ADExistsInVCP           bool
}

// CustomPerformanceParams is used to specify the custom performance parameters for a pool
type CustomPerformanceParams struct {
	Enabled         bool
	ThroughputMibps int64
	Iops            *int64
}

type TenancyInfo struct {
	RegionalTenantProject string
	Network               string
	SubnetworkNames       []string
	SnHostProject         string
	Gateway               string
}

// LocationInfo contains location-related information for pool operations
type LocationInfo struct {
	PrimaryZone   string
	SecondaryZone string
	Region        string
	MediatorZone  string
}

// HostParams FixMe: remove this once HostGroup table is created
type HostParams struct {
	HostName string
	HostIQNs []string
	OsType   string
}

type RevertVolumeParams struct {
	AccountName string
	Region      string
	VolumeID    string
	SnapshotID  string
}

// CreateVolumeParams describes parameters supplied to CreatePool
type CreateVolumeParams struct {
	AccountName                 string
	Region                      string
	Zone                        string
	Name                        string
	Description                 string
	Network                     string
	PoolID                      string
	PoolDBID                    int64
	VendorID                    string
	CreationToken               string
	DisplayName                 string
	QuotaInBytes                uint64
	IsDataProtection            bool
	Protocols                   []string
	BlockProperties             *BlockPropertiesRequest
	BlockDevices                *[]BlockDevice
	SnapReserve                 int64
	DataProtection              *models.DataProtection
	SnapshotID                  string
	SnapshotPolicy              *models.SnapshotPolicy
	FileProperties              *models.FileProperties
	Snapshot                    *datamodel.Snapshot
	AutoTieringPolicy           *AutoTieringPolicy
	BackupID                    string
	LargeCapacity               bool
	BackupPath                  string
	BackupSchedule              string
	Labels                      *datamodel.JSONB
	CacheParameters             *models.CacheParameters
	LargeVolumeConstituentCount int32
	SnapshotDirectory           bool
	KerberosEnabled             bool
	HybridReplicationParameters *models.HybridReplicationParameters
	ThroughputMibps             *int64
	Iops                        *int64
	VolumePerformanceGroupID    *string
	// Note: Iops is not supported for create requests; it is derived from ThroughputMibps if enableInferredIops is true.
	// IsExpertModeRestore is true when this restore was started from RestoreForOntapModeVolumeWorkflow (expert mode volume). When true, RestoreBackupWorkflow finalizes by updating expert_mode_volumes instead of volumes.
	IsExpertModeRestore bool
}

type SnapmirrorRelationshipParams struct {
	SourcePath      string
	DestinationPath string
	SourceUUID      *string
	IsRestore       bool
}

// AutoTieringPolicy describes the auto tiering policy for a volume
type AutoTieringPolicy struct {
	AutoTieringEnabled       bool
	CoolingThresholdDays     int32
	TieringPolicy            string
	RetrievalPolicy          string
	HotTierBypassModeEnabled bool
	CloudWriteModeEnabled    *bool // Only supported for file volumes
}

type BlockPropertiesRequest struct {
	OSType          string
	HostGroupUUIDs  []string
	LunSerialNumber string
}

// BlockDevice describes parameters for creating a block device
type BlockDevice struct {
	Name            string
	HostGroups      []string
	OSType          string
	LunSerialNumber string // read-only
	SizeInBytes     int64  // read-only
	LunUUID         string // read-only
}

// UpdateVolumeParams describes parameters supplied to UpdateVolume
type UpdateVolumeParams struct {
	AccountName                 string
	Region                      string
	Name                        string
	Description                 string
	Network                     string
	PoolID                      string
	VolumeId                    string
	VendorID                    string
	QuotaInBytes                int64
	Protocols                   []string
	Labels                      *datamodel.JSONB
	SnapReserve                 *int64
	BlockProperties             *BlockPropertiesRequest
	BlockDevices                []*BlockDevice
	SnapshotPolicy              *models.SnapshotPolicy
	DataProtection              *models.UpdateDataProtection
	InitiateSplit               bool
	AutoTieringPolicy           *AutoTieringPolicy
	FileProperties              *models.FileProperties
	BackupSchedule              string
	CorrelationID               string
	SnapshotDirectoryAccess     *bool
	CacheParameters             *models.CacheParameters
	SMBShareSettings            []string
	LargeCapacity               *bool
	LargeVolumeConstituentCount *int32
	ThroughputMibps             *int64
	Iops                        *int64
	VolumePerformanceGroupId    *string
}

type CreateLunMapParams struct {
	LunName   string
	SvmName   string
	HostNames []string
}

// DeletePoolParams describes parameters supplied to DeletePool
type DeletePoolParams struct {
	AccountName string
	PoolID      string
}

type SnapshotBaseParams struct {
	AccountName string
	VolumeID    string
}

type CreateSnapshotParams struct {
	SnapshotBaseParams
	Name            string
	Description     string
	IsAppConsistent bool
}

// ExpertModeVolumeParams describes parameters supplied to CreateExpertModeVolume, UpdateExpertModeVolume, and DeleteExpertModeVolume.
type ExpertModeVolumeParams struct {
	PoolUUID    string
	Action      string
	VolumeName  string
	VolumeUUID  string
	SizeInBytes int64
	Style       string // flexvol|flexgroup
	SvmUuid     string
	SvmName     string
	AccountName string
}

// ExpertModeVolumeRenameParams describes parameters for RenameExpertModeVolume.
type ExpertModeVolumeRenameParams struct {
	VolumeName  string // Current volume name
	NewName     string // New volume name after rename
	PoolUUID    string
	SvmName     string
	AccountName string
}

type UpdateResourceStateParams struct {
	Description        string
	State              string
	ProjectNumber      string
	LocationId         string
	XCorrelationID     string
	ResourceType       string
	ResourceId         string
	IsCommonResource   bool
	ParentResourceID   string
	ParentResourceType string
}

type HandleResourceCVPResponse struct {
	Name string
	Done bool
}

type StateUpdateParam struct {
	State string
}

type GetSnapshotParams struct {
	SnapshotBaseParams
	SnapshotUUID string
}

// DeleteSnapshotParams describes parameters supplied to DeleteSnapshot
type DeleteSnapshotParams struct {
	SnapshotBaseParams
	SnapshotID string
}

// UpgradeClusterParams describes parameters supplied to UpgradeCluster
type UpgradeClusterParams struct {
	ClusterID          string            `json:"clusterId"`
	VSABuildImage      string            `json:"vsaBuildImage"`          // Optional: VSA build image to upgrade to (requires forceUpgrade=true)
	MediatorBuildImage string            `json:"mediatorBuildImage"`     // Optional: Mediator build image to upgrade to (requires forceUpgrade=true)
	ForceUpgrade       bool              `json:"forceUpgrade,omitempty"` // Required when specifying build images, or when upgrade gap > 1
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type ListSnapshotsParams struct {
	SnapshotBaseParams
}

// ListQuotaRulesParams describes parameters for listing quota rules
type ListQuotaRulesParams struct {
	AccountName string
	VolumeID    string
}

type UpdateSnapshotParams struct {
	SnapshotBaseParams
	SnapshotUUID string
	Description  string
}

type ClusterPeerParams struct {
	PeerAddresses      []string
	PeerName           string
	AccountName        string
	ExpiryTime         *time.Time
	GeneratePassphrase bool
	Passphrase         *string
	UUID               string
}

type ClusterPeer struct {
	UUID                string
	PeerAddresses       []string
	PeerClusterName     string
	Availability        string
	AuthenticationState string
	Passphrase          *log.Secret
	IPSpace             string
	ExternalUUID        string
	HostUUID            string
	AccountUUID         string
	AccountName         string
	ExpiryTime          *strfmt.DateTime
}

type UpdatePoolParams struct {
	AccountName               string
	Region                    string
	PoolId                    string
	Description               string
	VendorID                  string
	QosType                   string
	Tags                      string
	SizeInBytes               uint64
	AllowAutoTiering          bool
	CurrentZone               string
	VendorSubNetID            string
	CustomThroughputMibps     uint64
	HostUUID                  string
	Zone                      string
	Labels                    *datamodel.JSONB
	ActiveDirectoryConfigId   string
	ActiveDirectoryId         string
	ActiveDirectory           *models.ActiveDirectory
	XCorrelationID            string
	HotTierSizeInBytes        uint64
	EnableHotTierAutoResize   bool
	CustomPerformanceEnabled  bool
	TotalThroughputMibps      int64
	TotalIops                 *int64
	LargeCapacity             *bool
	AutoResizeTriggeredUpdate bool
	IfADExistsInVCP           bool
}

// VolumeCountRange defines the volume count range for auto pool scaling
type VolumeCountRange struct {
	MinVolumeCount int64 `json:"min_volume_count"`
	MaxVolumeCount int64 `json:"max_volume_count"`
}

type AutoPoolScalingParams struct {
	VolLimitPerInstanceMap map[string]VolumeCountRange
	CurrentVolumeCount     int64
}

type CreateVolumeReplicationInternalParams struct {
	ReverseResync     bool
	VolumeReplication *models.VolumeReplication
}

type UpdateVolumeReplicationInternalParams struct {
	AccountName           string
	VolumeReplicationUuid string
	ReplicationSchedule   *string
	Description           *string
	LocationId            string
	XCorrelationID        string
	Labels                *datamodel.JSONB
	ClusterLocation       *string
}

// CreateVolumeReplicationParams describes parameters supplied to CreatingVolumeReplication
type CreateVolumeReplicationParams struct {
	AccountName      string
	Region           string
	LocationId       string
	Name             string
	Description      string
	SourceVolumeName string
	Body             *gcpserver.ReplicationCreateV1beta
	ReverseResync    bool
	CorrelationId    string
}

// BackupVaultParams describes parameters supplied to BackupVault
type BackupVaultParams struct {
	ID                           int64
	OwnerID                      string
	BackupVaultID                string
	Name                         string
	Description                  *string
	LifeCycleState               string
	LifeCycleStateDetails        string
	CreatedAt                    time.Time
	UpdatedAt                    time.Time
	DeletedAt                    *time.Time
	BackupRegion                 *string
	SourceRegion                 *string
	ExternalUUID                 string
	Region                       string
	AccountVendorID              string
	BackupRetentionPolicy        BackupRetentionPolicyParams
	BucketDetails                []*datamodel.BucketDetails
	SourceBackupVault            *string
	DestinationBackupVault       *string
	BackupVaultType              *string
	AccountName                  string
	CrossRegionBackupVaultName   *string
	BackupVaultIDs               []string
	CmekEncryptionState          *string
	CmekBackupsPrimaryKeyVersion *string
}

// BackupRetentionPolicyParams describes parameters supplied to BackupRetentionPolicy
type BackupRetentionPolicyParams struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 *bool
	IsMonthlyBackupImmutable               *bool
	IsWeeklyBackupImmutable                *bool
	IsAdhocBackupImmutable                 *bool
}

// CreateBackupVaultParams describes parameters for creating a backup vault in VCP (USE_VCP_REGION path).
type CreateBackupVaultParams struct {
	ResourceId               string
	Description              string
	BackupRegion             *string
	LocationId               string
	ProjectNumber            string
	BackupRetentionPolicy    BackupRetentionPolicyParams
	KmsConfigResourcePath    *string
	BackupsPrimaryKeyVersion *string
}

type DeleteBackupPolicyParams struct {
	Name           string
	OwnerID        string
	LocationID     string
	BackupPolicyID string
}

type BucketDetails struct {
	BucketName          string
	ServiceAccountName  string
	VendorSubnetID      string
	TenantProjectNumber string
	Location            string
	AccountId           string
	SatisfiesPzi        bool
	SatisfiesPzs        bool
}

type ResourceNames struct {
	Email            string
	BucketName       string
	ServiceAccountId string
}

type CreateBackupParams struct {
	AccountName              string
	Region                   string
	BackupVaultID            string
	VolumeUUID               string
	BackupName               string
	BackupUUID               string // ExternalUUID for cross-region backups
	Description              string
	SnapshotID               string
	BackupType               string
	LocationID               string
	XCorrelationID           string
	UseExistingSnapshot      bool
	VolumeName               string
	Protocols                []string
	SnapshotName             string
	BucketName               string
	EndpointUUID             string
	IsRegionalHA             bool
	CompletionTime           string
	BackupPolicyName         string
	OntapVolumeStyle         string
	SourceVolumeZone         string
	ServiceAccountName       string
	SnapshotCreationTime     string
	ConstituentCountOfBackup int32
	VolumeUsageBytes         int64
	BackupChainBytes         int64
	IsExpertModeVolume       bool   // Indicates if the volume is an expert mode volume (ONTAP mode)
	BackupVaultServiceType   string // Service type of the backup vault (GCBDR, GCNV, etc.)
}

type GetBackupsParams struct {
	AccountID     int64
	BackupVaultID string
	BackupUUIDs   []string
}

type GetBackupParams struct {
	AccountName   string
	BackupVaultID string
	BackupUUID    string
}

type UpdateBackupPolicyParams struct {
	Name               string
	AccountName        string
	BackupPolicyID     string
	LocationID         string
	Description        *string
	PolicyEnabled      *bool
	DailyBackupLimit   *int64
	WeeklyBackupLimit  *int64
	MonthlyBackupLimit *int64
}

type BackupPolicyParams struct {
	Name                 string
	OwnerID              string
	BackupPolicyUUID     string
	VolumesAssigned      int64
	DailyBackupsToKeep   int64
	WeeklyBackupsToKeep  int64
	MonthlyBackupsToKeep int64
	Enabled              bool
	Description          *string
	AccountName          string
}

type ReplicationInternalGetMultipleParams struct {
	ReplicationUUIDs    []string
	AccountName         string
	ReplicationsFromDB  []*datamodel.VolumeReplication
	PoolUUIDs           []string
	PoolNodeMap         map[int64]*datamodel.Node                // [poolUUID]Node
	PoolReplicationsMap map[int64][]*datamodel.VolumeReplication // [poolUUID][]VolumeReplication
	UpdatedReplications []*datamodel.VolumeReplication           // Replications updated from Ontap
}

type CloudTarget struct {
	Name string
	UUID string
}
type SnapmirrorRelationship struct {
	UUID            string
	DestinationUUID *string
	Healthy         *bool
	UnhealthyReason *[]string
	State           *string
	TotalTransferBytes *int64
}

type GetMultipleReplicationsParams struct {
	ReplicationURIs  []string
	AccountName      string
	LocationId       string
	XCorrelationID   string
	VolumeResourceId string
}

type GetMultipleReplicationsByExternalUUIDParams struct {
	ExternalUUIDs []string
	EndpointType  string
}

type CreateSMCTokenRotationParams struct {
	AccountName string
}

type ADCParams struct {
	ADCName          string
	DestEndpointUUID string
	SnapshotUUID     string
	BucketName       string
	AccessKey        string
	SecretKey        string
	ProvideType      string
	ServerURL        string
	AccountName      string
	Port             int64
}

type ADCResponse struct {
	StatusCode  int
	RedirectURL string
}

// SnapmirrorTransferFile represents a file entry for snapmirror transfer
type SnapmirrorTransferFile struct {
	SourcePath      string
	DestinationPath string
}

type DeleteBackupParams struct {
	AccountName     string
	BackupVaultUUID string
	BackupUUID      string
	Region          string
}

type UpdateBackupParams struct {
	AccountName     string
	BackupVaultUUID string
	BackupUUID      string
	Description     string
	State           *string
	StateDetails    *string
	Region          string
}

type HmacKeyCreateParams struct {
	ServiceAccount string
	ProjectNumber  string
}

type HmacKeys struct {
	AccessKey string
	SecretKey string
}

type ResumeReplicationParams struct {
	AccountName           string
	Region                string
	Zone                  string
	CorrelationId         string
	VolumeResourceId      string
	ReplicationResourceId string
	Force                 bool
}

type UpdateReplicationParams struct {
	AccountName           string
	Region                string
	Zone                  string
	CorrelationId         string
	VolumeResourceId      string
	ReplicationResourceId string
	ReplicationSchedule   *string
	Description           *string
	Labels                map[string]string
	ClusterLocation       *string
}

// UpdateHostGroupParams describes parameters supplied to UpdateHostGroup
type UpdateHostGroupParams struct {
	Hosts         *[]string
	Description   *string
	AccountName   string
	HostGroupUUID string
}

type CreateHostGroupParams struct {
	Name          string
	Description   string
	HostGroupType string
	Hosts         []string
	OSType        string
	AccountName   string
}

type SnapshotsInternalDeleteParams struct {
	SnapshotBaseParams
	Location           string
	Volume             *datamodel.Volume
	Nodes              []*datamodel.Node
	SnapshotsFromDB    []*datamodel.Snapshot
	SnapshotsFromOntap []*SnapshotListResponse
}

type SnapshotListResponse struct {
	Name               string
	ExternalUUID       string
	VolumeExternalUUID string
}

type StopReplicationParams struct {
	AccountName           string
	Region                string
	CorrelationId         string
	VolumeResourceId      string
	ReplicationResourceId string
	Zone                  string
	ForceStop             bool
}

type DeleteReplicationParams struct {
	AccountName           string
	Region                string
	CorrelationId         string
	VolumeResourceId      string
	ReplicationResourceId string
	Zone                  string
}

type Operations struct {
	Project            string
	OperationName      string
	IsDone             bool
	IsRegionalResource bool
	OperationType      string
}

type ReverseAndResumeReplicationParams struct {
	AccountName           string
	Zone                  string
	Region                string
	CorrelationId         string
	VolumeResourceId      string
	ReplicationResourceId string
}

type UpdateVolumeReplicationAttributesParams struct {
	AccountName            string
	Region                 string
	Zone                   string
	VolumeReplicationId    string
	UpdateAttributesParams *models.UpdateVolumeReplicationAttributesParams
}

type ProjectInfo struct {
	ProjectNumber string
	Region        string
	Location      string
	JwtToken      string
	BasePath      string
	PoolUUID      string
	VolumeUUID    string
}

type VolumeUpdateEventParams struct {
	URI           string
	CorrelationID string
	Local         ProjectInfo
	Remote        ProjectInfo
}

type EstablishVolumePeeringParams struct {
	AccountName     string
	Region          string
	Zone            string
	Name            string
	PeerClusterName string
	PeerAddresses   []string
	ExpiryTime      *time.Time
	PeerSvmName     string
	PeerVolumeName  string
}

type EstablishReplicationPeeringParams struct {
	AccountName              string
	Region                   string
	Zone                     string
	CorrelationId            string
	VolumeResourceId         string
	ReplicationResourceId    string
	PeeringCommandExpiryTime *time.Time
	PeerVolumeName           string
	PeerClusterName          string
	PeerSvmName              string
	PeerIPAddresses          []string
}

type CreateActiveDirectoryParams struct {
	AccountId                  string
	LocationId                 string `json:"LocationId" validate:"LocationId"`
	XCorrelationId             string
	Username                   string `json:"Username" validate:"Username"`
	ResourceId                 string `json:"ResourceId" validate:"ResourceId"`
	Password                   string
	Domain                     string
	DNS                        string `json:"DNS" validate:"DNS"`
	NetBIOS                    string `json:"netBIOS" validate:"NetBIOS"`
	OrganizationalUnit         string `json:"organizationalUnit" validate:"OrganizationalUnit"`
	Site                       string `json:"Site" validate:"Site"`
	KdcIP                      string `json:"kdcIP" validate:"KdcIP"`
	KdcHostname                string `json:"kdcHostname" validate:"KdcHostname"`
	LdapSigning                bool
	AllowLocalNFSUsersWithLdap bool
	EncryptDCConnections       bool
	SecurityOperators          []string `json:"securityOperators" validate:"SecurityOperators"`
	BackupOperators            []string `json:"backupOperators" validate:"BackupOperators"`
	Administrators             []string `json:"administrators" validate:"Administrators"`
	AesEncryption              bool
	Description                string
}

type UpdateActiveDirectoryParams struct {
	ActiveDirectoryId          string
	AccountId                  string
	LocationId                 string `json:"LocationId" validate:"LocationId"`
	XCorrelationId             string
	Username                   *string `json:"Username" validate:"omitempty,Username"`
	Password                   *string
	Domain                     *string
	DNS                        *string `json:"DNS" validate:"omitempty,DNS"`
	NetBIOS                    *string `json:"netBIOS" validate:"omitempty,NetBIOS"`
	OrganizationalUnit         *string `json:"organizationalUnit" validate:"omitempty,OrganizationalUnit"`
	Site                       *string `json:"Site" validate:"omitempty,Site"`
	KdcIP                      *string
	KdcHostname                *string
	LdapSigning                *bool
	AllowLocalNFSUsersWithLdap *bool
	EncryptDCConnections       *bool
	SecurityOperators          []string `json:"securityOperators" validate:"omitempty,SecurityOperators"`
	BackupOperators            []string `json:"backupOperators" validate:"omitempty,BackupOperators"`
	Administrators             []string `json:"administrators" validate:"omitempty,Administrators"`
	AesEncryption              *bool
	Description                *string
}

// GetADParams describes parameters to get Active Directory configuration
type GetADParams struct {
	UUID          string
	AccountName   string
	LocationID    string
	ProjectNumber string
	ResourceID    string
	CorrelationID string
}

type RestoreFilesFromBackupParams struct {
	AccountName     string
	BackupPath      string
	BackupID        string
	SourceFileList  []string
	RestoreFilePath string
	VolumeUUID      string
	Region          string
	// PoolID is required for expert mode restore; used to fetch the pool and verify it is an expert mode (ONTAP) pool (APIAccessMode == ONTAP). Supplied from API path (e.g. params.PoolId).
	PoolID string
	// IsExpertModeRestore when true causes the SFR workflow to update expert_mode_volumes table (UpdateExpertModeVolumeStateInDB) instead of volumes table (UpdateVolumeStateInDB).
	IsExpertModeRestore bool
}

// RestoreOntapModeBackupParams holds parameters for RestoreOntapModeBackup (pool endpoint full-volume or file-level restore for ONTAP mode).
type RestoreOntapModeBackupParams struct {
	AccountName     string
	BackupPath      string
	SourceFileList  []string
	RestoreFilePath string
	VolumeUUID      string
	Region          string
	PoolID          string
}

// RestoreForOntapModeParams holds parameters for the RestoreForOntapModeVolumeWorkflow (full volume restore from backup for ONTAP mode).
type RestoreForOntapModeParams struct {
	AccountName      string
	BackupPath       string
	Region           string
	ExpertModeVolume *datamodel.ExpertModeVolumes // Expert mode volume with Pool, Account, Svm loaded
}

// ActiveDirectoryStateResult holds state and stateDetails computed from SVM usage (e.g. by GetActiveDirectoryStateFromSVMUsage activity).
type ActiveDirectoryStateResult struct {
	State        string
	StateDetails string
}

type AdSdeUpdateResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}

type DeleteActiveDirectoryParams struct {
	ProjectNumber       string
	AccountId           int64
	ActiveDirectoryUUID string
}

type SplitCloneVolumeParams struct {
	AccountName string
	Region      string
	VolumeID    string
}

// CreateQuotaRulesParam describes parameters supplied to create a quota rule
type CreateQuotaRulesParam struct {
	Name           string
	VolumeUUID     string
	QuotaType      string
	DiskLimitInMib int64
	QuotaTarget    string
	ProjectId      string
	Description    string
	LocationId     string // Region where the quota rule is being created
}

// UpdateQuotaRulesParam describes parameters supplied to update a quota rule
type UpdateQuotaRulesParam struct {
	QuotaRuleUUID  string // UUID of the quota rule to update
	DiskLimitInMib int64  // New disk limit (optional, can be 0 if not updating)
	Description    string // New description (optional, can be empty if not updating)
	ProjectId      string // Project number for validation
	LocationId     string // Location/region for validation
}

// DeleteQuotaRulesParam describes parameters supplied to delete a quota rule
type DeleteQuotaRulesParam struct {
	QuotaRuleUUID string // UUID of the quota rule to delete
	ProjectId     string // Project number for validation
	LocationId    string // Location/region for validation
}

// CreateVolumePerformanceGroupParams describes parameters supplied to CreateVolumePerformanceGroup
type CreateVolumePerformanceGroupParams struct {
	AccountName     string
	PoolID          string
	Name            string // resourceId
	ThroughputMibps int64
	Iops            int64
	IsShared        bool
}

// UpdateVolumePerformanceGroupParams describes parameters supplied to UpdateVolumePerformanceGroup
type UpdateVolumePerformanceGroupParams struct {
	AccountName              string
	PoolID                   string
	VolumePerformanceGroupID string
	Name                     string // resourceId (optional; empty means do not change)
	ThroughputMibps          *int64 // optional; nil means do not update
	Iops                     *int64 // optional; nil means do not update
}

// DeleteVolumePerformanceGroupParams describes parameters supplied to DeleteVolumePerformanceGroup
type DeleteVolumePerformanceGroupParams struct {
	AccountName              string
	PoolID                   string
	VolumePerformanceGroupID string
}

// GetVolumePerformanceGroupParams describes parameters supplied to GetVolumePerformanceGroup
type GetVolumePerformanceGroupParams struct {
	AccountName              string
	PoolID                   string
	VolumePerformanceGroupID string
}

// ListVolumePerformanceGroupsParams describes parameters supplied to ListVolumePerformanceGroups
type ListVolumePerformanceGroupsParams struct {
	AccountName string
	PoolID      string
}

// ReplicationV1beta represents a volume replication in v1beta format
type ReplicationV1beta struct {
	ReplicationId                 *string
	ResourceId                    *string
	Description                   *string
	Source                        *ReplicationVolumeInformationV1beta
	Destination                   *ReplicationVolumeInformationV1beta
	State                         *string
	StateDetails                  *string
	StateDetailsCode              *int32
	Role                          *string
	ReplicationSchedule           *string
	MirrorState                   *string
	Healthy                       *bool
	TransferStats                 *TransferStatsV1beta
	Created                       *time.Time
	Labels                        map[string]string
	ClusterLocation               *string
	HybridReplicationType         *string
	HybridPeeringDetails          *HybridPeeringV1beta
	HybridReplicationUserCommands *HybridReplicationUserCommandsV1beta
}

// ReplicationVolumeInformationV1beta represents volume information for replication
type ReplicationVolumeInformationV1beta struct {
	VolumeName *string
	VolumeId   *string
}

// TransferStatsV1beta represents transfer statistics for replication
type TransferStatsV1beta struct {
	TotalTransferBytes    *float64
	TotalTransferTimeSecs *float64
	LastTransferSize      *float64
	LastTransferError     *string
	LastTransferDuration  *float64
	LastTransferEndTime   *time.Time
	TotalProgress         *float64
	ProgressLastUpdated   *time.Time
	LagTime               *float64
}

// HybridPeeringV1beta represents hybrid peering details for replication
type HybridPeeringV1beta struct {
	SubnetIp          *string
	Command           *string
	Passphrase        *string
	CommandExpiryTime *time.Time
	PeerVolumeName    *string
	PeerClusterName   *string
	PeerSvmName       *string
}

// HybridReplicationUserCommandsV1beta represents user commands for hybrid replication
type HybridReplicationUserCommandsV1beta struct {
	Commands []string
}

// UpdateDstWithSrcQuotaRulesV1beta represents parameters for updating destination quota rules with source quota rules
type UpdateDstWithSrcQuotaRulesV1beta struct {
	SrcQuotaRules []QuotaRulesV1beta
	DstQuotaRules []QuotaRulesV1beta
}

// QuotaRulesV1beta represents a quota rule in v1beta format
type QuotaRulesV1beta struct {
	QuotaId        *string
	ResourceId     string
	QuotaType      string
	DiskLimitInMib int64
	QuotaTarget    *string
	State          *string
	StateDetails   *string
	Description    *string
	CreatedAt      *time.Time
	UpdatedAt      *time.Time
}

// V1betaUpdateDestinationQuotaRulesVCPParams represents parameters for updating destination quota rules
type V1betaUpdateDestinationQuotaRulesVCPParams struct {
	ProjectNumber  string
	LocationId     string
	VolumeId       string
	XCorrelationID *string
}
