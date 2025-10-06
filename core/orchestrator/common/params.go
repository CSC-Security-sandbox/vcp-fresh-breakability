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
	HTTPStatusBadRequest          = 400
	HTTPStatusUnauthorized        = 401
	HTTPStatusForbidden           = 403
	HTTPStatusNotFound            = 404
	HTTPStatusInternalServerError = 500
	HTTPStatusTooManyRequests     = 429
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
	AccountName             string
	Region                  string
	Name                    string
	Description             string
	Network                 string
	PoolID                  string
	VolumeId                string
	VendorID                string
	QuotaInBytes            int64
	Protocols               []string
	Labels                  *datamodel.JSONB
	SnapReserve             *int64
	BlockProperties         *BlockPropertiesRequest
	BlockDevices            []*BlockDevice
	SnapshotPolicy          *models.SnapshotPolicy
	DataProtection          *models.UpdateDataProtection
	InitiateSplit           bool
	AutoTieringPolicy       *AutoTieringPolicy
	FileProperties          *models.FileProperties
	BackupSchedule          string
	CorrelationID           string
	SnapshotDirectoryAccess *bool
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

type ListSnapshotsParams struct {
	SnapshotBaseParams
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
	HotTierSizeInBytes        uint64
	EnableHotTierAutoResize   bool
	CustomPerformanceEnabled  bool
	TotalThroughputMibps      int64
	TotalIops                 *int64
	LargeCapacity             bool
	AutoResizeTriggeredUpdate bool
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
	ID                         int64
	OwnerID                    string
	BackupVaultID              string
	Name                       string
	Description                *string
	LifeCycleState             string
	LifeCycleStateDetails      string
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
	DeletedAt                  *time.Time
	BackupRegion               *string
	SourceRegion               *string
	ExternalUUID               string
	Region                     string
	AccountVendorID            string
	BackupRetentionPolicy      BackupRetentionPolicyParams
	SourceBackupVault          *string
	DestinationBackupVault     *string
	BackupVaultType            *string
	AccountName                string
	CrossRegionBackupVaultName *string
	BackupVaultIDs             []string
}

// BackupRetentionPolicyParams describes parameters supplied to BackupRetentionPolicy
type BackupRetentionPolicyParams struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 *bool
	IsMonthlyBackupImmutable               *bool
	IsWeeklyBackupImmutable                *bool
	IsAdhocBackupImmutable                 *bool
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
	AccountName         string
	BackupVaultID       string
	VolumeUUID          string
	BackupName          string
	Description         string
	SnapshotID          string
	BackupType          string
	LocationID          string
	XCorrelationID      string
	UseExistingSnapshot bool
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

type DeleteBackupParams struct {
	AccountName     string
	BackupVaultUUID string
	BackupUUID      string
}

type UpdateBackupParams struct {
	AccountName     string
	BackupVaultUUID string
	BackupUUID      string
	Description     string
	State           *string
	StateDetails    *string
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
