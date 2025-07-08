package vsa

import (
	"fmt"
	"time"

	"github.com/go-openapi/strfmt"
	ontaprestmodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type ProviderDetails struct {
	IPAddress          string   `json:"ipAddress"`
	IPAddresses        []string `json:"ipAddresses"`
	UserName           string   `json:"userName"`
	Password           string   `json:"password"`
	Port               *int     `json:"port"`
	UseHTTPS           bool     `json:"useHTTPS"`
	Protocol           string   `json:"protocol"`
	InsecureSkipVerify bool     `json:"insecureSkipVerify"`
}

type CreateSvmParams struct {
	Name      string
	Protocols Protocols
}

type Protocols struct {
	EnableIscsi bool
}

type ProviderResponse struct {
	Name         string
	ExternalUUID string
}

type VolumeResponse struct {
	ProviderResponse
	AvailableSpace int64
	Size           int64
	State          string
}

type CreateLifParams struct {
	Name      string
	SvmName   string
	IpAddress string
	NodeName  string
	HomePort  string
}

type Lif struct {
	Name         string
	ExternalUUID string
	IPAddress    string
	SubnetMask   string
}

// SvmPeer describes SvmPeer information retrieved from ONTAP
type SvmPeer struct {
	UUID            string
	Applications    []string
	State           string
	LocalSvmName    string
	LocalSvmUUID    string
	PeerSvmName     string
	PeerSvmUUID     string
	PeerClusterName string
}

type CreateNetworkIPRouteParams struct {
	SvmName string
	Gateway string
}

type Node struct {
	Name         string
	State        string
	ExternalUUID string
}

type Aggregate struct {
	Name  string
	State string
}

type RestoreFromSnapshotParams struct {
	ParentVolumeExternalUUID string // External UUID of the source/parent volume
	ParentVolumeName         string // Name of the Volume
	SnapshotUUID             string // UUID of the snapshot to restore from
	SnapshotName             string // Name of the snapshot to restore from
	ParentVolumeSvmName      string // Name of the SVM where the parent volume resides
	// Add more fields as needed
}

type CreateVolumeParams struct {
	VolumeName         string
	SvmName            string
	AggregateName      string
	Size               int64
	VolumeType         string
	SnapshotPolicyName string
	// Reference to a snapshot for restore/clone
	RestoreFromSnapshot *RestoreFromSnapshotParams // Optional: parameters for restoring from a snapshot
	TieringPolicy       *TieringPolicy
}

// TieringPolicy describes the auto tiering policy for a volume
type TieringPolicy struct {
	CoolnessPeriod            int64
	CoolAccessRetrievalPolicy string
	CoolAccessTieringPolicy   string
}

type UpdateVolumeParams struct {
	UUID               string
	VolumeName         string
	SvmName            string
	AggregateName      string
	Size               int64
	SnapshotPolicyName string
	InitiateSplit      bool // Indicates whether to initiate a split for volume restore or clone
	TieringPolicy      *TieringPolicy
}

type GetVolumeParams struct {
	UUID       string
	VolumeName string
	SvmName    string
}

type IgroupCreateParams struct {
	IgroupName string
	SvmName    string
	OsType     string
	Initiator  []string
}

type IgroupModifyParams struct {
	IgroupName string
	SvmName    string
	Initiator  []string
}

type IgroupAddInitiator struct {
	Initiator  []string
	IgroupUUID string
}

type IgroupDeleteInitiator struct {
	InitiatorName string
	IgroupUUID    string
}

type LunCreateParams struct {
	LunName    string
	SvmName    string
	OsType     string
	VolumeName string
	Size       int64
}

type LunGetParams struct {
	SvmName    string
	VolumeName string
	LunName    string
}

type LunUpdateParams struct {
	UUID       string
	LunName    string
	VolumeName string
	SvmName    string
	Size       int64
}

type LunMapCreateParams struct {
	LunName    string
	SvmName    string
	IGroupName []string
}

type LunMapDeleteParams struct {
	LunUUID    string
	IGroupUUID string
}

// CreateVolumeReplicationParams describes parameters supplied to Provider.CreateVolumeReplication
type CreateVolumeReplicationParams struct {
	VolumeReplication *VolumeReplication
	ReverseResync     bool
}

// DeleteVolumeReplicationParams describes parameters supplied to Provider.DeleteVolumeReplication
type DeleteVolumeReplicationParams struct {
	VolumeReplication *VolumeReplication
	DestinationOnly   *bool
	SourceOnly        *bool
}

// VolumeReplication describes a Volume Replication relationship object in the cloud volumes model
type VolumeReplication struct {
	UUID                          string
	AccountUUID                   string
	AccountName                   string
	ClusterPeerID                 *uint64
	Name                          *string
	EndpointType                  string
	RemoteRegion                  string
	RemoteResourceID              string
	SourceHostName                string
	SourceSVMName                 string
	SourceVolumeName              string
	DestinationHostName           string
	DestinationSVMName            string
	DestinationVolumeName         string
	DestinationVolumeUUID         string
	DestinationVolumeExternalUUID string
	ReplicationPolicy             string
	ReplicationSchedule           string
	LifeCycleState                string
	LifeCycleStateDetails         string
	MirrorState                   string
	RelationshipStatus            string
	Healthy                       bool
	UnhealthyReason               string
	Volume                        *Volume
	Jobs                          []*ontaprestmodel.Job
	TotalTransferBytes            int64
	TotalTransferTimeSecs         int64
	LastTransferSize              int64
	LastTransferError             string
	LastTransferDuration          int64
	LastTransferEndTime           *time.Time
	LagTime                       int64
	Mounted                       bool
	MaxTransferRate               int64
	RelationshipID                string
	CreatedAt                     time.Time
	UpdatedAt                     time.Time
	DeletedAt                     *time.Time
	Description                   *string
	DestinationOnly               *bool
	SourceOnly                    *bool
	Force                         *bool
	Tags                          *string
	ExternalUUID                  string
	DestinationVolumeStorageClass string
	SkipPeeringCleanup            *bool
	ReplicationType               string
	TotalProgress                 int64
	CurrentTransferType           string
	CurrentTransferError          string
	ProgressLastUpdated           *time.Time
}

// SnapmirrorDestination describes SnapmirrorDestination information retrieved from ONTAP
type SnapmirrorDestination struct {
	DestinationPath    string
	DestinationSVMName string
	SourcePath         string
	SourceSVMName      string
	RelationshipUUID   string
}

type Volume struct {
	ontaprestmodel.Volume
	ExternalUUID      string
	IsOnPremMigration bool
}

// SourcePath returns the source path of an ONTAP snapmirror relationship in a <svm_name>:<volume_name> format
func (v *VolumeReplication) SourcePath() string {
	return fmt.Sprintf("%s:%s", v.SourceSVMName, v.SourceVolumeName)
}

// DestinationPath returns the destination path of an ONTAP snapmirror relationship in a <svm_name>:<volume_name> format
func (v *VolumeReplication) DestinationPath() string {
	return fmt.Sprintf("%s:%s", v.DestinationSVMName, v.DestinationVolumeName)
}

type CreateClusterPeerParams struct {
	PeerAddresses       []string
	PeerName            string
	IPSpace             string
	VolumeUUID          string
	AccountUUID         string
	InterclusterLifList []string
	ExpiryTime          *strfmt.DateTime
	GeneratePassphrase  bool
	Passphrase          *string
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

// InterclusterLif describes the storage model for intercluster LIFs
type InterclusterLif struct {
	Name     string
	Address  ontaprestmodel.IPAddress
	NetMask  string
	IPSpace  string
	NodeUUID string
	UUID     string
}

type Snapshot struct {
	ontaprestmodel.Snapshot
	ExternalUUID           string
	ExternalVersionUUID    string
	ExternalVolumeUUID     string
	SizeInBytes            int64
	LogicalSizeUsedInBytes int64
	CreationTime           *strfmt.DateTime
	Type                   string
}

type CreateSnapshotParams struct {
	VolumeUUID string
	Name       string
	Comment    string
}

type SnapshotProviderResponse struct {
	ProviderResponse
	SizeInBytes        int64
	LogicalSizeInBytes int64
}

type LunResponse struct {
	ProviderResponse
	SerialNumber string
}

type CloudTargetCreateParams struct {
	Name      *string
	Container *string
}

type CloudTargetModifyParams struct {
	Name *string
}

type CloudTargeCollectiontGetParams struct {
	Name *string
}

type CloudTarget struct {
	Name *string
	UUID *string
}

// SnapshotPolicy describes a snapshot policy in the cloud volume model
type SnapshotPolicy struct {
	Name      string
	Comment   string
	IsEnabled bool
	Schedules []*SnapshotPolicySchedule
}

// SnapshotPolicySchedule describes a snapshot policy schedule in the cloud volume model
type SnapshotPolicySchedule struct {
	Schedule        *Schedule
	Prefix          string
	Count           int64
	SnapmirrorLabel string
}

// Schedule describes a schedule in the cloud volume model
type Schedule struct {
	Name        string
	Description string
	Type        string
	Months      []int
	DaysOfMonth []int
	DaysOfWeek  []int
	Hours       []int
	Minutes     []int
}

type OntapAsyncResponse struct {
	JobUUID string
}

type OntapJob struct {
	UUID  string
	State string
	Error *OntapError
}

type OntapError struct {
	Code    string
	Message string
}

// UpdateSnapshotPolicyParams describes parameters supplied to Provider.UpdateSnapshotPolicy
type UpdateSnapshotPolicyParams struct {
	CurrentSnapshotPolicy  *SnapshotPolicy
	UpdatingSnapshotPolicy *SnapshotPolicy
}

// SnapshotPolicyScheduleUpdate describes a snapshot policy schedule update payload
type SnapshotPolicyScheduleUpdate struct {
	Action                 action
	SnapshotPolicySchedule SnapshotPolicySchedule
}
