package models

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

type HybridReplicationHydrateType string
type VolumeReplicationHydrateState string
type HybridReplicationParametersReplicationType string

type VolumeReplicationUpdateMaskRequest struct {
	State                 VolumeReplicationHydrateState              `json:"state"`
	HybridReplicationType HybridReplicationParametersReplicationType `json:"hybridReplicationType,omitempty"`
}

const (
	OntapSnapmirrored                  = "snapmirrored"
	OntapUninitialized                 = "uninitialized"
	OntapBrokenOff                     = "broken_off"
	SnapmirrorRelationshipSuccess      = "success"
	SnapmirrorRelationshipFinalizing   = "finalizing"
	SnapmirrorRelationshipIdle         = "idle"
	SnapmirrorRelationshipTransferring = "transferring"
	SnapmirrorRelationshipFailed       = "failed"
	SnapmirrorRelationshipAborted      = "aborted"
	SnapmirrorRelationshipQueued       = "queued"
	SnapmirrorRelationshipHardAborted  = "hard_aborted"

	DstEndpoint = "dst"
	SrcEndpoint = "src"
)

var (
	VolumeReplicationHydrateStateUnspecified           VolumeReplicationHydrateState = "UNSPECIFIED"
	VolumeReplicationHydrateStateCreating              VolumeReplicationHydrateState = "CREATING"
	VolumeReplicationHydrateStateReady                 VolumeReplicationHydrateState = "READY"
	VolumeReplicationHydrateStateUpdating              VolumeReplicationHydrateState = "UPDATING"
	VolumeReplicationHydrateStateDeleting              VolumeReplicationHydrateState = "DELETING"
	VolumeReplicationHydrateStateError                 VolumeReplicationHydrateState = "ERROR"
	VolumeReplicationHydrateStatePendingClusterPeering VolumeReplicationHydrateState = "PENDING_CLUSTER_PEERING"
	VolumeReplicationHydrateStatePendingSvmPeering     VolumeReplicationHydrateState = "PENDING_SVM_PEERING"
	VolumeReplicationHydrateStateExternalManaged       VolumeReplicationHydrateState = "EXTERNALLY_MANAGED_REPLICATION"
)

var (
	HybridReplicationParametersReplicationTypeMIGRATION   HybridReplicationParametersReplicationType = "MIGRATION"
	HybridReplicationParametersReplicationTypeUNSPECIFIED HybridReplicationParametersReplicationType = "REPLICATION_TYPE_UNSPECIFIED"
	HybridReplicationParametersReplicationTypeCONTINUOUS  HybridReplicationParametersReplicationType = "CONTINUOUS_REPLICATION"
	HybridReplicationParametersReplicationTypeONPREM      HybridReplicationParametersReplicationType = "ONPREM_REPLICATION"
	HybridReplicationParametersReplicationTypeREVERSE     HybridReplicationParametersReplicationType = "REVERSE_ONPREM_REPLICATION"
)

type VolumeReplication struct {
	BaseModel
	Name                        string
	Description                 string
	State                       string
	StateDetails                string
	Uri                         string
	RemoteUri                   string
	ReplicationAttributes       *ReplicationDetails
	MirrorState                 *string
	RelationshipStatus          *string
	TotalProgress               int64
	TotalTransferBytes          int64
	TotalTransferTimeSecs       int64
	LastTransferSize            int64
	LastTransferError           string
	LastTransferDuration        int64
	LastTransferEndTime         *time.Time
	ProgressLastUpdated         *time.Time
	LastUpdatedFromOntap        time.Time
	LagTime                     int64
	AccountID                   int64
	Account                     *Account
	VolumeID                    int64
	Volume                      *Volume
	Jobs                        []*Job
	Healthy                     bool
	HybridReplicationAttributes *HybridReplicationParameters
}

type ReplicationDetails struct {
	EndpointType               string
	ReplicationType            string
	ReplicationSchedule        string
	SourcePoolUUID             string
	SourceVolumeUUID           string
	SourceRegion               string
	SourceHostName             string
	SourceReplicationUUID      string
	SourceSvmName              string
	SourceVolumeName           string
	DestinationPoolUUID        string
	DestinationVolumeUUID      string
	DestinationRegion          string
	DestinationHostName        string
	DestinationReplicationUUID string
	DestinationSvmName         string
	DestinationVolumeName      string
	ReplicationPolicy          string
	ExternalUUID               string
	Labels                     map[string]string
}

type GcpHydrateCreate struct {
	Requests []Request `json:"requests"`
}

type GcpHydrateDelete struct {
	Names []string `json:"names"`
}

type Request struct {
	Volume      *VolumeHydrateObject      `json:"volume,omitempty"`
	Replication *ReplicationHydrateObject `json:"replication,omitempty"`
	QuotaRule   *QuotaRuleHydrateObject   `json:"quotaRule,omitempty"`
	Snapshot    *HydrateSnapshot          `json:"snapshot,omitempty"`
	Backup      *HydrateBackup            `json:"backup,omitempty"`
	BackupVault *HydrateBackupVault       `json:"backup_vault,omitempty"`
}

type ReplicationHydrateObject struct {
	ResourceId            string                        `json:"name"`
	ReplicationState      string                        `json:"state"`
	HybridReplicationType *HybridReplicationHydrateType `json:"hybridReplicationType,omitempty"`
	Labels                map[string]string             `json:"labels,omitempty"`
}

type QuotaRuleHydrateObject struct {
	ResourceId  string `json:"name"`
	QuotaRuleId string `json:"netapp_uuid"`
}

type CcfeErrorObject struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Status  string `json:"status"`
	Details string `json:"-"`
}

type CcfeErrorResponseObject struct {
	Error *CcfeErrorObject `json:"error"`
}

type VolumeHydrateObject struct {
	ResourceId    string   `json:"name"`
	VolumeId      string   `json:"netapp_uuid"`
	PoolId        string   `json:"storage_pool_id"`
	ServiceLevel  string   `json:"service_level"`
	QuotaInGib    int64    `json:"capacity_gib"`
	Protocols     []string `json:"protocols"`
	State         string   `json:"state"`
	LargeCapacity bool     `json:"large_capacity"`
}

type UpdateVolumeReplicationAttributesParams struct {
	ProjectNumber             string
	LocationId                string
	VolumeReplicationId       string
	VolumeReplicationInternal *gcpgenserver.VolumeReplicationInternalV1beta
}

type UpdateVolumeReplicationStateParams struct {
	ProjectNumber       string
	LocationId          string
	VolumeReplicationId string
	State               string
	StateDetails        string
}

type HybridReplicationParameters struct {
	ResourceID                    string
	ReplicationType               HybridReplicationParametersReplicationType
	PeerVolumeName                string
	PeerClusterName               string
	PeerSvmName                   string
	PeerIPAddresses               []string
	Labels                        map[string]string
	Description                   string
	ClusterLocation               string
	PeeringCommandExpiryTime      *time.Time
	ReplicationSchedule           string
	LargeVolumeConstituentCount   *int32
	HybridReplicationUserCommands []string
}

type HybridReplicationStatus = datamodel.HybridReplicationStatus

type ClusterPeeringStatus = datamodel.ClusterPeeringStatus

const (
	CvpClusterPeeringStatusCREATING              = datamodel.CvpClusterPeeringStatusCREATING
	CvpClusterPeeringStatusPENDINGCLUSTERPEERING = datamodel.CvpClusterPeeringStatusPENDINGCLUSTERPEERING
	CvpClusterPeeringStatusPEERED                = datamodel.CvpClusterPeeringStatusPEERED
	CvpClusterPeeringStatusDELETED               = datamodel.CvpClusterPeeringStatusDELETED
	CvpClusterPeeringStatusERROR                 = datamodel.CvpClusterPeeringStatusERROR
)
const (
	HybridReplicationStatusPendingClusterPeer  = datamodel.HybridReplicationStatusPendingClusterPeer
	HybridReplicationStatusPendingSVMPeer      = datamodel.HybridReplicationStatusPendingSVMPeer
	HybridReplicationStatusSVMPeered           = datamodel.HybridReplicationStatusSVMPeered
	HybridReplicationStatusPeered              = datamodel.HybridReplicationStatusPeered
	HybridReplicationStatusPendingRemoteResync = datamodel.HybridReplicationStatusPendingRemoteResync
	HybridReplicationStatusExternalManaged     = datamodel.HybridReplicationStatusExternalManaged
)
const (
	AuthenticationStateOk      string = "ok"
	AuthenticationStateAbsent  string = "absent"
	AuthenticationStatePending string = "pending"
	AuthenticationStateProblem string = "problem"
)

const (
	AvailabilityAvailable    string = "available"
	AvailabilityPartial      string = "partial"
	AvailabilityUnavailable  string = "unavailable"
	AvailabilityPending      string = "pending"
	AvailabilityUnidentified string = "unidentified"
)
