package models

import "time"

type HybridReplicationHydrateType string
type VolumeReplicationHydrateState string

type VolumeReplicationUpdateMaskRequest struct {
	State                 VolumeReplicationHydrateState `json:"state"`
	HybridReplicationType HybridReplicationHydrateType  `json:"hybridReplicationType,omitempty"`
}

const (
	OntapSnapmirrored                  = "snapmirrored"
	OntapUninitialized                 = "uninitialized"
	OntapBrokenOff                     = "broken-off"
	SnapmirrorRelationshipSuccess      = "success"
	SnapmirrorRelationshipIdle         = "idle"
	SnapmirrorRelationshipTransferring = "transferring"
	SnapmirrorRelationshipFailed       = "failed"
	SnapmirrorRelationshipAborted      = "aborted"
	SnapmirrorRelationshipQueued       = "queued"
	SnapmirrorRelationshipHardAborted  = "hard_aborted"

	DstEndpoint = "dst"
	SrcEndpoint = "src"
)

type VolumeReplication struct {
	BaseModel
	Name                  string
	Description           string
	State                 string
	StateDetails          string
	Uri                   string
	RemoteUri             string
	ReplicationAttributes *ReplicationDetails
	MirrorState           *string
	RelationshipStatus    *string
	TotalProgress         int64
	TotalTransferBytes    int64
	TotalTransferTimeSecs int64
	LastTransferSize      int64
	LastTransferError     string
	LastTransferDuration  int64
	LastTransferEndTime   *time.Time
	ProgressLastUpdated   *time.Time
	LastUpdatedFromOntap  time.Time
	LagTime               int64
	AccountID             int64
	Account               *Account
	VolumeID              int64
	Volume                *Volume
	Jobs                  []*Job
	Healthy               bool
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
	RemoteResourceID           string
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
}

type ReplicationHydrateObject struct {
	ResourceId            string                        `json:"name"`
	ReplicationState      string                        `json:"state"`
	HybridReplicationType *HybridReplicationHydrateType `json:"hybridReplicationType,omitempty"`
	Labels                map[string]string             `json:"labels,omitempty"`
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
