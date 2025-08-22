package replication

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

type ReplicationEventBase struct {
	common.EventBase
	CCFEUri       string
	CCFERemoteUri string
}

type CreateReplicationEvent struct {
	ReplicationEventBase
	SourceVolume        datamodel.Volume `json:"sourceVolume,omitempty"`
	SourcePool          datamodel.Pool   `json:"sourcePool,omitempty"`
	DestinationPoolName string           `json:"destinationPoolName,omitempty"`

	CreateReplicationParams  *CreateReplicationParamsBody `json:"Body,omitempty"`
	LocationID               string                       `json:"LocationID,omitempty"`
	DestinationLocationID    string                       `json:"DestinationLocationID,omitempty"`
	SourceProjectNumber      string                       `json:"SourceProjectNumber,omitempty"`
	DestinationProjectNumber string                       `json:"DestinationProjectNumber,omitempty"`
	VolumeResourceID         string                       `json:"VolumeResourceID,omitempty"`
	XCorrelationID           *string                      `json:"XCorrelationID,omitempty"`
	RequestUri               string                       `json:"RequestUri,omitempty"`
}

type CreateReplicationParamsBody struct {
	Description                 *string                  `json:"description,omitempty"`
	DestinationVolumeParameters *DestinationVolumeParams `json:"destinationVolumeParameters"`
	ReplicationSchedule         *string                  `json:"replicationSchedule"`
	ResourceID                  *string                  `json:"resourceId"`
	Labels                      map[string]string        `json:"labels,omitempty"`
	HybridReplicationType       *string                  `json:"hybridReplicationType,omitempty"`
}

type DestinationVolumeParams struct {
	Description *string `json:"description,omitempty"`
	VolumeID    string  `json:"volumeID,omitempty"`
	ShareName   string  `json:"shareName,omitempty"`
	StoragePool *string `json:"storagePool"`
}

type CreateReplicationResult struct {
	Ctx              context.Context
	EventBytes       []byte
	Event            *CreateReplicationEvent
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume        *gcpgenserver.VolumeV1beta
	SrcVolume        *gcpgenserver.VolumeV1beta
	DstPool          *googleproxyclient.PoolInternalV1beta
	SrcIps           []string
	DstIps           []string
	Error            error
	Passphrase       *string
	ClusterPeerUUID  *string
	JobId            *string
	SrcNode          *models.Node
	SrcSvm           *string
	DstSvm           *string
	DbVolReplication *datamodel.VolumeReplication
}
type StopReplicationResult struct {
	Ctx              context.Context
	Event            *StopReplicationEvent
	EventBytes       []byte
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume        *googleproxyclient.VolumeV1beta
	SrcVolume        *googleproxyclient.VolumeV1beta
	Error            error
	JobId            *string
	DbVolReplication *datamodel.VolumeReplication
}

type StopReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
	ForceStop bool `json:"forceStop,omitempty"`
}

type CommonReplicationEventParams struct {
	ReplicationModel         *datamodel.VolumeReplication
	SourceProjectNumber      string  `json:"SourceProjectNumber,omitempty"`
	DestinationProjectNumber string  `json:"DestinationProjectNumber,omitempty"`
	XCorrelationID           *string `json:"XCorrelationID,omitempty"`
	VolumeResourceID         string  `json:"VolumeResourceID,omitempty"`
	ReplicationResourceID    string  `json:"ReplicationResourceID,omitempty"`
	Location                 string  `json:"Location,omitempty"`
	Zone                     string  `json:"Zone,omitempty"`
	AccountName              string  `json:"AccountName,omitempty"`
	SrcBasePath              string  `json:"SrcBasePath,omitempty"`
	DstBasePath              string  `json:"DstBasePath,omitempty"`
	SrcToken                 string  `json:"SrcToken,omitempty"`
	DstToken                 string  `json:"DstToken,omitempty"`
}

type ResumeReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
}

type UpdateReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
	ReplicationSchedule *string
	Description         *string
}

type ResumeReplicationResult struct {
	Ctx              context.Context
	Event            *ResumeReplicationEvent
	EventBytes       []byte
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume        *googleproxyclient.VolumeV1beta
	SrcVolume        *googleproxyclient.VolumeV1beta
	Error            error
	JobId            *string
	DbVolReplication *datamodel.VolumeReplication
}

type DeleteReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
}

type DeleteReplicationResult struct {
	Ctx              context.Context
	Event            *DeleteReplicationEvent
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume        *googleproxyclient.VolumeV1beta
	Error            error
	JobId            string
}

type UpdateReplicationResult struct {
	Ctx              context.Context
	Event            *UpdateReplicationEvent
	EventBytes       []byte
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume        *googleproxyclient.VolumeV1beta
	SrcVolume        *googleproxyclient.VolumeV1beta
	Error            error
	JobId            *string
	DbVolReplication *datamodel.VolumeReplication
}
