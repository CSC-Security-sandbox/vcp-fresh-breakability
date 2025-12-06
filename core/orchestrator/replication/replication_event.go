package replication

import (
	"context"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
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
	SourceRegion             string                       `json:"SourceRegion,omitempty"`
	DestinationLocationID    string                       `json:"DestinationLocationID,omitempty"`
	DestinationRegion        string                       `json:"DestinationRegion,omitempty"`
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
	Description   *string                                `json:"description,omitempty"`
	VolumeID      string                                 `json:"volumeID,omitempty"`
	ShareName     string                                 `json:"shareName,omitempty"`
	StoragePool   *string                                `json:"storagePool"`
	TieringPolicy *googleproxyclient.TieringPolicyV1beta `json:"tieringPolicy,omitempty"`
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
	Operation        *common.Operations
}
type StopReplicationResult struct {
	Ctx                       context.Context
	Event                     *StopReplicationEvent
	EventBytes                []byte
	DstBasePath               *string
	SrcBasePath               *string
	DstProjectNumber          *string
	SrcProjectNumber          *string
	DstJwtToken               *string
	SrcJwtToken               *string
	DstReplication            *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume                 *googleproxyclient.VolumeV1beta
	SrcVolume                 *googleproxyclient.VolumeV1beta
	Error                     error
	JobId                     *string
	DbVolReplication          *datamodel.VolumeReplication
	CorrelationID             *string
	IsHybridReplicationVolume bool
	IsSrcForHybridReplication bool
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
	AccountID                int64   `json:"AccountID,omitempty"`
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
	Labels              map[string]string
}

type ResumeReplicationResult struct {
	Ctx                       context.Context
	Event                     *ResumeReplicationEvent
	EventBytes                []byte
	DstBasePath               *string
	SrcBasePath               *string
	DstProjectNumber          *string
	SrcProjectNumber          *string
	DstJwtToken               *string
	SrcJwtToken               *string
	DstReplication            *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume                 *googleproxyclient.VolumeV1beta
	SrcVolume                 *googleproxyclient.VolumeV1beta
	Error                     error
	JobId                     *string
	DbVolReplication          *datamodel.VolumeReplication
	IsHybridReplicationVolume bool
	IsSrcForHybridReplication bool
}

type DeleteReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
}

type DeleteReplicationResult struct {
	Ctx                       context.Context
	Event                     *DeleteReplicationEvent
	DstBasePath               *string
	SrcBasePath               *string
	DstProjectNumber          *string
	SrcProjectNumber          *string
	DstJwtToken               *string
	SrcJwtToken               *string
	DstReplication            *googleproxyclient.VolumeReplicationInternalV1beta
	DstVolume                 *googleproxyclient.VolumeV1beta
	Error                     error
	JobId                     string
	CorrelationID             *string
	IsHybridReplicationVolume bool
	CleanupClusterPeering     bool
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

type ReverseReplicationEvent struct {
	ReplicationEventBase
	CommonReplicationEventParams
}

type ReverseReplicationResult struct {
	Ctx              context.Context
	Event            *ReverseReplicationEvent
	EventBytes       []byte
	DstBasePath      *string
	SrcBasePath      *string
	DstProjectNumber *string
	SrcProjectNumber *string
	DstJwtToken      *string
	SrcJwtToken      *string
	DstReplication   *googleproxyclient.VolumeReplicationInternalV1beta
	NewDstVolume     *googleproxyclient.VolumeV1beta
	NewSrcVolume     *googleproxyclient.VolumeV1beta
	Error            error
	JobId            *string
	DBUpdateJobId    *string
	DbVolReplication *datamodel.VolumeReplication
	// ReplicationDetails stores the fetched replication details from destination
	ReplicationDetails *vsa.VolumeReplication
	NodeProvider       *models.Node
}

type ReverseHybridReplicationResult struct {
	Event                         *ReverseReplicationEvent
	DbVolReplication              *datamodel.VolumeReplication
	ClusterPeeringRow             *datamodel.ClusterPeerings
	ClusterPeer                   *vsa.ClusterPeer
	HybridReplicationUserCommands []string
	DstBasePath                   *string
	SrcBasePath                   *string
	DstProjectNumber              *string
	SrcProjectNumber              *string
	DstJwtToken                   *string
	SrcJwtToken                   *string
	NodeProvider                  *models.Node
	FirstProblemStateTime         *time.Time
	JobId                         *string
	IsHybridReplicationVolume     bool
	IsSrcForHybridReplication     bool
	HydrateState                  models.VolumeReplicationHydrateState
	HydrateType                   models.HybridReplicationHydrateType
}

type UpdateVolumeReplicationAttributesEvent struct {
	ReplicationEventBase
	UpdateVolumeReplicationAttributesParams *models.UpdateVolumeReplicationAttributesParams
}

type UpdateVolumeReplicationAttributesResult struct {
	Ctx                context.Context
	Event              *UpdateVolumeReplicationAttributesEvent
	EventBytes         []byte
	DbVolReplication   *datamodel.VolumeReplication
	ReplicationDetails *vsa.VolumeReplication
	Error              error
}

type CreateHybridReplicationResult struct {
	Ctx                            context.Context
	DestinationVolume              *datamodel.Volume
	DestinationRegion              string
	DestinationZone                string
	DestinationProjectNumber       string
	HybridReplicationParameters    *models.HybridReplicationParameters
	CorrelationID                  *string
	RequestID                      *string
	DstBasePath                    *string
	DstJwtToken                    *string
	JobId                          *string
	NodeProvider                   *models.Node
	ClusterPeeringRow              *datamodel.ClusterPeerings
	DbVolReplication               *datamodel.VolumeReplication
	ClusterPeer                    *vsa.ClusterPeer
	CurrentHydrateState            models.VolumeReplicationHydrateState
	DstReplication                 *googleproxyclient.VolumeReplicationInternalV1beta
	ReplicationCreateResponseONTAP *vsa.VolumeReplication
	Operation                      *common.Operations
	DbNodes                        []*datamodel.Node
	FirstProblemStateTime          *time.Time
}
