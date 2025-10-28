package flexcache

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
)

type CreateFlexCacheResult struct {
	DBVolume          *datamodel.Volume
	Node              *models.Node
	ClusterPeer       *vsa.ClusterPeer
	SVMPeer           *vsa.SvmPeer
	VolumeResponse    *vsa.VolumeResponse
	JobInput          *JobActivityInput
	ActiveJobType     models.JobType
	ErrorTrackingID   int
	ErrorMessage      string
	ClusterPeerAction Action
	SVMPeerAction     Action
}

type JobActivityInput struct {
	ResourceName  string
	ResourceUUID  string
	AccountID     int64
	CorrelationID string
	RequestID     string
	WorkflowID    string
	Metadata      map[string]interface{}
}

type DeleteFlexCacheResult struct {
	DBVolume           *datamodel.Volume
	Node               *models.Node
	UnmountJobResponse *vsa.OntapAsyncResponse
	DeleteJobResponse  *vsa.OntapAsyncResponse
}
