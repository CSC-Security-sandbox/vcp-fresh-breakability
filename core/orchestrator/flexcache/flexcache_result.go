package flexcache

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
)

type CreateFlexCacheResult struct {
	DBVolume       *datamodel.Volume
	Node           *models.Node
	ClusterPeer    *vsa.ClusterPeer
	VolumeResponse *vsa.VolumeResponse
}

type DeleteFlexCacheResult struct {
	DBVolume           *datamodel.Volume
	Node               *models.Node
	UnmountJobResponse *vsa.OntapAsyncResponse
	DeleteJobResponse  *vsa.OntapAsyncResponse
}
