package ontap_rest

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	securitypriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

var (
	deleteClusterPeerTimeout = 10 * time.Second
)

// ClusterClient describes a cluster client
type ClusterClient interface { // generate:mock
	NodesGet(params *NodesGetParams, ucbf UserCallbackFunc[[]*Node]) error
	GetONTAPVersion() (*string, error)
	ClusterPeersList() ([]*ClusterPeerResponse, error)
	ClusterPeerCreate(params ClusterPeerCreateParams) (*ClusterPeerCreateResponse, error)
	ClusterPeerAccept(params ClusterPeerCreateParams) (*ClusterPeerCreateResponse, error)
	ClusterPeerDelete(clusterPeerID string) error
	ClusterPeerGet(clusterPeerID string) (*ClusterPeerResponse, error)
	ScheduleCreate(params *ScheduleCreateParams) error
	ScheduleCollectionGet(sfp *ScheduleCollectionGetParams, ucbf UserCallbackFunc[[]*Schedule]) error
	GetJob(UUID string) (*cluster.JobGetOK, error)
}

type clusterClient struct {
	api     cluster.ClientService
	apiPriv *securitypriv.ClientService
}

var paginateNodesGet = _paginate[[]*Node]

// NodesGet invokes pkg/ontap-rest/client/cluster/Client.NodesGet
func (cc clusterClient) NodesGet(params *NodesGetParams, ucbf UserCallbackFunc[[]*Node]) error {
	otParams := nodesGetParamsToONTAP(params)
	return paginateNodesGet(func(next string) ([]*Node, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := cc.api.NodesGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		nodes := make([]*Node, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, res := range rsp.Payload.NodeResponseInlineRecords {
			nodes[i] = &Node{NodeResponseInlineRecordsInlineArrayItem: *res}
		}

		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return nodes, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}
		return nodes, "", nil
	}, ucbf)
}

// GetONTAPVersion returns the ONTAP version
func (cc clusterClient) GetONTAPVersion() (*string, error) {
	cluster, err := cc.api.ClusterGet(cluster.NewClusterGetParams().WithFields([]string{"version"}), nil)
	if err != nil {
		return nil, err
	}
	return cluster.Payload.Version.Full, nil
}

// ClusterPeersList returns all cluster peers for the specific host
func (cc *clusterClient) ClusterPeersList() ([]*ClusterPeerResponse, error) {
	resp, err := cc.api.ClusterPeerCollectionGet(getListClusterPeerParams(), nil)
	if err != nil {
		return nil, err
	}
	clusterPeers := convertListClusterPeerFromREST(resp)
	return clusterPeers, nil
}

// ClusterPeerCreate creates a cluster peer for the specific host
func (cc *clusterClient) ClusterPeerCreate(params ClusterPeerCreateParams) (*ClusterPeerCreateResponse, error) {
	resp, err := (*cc.apiPriv).ClusterPeerCreate(clusterPeerToONTAPCreate(params))
	if err != nil {
		return nil, err
	}
	return convertClusterPeerCreateFromREST(resp), nil
}

func (cc *clusterClient) ClusterPeerAccept(params ClusterPeerCreateParams) (*ClusterPeerCreateResponse, error) {
	resp, err := (*cc.apiPriv).ClusterPeerCreate(clusterPeerToONTAPAccept(params))
	if err != nil {
		return nil, err
	}
	return convertClusterPeerCreateFromREST(resp), nil
}

// ClusterPeerDelete deletes a cluster peer for the specific host
func (cc *clusterClient) ClusterPeerDelete(clusterPeerID string) error {
	_, err := cc.api.ClusterPeerDelete(clusterPeerIDToONTAPDelete(clusterPeerID, deleteClusterPeerTimeout), nil)
	if err != nil {
		return err
	}
	return nil
}

// ClusterPeerGet gets a single cluster peer by clusterPeerID
func (cc *clusterClient) ClusterPeerGet(clusterPeerID string) (*ClusterPeerResponse, error) {
	response, err := cc.api.ClusterPeerGet(clusterPeerIDToONTAPGet(clusterPeerID), nil)
	if err != nil {
		return nil, err
	}
	return convertClusterPeerFromREST(response), nil
}

func getListClusterPeerParams() *cluster.ClusterPeerCollectionGetParams {
	return cluster.NewClusterPeerCollectionGetParams().
		WithFields([]string{"authentication", "name", "remote", "status", "uuid"})
}

// ScheduleCreate invokes pkg/ontap-rest/client/cluster/Client.ScheduleCreate
func (cc *clusterClient) ScheduleCreate(params *ScheduleCreateParams) error {
	_, err := cc.api.ScheduleCreate(scheduleCreateParamsToONTAP(params), nil)
	if err != nil {
		return err
	}
	return nil
}

var paginateScheduleCollectionGet = _paginate[[]*Schedule]

// ScheduleCollectionGet invokes pkg/ontap-rest/client/cluster/Client.ScheduleCollectionGet
func (cc *clusterClient) ScheduleCollectionGet(sfp *ScheduleCollectionGetParams, ucbf UserCallbackFunc[[]*Schedule]) error {
	if sfp == nil || sfp.Name == "" {
		return errors.New("no name filter provided for ScheduleCollectionGet")
	}
	otParams := scheduleCollectionGetParamsToONTAP(sfp)

	return paginateScheduleCollectionGet(func(next string) ([]*Schedule, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := cc.api.ScheduleCollectionGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*Schedule, nillable.FromPointer(rsp.Payload.NumRecords))
		for i, s := range rsp.Payload.ScheduleResponseInlineRecords {
			resp[i] = &Schedule{Schedule: *s}
		}
		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, ucbf)
}

// GetJob returns the ONTAP Job
func (cc clusterClient) GetJob(UUID string) (*cluster.JobGetOK, error) {
	params := cluster.NewJobGetParams().WithUUID(UUID).WithFields([]string{"*", "node.name"})
	job, err := cc.api.JobGet(params, nil)
	if err != nil {
		return nil, err
	}
	return job, nil
}
