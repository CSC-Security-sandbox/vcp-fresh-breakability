package ontap_rest

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	securitypriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
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
	ClusterPeerDelete(clusterPeerID string) error
	ClusterPeerGet(clusterPeerID string) (*ClusterPeerResponse, error)
}

type clusterClient struct {
	api     cluster.ClientService
	apiPriv securitypriv.ClientService
}

var paginateNodesGet = _paginate[[]*Node]

// NodesGet invokes pkg/ontap-rest/client/cluster/Client.NodesGet
func (c clusterClient) NodesGet(params *NodesGetParams, ucbf UserCallbackFunc[[]*Node]) error {
	otParams := nodesGetParamsToONTAP(params)
	return paginateNodesGet(func(next string) ([]*Node, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := c.api.NodesGet(otParams, nil)
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
func (c clusterClient) GetONTAPVersion() (*string, error) {
	cluster, err := c.api.ClusterGet(cluster.NewClusterGetParams().WithFields([]string{"version"}), nil)
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
	resp, err := cc.apiPriv.ClusterPeerCreate(clusterPeerToONTAPCreate(params))
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
