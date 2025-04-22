package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// ClusterClient describes a cluster client
type ClusterClient interface { // generate:mock
	NodesGet(params *NodesGetParams, ucbf UserCallbackFunc[[]*Node]) error
}

type clusterClient struct {
	api cluster.ClientService
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
