package vsa

import (
	"fmt"

	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

const (
	stateUp = "up"
)

func (rc *OntapRestProvider) AreAllNodeUpAndRunning() (bool, error) {
	// Retrieve nodes
	nodes, err := rc.GetNodes()
	if err != nil {
		rc.Logger.Errorf("Failed to retrieve nodes: %v\n", err)
		return false, err
	}

	// Check if the expected number of nodes is present
	if len(nodes) < expectedNodeCount {
		return false, fmt.Errorf("expected %d nodes, got %d", expectedNodeCount, len(nodes))
	}

	// Verify all nodes are in the "up" state
	for _, node := range nodes {
		if node.State != stateUp {
			return false, fmt.Errorf("node %s is not up", node.Name)
		}
	}

	return true, nil
}

func (rc *OntapRestProvider) GetNodes() ([]*Node, error) {
	var resultNodes []*Node
	// Call the NodesGet method with proper parameters
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Cluster().NodesGet(&ontapRest.NodesGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"name", "uuid", "state"},
		},
	}, func(apiNodes []*ontapRest.Node) error {
		for _, apiNode := range apiNodes {
			// Append converted nodes to the result slice
			resultNodes = append(resultNodes, &Node{
				Name:         nillable.FromPointer(apiNode.Name),
				ExternalUUID: apiNode.UUID.String(),
				State:        nillable.FromPointer(apiNode.State),
			})
		}
		return nil
	})
	// Handle errors from the NodesGet call
	if err != nil {
		return nil, err
	}
	return resultNodes, nil
}

func (rc *OntapRestProvider) GetNodeByName(name string) (*Node, error) {
	var resultNode *Node
	// Call the NodesGet method with proper parameters
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Cluster().NodesGet(&ontapRest.NodesGetParams{
		BaseParams: ontapRest.BaseParams{
			Fields: []string{"name", "uuid"},
		},
	}, func(apiNodes []*ontapRest.Node) error {
		for _, apiNode := range apiNodes {
			// Check if the node name matches the provided name
			if nillable.FromPointer(apiNode.Name) == name {
				resultNode = &Node{
					Name:         nillable.FromPointer(apiNode.Name),
					ExternalUUID: apiNode.UUID.String(),
				}
				return nil
			}
		}
		return nil
	})

	// Handle errors from the NodesGet call
	if err != nil {
		return nil, err
	}

	if resultNode == nil {
		// If no node was found, return an error
		return nil, fmt.Errorf("node with name %s not found", name)
	}

	return resultNode, nil
}

func (rc *OntapRestProvider) GetONTAPVersion() (*string, error) {
	client := getOntapClientFunc(rc.ClientParams)
	version, err := client.Cluster().GetONTAPVersion()
	if err != nil {
		return nil, err
	}
	return version, nil
}
