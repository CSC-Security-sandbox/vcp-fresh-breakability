package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
)

var CreateNodeForProvider = _createNodeForProvider

// PrepareOperationID constructs a GCP operation ID from the provided project number, location ID, and job ID.
func PrepareOperationID(projectNumber, locationId, jobId string) string {
	if projectNumber == "" || locationId == "" || jobId == "" {
		return ""
	}
	return "/v1beta/projects/" + projectNumber + "/locations/" + locationId + "/operations/" + jobId
}

type NodeProviderInput struct {
	Nodes    []*datamodel.Node
	Username string
	Password string
	SecretID string
}

// CreateNodeForProvider creates a node for a given provider using the provided information.
func _createNodeForProvider(inp NodeProviderInput) *models.Node {
	ipAddrs := make([]string, 0)
	for _, node := range inp.Nodes {
		if node.EndpointAddress != "" {
			ipAddrs = append(ipAddrs, node.EndpointAddress)
		}
	}

	node := &models.Node{
		EndpointAddresses: ipAddrs,
		Username:          inp.Username,
		Password:          inp.Password,
		SecretID:          inp.SecretID,
	}
	return node
}
