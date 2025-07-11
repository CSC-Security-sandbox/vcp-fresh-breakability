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
	Nodes          []*datamodel.Node
	Password       string
	SecretID       string
	CertificateID  string
	DeploymentName string
}

// CreateNodeForProvider creates a node for a given provider using the provided information.
func _createNodeForProvider(inp NodeProviderInput) *models.Node {
	endpointAddressToHostNameMap := make(map[string]string)
	if AuthType == USER_CERTIFICATE {
		for _, node := range inp.Nodes {
			if node.EndpointAddress != "" {
				endpointAddressToHostNameMap[node.EndpointAddress] = node.HostDNSName
			}
		}
		return &models.Node{
			EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
			DeploymentName:                 inp.DeploymentName,
			CertificateID:                  inp.CertificateID,
			SecretID:                       inp.SecretID,
		}
	}

	for _, node := range inp.Nodes {
		if node.EndpointAddress != "" {
			endpointAddressToHostNameMap[node.EndpointAddress] = node.EndpointAddress
		}
	}

	return &models.Node{
		EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
		Password:                       inp.Password,
		DeploymentName:                 inp.DeploymentName,
		SecretID:                       inp.SecretID,
	}
}
