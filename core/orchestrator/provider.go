package orchestrator

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

var GetProviderByNode = _getProviderByNode

func _getProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	var password string
	if node.SecretID != "" {
		password = activities.GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
	} else {
		password = node.Password
	}

	// if ipAddress in empty, populate it with the node's endpoint address
	if len(node.EndpointAddresses) == 0 {
		if node.EndpointAddress == "" {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterNodeIPAddressNotFound, errors.New("node endpoint address is empty"))
		}
		node.EndpointAddresses = []string{node.EndpointAddress}
	}

	return vsa.NewProvider(ctx, vsa.ProviderDetails{
		IPAddresses: node.EndpointAddresses,
		UserName:    node.Username,
		Password:    password,
		// TODO : need to fix once we have certs
		InsecureSkipVerify: true,
	}), nil
}
