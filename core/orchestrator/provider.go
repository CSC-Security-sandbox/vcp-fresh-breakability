package orchestrator

import (
	"context"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var GetProviderByNode = _getProviderByNode

// _getProviderByNode creates a new vsa.Provider instance using the details from the provided node.
func _getProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	if commonparams.AuthType == commonparams.USER_CERTIFICATE {
		certificate, err := activities.GetCertificateFromCacheOrSecretManager(ctx, node.CertificateID)
		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get certificate for node %s: %v", node.Name, err)
			return nil, err
		}

		return vsa.NewProvider(ctx, vsa.ProviderDetails{
			Hosts: node.EndpointAddressesToHostNameMap,
			Certificate: &vsa.Certificate{
				SignedCertificate:        certificate.SignedCertificate,
				InterMediateCertificates: certificate.InterMediateCertificates,
				CommonName:               certificate.CommonName,
				PrivateKey:               certificate.PrivateKey,
				RootCaCertificate:        certificate.RootCaCertificate,
			},
			InsecureSkipVerify: false,
		}), nil
	}

	var password string
	if commonparams.AuthType == commonparams.USERNAME_PWD_SEC_MGR {
		secret, err := activities.GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get password for node %s: %v", node.Name, err)
			return nil, err
		}
		password = secret
	} else {
		password = node.Password
	}
	// if ipAddress in empty, populate it with the node's endpoint address
	if len(node.EndpointAddressesToHostNameMap) == 0 {
		if node.EndpointAddress == "" {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrVSAClusterNodeIPAddressNotFound, errors.New("node endpoint address is empty"))
		}
		node.EndpointAddressesToHostNameMap[node.EndpointAddress] = node.EndpointAddress
	}

	return vsa.NewProvider(ctx, vsa.ProviderDetails{
		Hosts:              node.EndpointAddressesToHostNameMap,
		Password:           password,
		InsecureSkipVerify: true,
	}), nil
}
