package activities

import (
	"context"

	"github.com/go-openapi/strfmt"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
)

var (
	IpSpace = env.GetString("VSA_IC_LIF_IPSPACE", "Gcnv")
)

const clusterPeerAvailable = "available"

type ClusterPeerActivity struct {
	SE database.Storage
}

func (j *ClusterPeerActivity) AcceptClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, node *models.Node) (*commonparams.ClusterPeerParams, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	var expiryTime *strfmt.DateTime
	if params.ExpiryTime != nil {
		convertedTime := strfmt.DateTime(*params.ExpiryTime)
		expiryTime = &convertedTime
	}
	clusterPeers, err := provider.ListClusterPeers()
	if err != nil {
		return nil, err
	}
	for _, peer := range clusterPeers {
		if peer.PeerClusterName == params.PeerName && areIPsMatching(peer.PeerAddresses, params.PeerAddresses) && peer.Availability == clusterPeerAvailable {
			params.UUID = peer.ExternalUUID
			return params, nil
		}
	}

	clusterPeer, err := provider.AcceptClusterPeer(vsa.CreateClusterPeerParams{
		PeerAddresses: params.PeerAddresses,
		PeerName:      params.PeerName,
		Passphrase:    params.Passphrase,
		ExpiryTime:    expiryTime,
		IPSpace:       IpSpace,
	})
	if err != nil {
		return nil, err
	}
	params.UUID = clusterPeer.ExternalUUID
	return params, nil
}

func areIPsMatching(existingIPs, newIPs []string) bool {
	ipSet := make(map[string]struct{})
	for _, ip := range existingIPs {
		ipSet[ip] = struct{}{}
	}
	for _, ip := range newIPs {
		if _, exists := ipSet[ip]; !exists {
			return false
		}
	}
	return true
}

func (j *ClusterPeerActivity) CreateClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, node *models.Node) (*commonparams.ClusterPeerParams, error) {
	return CreateClusterPeer(ctx, params, node)
}

func CreateClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, node *models.Node) (*commonparams.ClusterPeerParams, error) {
	provider, err := GetProviderByNode(ctx, node)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	clusterPeers, err := provider.ListClusterPeers()
	if err != nil {
		return nil, err
	}
	for _, peer := range clusterPeers {
		if peer.PeerClusterName == params.PeerName && areIPsMatching(peer.PeerAddresses, params.PeerAddresses) && peer.Availability == clusterPeerAvailable {
			params.UUID = peer.ExternalUUID
			return params, nil
		}
	}
	var expiryTime *strfmt.DateTime
	if params.ExpiryTime != nil {
		convertedTime := strfmt.DateTime(*params.ExpiryTime)
		expiryTime = &convertedTime
	}
	clusterPeer, err := provider.CreateClusterPeer(vsa.CreateClusterPeerParams{
		PeerAddresses: params.PeerAddresses,
		PeerName:      params.PeerName,
		ExpiryTime:    expiryTime,
		IPSpace:       IpSpace,
	})
	if err != nil {
		return nil, err
	}
	params.UUID = clusterPeer.ExternalUUID
	params.Passphrase = (*string)(clusterPeer.Passphrase)
	return params, nil
}
