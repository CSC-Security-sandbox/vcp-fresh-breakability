package vsa

import (
	"time"

	"github.com/go-openapi/strfmt"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// CreateClusterPeer creates a cluster peer for the specific host
func (rc *OntapRestProvider) CreateClusterPeer(params CreateClusterPeerParams) (*ClusterPeer, error) {
	createParams := ontapRest.ClusterPeerCreateParams{
		IPAddresses:        params.PeerAddresses,
		Name:               params.PeerName,
		GeneratePassphrase: true,
		IPSpace:            params.IPSpace,
		ExpiryTime:         convertToOntapTime(params.ExpiryTime),
	}
	client := getOntapClientFunc(rc.ClientParams)
	createdClusterPeer, err := client.Cluster().ClusterPeerCreate(createParams)
	if err != nil {
		return nil, err
	}

	clusterPeer := &ClusterPeer{
		ExternalUUID:    createdClusterPeer.ClusterPeerUUID,
		PeerClusterName: params.PeerName,
		PeerAddresses:   params.PeerAddresses,
		IPSpace:         params.IPSpace,
	}
	if createdClusterPeer.GeneratedPassphrase != nil {
		clusterPeer.Passphrase = (*log.Secret)(createdClusterPeer.GeneratedPassphrase)
	}
	if createdClusterPeer.ExpiryTime != nil {
		clusterPeer.ExpiryTime = createdClusterPeer.ExpiryTime
	}
	return clusterPeer, nil
}

// AcceptClusterPeer accepts a cluster peer from a remote cluster
func (rc *OntapRestProvider) AcceptClusterPeer(params CreateClusterPeerParams) (*ClusterPeer, error) {
	createParams := ontapRest.ClusterPeerCreateParams{
		IPAddresses:        params.PeerAddresses,
		Name:               params.PeerName,
		GeneratePassphrase: false,
		IPSpace:            params.IPSpace,
		Passphrase:         params.Passphrase,
	}
	client := getOntapClientFunc(rc.ClientParams)
	createdClusterPeer, err := client.Cluster().ClusterPeerCreate(createParams)
	if err != nil {
		return nil, err
	}

	clusterPeer := &ClusterPeer{
		ExternalUUID:    createdClusterPeer.ClusterPeerUUID,
		PeerClusterName: params.PeerName,
		PeerAddresses:   params.PeerAddresses,
		IPSpace:         params.IPSpace,
	}
	return clusterPeer, nil
}

// DeleteClusterPeer deletes a cluster peer for the specific host
func (rc *OntapRestProvider) DeleteClusterPeer(clusterPeerID string) error {
	client := getOntapClientFunc(rc.ClientParams)
	err := client.Cluster().ClusterPeerDelete(clusterPeerID)
	if err != nil {
		return err
	}
	return nil
}

// GetClusterPeer Gets a single cluster peer by clusterPeerID
func (rc *OntapRestProvider) GetClusterPeer(clusterPeerID string) (*ClusterPeer, error) {
	client := getOntapClientFunc(rc.ClientParams)
	peer, err := client.Cluster().ClusterPeerGet(clusterPeerID)
	if err != nil {
		return nil, err
	}
	clusterPeer := &ClusterPeer{
		ExternalUUID:        peer.UUID,
		PeerClusterName:     peer.PeerClusterName,
		PeerAddresses:       peer.IPAddresses,
		Availability:        peer.Availability,
		AuthenticationState: peer.AuthenticationState,
		ExpiryTime:          convertFromOntapTime(peer.ExpiryTime),
	}
	return clusterPeer, nil
}

// ListClusterPeers returns all cluster peers for the specific host
func (rc *OntapRestProvider) ListClusterPeers() ([]*ClusterPeer, error) {
	client := getOntapClientFunc(rc.ClientParams)
	ontapClusterPeers, err := client.Cluster().ClusterPeersList()
	if err != nil {
		return nil, err
	}
	var clusterPeers []*ClusterPeer
	for _, peer := range ontapClusterPeers {
		clusterPeer := &ClusterPeer{
			ExternalUUID:        peer.UUID,
			PeerClusterName:     peer.PeerClusterName,
			AuthenticationState: peer.AuthenticationState,
			Availability:        peer.Availability,
			PeerAddresses:       peer.IPAddresses,
			ExpiryTime:          convertFromOntapTime(peer.ExpiryTime),
		}
		clusterPeers = append(clusterPeers, clusterPeer)
	}
	return clusterPeers, nil
}

// convertToOntapTime converts a *strfmt.DateTime to a string RFC3339 time format for ONTAP
func convertToOntapTime(timeDateTime *strfmt.DateTime) *string {
	if timeDateTime == nil {
		return nil
	}

	date := (*time.Time)(timeDateTime)

	expiryTimeStr := date.Format(time.RFC3339)

	return nillable.ToPointer(expiryTimeStr)
}

// convertFromOntapTime converts ONTAP expiryTime string to *strfmt.DateTime format
func convertFromOntapTime(timeStr string) *strfmt.DateTime {
	if timeStr == "" {
		return nil
	}

	expTime, err := strfmt.ParseDateTime(timeStr)
	if err != nil {
		return nil
	}

	return nillable.ToPointer(expTime)
}
