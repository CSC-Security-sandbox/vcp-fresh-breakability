package vsa

import (
	"time"

	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

var (
	svmPeerTimeoutMinutes      = env.GetUint64("SVM_PEER_TIMEOUT", 5)
	svmPeerPollIntervalSeconds = 15
)

func (rc *OntapRestProvider) GetSVMPeer(localSVMName, remoteSVMName *string) (*SvmPeer, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	svmPeers, err := client.SVM().SvmPeerCollectionGet(&ontapRest.SvmPeerGetCollectionParams{BaseParams: ontapRest.BaseParams{Fields: []string{"state", "svm", "applications"}}, SvmName: localSVMName, PeerSvmName: remoteSVMName})
	if err != nil {
		return nil, err
	}

	if len(svmPeers) == 0 {
		return nil, errors.NewNotFoundErr("SVM peer", nil)
	}

	if len(svmPeers) > 1 {
		return nil, errors.New("Multiple SVM peers found")
	}

	// There can only be one SVM peer between two SVMs
	svmPeer := svmPeers[0]

	if svmPeer.UUID == nil || *svmPeer.UUID == "" {
		return nil, errors.New("SVM peer UUID is nil or empty")
	}
	var applications []string
	for _, app := range svmPeer.SvmPeerInlineApplications {
		applications = append(applications, string(*app))
	}
	storageSvmPeer := &SvmPeer{
		State:        nillable.GetString(svmPeer.State, ""),
		UUID:         *svmPeer.UUID,
		Applications: applications,
		LocalSvmName: nillable.GetString(svmPeer.Svm.UUID, ""),
		LocalSvmUUID: nillable.GetString(svmPeer.Svm.Name, ""),
	}

	return storageSvmPeer, nil
}

// ListSVMPeersByRemoteSVMName returns all SVM peers matching the given criteria
func (rc *OntapRestProvider) ListSVMPeersByRemoteSVMName(remoteSVMName *string) ([]*SvmPeer, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}

	svmPeers, err := client.SVM().SvmPeerCollectionGet(&ontapRest.SvmPeerGetCollectionParams{
		BaseParams:  ontapRest.BaseParams{Fields: []string{"state", "svm", "applications", "peer"}},
		PeerSvmName: remoteSVMName,
	})
	if err != nil {
		return nil, err
	}

	var result []*SvmPeer
	for _, svmPeer := range svmPeers {
		if svmPeer.UUID == nil || *svmPeer.UUID == "" {
			continue // Skip peers with invalid UUID
		}

		var applications []string
		for _, app := range svmPeer.SvmPeerInlineApplications {
			applications = append(applications, string(*app))
		}

		// Extract peer information safely
		var peerSvmName, peerSvmUUID, peerClusterName string
		if svmPeer.Peer != nil {
			if svmPeer.Peer.Svm != nil {
				peerSvmName = nillable.GetString(svmPeer.Peer.Svm.Name, "")
				peerSvmUUID = nillable.GetString(svmPeer.Peer.Svm.UUID, "")
			}
			if svmPeer.Peer.Cluster != nil {
				peerClusterName = nillable.GetString(svmPeer.Peer.Cluster.Name, "")
			}
		}

		storageSvmPeer := &SvmPeer{
			State:           nillable.GetString(svmPeer.State, ""),
			UUID:            *svmPeer.UUID,
			Applications:    applications,
			LocalSvmName:    nillable.GetString(svmPeer.Svm.Name, ""),
			LocalSvmUUID:    nillable.GetString(svmPeer.Svm.UUID, ""),
			PeerSvmName:     peerSvmName,
			PeerSvmUUID:     peerSvmUUID,
			PeerClusterName: peerClusterName,
		}

		result = append(result, storageSvmPeer)
	}

	return result, nil
}

func (rc *OntapRestProvider) acceptSVMPeer(svmPeerUUID string) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	params := &ontapRest.SvmPeerModifyParams{
		UUID: svmPeerUUID,
		SvmPeer: ontaprestmodels.SvmPeer{
			State: nillable.ToPointer(ontaprestmodels.SvmPeerStatePeered),
		},
	}
	return client.SVM().SvmPeerModify(params)
}

func (rc *OntapRestProvider) DeleteSVMPeer(svmPeerUUID string, force bool) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	params := &ontapRest.SvmPeerDeleteParams{
		SvmPeerUUID: svmPeerUUID,
		Force:       force,
	}
	return client.SVM().SvmPeerDelete(params)
}

func (rc *OntapRestProvider) CreateSvmPeering(srcClusterName, srcSVMName, dstSVMName string, snapmirrorApplication ontaprestmodels.SvmPeerApplications) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}

	params := CreateSVMPeerParams{
		LocalSVMName:    dstSVMName,
		PeerSVMName:     srcSVMName,
		PeerClusterName: srcClusterName,
		Applications:    []ontaprestmodels.SvmPeerApplications{snapmirrorApplication},
	}
	// Destination is local, Source is remote
	svmPeer, err := rc.GetSVMPeer(&dstSVMName, &srcSVMName)
	if err != nil {
		if !errors.IsNotFoundErr(err) {
			return err
		}
		// SVM peer does not exist
		_, err = rc.CreateSVMPeer(params)
		if err != nil {
			return err
		}
	} else {
		// SVM peer already exists
		if svmPeer.State == ontaprestmodels.SvmPeerStatePeered {
			return nil
		} else if svmPeer.State != ontaprestmodels.SvmPeerStateInitializing && svmPeer.State != ontaprestmodels.SvmPeerStatePending {
			// SVM peer exists in an unexpected state, delete and recreate
			err = client.SVM().SvmPeerDelete(&ontapRest.SvmPeerDeleteParams{SvmPeerUUID: svmPeer.UUID})
			if err != nil {
				return err
			}
			_, err = rc.CreateSVMPeer(params)
			if err != nil {
				return err
			}
		}
	}

	timeOut := time.Now().Add(time.Duration(svmPeerTimeoutMinutes) * time.Minute)
	svmPeerUUID := ""
	for time.Now().Before(timeOut) {
		// Destination is local, Source is remote
		svmPeer, err = rc.GetSVMPeer(&dstSVMName, &srcSVMName)
		if err != nil && !errors.IsNotFoundErr(err) {
			return err
		}
		if svmPeer != nil {
			svmPeerUUID = svmPeer.UUID
			switch svmPeer.State {
			case ontaprestmodels.SvmPeerStateInitializing, ontaprestmodels.SvmPeerStateInitiated, ontaprestmodels.SvmPeerStatePending:
				// Wait
			case ontaprestmodels.SvmPeerStatePeered:
				// Return
				return nil
			default:
				// Error
				err = client.SVM().SvmPeerDelete(&ontapRest.SvmPeerDeleteParams{SvmPeerUUID: svmPeerUUID})
				if err != nil {
					return err
				}
				return errors.New("Error setting up peering infrastructure")
			}
		}
		time.Sleep(time.Duration(svmPeerPollIntervalSeconds) * time.Second)
	}
	// Timeout - No authorization was received from source SVM
	err = client.SVM().SvmPeerDelete(&ontapRest.SvmPeerDeleteParams{SvmPeerUUID: svmPeerUUID})
	if err != nil {
		return errors.New("Timeout during peering infrastructure setup")
	}
	return nil
}

func (rc *OntapRestProvider) AcceptSvmPeering(srcSVMName, dstSVMName string) error {
	timeOut := time.Now().Add(time.Duration(svmPeerTimeoutMinutes) * time.Minute)
	for time.Now().Before(timeOut) {
		// Source is local, Destination is remote
		svmPeer, err := rc.GetSVMPeer(&srcSVMName, &dstSVMName)
		if err != nil {
			return err
		}
		switch svmPeer.State {
		case ontaprestmodels.SvmPeerStateInitializing, ontaprestmodels.SvmPeerStateInitiated:
			// Wait
		case ontaprestmodels.SvmPeerStatePending:
			// Accept
			return rc.acceptSVMPeer(svmPeer.UUID)
		case ontaprestmodels.SvmPeerStatePeered:
			// Return
			return nil
		default:
			// Error
			return errors.New("Error setting up peering infrastructure")
		}
		time.Sleep(time.Duration(svmPeerPollIntervalSeconds) * time.Second)
	}
	return errors.New("Timeout during peering infrastructure setup")
}

func (rc *OntapRestProvider) CreateSVMPeer(params CreateSVMPeerParams) (*SvmPeer, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	createParams := &ontapRest.SvmPeerCreateParams{
		LocalSVMName:    params.LocalSVMName,
		PeerSVMName:     params.PeerSVMName,
		PeerClusterName: params.PeerClusterName,
		Applications:    params.Applications,
	}
	err = client.SVM().SvmPeerCreate(createParams)
	if err != nil {
		return nil, err
	}

	// We must fetch the SVM peer by local and remote SVM names
	// because the create SVM peer API does not return the UUID
	clusterPeer, err := rc.GetSVMPeer(&params.LocalSVMName, &params.PeerSVMName)
	if err != nil {
		return nil, err
	}

	return clusterPeer, nil
}
