package activities

import (
	"context"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	IpSpace = env.GetString("VSA_IC_LIF_IPSPACE", "Gcnv")
)

const clusterPeerAvailable = "available"

type ClusterPeerActivity struct {
	SE database.Storage
}

func (j *ClusterPeerActivity) AcceptClusterPeer(ctx context.Context, params *commonparams.ClusterPeerParams, node *models.Node) (*commonparams.ClusterPeerParams, error) {
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
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
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
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

type ClusterUpgradeActivity struct {
	SE database.Storage
}

// UpdateClusterUpgradeJobStatusActivity updates the status of a cluster upgrade job
func (j *ClusterUpgradeActivity) UpdateClusterUpgradeJobStatusActivity(ctx context.Context, jobUUID, status, errorMessage string) error {
	logger := util.GetLogger(ctx)
	logger.Info("Updating cluster upgrade job status", "jobUUID", jobUUID, "status", status)

	se := j.SE

	// Get the upgrade job
	upgradeJob, err := se.GetClusterUpgradeJobByUUID(ctx, jobUUID)
	if err != nil {
		logger.Error("Failed to get cluster upgrade job", "jobUUID", jobUUID, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Update the status
	upgradeJob.Status = status
	upgradeJob.UpdatedAt = time.Now()

	// Set error details if provided
	if errorMessage != "" {
		upgradeJob.ErrorDetails = &datamodel.UpgradeErrorDetails{
			ErrorCode:    "UPGRADE_FAILED",
			ErrorMessage: errorMessage,
			ErrorType:    "UPGRADE_ERROR",
			Retryable:    true,
		}
	}

	// Set timestamps based on status
	if status == string(models.UpgradeStatusInProgress) {
		now := time.Now()
		upgradeJob.StartedAt = &now
	} else if status == string(models.UpgradeStatusCompleted) || status == string(models.UpgradeStatusFailed) {
		now := time.Now()
		upgradeJob.CompletedAt = &now
		upgradeJob.DeletedAt = &gorm.DeletedAt{Time: now, Valid: true}
	}

	// Save the updated job
	err = se.UpdateClusterUpgradeJob(ctx, upgradeJob)
	if err != nil {
		logger.Error("Failed to update cluster upgrade job", "jobUUID", jobUUID, "error", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Info("Successfully updated cluster upgrade job status", "jobUUID", jobUUID, "status", status)
	return nil
}
