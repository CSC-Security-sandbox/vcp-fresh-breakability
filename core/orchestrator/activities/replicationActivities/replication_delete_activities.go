package replicationActivities

import (
	"context"
	"fmt"
	"strings"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	deHydrateVolumeReplication   = DeHydrateVolumeReplication
	deHydrateVolume              = DeHydrateVolume
	hyperscalerGetProviderByNode = hyperscaler.GetProviderByNode
)

type DeleteVolumeReplicationActivity struct {
	SE database.Storage
}

// removePathFromSnapmirrorQuery removes a path from the snapmirror query string.
// Returns the updated query string, or empty string if no paths remain.
func removePathFromSnapmirrorQuery(existingPrivilege *vsa.RolePrivilege, pathToRemove string) string {
	if existingPrivilege == nil || existingPrivilege.Query == "" {
		return "" // No existing privilege, nothing to remove
	}

	existingQuery := existingPrivilege.Query
	// Extract paths from existing query: -source-path path1|path2|path3
	pathsStr := strings.TrimPrefix(existingQuery, "-source-path ")
	existingPaths := strings.Split(pathsStr, "|")

	// Remove the path to delete
	var remainingPaths []string
	for _, path := range existingPaths {
		if strings.TrimSpace(path) != pathToRemove {
			remainingPaths = append(remainingPaths, strings.TrimSpace(path))
		}
	}

	// If no paths remain, return empty string
	if len(remainingPaths) == 0 {
		return ""
	}

	// Return updated query with remaining paths
	return fmt.Sprintf("-source-path %s", strings.Join(remainingPaths, "|"))
}

func (a *DeleteVolumeReplicationActivity) SetHybridReplicationVariablesDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	if result.Event != nil && result.Event.ReplicationModel != nil && result.Event.ReplicationModel.HybridReplicationAttributes != nil {
		logger.Infof("Replication is a hybrid replication")
		result.IsHybridReplicationVolume = true
		if result.Event.ReplicationModel.ClusterPeerId.Valid {
			se := a.SE
			volumeReplicationCount, err := se.GetVolumeReplicationCountByClusterPeerID(ctx, result.Event.ReplicationModel.ClusterPeerId.Int64)
			if err != nil {
				return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
			}
			flexCacheCount, err := se.GetFlexCacheVolumeCountByClusterPeerID(ctx, result.Event.ReplicationModel.ClusterPeerId.Int64)
			if err != nil {
				return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
			}
			if volumeReplicationCount == 1 && flexCacheCount == 0 {
				logger.Infof("This is the last replication for cluster peer ID %d, setting cleanup cluster peering flag", result.Event.ReplicationModel.ClusterPeerId.Int64)
				result.CleanupClusterPeering = true
			}
		}
		if replication.IsSrcForHybridReplication(result.Event.ReplicationModel) {
			result.IsSrcForHybridReplication = true
		}
	}
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteClusterPeeringInOntap(ctx context.Context, result *replication.DeleteReplicationResult, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}
	clusterPeerUUID := result.Event.ReplicationModel.ClusterPeer.OntapPeerUUID

	err = provider.DeleteClusterPeer(clusterPeerUUID)
	if err != nil {
		return errors.NewVCPError(errors.ErrDeletingClusterPeer, err)
	}
	logger.Debugf("Cluster peering with UUID %s deleted successfully", clusterPeerUUID)
	return nil
}

func (a *DeleteVolumeReplicationActivity) DeleteRoleInOntap(ctx context.Context, node *models.Node) error {
	logger := util.GetLogger(ctx)
	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	roleName := onPremPeerRole
	getRoleParams := vsa.GetRoleCollectionParams{
		Name: &roleName,
	}
	roles, err := provider.GetRoleCollection(getRoleParams)
	if err != nil {
		logger.Errorf("Failed to get role %s: %v", roleName, err)
		return errors.NewVCPError(errors.ErrInternalServerError, err)
	}
	if len(roles) == 0 {
		logger.Infof("Role %s does not exist, skipping deletion", roleName)
		return nil
	}
	role := roles[0]
	deleteRoleParams := vsa.DeleteRoleParams{
		Name:      roleName,
		OwnerUUID: &role.OwnerID,
	}

	err = provider.DeleteRole(deleteRoleParams)
	if err != nil {
		logger.Errorf("Failed to delete role %s: %v", roleName, err)
		return errors.NewVCPError(errors.ErrInternalServerError, err)
	}
	logger.Debugf("Role %s deleted successfully", roleName)
	return nil
}

func (a *DeleteVolumeReplicationActivity) ReleaseReplicationOnSrc(ctx context.Context, result *replication.DeleteReplicationResult, node *models.Node) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("ReleaseReplicationOnSrc")

	// Get provider from node
	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Construct VSA VolumeReplication from datamodel
	replicationAttrs := result.Event.ReplicationModel.ReplicationAttributes
	vsaVolumeReplication := &vsa.VolumeReplication{
		EndpointType:          replicationAttrs.EndpointType,
		SourceHostName:        replicationAttrs.SourceHostName,
		SourceSVMName:         replicationAttrs.SourceSvmName,
		SourceVolumeName:      replicationAttrs.SourceVolumeName,
		DestinationHostName:   replicationAttrs.DestinationHostName,
		DestinationSVMName:    replicationAttrs.DestinationSvmName,
		DestinationVolumeName: replicationAttrs.DestinationVolumeName,
		ReplicationSchedule:   replicationAttrs.ReplicationSchedule,
		Volume: &vsa.Volume{
			ExternalUUID: result.Event.ReplicationModel.Volume.VolumeAttributes.ExternalUUID,
		},
		ReplicationType: replicationAttrs.ReplicationType,
	}

	// Create release params
	releaseParams := &vsa.ReleaseVolumeReplicationParams{
		VolumeReplication: vsaVolumeReplication,
		ReverseResync:     false,
	}

	// Call provider to release replication
	_, err = provider.ReleaseVolumeReplication(releaseParams)
	if err != nil {
		logger.Errorf("Failed to release volume replication: %v", err)
		if strings.Contains(err.Error(), "Timeout during cleanup of peering infrastructure") {
			return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrCleanupSvmPeering, err))
		}
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, err)
	}

	logger.Debugf("Replication released successfully on source")
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) UpdateRbacRole(ctx context.Context, result *replication.DeleteReplicationResult, node *models.Node) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteRbacRole")

	if !result.IsHybridReplicationVolume || !result.IsSrcForHybridReplication {
		// Only delete RBAC role for hybrid replications where GCNV is source
		return result, nil
	}

	if result.Event == nil || result.Event.ReplicationModel == nil || result.Event.ReplicationModel.ReplicationAttributes == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication or replication attributes is nil"))
	}

	// Get provider from node
	provider, err := hyperscalerGetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Role name for hybrid replication
	roleName := onPremPeerRole

	// Get the role - it must exist
	roles, err := provider.GetRoleCollection(vsa.GetRoleCollectionParams{
		Name: &roleName,
	})
	if err != nil {
		logger.Errorf("Failed to get role collection: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	if len(roles) == 0 {
		logger.Infof("Role %s not found, skipping RBAC deletion", roleName)
		return result, nil
	}

	// Use the first matching role
	targetRole := roles[0]

	// Format: svm:volume
	sourcePath := getPath(result.Event.ReplicationModel.ReplicationAttributes.SourceSvmName, result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeName)

	// Check if snapmirror resync privilege exists
	var existingSnapmirrorPrivilege *vsa.RolePrivilege
	for _, privilege := range targetRole.Privileges {
		if privilege.Path == SnapmirrorResyncPrivilegePath {
			existingSnapmirrorPrivilege = privilege
			break
		}
	}

	if existingSnapmirrorPrivilege == nil {
		logger.Infof("Snapmirror resync privilege not found for role %s, skipping deletion", roleName)
		return result, nil
	}

	// Remove path from query
	updatedQuery := removePathFromSnapmirrorQuery(existingSnapmirrorPrivilege, sourcePath)

	ownerID := targetRole.OwnerID

	if updatedQuery == "" {
		// Delete the entire privilege if no paths remaining
		err = provider.DeleteRolePrivilege(vsa.DeleteRolePrivilegeParams{
			OwnerID: ownerID,
			Name:    roleName,
			Path:    SnapmirrorResyncPrivilegePath,
		})
		if err != nil {
			logger.Errorf("Failed to delete snapmirror resync privilege: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully deleted RBAC role %s snapmirror resync privilege as no paths remain", roleName)
		return result, nil
	}

	// Modify existing privilege with remaining paths
	err = provider.ModifyRolePrivilege(vsa.ModifyRolePrivilegeParams{
		OwnerID: ownerID,
		Name:    roleName,
		Path:    SnapmirrorResyncPrivilegePath,
		Access:  SnapmirrorResyncPrivilegeAccess,
		Query:   updatedQuery,
	})
	if err != nil {
		logger.Errorf("Failed to modify snapmirror resync privilege: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	logger.Infof("Successfully modified RBAC role %s snapmirror resync privilege (%s) for remaining source path(s): %s", roleName, SnapmirrorResyncPrivilegeAccess, updatedQuery)

	return result, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteClusterPeeringDB(ctx context.Context, result *replication.DeleteReplicationResult) error {
	logger := util.GetLogger(ctx)
	clusterPeeringRow := result.Event.ReplicationModel.ClusterPeer
	se := a.SE
	clusterPeeringRow.State = models.CvpClusterPeeringStatusDELETED
	timeNow := time.Now()
	clusterPeeringRow.DeletedAt = &gorm.DeletedAt{Time: timeNow, Valid: true}
	clusterPeeringRow.UpdatedAt = clusterPeeringRow.DeletedAt.Time
	if err := se.UpdateClusterPeeringRow(ctx, clusterPeeringRow); err != nil {
		logger.Errorf("Failed to update cluster peering row in DB: %v", err)
		return errors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Cluster peering row with UUID %s updated to state %s", clusterPeeringRow.UUID, clusterPeeringRow.State)
	return nil
}

func (a *DeleteVolumeReplicationActivity) GetSrcBasePathDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if result.Event.ReplicationModel.ReplicationAttributes.SourceLocation == RemoteRegionCustomer {
		return result, nil
	}
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) GetDstBasePathDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation == RemoteRegionCustomer {
		return result, nil
	}
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) GetSignedSrcTokenDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) GetSignedDstTokenDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if *result.SrcProjectNumber == *result.DstProjectNumber {
		result.DstJwtToken = result.SrcJwtToken
		return result, nil
	}
	dstJwt, err := GetSignedToken(ctx, *result.DstProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.DstJwtToken = dstJwt
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteReplicationOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteReplicationOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.JobId = r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeReplicationError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) GetReplicationOnDestinationForDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetReplicationOnDestinationForDelete")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  *result.DstProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID}}
	res, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalOK:
		result.DstReplication = &r.Replications[0]
		return result, nil
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForDeleteError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) DeleteVolumeOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteVolumeOnDestination")
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaDeleteVolumeParams{
		ProjectNumber:  *result.DstProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	body := googleproxyclient.OptV1betaDeleteVolumeReq{
		Set:   true,
		Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
	}
	res, err := googleProxyClient.Invoker.V1betaDeleteVolume(ctx, body, *params)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrDeleteVolume, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		volume := googleproxyclient.VolumeV1beta{}
		err := replication.JsonUnMarshal(r.Response, &volume)
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrorFailedToUnmarshal, err)
		}
		result.JobId = strings.Split(r.Name.Value, "/")[7]
		result.DstVolume = &volume
		return result, nil
	case *googleproxyclient.V1betaDeleteVolumeBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New(r.Message))
	case *googleproxyclient.V1betaDeleteVolumeInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New(r.Message))
	case *googleproxyclient.V1betaDeleteVolumeUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New(r.Message))
	case *googleproxyclient.V1betaDeleteVolumeForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New(r.Message))
	case *googleproxyclient.V1betaDeleteVolumeNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyDeleteVolumeError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) DeHydrateDestinationVolume(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		err := deHydrateVolume(ctx, convertVolumeV1BetaToVolumeModelForCleanup(*result.DstVolume, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation), *result.DstProjectNumber)
		if err != nil {
			if strings.Contains(err.Error(), "has nested resources") {
				return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrDeHydrateVolume, err))
			}
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrDeHydrateVolume, err))
		}
	}
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) UpdateReplicationRecordOnSource(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Release ReplicationOn Source")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
	releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		result.JobId = strings.Split(r.Name.Value, "/")[7]
		return result, nil
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) UpdateReplicationRecordOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Release ReplicationOn Destination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		result.JobId = strings.Split(r.Name.Value, "/")[7]
		return result, nil
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReleaseVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) UpdateReplicationOnDestinationToErrorState(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	return a.updateReplicationToErrorState(ctx, result, "destination")
}

func (a *DeleteVolumeReplicationActivity) UpdateReplicationOnSourceToErrorState(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	return a.updateReplicationToErrorState(ctx, result, "source")
}

func (a *DeleteVolumeReplicationActivity) updateReplicationToErrorState(ctx context.Context, result *replication.DeleteReplicationResult, target string) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Release ReplicationOn %s", target)

	var basePath, jwtToken, projectNumber, locationId, replicationUUID string
	if target == "source" {
		basePath = *result.SrcBasePath
		jwtToken = *result.SrcJwtToken
		projectNumber = *result.SrcProjectNumber
		locationId = result.Event.ReplicationModel.ReplicationAttributes.SourceLocation
		replicationUUID = result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID
	} else {
		basePath = *result.DstBasePath
		jwtToken = *result.DstJwtToken
		projectNumber = *result.DstProjectNumber
		locationId = result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation
		replicationUUID = result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID
	}

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)
	updateRequest := googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
		State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
		StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
	}
	updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
		ProjectNumber:       projectNumber,
		LocationId:          locationId,
		VolumeReplicationId: replicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalUpdateState(ctx, &updateRequest, updateParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, err)
	}
	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationUpdateStateInternalV1beta:
		return result, nil
	case *googleproxyclient.V1betaInternalUpdateStateBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateStateInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateStateUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateStateForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalUpdateStateNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationState, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) UpdateReplicationInDBToErrorState(ctx context.Context, result *replication.DeleteReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("Update ReplicationInDBToErrorState")
	se := a.SE

	if result.Event == nil || result.Event.ReplicationModel == nil {
		logger.Error("Replication model is nil")
		return errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication model is nil"))
	}

	volumeRep := result.Event.ReplicationModel
	volumeRep.State = models.LifeCycleStateError
	volumeRep.StateDetails = models.LifeCycleStateDeletionErrorDetails

	if err := se.UpdateVolumeReplicationStates(ctx, volumeRep); err != nil {
		logger.Errorf("Failed to update volume replication state in database: %v", err)
		return errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	logger.Debugf("Successfully updated volume replication state to error in database")
	return nil
}

func (a *DeleteVolumeReplicationActivity) DeleteSnapmirrorSnapshotsOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteSnapmirrorSnapshotsOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	deleteSmSnapshotsParam := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
		ProjectNumber:  *result.DstProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteSmSnapshotsParam)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrDeleteSnapshot, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		result.JobId = strings.Split(r.Name.Value, "/")[7]
		return result, nil
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotDestinationError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) DeleteSnapmirrorSnapshotsOnSource(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteSnapmirrorSnapshotsOnSource")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
	deleteSmSnapshotsParam := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
		ProjectNumber:  *result.SrcProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteSmSnapshotsParam)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrDeleteSnapshot, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		result.JobId = strings.Split(r.Name.Value, "/")[7]
		return result, nil
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalDeleteVolumeSnapmirrorSnapshotSourceError, errors.New("unknown response type"))
	}
}

func (a *DeleteVolumeReplicationActivity) DeHydrateDestinationVolumeReplication(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		currentEndpoint := result.Event.ReplicationModel.ReplicationAttributes.EndpointType
		var remoteLocation, remoteVolume, remoteProject string
		var err error
		if currentEndpoint == database.VolumeReplicationEndpointTypeSource {
			remoteLocation = result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation
			remoteVolume = result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeName
			remoteProject = result.Event.DestinationProjectNumber
		} else {
			remoteLocation = result.Event.ReplicationModel.ReplicationAttributes.SourceLocation
			remoteVolume = result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeName
			remoteProject = result.Event.SourceProjectNumber
		}
		err = deHydrateVolumeReplication(ctx, convertVolumeReplicationV1BetaToVolumeModel(result.Event.ReplicationModel.Name, remoteLocation, remoteVolume), remoteProject)
		if err != nil {
			if strings.Contains(err.Error(), "Unable to extract the resource from the request") {
				return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrDeHydrateVolumeReplication, err))
			}
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrDeHydrateVolumeReplication, err))
		}
	}
	return result, nil
}

func convertVolumeReplicationV1BetaToVolumeModel(destinationReplicationName string, dstLocation string, destinationVolumeName string) models.VolumeReplication {
	return models.VolumeReplication{
		Name:                  destinationReplicationName,
		ReplicationAttributes: &models.ReplicationDetails{DestinationRegion: dstLocation},
		Volume: &models.Volume{
			DisplayName: destinationVolumeName,
		},
	}
}

func (a *DeleteVolumeReplicationActivity) DescribeRemoteJobForDelete(ctx context.Context, result *replication.DeleteReplicationResult) error {
	err := activities.DescribeJob(ctx, &result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func (a *DeleteVolumeReplicationActivity) DescribeSourceJobForDelete(ctx context.Context, result *replication.DeleteReplicationResult) error {
	err := activities.DescribeJob(ctx, &result.JobId, result.SrcBasePath, result.SrcJwtToken, result.SrcProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.SourceLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func (a *DeleteVolumeReplicationActivity) DeleteReplicationRecordOnSource(ctx context.Context, result *replication.DeleteReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteReplicationRecordOnSource")

	if result.Event == nil || result.Event.ReplicationModel == nil {
		logger.Error("Replication model is nil")
		return errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication model is nil"))
	}

	se := a.SE
	replicationModel := result.Event.ReplicationModel

	_, err := se.DeleteVolumeReplication(ctx, replicationModel)
	if err != nil {
		logger.Errorf("Failed to delete volume replication record in database: %v", err)
		return errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err))
	}

	return nil
}
