package replicationActivities

import (
	"context"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	deHydrateVolumeReplication = DeHydrateVolumeReplication
	deHydrateVolume            = DeHydrateVolume
)

type DeleteVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *DeleteVolumeReplicationActivity) SetHybridReplicationVariablesDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	if result.Event != nil && result.Event.ReplicationModel != nil && result.Event.ReplicationModel.HybridReplicationAttributes != nil {
		logger.Infof("Replication is a hybrid replication")
		result.IsHybridReplicationVolume = true
		// TODO: check replication count for hybrid replication, if last then set peering cleanup flag also
	}
	return result, nil
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
	logger := util.GetLogger(ctx)
	logger.Debugf("Release ReplicationOn Destination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	updateRequest := googleproxyclient.VolumeReplicationUpdateStateInternalV1beta{
		State:        googleproxyclient.NewOptString(models.LifeCycleStateError),
		StateDetails: googleproxyclient.NewOptString(models.LifeCycleStateDeletionErrorDetails),
	}
	updateParams := googleproxyclient.V1betaInternalUpdateStateParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
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
		currentLocation := result.Event.Location
		var remoteLocation, remoteVolume, remoteProject string
		var err error
		if currentLocation == result.Event.ReplicationModel.ReplicationAttributes.SourceLocation {
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
