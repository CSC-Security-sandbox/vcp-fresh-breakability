package replicationActivities

import (
	"context"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	deHydrateVolumeReplication = DeHydrateVolumeReplication
	deHydrateVolume            = DeHydrateVolume
)

type DeleteVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *DeleteVolumeReplicationActivity) GetSrcBasePathDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) GetDstBasePathDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
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
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams)
	if err != nil {
		return nil, err
	}
	response, ok := res.(*googleproxyclient.VolumeReplicationInternalV1beta)
	if ok {
		result.JobId = response.Jobs[0].JobId.Value
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) GetReplicationOnDestinationForDelete(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetReplicationOnDestinationForDelete")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID}}
	res, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		return nil, err
	}
	response, ok := res.(*googleproxyclient.V1betaGetMultipleReplicationsInternalOK)
	if ok {
		result.DstReplication = &response.Replications[0]
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteVolumeOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteVolumeOnDestination")
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaDeleteVolumeParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeId:      result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
	}
	body := googleproxyclient.OptV1betaDeleteVolumeReq{
		Set:   true,
		Value: googleproxyclient.V1betaDeleteVolumeReq{DeleteAssociatedBackups: googleproxyclient.OptBool{Set: true, Value: false}},
	}
	res, err := googleProxyClient.Invoker.V1betaDeleteVolume(ctx, body, *params)
	if err != nil {
		return nil, err
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		volume := googleproxyclient.VolumeV1beta{}
		err := replication.JsonUnMarshal(operation.Response, &volume)
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrorFailedToUnmarshal, err)
		}
		result.JobId = strings.Split(operation.Name.Value, "/")[7]
		result.DstVolume = &volume
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) DeHydrateDestinationVolume(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		err := deHydrateVolume(ctx, convertVolumeV1BetaToVolumeModelForCleanup(*result.DstVolume, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation), *result.DstProjectNumber)
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrDeHydrateVolume, err)
		}
	}
	return result, nil
}

func (a *DeleteVolumeReplicationActivity) ReleaseReplicationOnSource(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Release ReplicationOn Source")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
	releaseReplicationParams := &googleproxyclient.V1betaInternalReleaseVolumeReplicationParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
	}
	res, err := googleProxyClient.Invoker.V1betaInternalReleaseVolumeReplication(ctx, *releaseReplicationParams)
	if err != nil {
		return nil, err
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		result.JobId = strings.Split(operation.Name.Value, "/")[7]
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteSnapmirrorSnapshotsOnDestination(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteSnapmirrorSnapshotsOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	deleteSmSnapshotsParam := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
		ProjectNumber: *result.DstProjectNumber,
		LocationId:    result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeId:      result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteSmSnapshotsParam)
	if err != nil {
		return nil, err
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		result.JobId = strings.Split(operation.Name.Value, "/")[7]
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) DeleteSnapmirrorSnapshotsOnSource(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteSnapmirrorSnapshotsOnSource")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
	deleteSmSnapshotsParam := &googleproxyclient.V1betaInternalDeleteVolumeSnapmirrorSnapshotParams{
		ProjectNumber: *result.SrcProjectNumber,
		LocationId:    result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeId:      result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
	}
	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeSnapmirrorSnapshot(ctx, *deleteSmSnapshotsParam)
	if err != nil {
		return nil, err
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		result.JobId = strings.Split(operation.Name.Value, "/")[7]
		return result, nil
	}
	return nil, nil
}

func (a *DeleteVolumeReplicationActivity) DeHydrateDestinationVolumeReplication(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		err := deHydrateVolumeReplication(ctx, convertVolumeReplicationV1BetaToVolumeModel(result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID), *result.DstProjectNumber)
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrDeHydrateVolumeReplication, err)
		}
	}
	return result, nil
}

func convertVolumeReplicationV1BetaToVolumeModel(destinationReplicationUUID string, dstLocation string, destinationVolumeUUID string) models.VolumeReplication {
	return models.VolumeReplication{
		BaseModel: models.BaseModel{
			UUID: destinationReplicationUUID,
		},
		ReplicationAttributes: &models.ReplicationDetails{DestinationRegion: dstLocation},
		Volume: &models.Volume{
			BaseModel: models.BaseModel{UUID: destinationVolumeUUID},
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
