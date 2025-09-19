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
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type CleanupVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *CleanupVolumeReplicationActivity) GetSrcBasePathCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *CleanupVolumeReplicationActivity) GetDstBasePathCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *CleanupVolumeReplicationActivity) GetSignedSrcTokenCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *CleanupVolumeReplicationActivity) GetSignedDstTokenCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
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

func (a *CleanupVolumeReplicationActivity) DeleteReplicationOnDestinationForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DeleteReplicationOnDestinationForCleanup")

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

func (a *CleanupVolumeReplicationActivity) GetReplicationOnDestinationForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetReplicationOnDestinationForCleanup")
	if result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID == "" {
		logger.Debugf("DestinationReplicationUUID is empty, skipping replication retrieval")
		return result, nil
	}
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  *result.DstProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID}}
	res, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		logger.Error("Failed to get multiple replications from Google Proxy", "error", err)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalOK:
		result.DstReplication = &r.Replications[0]
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForbidden, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound:
		result.Error = nil
	}

	return result, nil
}

func (a *CleanupVolumeReplicationActivity) GetDestinationVolumeForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetDestinationVolumeForCleanup")
	if result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID == "" {
		logger.Debugf("DestinationVolumeUUID is empty, skipping volume retrieval")
		return result, nil
	}
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaDescribeVolumeParams{
		ProjectNumber:  *result.DstProjectNumber,
		LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	res, err := googleProxyClient.Invoker.V1betaDescribeVolume(ctx, *params)
	if err != nil {
		logger.Error("Failed to get volume from Google Proxy", "error", err)
		return nil, errors.NewVCPError(errors.ErrListVolumes, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.V1betaDescribeVolumeNotFound:
		result.Error = nil
	case *googleproxyclient.VolumeV1beta:
		volumeResponse := res.(*googleproxyclient.VolumeV1beta)
		result.DstVolume = volumeResponse
	case *googleproxyclient.V1betaDescribeVolumeBadRequest:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribeVolumeInternalServerError:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribeVolumeUnauthorized:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaDescribeVolumeForbidden:
		result.Error = errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForbidden, errors.New(r.Message))
	}
	return result, nil
}

func (a *CleanupVolumeReplicationActivity) DescribeRemoteJobForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) error {
	err := activities.DescribeJob(ctx, &result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func (a *CleanupVolumeReplicationActivity) StopReplicationOnDestinationForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("StopReplicationOnDestinationForCleanup")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
		Force: googleproxyclient.NewOptBool(true),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopReplication, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.DstReplication = r
		result.JobId = r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalStopVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalStopVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopVolumeReplicationError, errors.New("unknown response type"))
	}
}

func (a *CleanupVolumeReplicationActivity) ReleaseReplicationOnSourceForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ReleaseReplicationOnSourceForCleanup")
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

func (a *CleanupVolumeReplicationActivity) DescribeSourceJobForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) error {
	err := activities.DescribeJob(ctx, &result.JobId, result.SrcBasePath, result.SrcJwtToken, result.SrcProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.SourceLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}

func (a *CleanupVolumeReplicationActivity) DeHydrateDestinationVolumeReplicationForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		err := deHydrateVolumeReplication(ctx, convertVolumeReplicationV1BetaToVolumeModel(result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID), *result.DstProjectNumber)
		if err != nil {
			if !vsaerrors.IsNotFoundErr(err) {
				return nil, errors.NewVCPError(errors.ErrDeHydrateVolumeReplication, err)
			}
		}
	}
	return result, nil
}

func (a *CleanupVolumeReplicationActivity) DeleteVolumeOnDestinationForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
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

func (a *CleanupVolumeReplicationActivity) DeHydrateDestinationVolumeForCleanup(ctx context.Context, result *replication.DeleteReplicationResult) (*replication.DeleteReplicationResult, error) {
	if hydrationEnabled {
		err := deHydrateVolume(ctx, convertVolumeV1BetaToVolumeModelForCleanup(*result.DstVolume, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation), *result.DstProjectNumber)
		if err != nil {
			return nil, errors.NewVCPError(errors.ErrDeHydrateVolume, err)
		}
	}
	return result, nil
}

func convertVolumeV1BetaToVolumeModelForCleanup(vol googleproxyclient.VolumeV1beta, dstLocation string) models.Volume {
	protocols := make([]string, 0)
	for _, protocol := range vol.Protocols {
		protocolStr, err := protocol.MarshalText()
		if err != nil {
			return models.Volume{}
		}
		protocols = append(protocols, string(protocolStr))
	}
	return models.Volume{
		BaseModel: models.BaseModel{
			UUID: vol.VolumeId.Value,
		},
		DisplayName:    vol.ResourceId,
		QuotaInBytes:   uint64(vol.QuotaInBytes.Value),
		LifeCycleState: string(vol.VolumeState.Value),
		ProtocolTypes:  protocols,
		Region:         dstLocation,
	}
}
