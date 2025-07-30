package replicationActivities

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type VolumeReplicationUpdateActivity struct {
	SE database.Storage
}

func (a *VolumeReplicationUpdateActivity) GetSrcBasePathUpdate(ctx context.Context, result *replication.UpdateReplicationResult) (*replication.UpdateReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *VolumeReplicationUpdateActivity) GetDstBasePathUpdate(ctx context.Context, result *replication.UpdateReplicationResult) (*replication.UpdateReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *VolumeReplicationUpdateActivity) GetSignedSrcTokenUpdate(ctx context.Context, result *replication.UpdateReplicationResult) (*replication.UpdateReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *VolumeReplicationUpdateActivity) GetSignedDstTokenUpdate(ctx context.Context, result *replication.UpdateReplicationResult) (*replication.UpdateReplicationResult, error) {
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

func (a *VolumeReplicationUpdateActivity) UpdateReplicationOnDestination(ctx context.Context, result *replication.UpdateReplicationResult) (*replication.UpdateReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("updateReplicationOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	updateReplicationParams := &googleproxyclient.V1betaInternalUpdateVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
	}
	body := &googleproxyclient.VolumeReplicationUpdateInternalV1beta{}
	if result.Event.Description != nil {
		body.Description = googleproxyclient.NewOptNilString(nillable.GetString(result.Event.Description, ""))
	}
	if result.Event.ReplicationSchedule != nil {
		body.ReplicationSchedule = googleproxyclient.NewOptNilVolumeReplicationUpdateInternalV1betaReplicationSchedule(convertReplicationScheduleToInternalUpdateReplicationSchedule(*result.Event.ReplicationSchedule))
	}
	res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolumeReplication(ctx, body, *updateReplicationParams)
	if err != nil {
		return nil, err
	}
	response, ok := res.(*googleproxyclient.VolumeReplicationInternalV1beta)
	if ok {
		result.DstReplication = response
		result.JobId = &response.Jobs[0].JobId.Value
		return result, nil
	}
	return nil, nil
}

func (a *VolumeReplicationUpdateActivity) DescribeRemoteUpdateJob(ctx context.Context, result *replication.UpdateReplicationResult) error {
	err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return err
	}
	return nil
}

func convertReplicationScheduleToInternalUpdateReplicationSchedule(in string) googleproxyclient.VolumeReplicationUpdateInternalV1betaReplicationSchedule {
	switch in {
	case "EVERY_10_MINUTES":
		return googleproxyclient.VolumeReplicationUpdateInternalV1betaReplicationSchedule10minutely
	case "HOURLY":
		return googleproxyclient.VolumeReplicationUpdateInternalV1betaReplicationScheduleHourly
	case "DAILY":
		return googleproxyclient.VolumeReplicationUpdateInternalV1betaReplicationScheduleDaily
	default:
		return ""
	}
}
