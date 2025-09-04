package replicationActivities

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type StopVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *StopVolumeReplicationActivity) GetSrcBasePathStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetDstBasePathStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetSignedSrcTokenStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *StopVolumeReplicationActivity) GetSignedDstTokenStop(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
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

func (a *StopVolumeReplicationActivity) StopReplicationOnDestination(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("stopReplicationOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	stopReplicationParams := &googleproxyclient.V1betaInternalStopVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
	}
	stopReplicationReq := &googleproxyclient.V1betaInternalStopVolumeReplicationReq{
		Force: googleproxyclient.NewOptBool(result.Event.ForceStop),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalStopVolumeReplication(ctx, stopReplicationReq, *stopReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalStopReplication, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.DstReplication = r
		result.JobId = &r.Jobs[0].JobId.Value
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

func (a *StopVolumeReplicationActivity) DescribeDestJobStop(ctx context.Context, result *replication.StopReplicationResult) error {
	err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}
