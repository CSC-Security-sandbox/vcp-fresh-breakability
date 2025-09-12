package replicationActivities

import (
	"context"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilError "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	verifyDstVolume = replication.VerifyDstVolume
)

type ResumeVolumeReplicationActivity struct {
	SE database.Storage
}

func (a *ResumeVolumeReplicationActivity) GetSrcBasePathResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) GetDstBasePathResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) GetSignedSrcTokenResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) GetSignedDstTokenResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
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

func (a *ResumeVolumeReplicationActivity) VerifyDstVolume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	srcVolume, dstVolume, err := verifyDstVolume(ctx, result.Event.ReplicationModel.ReplicationAttributes, *result.SrcBasePath, *result.DstBasePath, *result.SrcJwtToken, *result.DstJwtToken, result.Event.SourceProjectNumber, result.Event.DestinationProjectNumber, result.Event.XCorrelationID, false)
	if err != nil {
		if err.(*errors.CustomError).TrackingID == errors.ErrVolumeNotFound {
			return nil, utilError.NewNonRetryableErr(err.Error())
		}
		return nil, err
	}
	result.SrcVolume = &srcVolume
	result.DstVolume = &dstVolume
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) ResizeVolumeIfNeeded(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	var srcVolumeQuota float64
	var dstVolumeQuota float64
	if result.SrcVolume.QuotaInBytes.Set {
		srcVolumeQuota = result.SrcVolume.QuotaInBytes.Value
	}
	if result.DstVolume.QuotaInBytes.Set {
		dstVolumeQuota = result.DstVolume.QuotaInBytes.Value
	}
	if srcVolumeQuota > dstVolumeQuota {
		logger.Debugf("Resizing destination volume from %f to %f", dstVolumeQuota, srcVolumeQuota)
		updateRequest := &googleproxyclient.VolumeUpdateV1beta{
			QuotaInBytes: googleproxyclient.NewOptNilFloat64(srcVolumeQuota),
		}
		updateVolumeParams := &googleproxyclient.V1betaInternalUpdateVolumeParams{
			ProjectNumber:  *result.DstProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
			VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*result.Event.XCorrelationID),
		}

		googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
		res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolume(ctx, updateRequest, *updateVolumeParams)
		if err != nil {
			logger.Errorf("Failed to resize destination volume: %v", err)
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, err)
		}

		switch r := res.(type) {
		case *googleproxyclient.OperationV1beta:
			jobId := ""
			parts := strings.Split(r.Name.Value, "/")
			jobId = parts[len(parts)-1]
			result.JobId = &jobId
			return result, nil
		case *googleproxyclient.V1betaInternalUpdateVolumeBadRequest:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
		case *googleproxyclient.V1betaInternalUpdateVolumeUnauthorized:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
		case *googleproxyclient.V1betaInternalUpdateVolumeForbidden:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
		case *googleproxyclient.V1betaInternalUpdateVolumeNotFound:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
		case *googleproxyclient.V1betaInternalUpdateVolumeInternalServerError:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New(r.Message))
		default:
			return result, errors.NewVCPError(errors.ErrGoogleProxyInternalUpdateVolume, errors.New("unexpected response type from Google Proxy"))
		}
	}
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) ResumeReplicationOnDestination(ctx context.Context, result *replication.ResumeReplicationResult, params *common.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("resumeReplicationOnDestination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	resumeReplicationParams := &googleproxyclient.V1betaInternalResumeVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		ForceResume:         googleproxyclient.NewOptBool(params.Force),
		XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
	}
	res, err := googleProxyClient.Invoker.V1betaInternalResumeVolumeReplication(ctx, *resumeReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, err)
	}
	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.DstReplication = r
		result.JobId = &r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationConflict:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalResumeVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalResumeReplication, errors.New("unexpected response type from Google Proxy"))
	}
}

func (a *ResumeVolumeReplicationActivity) DescribeRemoteJobResume(ctx context.Context, result *replication.ResumeReplicationResult) error {
	err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
	if err != nil {
		return err
	}
	return nil
}
