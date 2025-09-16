package replicationActivities

import (
	"context"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilError "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type ReverseVolumeReplicationActivity struct {
	SE database.Storage
}

// GetSrcBasePathReverse gets the base path for the current destination (which will become new source)
func (a *ReverseVolumeReplicationActivity) GetSrcBasePathReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	srcBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.SourceLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.SrcBasePath = srcBasePath
	return result, nil
}

// GetDstBasePathReverse gets the base path for the current source (which will become new destination)
func (a *ReverseVolumeReplicationActivity) GetDstBasePathReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	dstBasePath, err := GetBasePath(ctx, result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

// GetSignedSrcTokenReverse gets signed token for the current destination (new source)
func (a *ReverseVolumeReplicationActivity) GetSignedSrcTokenReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	srcJwt, err := GetSignedToken(ctx, *result.SrcProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.SrcJwtToken = srcJwt
	return result, nil
}

// GetSignedDstTokenReverse gets signed token for the current source (new destination)
func (a *ReverseVolumeReplicationActivity) GetSignedDstTokenReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	// Check if same project to avoid duplicate token generation
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

// ReverseAndResumeReplication creates a replication in reverse side
func (a *ReverseVolumeReplicationActivity) ReverseAndResumeReplication(ctx context.Context, result *replication.ReverseReplicationResult, params *common.ReverseAndResumeReplicationParams) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ReverseAndResumeReplicationOnSource")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
	// Call the combined reverse and resume endpoint on current source location (which will become the new destination)
	reverseReplicationParams := &googleproxyclient.V1betaInternalReverseVolumeReplicationParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(params.CorrelationId),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalReverseVolumeReplication(ctx, *reverseReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.DstReplication = r
		result.JobId = &r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationConflict:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalReverseVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReverseReplication, errors.New("unexpected response type from Google Proxy"))
	}
}

// UpdateVolumeReplicationAttributes calls the new updateVolumeReplicationAttributes endpoint
func (a *ReverseVolumeReplicationActivity) UpdateVolumeReplicationAttributesSrc(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateVolumeReplicationAttributes called on new destination")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)

	// Get the original replication attributes before reversal
	originalAttrs := result.Event.ReplicationModel.ReplicationAttributes
	updateRequest := convertToReversedAttributes(originalAttrs)
	updateRequest.SetVolumeReplicationUuid(googleproxyclient.NewOptString(originalAttrs.SourceReplicationUUID))
	updateRequest.SetEndpointType(googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeDst)

	// Create parameters for the updateVolumeReplicationAttributes endpoint
	updateParams := googleproxyclient.V1betaInternalUpdateVolumeReplicationAttributesParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          originalAttrs.SourceLocation,
		VolumeReplicationId: originalAttrs.SourceReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.CommonReplicationEventParams.XCorrelationID),
	}

	logger.Info("Calling updateVolumeReplicationAttributes with reversed attributes")

	// Call the new updateVolumeReplicationAttributes endpoint
	res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolumeReplicationAttributes(ctx, updateRequest, updateParams)
	if err != nil {
		logger.Error("Failed to update volume replication attributes", "error", err)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, err)
	}

	// Check if it's an Operation response (202)
	if operation, ok := res.(*googleproxyclient.OperationV1beta); ok {
		logger.Info("Update volume replication attributes in new destination (original source) initiated",
			"operationName", operation.Name.Value,
			"operationDone", operation.Done.Value)
		result.JobId = &strings.Split(operation.Name.Value, "/")[7]
		return result, nil
	} else {
		logger.Warn("Unexpected response from updateVolumeReplicationAttributes")
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, err)
	}
}

// UpdateVolumeReplicationAttributes calls the new updateVolumeReplicationAttributes endpoint
func (a *ReverseVolumeReplicationActivity) UpdateVolumeReplicationAttributesDst(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateVolumeReplicationAttributes called on new source")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	// Get the original replication attributes before reversal
	originalAttrs := result.Event.ReplicationModel.ReplicationAttributes
	updateRequest := convertToReversedAttributes(originalAttrs)
	updateRequest.SetVolumeReplicationUuid(googleproxyclient.NewOptString(originalAttrs.DestinationReplicationUUID))
	updateRequest.SetEndpointType(googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeSrc)

	// Create parameters for the updateVolumeReplicationAttributes endpoint
	updateParams := googleproxyclient.V1betaInternalUpdateVolumeReplicationAttributesParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          originalAttrs.DestinationLocation,
		VolumeReplicationId: originalAttrs.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.CommonReplicationEventParams.XCorrelationID),
	}

	logger.Info("Calling updateVolumeReplicationAttributes with reversed attributes")

	// Call the new updateVolumeReplicationAttributes endpoint
	res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolumeReplicationAttributes(ctx, updateRequest, updateParams)
	if err != nil {
		logger.Error("Failed to update volume replication attributes", "error", err)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, err)
	}

	// Check if it's an Operation response (202)
	if operation, ok := res.(*googleproxyclient.OperationV1beta); ok {
		logger.Info("Update volume replication attributes in new source (original destination) initiated",
			"operationName", operation.Name.Value,
			"operationDone", operation.Done.Value)
		result.JobId = &strings.Split(operation.Name.Value, "/")[7]
		return result, nil
	} else {
		logger.Warn("Unexpected response from updateVolumeReplicationAttributes")
		return nil, errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, err)
	}
}

func convertToReversedAttributes(originalAttrs *datamodel.ReplicationDetails) *googleproxyclient.VolumeReplicationInternalV1beta {
	// Create the request body with REVERSED source/destination attributes
	// After reverse, what was destination becomes source and vice versa
	updateRequest := &googleproxyclient.VolumeReplicationInternalV1beta{
		// REVERSED: Original destination becomes new source
		SourceHostName:   originalAttrs.DestinationHostName,
		SourceServerName: originalAttrs.DestinationSvmName,
		SourceVolumeName: originalAttrs.DestinationVolumeName,
		SourceVolumeUuid: googleproxyclient.OptString{
			Value: originalAttrs.DestinationVolumeUUID,
			Set:   originalAttrs.DestinationVolumeUUID != "",
		},
		SourcePoolUuid: googleproxyclient.OptString{
			Value: originalAttrs.DestinationPoolUUID,
			Set:   originalAttrs.DestinationPoolUUID != "",
		},

		// REVERSED: Original source becomes new destination
		DestinationHostName:   originalAttrs.SourceHostName,
		DestinationServerName: originalAttrs.SourceSvmName,
		DestinationVolumeName: originalAttrs.SourceVolumeName,
		DestinationVolumeUuid: googleproxyclient.OptString{
			Value: originalAttrs.SourceVolumeUUID,
			Set:   originalAttrs.SourceVolumeUUID != "",
		},
		DestinationPoolUuid: googleproxyclient.OptString{
			Value: originalAttrs.SourcePoolUUID,
			Set:   originalAttrs.SourcePoolUUID != "",
		},
	}

	return updateRequest
}

// DescribeRemoteJobOnDst describes remote jobs for reverse operations
func (a *ReverseVolumeReplicationActivity) DescribeRemoteJobOnDst(ctx context.Context, result *replication.ReverseReplicationResult) error {
	// Describe the reverse job if it exists
	if result.JobId != nil {
		logger := util.GetLogger(ctx)
		logger.Debugf("DescribeRemoteJobOnDst: JobId=%v", result.JobId)

		err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation, result.Event.XCorrelationID)
		if err != nil {
			return err
		}
	}
	return nil
}

// DescribeRemoteJobOnSrc describes remote jobs for reverse operations
func (a *ReverseVolumeReplicationActivity) DescribeRemoteJobOnSrc(ctx context.Context, result *replication.ReverseReplicationResult) error {
	// Describe the reverse job if it exists
	if result.JobId != nil {
		logger := util.GetLogger(ctx)
		logger.Debugf("DescribeRemoteJobOnSrc: JobId=%v", result.JobId)

		err := activities.DescribeJob(ctx, result.JobId, result.SrcBasePath, result.SrcJwtToken, result.SrcProjectNumber, &result.Event.ReplicationModel.ReplicationAttributes.SourceLocation, result.Event.XCorrelationID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *ReverseVolumeReplicationActivity) VerifyNewDstVolume(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("verifyNewDstVolume")

	srcVolume, dstVolume, err := verifyDstVolume(ctx, result.Event.ReplicationModel.ReplicationAttributes, *result.SrcBasePath, *result.DstBasePath, *result.SrcJwtToken, *result.DstJwtToken, result.Event.SourceProjectNumber, result.Event.DestinationProjectNumber, result.Event.XCorrelationID, true)
	if err != nil {
		if err.(*errors.CustomError).TrackingID == errors.ErrVolumeNotFound {
			return nil, utilError.NewNonRetryableErr(err.Error())
		}
		return nil, err
	}
	// Set the new source and destination volumes in the result, verifyDstVolume returns srcVolume as the new source (original destination) and dstVolume as the new destination (original source)
	result.NewSrcVolume = &srcVolume
	result.NewDstVolume = &dstVolume
	return result, nil
}

func (a *ReverseVolumeReplicationActivity) ResizeNewDstVolumeIfNeeded(ctx context.Context, result *replication.ReverseReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("resizeNewDstVolumeIfNeeded")

	var srcVolumeQuota float64
	var dstVolumeQuota float64
	if result.NewSrcVolume.QuotaInBytes.Set {
		srcVolumeQuota = result.NewSrcVolume.QuotaInBytes.Value
	}
	if result.NewDstVolume.QuotaInBytes.Set {
		dstVolumeQuota = result.NewDstVolume.QuotaInBytes.Value
	}
	if srcVolumeQuota != dstVolumeQuota {
		// TODO: Resize the destination volume
		logger.Debugf("Resizing destination volume from %f to %f", dstVolumeQuota, srcVolumeQuota)
	}
	return nil
}

func (a *ReverseVolumeReplicationActivity) CleanupOldReplication(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	// Run CleanupReplicationAfterReverse on the original destination (new source)
	logger := util.GetLogger(ctx)
	logger.Debugf("CleanupReplicationAfterReverse")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.XCorrelationID),
		CleanupAfterReverse: googleproxyclient.NewOptBool(true),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		result.JobId = &r.Jobs[0].JobId.Value
		return result, nil
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New("unknown response type"))
	}
}

func (a *ReverseVolumeReplicationActivity) MountReplicationAfterReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	// Run mountVolumeReplication on the new destination (original source)
	logger := util.GetLogger(ctx)
	logger.Debugf("MountReplicationAfterReverse")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)

	createVolumeParams := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
	}

	res, err := googleProxyClient.Invoker.V1betaInternalMountVolumeReplication(ctx, *createVolumeParams)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.InternalJobV1beta:
		result.JobId = &r.JobUuid.Value
		return result, nil
	case *googleproxyclient.V1betaInternalMountVolumeReplicationBadRequest:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationUnauthorized:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationForbidden:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationNotFound:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationConflict:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationMethodNotAllowed:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationUnprocessableEntity:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalMountVolumeReplicationInternalServerError:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New(r.Message))
	default:
		return nil, errors.NewVCPError(errors.ErrMountingVolumeReplication, errors.New("unexpected response type from Google Proxy"))
	}
}
