package replicationActivities

import (
	"context"
	"fmt"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
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

func convertToOriginalAttributes(originalAttrs *datamodel.ReplicationDetails) *googleproxyclient.VolumeReplicationInternalV1beta {
	return &googleproxyclient.VolumeReplicationInternalV1beta{
		SourceHostName:   originalAttrs.SourceHostName,
		SourceServerName: originalAttrs.SourceSvmName,
		SourceVolumeName: originalAttrs.SourceVolumeName,
		SourceVolumeUuid: googleproxyclient.OptString{
			Value: originalAttrs.SourceVolumeUUID,
			Set:   originalAttrs.SourceVolumeUUID != "",
		},
		SourcePoolUuid: googleproxyclient.OptString{
			Value: originalAttrs.SourcePoolUUID,
			Set:   originalAttrs.SourcePoolUUID != "",
		},
		DestinationHostName:   originalAttrs.DestinationHostName,
		DestinationServerName: originalAttrs.DestinationSvmName,
		DestinationVolumeName: originalAttrs.DestinationVolumeName,
		DestinationVolumeUuid: googleproxyclient.OptString{
			Value: originalAttrs.DestinationVolumeUUID,
			Set:   originalAttrs.DestinationVolumeUUID != "",
		},
		DestinationPoolUuid: googleproxyclient.OptString{
			Value: originalAttrs.DestinationPoolUUID,
			Set:   originalAttrs.DestinationPoolUUID != "",
		},
	}
}

// DeleteNewReplicationOnSrc deletes the new replication created by ReverseAndResumeReplication on the source side.
// Used as a rollback activity when subsequent steps fail after the reverse replication is created.
func (a *ReverseVolumeReplicationActivity) DeleteNewReplicationOnSrc(ctx context.Context, result *replication.ReverseReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Info("Rollback: deleting new replication on source created by reverse")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)

	deleteParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.SourceReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.XCorrelationID),
		CleanupAfterReverse: googleproxyclient.NewOptBool(true),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeReplication(ctx, *deleteParams)
	if err != nil {
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		logger.Info("Rollback: successfully deleted new replication on source")
		return nil
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationNotFound:
		logger.Info("Rollback: replication already deleted on source, skipping")
		return nil
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationBadRequest:
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationInternalServerError:
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationUnauthorized:
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	case *googleproxyclient.V1betaInternalDeleteVolumeReplicationForbidden:
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New(r.Message))
	default:
		return errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, errors.New("unknown response type"))
	}
}

// RevertVolumeReplicationAttributesSrc reverts the attribute changes made by UpdateVolumeReplicationAttributesSrc
// by restoring the original (pre-reverse) attributes on the source side.
func (a *ReverseVolumeReplicationActivity) RevertVolumeReplicationAttributesSrc(ctx context.Context, result *replication.ReverseReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Info("Rollback: reverting volume replication attributes on source")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)

	originalAttrs := result.Event.ReplicationModel.ReplicationAttributes
	revertRequest := convertToOriginalAttributes(originalAttrs)
	revertRequest.SetVolumeReplicationUuid(googleproxyclient.NewOptString(originalAttrs.SourceReplicationUUID))
	revertRequest.SetEndpointType(googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeSrc)

	revertParams := googleproxyclient.V1betaInternalUpdateVolumeReplicationAttributesParams{
		ProjectNumber:       *result.SrcProjectNumber,
		LocationId:          originalAttrs.SourceLocation,
		VolumeReplicationId: originalAttrs.SourceReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.CommonReplicationEventParams.XCorrelationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalUpdateVolumeReplicationAttributes(ctx, revertRequest, revertParams)
	if err != nil {
		logger.Error("Rollback: failed to revert volume replication attributes on source", "error", err)
		return errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, err)
	}

	if _, ok := res.(*googleproxyclient.OperationV1beta); ok {
		logger.Info("Rollback: successfully reverted volume replication attributes on source")
		return nil
	}

	logger.Warn("Rollback: unexpected response from revert volume replication attributes")
	return errors.NewVCPError(errors.ErrGoogleProxyUpdateReplicationAttributes, errors.New("unexpected response type"))
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

func (a *ReverseVolumeReplicationActivity) ResizeNewDstVolumeIfNeeded(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
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
	if srcVolumeQuota > dstVolumeQuota {
		logger.Debugf("Resizing new destination volume from %f to %f", dstVolumeQuota, srcVolumeQuota)
		updateRequest := &googleproxyclient.VolumeUpdateV1beta{
			QuotaInBytes: googleproxyclient.NewOptNilFloat64(srcVolumeQuota),
		}
		// resize the new destination volume (original source)
		updateVolumeParams := &googleproxyclient.V1betaInternalUpdateVolumeParams{
			ProjectNumber:  *result.SrcProjectNumber,
			LocationId:     result.Event.ReplicationModel.ReplicationAttributes.SourceLocation,
			VolumeId:       result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID,
			XCorrelationID: googleproxyclient.NewOptString(*result.Event.XCorrelationID),
		}

		googleProxyClient := googleproxyclient.GetGProxyClient(*result.SrcBasePath, *result.SrcJwtToken, logger)
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

	return nil, nil
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

// SetVolumeReplicationStatusToOnpremReplication updates hybrid replication attributes to ONPREM status
func (a *ReverseVolumeReplicationActivity) SetVolumeReplicationStatusToOnpremReplication(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("setVolumeReplicationStatusToOnpremReplication")

	// Get the latest replication from database
	dbReplication, err := a.SE.GetVolumeReplication(ctx, result.Event.ReplicationModel.UUID)
	if err != nil {
		logger.Errorf("Failed to get replication from database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}

	// Set hybrid replication type to ONPREM
	hybridReplicationType := string(datamodel.HybridReplicationParametersReplicationTypeONPREM)
	dbReplication.HybridReplicationAttributes.HybridReplicationType = &hybridReplicationType

	// Set status to Peered
	dbReplication.HybridReplicationAttributes.Status = datamodel.HybridReplicationStatusPeered

	// Set status details to empty string
	dbReplication.HybridReplicationAttributes.StatusDetails = ""

	// Clear user commands (set to nil)
	dbReplication.HybridReplicationAttributes.HybridReplicationUserCommands = nil

	// Update the replication in database
	err = a.SE.UpdateVolumeReplication(ctx, dbReplication)
	if err != nil {
		logger.Errorf("Failed to update replication in database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	logger.Infof("Successfully updated replication to ONPREM status: UUID=%s", dbReplication.UUID)
	return result, nil
}

// ReleaseReplicationOnOldSrc releases the replication on the source side
func (a *ReverseVolumeReplicationActivity) ReleaseReplicationOnOldSrc(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("ReleaseReplicationOnOldSrc")

	// Get provider from node
	provider, err := vsa.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Construct VSA VolumeReplication from datamodel
	replicationAttrs := result.DbVolReplication.ReplicationAttributes
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
			ExternalUUID: result.DbVolReplication.Volume.VolumeAttributes.ExternalUUID,
		},
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
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalReleaseVolumeReplicationError, err)
	}

	return result, nil
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
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.XCorrelationID),
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

func ConvertToReversedAttributesForHybridRep(originalAttrs *datamodel.ReplicationDetails) *gcpserver.VolumeReplicationInternalV1beta {
	// Create the request body with REVERSED source/destination attributes
	// After reverse, what was destination becomes source and vice versa
	updateRequest := &gcpserver.VolumeReplicationInternalV1beta{
		// REVERSED: Original destination becomes new source
		SourceHostName:   originalAttrs.DestinationHostName,
		SourceServerName: originalAttrs.DestinationSvmName,
		SourceVolumeName: originalAttrs.DestinationVolumeName,
		SourceVolumeUuid: gcpserver.OptString{
			Value: originalAttrs.DestinationVolumeUUID,
			Set:   originalAttrs.DestinationVolumeUUID != "",
		},
		SourcePoolUuid: gcpserver.OptString{
			Value: originalAttrs.DestinationPoolUUID,
			Set:   originalAttrs.DestinationPoolUUID != "",
		},

		// REVERSED: Original source becomes new destination
		DestinationHostName:   originalAttrs.SourceHostName,
		DestinationServerName: originalAttrs.SourceSvmName,
		DestinationVolumeName: originalAttrs.SourceVolumeName,
		DestinationVolumeUuid: gcpserver.OptString{
			Value: originalAttrs.SourceVolumeUUID,
			Set:   originalAttrs.SourceVolumeUUID != "",
		},
		DestinationPoolUuid: gcpserver.OptString{
			Value: originalAttrs.SourcePoolUUID,
			Set:   originalAttrs.SourcePoolUUID != "",
		},
		EndpointType: gcpserver.VolumeReplicationInternalV1betaEndpointType(googleproxyclient.VolumeReplicationInternalV1betaEndpointTypeDst),
	}

	return updateRequest
}

func (a *ReverseVolumeReplicationActivity) HydrateReplicationSateAndTypeForReverseFallbackHybridReplication(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	err := HydrateReplicationStateAndTypeForHybridReplication(ctx, result.DbVolReplication, models.VolumeReplicationHydrateStateReady, datamodel.HybridReplicationParametersReplicationTypeONPREM, result.Event.Location, result.Event.VolumeResourceID)
	if err != nil {
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	return result, nil
}

// ListQuotaRulesOnNewSourceReverse lists quota rules on the new source volume (current destination) via VCP API
func (a *ReverseVolumeReplicationActivity) ListQuotaRulesOnNewSourceReverse(ctx context.Context, result *replication.ReverseReplicationResult) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ListQuotaRulesOnNewSourceReverse")

	// Extract new source volume details (current destination)
	newSourceLocation := result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation
	newSourceVolumeUUID := result.NewSrcVolume.VolumeId.Value
	newSourceProjectNumber := result.Event.DestinationProjectNumber

	// Get correlation ID
	correlationID := *result.Event.XCorrelationID

	// Call the generic helper function to list quota rules
	quotaRules, err := ListAllQuotaRulesOnVolume(
		ctx,
		logger,
		*result.DstBasePath,
		*result.DstJwtToken,
		newSourceProjectNumber,
		newSourceLocation,
		newSourceVolumeUUID,
		correlationID,
	)
	if err != nil {
		return nil, err
	}

	return quotaRules, nil
}

// ListQuotaRulesOnNewDestinationReverse lists quota rules on the new destination volume (current source) via VCP API
func (a *ReverseVolumeReplicationActivity) ListQuotaRulesOnNewDestinationReverse(ctx context.Context, result *replication.ReverseReplicationResult) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ListQuotaRulesOnNewDestinationReverse")

	// Extract new destination volume details (current source)
	newDestinationLocation := result.Event.ReplicationModel.ReplicationAttributes.SourceLocation
	newDestinationVolumeUUID := result.NewDstVolume.VolumeId.Value
	newDestinationProjectNumber := result.Event.SourceProjectNumber

	// Get correlation ID
	correlationID := *result.Event.XCorrelationID

	// Call the generic helper function to list quota rules
	quotaRules, err := ListAllQuotaRulesOnVolume(
		ctx,
		logger,
		*result.SrcBasePath,
		*result.SrcJwtToken,
		newDestinationProjectNumber,
		newDestinationLocation,
		newDestinationVolumeUUID,
		correlationID,
	)
	if err != nil {
		return nil, err
	}

	return quotaRules, nil
}

// DehydrateQuotaRulesReverse notifies CCFE about quota rule deletions (dehydration) during reverse resume
func (a *ReverseVolumeReplicationActivity) DehydrateQuotaRulesReverse(ctx context.Context, quotaRules []*datamodel.QuotaRule, volumeResourceId string, location string, projectNumber string) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DehydrateQuotaRulesReverse: volumeResourceId=%s, location=%s, quotaRuleCount=%d",
		volumeResourceId, location, len(quotaRules))

	// Generate callback token for CCFE authentication
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Errorf("Failed to generate callback token for quota rule dehydration: %v", err)
		return []*datamodel.QuotaRule{}, errors.NewVCPError(errors.ErrFailedToGenerateAccessToken, fmt.Errorf("failed to generate callback token: %w", err))
	}

	// Call the tracking function that dehydrates one by one and tracks successes
	dehydratedQuotaRules, err := DehydrateQuotaRules(
		ctx,
		logger,
		quotaRules,
		volumeResourceId,
		location,
		projectNumber,
		callbackToken,
	)

	// Return the list of successfully dehydrated quota rules (even on error)
	// This allows the caller to handle partial failures
	return dehydratedQuotaRules, err
}

// AddNewSrcQuotaRulesToNewDstDBReverse syncs new source quota rules to new destination volume via API and returns the quota rules for hydration
func (a *ReverseVolumeReplicationActivity) AddNewSrcQuotaRulesToNewDstDBReverse(ctx context.Context, result *replication.ReverseReplicationResult) (*replication.ReverseReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("AddNewSrcQuotaRulesToNewDstDBReverse")

	// Check if there are any quota rules to sync
	if len(result.SourceQuotaRules) == 0 && len(result.DestinationQuotaRules) == 0 {
		logger.Infof("No quota rules to sync, skipping")
		return result, nil
	}

	// Check for failure recovery scenario: nil source with non-nil destination
	// This indicates we need to re-sync dehydrated quota rules back to destination
	if len(result.SourceQuotaRules) == 0 && len(result.DestinationQuotaRules) > 0 {
		logger.Warnf("Failure recovery mode: re-syncing %d dehydrated quota rules back to new destination",
			len(result.DestinationQuotaRules))
		// Continue with the API call using nil source and provided destination quota rules
	}

	// Extract new destination volume details (current source)
	newDestinationLocation := result.Event.ReplicationModel.ReplicationAttributes.SourceLocation
	newDestinationVolumeUUID := result.NewDstVolume.VolumeId.Value
	newDestinationProjectNumber := result.Event.SourceProjectNumber

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Call the generic helper function and receive the quota rules returned from the API
	destinationQuotaRules, err := CreateQuotaRulesRemote(
		ctx,
		logger,
		*result.SrcBasePath,
		*result.SrcJwtToken,
		newDestinationProjectNumber,
		newDestinationLocation,
		newDestinationVolumeUUID,
		correlationID,
		result.SourceQuotaRules,
		result.DestinationQuotaRules,
	)
	if err != nil {
		return nil, err
	}

	// Store the quota rules returned from the API in the result
	result.DestinationQuotaRules = destinationQuotaRules
	logger.Infof("Stored %d destination quota rules from CreateQuotaRulesRemote response", len(destinationQuotaRules))

	return result, nil
}

// HydrateQuotaRulesReverse hydrates quota rules on the new destination volume by calling the callback API.
// This activity takes the destination quota rules (with their UUIDs) and hydrates them to CCFE.
func (a *ReverseVolumeReplicationActivity) HydrateQuotaRulesReverse(ctx context.Context, quotaRules []*datamodel.QuotaRule, volumeResourceId string, location string, projectNumber string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("HydrateQuotaRulesReverse")

	// Call the common hydration function
	return HydrateQuotaRulesList(ctx, logger, quotaRules, volumeResourceId, location, projectNumber)
}
