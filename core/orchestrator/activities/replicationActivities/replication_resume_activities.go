package replicationActivities

import (
	"context"
	"fmt"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
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

func (a *ResumeVolumeReplicationActivity) GetDstBasePathResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
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
	if result.IsHybridReplicationVolume {
		return result, nil
	}
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
	if result.IsHybridReplicationVolume {
		return result, nil
	}
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

func (a *ResumeVolumeReplicationActivity) SetHybridReplicationVariablesResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	if result.DbVolReplication != nil && result.DbVolReplication.HybridReplicationAttributes != nil {
		logger.Infof("Replication is a hybrid replication")
		result.IsHybridReplicationVolume = true
	}
	if replication.IsSrcForHybridReplication(result.DbVolReplication) {
		result.IsSrcForHybridReplication = true
	}
	return result, nil
}

func (a *ResumeVolumeReplicationActivity) MountReplicationAfterResume(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("MountReplicationAfterResume")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	mountVolumeParams := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.Event.ReplicationModel.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.XCorrelationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalMountVolumeReplication(ctx, *mountVolumeParams)
	if err != nil {
		logger.Errorf("MountReplicationAfterResume err: %v", err)
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

func (a *ResumeVolumeReplicationActivity) HandleHybridReplicationResumeWhenGcnvIsSrc(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("HandleHybridReplicationWhenGcnvIsSrc")

	// Query the replication from database
	dbReplication, err := a.SE.GetVolumeReplication(ctx, result.Event.ReplicationModel.UUID)
	if err != nil {
		logger.Errorf("Failed to get replication from database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}

	// Ensure HybridReplicationAttributes exists
	if dbReplication.HybridReplicationAttributes == nil {
		dbReplication.HybridReplicationAttributes = &datamodel.HybridReplicationAttribute{}
	}

	// Generate commands for resuming replication
	if dbReplication.ReplicationAttributes != nil {
		extOntapPath := getPath(dbReplication.ReplicationAttributes.DestinationSvmName, dbReplication.ReplicationAttributes.DestinationVolumeName)
		gcnvPath := getPath(dbReplication.ReplicationAttributes.SourceSvmName, dbReplication.ReplicationAttributes.SourceVolumeName)
		commands := []string{
			"# Please run the following command once on your ONTAP system.",
			fmt.Sprintf("snapmirror resync -source-path %s -destination-path %s", gcnvPath, extOntapPath),
			"# If ran successfully, MirrorState will switch to SnapMirrored after a few minutes. Please check by running:",
			fmt.Sprintf("snapmirror show -source-path %s -destination-path %s", gcnvPath, extOntapPath),
		}
		dbReplication.HybridReplicationAttributes.HybridReplicationUserCommands = commands
	}

	// Update the replication in database
	err = a.SE.UpdateVolumeReplication(ctx, dbReplication)
	if err != nil {
		logger.Errorf("Failed to update replication in database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	logger.Infof("Successfully updated HybridReplicationUserCommands for replication %s", dbReplication.UUID)
	return result, nil
}

// ListQuotaRulesOnSourceResume lists quota rules on the source volume via VCP API
func (a *ResumeVolumeReplicationActivity) ListQuotaRulesOnSourceResume(ctx context.Context, result *replication.ResumeReplicationResult) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ListQuotaRulesOnSourceResume")

	// Extract source volume details from the result
	sourceLocation := result.Event.ReplicationModel.ReplicationAttributes.SourceLocation
	sourceVolumeUUID := result.Event.ReplicationModel.ReplicationAttributes.SourceVolumeUUID
	sourceProjectNumber := result.Event.SourceProjectNumber

	// Get correlation ID
	correlationID := *result.Event.XCorrelationID

	// Call the generic helper function to list quota rules
	quotaRules, err := ListAllQuotaRulesOnVolume(
		ctx,
		logger,
		*result.SrcBasePath,
		*result.SrcJwtToken,
		sourceProjectNumber,
		sourceLocation,
		sourceVolumeUUID,
		correlationID,
	)
	if err != nil {
		return nil, err
	}

	return quotaRules, nil
}

// ListQuotaRulesOnDestinationResume lists quota rules on the destination volume via VCP API
func (a *ResumeVolumeReplicationActivity) ListQuotaRulesOnDestinationResume(ctx context.Context, result *replication.ResumeReplicationResult) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ListQuotaRulesOnDestinationResume")

	// Extract destination volume details from the result
	destinationLocation := result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation
	destinationVolumeUUID := result.Event.ReplicationModel.ReplicationAttributes.DestinationVolumeUUID
	destinationProjectNumber := result.Event.DestinationProjectNumber

	// Get correlation ID
	correlationID := *result.Event.XCorrelationID

	// Call the generic helper function to list quota rules
	quotaRules, err := ListAllQuotaRulesOnVolume(
		ctx,
		logger,
		*result.DstBasePath,
		*result.DstJwtToken,
		destinationProjectNumber,
		destinationLocation,
		destinationVolumeUUID,
		correlationID,
	)
	if err != nil {
		return nil, err
	}

	return quotaRules, nil
}

// DehydrateQuotaRulesResume notifies CCFE about quota rule deletions (dehydration)
func (a *ResumeVolumeReplicationActivity) DehydrateQuotaRulesResume(ctx context.Context, quotaRules []*datamodel.QuotaRule, volumeResourceId string, location string, projectNumber string) ([]*datamodel.QuotaRule, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("DehydrateQuotaRulesResume: volumeResourceId=%s, location=%s, quotaRuleCount=%d",
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

// AddSrcQuotaRulesToDstDB syncs source quota rules to destination volume via API and returns the quota rules for hydration
func (a *ResumeVolumeReplicationActivity) AddSrcQuotaRulesToDstDB(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("AddSrcQuotaRulesToDstDB")

	// Check if there are any quota rules to sync
	if len(result.SourceQuotaRules) == 0 && len(result.DestinationQuotaRules) == 0 {
		logger.Infof("No quota rules to sync, skipping")
		return result, nil
	}

	// Check for failure recovery scenario: nil source with non-nil destination
	// This indicates we need to re-sync dehydrated quota rules back to destination
	if len(result.SourceQuotaRules) == 0 && len(result.DestinationQuotaRules) > 0 {
		logger.Warnf("Failure recovery mode: re-syncing %d dehydrated quota rules back to destination",
			len(result.DestinationQuotaRules))
		// Continue with the API call using nil source and provided destination quota rules
	}

	// Extract destination volume details
	destinationLocation := result.Event.ReplicationModel.ReplicationAttributes.DestinationLocation
	destinationVolumeUUID := result.DstVolume.VolumeId.Value
	destinationProjectNumber := result.Event.DestinationProjectNumber

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Call the generic helper function and receive the quota rules returned from the API
	destinationQuotaRules, err := CreateQuotaRulesRemote(
		ctx,
		logger,
		*result.DstBasePath,
		*result.DstJwtToken,
		destinationProjectNumber,
		destinationLocation,
		destinationVolumeUUID,
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

// HydrateQuotaRulesResume hydrates quota rules on the destination volume by calling the callback API.
// This activity takes the destination quota rules (with their UUIDs) and hydrates them to CCFE.
func (a *ResumeVolumeReplicationActivity) HydrateQuotaRulesResume(ctx context.Context, quotaRules []*datamodel.QuotaRule, volumeResourceId string, location string, projectNumber string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("HydrateQuotaRulesResume")

	// Call the common hydration function
	return HydrateQuotaRulesList(ctx, logger, quotaRules, volumeResourceId, location, projectNumber)
}
