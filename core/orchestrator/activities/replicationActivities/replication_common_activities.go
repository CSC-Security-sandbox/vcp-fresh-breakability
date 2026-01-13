package replicationActivities

import (
	"context"
	"errors"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	MapReplicationBetaToReplicationHydrateObject                = _mapReplicationBetaToReplicationHydrateObject
	mapReplicationLifeCycleStateBetaToReplicationHydrationState = _mapReplicationLifeCycleStateBetaToReplicationHydrationState
	mapVolumeBetaToVolumeHydrateObject                          = _mapVolumeBetaToVolumeHydrateObject
	HydrateVolumeReplication                                    = _hydrateVolumeReplication
	hydrationEnabled                                            = env.GetBool("GCP_HYDRATE_ENABLED", true)
	hydrateReplicationCreate                                    = common.ReplicationCreate
	hydrateVolumeCreate                                         = common.VolumeCreate
	hydrateVolumeDelete                                         = common.VolumeDelete
	hydrateReplicationState                                     = common.HydrateReplicationState
	hydrateReplicationStateAndType                              = common.HydrateReplicationStateAndType
	hydrateReplicationDelete                                    = common.ReplicationDelete
	getQuotaLimit                                               = common.GetQuotaLimit
	replicationInternalParseRegionAndZone                       = replication.InternalParseRegionAndZone
	replicationInternalUtilGetPairedRegionURI                   = replication.InternalUtilGetPairedRegionURI
	hydrateQuotaRuleCreate                                      = common.HydrateQuotaRuleCreate
)

const (
	// VolumeV1betaServiceLevelFLEX captures enum value "FLEX"
	VolumeV1betaServiceLevelFLEX string = "FLEX"
)

func _mapVolumeBetaToVolumeHydrateObject(volume models.Volume, poolResourceId string) models.VolumeHydrateObject {
	quotaInBytes := float64(volume.QuotaInBytes)
	return models.VolumeHydrateObject{
		ResourceId:   volume.DisplayName,
		VolumeId:     volume.UUID,
		PoolId:       poolResourceId,
		Protocols:    volume.ProtocolTypes,
		State:        "READY",
		QuotaInGib:   utils.ConvertBytesToGib(quotaInBytes),
		ServiceLevel: VolumeV1betaServiceLevelFLEX,
	}
}

func _mapReplicationBetaToReplicationHydrateObject(replication models.VolumeReplication) models.ReplicationHydrateObject {
	replicationHydrate := models.ReplicationHydrateObject{
		ResourceId:       replication.Name,
		ReplicationState: mapReplicationLifeCycleStateBetaToReplicationHydrationState(replication.State),
	}
	if replication.HybridReplicationAttributes != nil {
		replicationType := models.HybridReplicationHydrateType(replication.HybridReplicationAttributes.ReplicationType)
		replicationHydrate.HybridReplicationType = &replicationType
		if replication.HybridReplicationAttributes.Labels != nil {
			replicationHydrate.Labels = replication.HybridReplicationAttributes.Labels
		}
	}
	return replicationHydrate
}

func GetQuotaLimit(ctx context.Context, region string, project string) (int, error) {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return 0, err
	}
	// Hydrate GetQuotaLimit to CFFE
	quota, err := getQuotaLimit(ctx, logger, region, project, callbackToken, common.ResourceTypeVolume)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return 0, err
	}
	return quota, nil
}

func _hydrateVolumeReplication(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	replicationHydrateObject := MapReplicationBetaToReplicationHydrateObject(createReplicationResponse)
	// Hydrate Replication to CFFE
	err = hydrateReplicationCreate(ctx, logger, replicationHydrateObject, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, createReplicationResponse.ReplicationAttributes.DestinationVolumeName, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func DeHydrateVolumeReplication(ctx context.Context, createReplicationResponse models.VolumeReplication, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// DeHydrate Replication to CFFE
	err = hydrateReplicationDelete(ctx, logger, createReplicationResponse.Name, createReplicationResponse.Volume.DisplayName, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func HydrateVolume(ctx context.Context, destVolume models.Volume, project string, poolResourceId string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// Hydrate Volume to CFFE
	hydrateVolume := mapVolumeBetaToVolumeHydrateObject(destVolume, poolResourceId)
	err = hydrateVolumeCreate(ctx, logger, hydrateVolume, destVolume.Region, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func DeHydrateVolume(ctx context.Context, destVolume models.Volume, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// DeHydrate Volume to CFFE
	err = hydrateVolumeDelete(ctx, logger, destVolume.UUID, destVolume.Region, project, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func HydrateReplicationState(ctx context.Context, createReplicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// Hydrate Replication State to CFFE
	err = hydrateReplicationState(ctx, logger, createReplicationResponse.ReplicationAttributes.DestinationRegion, project, createReplicationResponse.ReplicationAttributes.DestinationVolumeName, createReplicationResponse.Name, replicationState, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func HydrateReplicationStateAndType(ctx context.Context, replicationResponse models.VolumeReplication, replicationState models.VolumeReplicationHydrateState, hybridReplicationType models.HybridReplicationParametersReplicationType, project string) error {
	logger := util.GetLogger(ctx)
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Error("Error when getting callback token", err)
		return err
	}
	// Hydrate Replication State & Type to CFFE
	err = hydrateReplicationStateAndType(ctx, logger, replicationResponse.ReplicationAttributes.DestinationRegion, project, replicationResponse.ReplicationAttributes.DestinationVolumeName, replicationResponse.Name, replicationState, hybridReplicationType, callbackToken)
	if err != nil {
		logger.Errorf("Error when hydrating replication: %v", err)
		return err
	}
	return nil
}

func _mapReplicationLifeCycleStateBetaToReplicationHydrationState(state string) string {
	switch state {
	case "creating":
		return "CREATING"
	case "available":
		return "READY"
	case "updating":
		return "UPDATING"
	case "disabled":
		return "STOPPED"
	case "deleting":
		return "DELETING"
	case "PENDING_CLUSTER_PEERING":
		return "PENDING_CLUSTER_PEERING"
	case "error":
		return "ERROR"
	default:
		return "STATE_UNSPECIFIED"
	}
}

func GetBasePath(ctx context.Context, location string) (*string, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("getBasePath")

	region, _, parseError := replicationInternalParseRegionAndZone(location)
	if parseError != nil {
		logger.Error("Parse Source Location Error")
		return nil, parseError
	}

	basePath, err := replicationInternalUtilGetPairedRegionURI(region)
	if err != nil {
		return nil, err
	}
	return &basePath, nil
}

func GetSignedToken(ctx context.Context, projectNumber string) (*string, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("getSignedToken")
	jwt, err := replication.InternalUtilGetSignedToken(projectNumber)
	if err != nil {
		return nil, err
	}
	return &jwt, nil
}

// convertDbModelToQuotaRulesV1beta converts a datamodel.QuotaRule to googleproxyclient.QuotaRulesV1beta
func convertDbModelToQuotaRulesV1beta(rule *datamodel.QuotaRule) googleproxyclient.QuotaRulesV1beta {
	clientRule := googleproxyclient.QuotaRulesV1beta{
		QuotaId:        googleproxyclient.NewOptString(rule.UUID),
		ResourceId:     rule.Name,
		DiskLimitInMib: rule.DiskLimitInKib / 1024, // Convert KiB to MiB
	}

	// Convert quota type string to enum
	switch rule.QuotaType {
	case "INDIVIDUAL_USER_QUOTA":
		clientRule.QuotaType = googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA
	case "INDIVIDUAL_GROUP_QUOTA":
		clientRule.QuotaType = googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA
	case "DEFAULT_USER_QUOTA":
		clientRule.QuotaType = googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA
	case "DEFAULT_GROUP_QUOTA":
		clientRule.QuotaType = googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA
	}

	// Set quota target if not empty
	if rule.QuotaTarget != "" {
		clientRule.QuotaTarget = googleproxyclient.NewOptString(rule.QuotaTarget)
	}

	return clientRule
}

// convertQuotaRulesV1betaToDbModel converts a googleproxyclient.QuotaRulesV1beta to datamodel.QuotaRule
func convertQuotaRulesV1betaToDbModel(clientRule googleproxyclient.QuotaRulesV1beta) *datamodel.QuotaRule {
	rule := &datamodel.QuotaRule{
		Name:           clientRule.ResourceId,
		DiskLimitInKib: clientRule.DiskLimitInMib * 1024, // Convert MiB to KiB
	}

	// Get the quota ID (UUID) from the client rule
	if quotaId, hasQuotaId := clientRule.QuotaId.Get(); hasQuotaId {
		rule.UUID = quotaId
	}

	// Convert quota type enum to string
	switch clientRule.QuotaType {
	case googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALUSERQUOTA:
		rule.QuotaType = "INDIVIDUAL_USER_QUOTA"
	case googleproxyclient.QuotaRulesV1betaQuotaTypeINDIVIDUALGROUPQUOTA:
		rule.QuotaType = "INDIVIDUAL_GROUP_QUOTA"
	case googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTUSERQUOTA:
		rule.QuotaType = "DEFAULT_USER_QUOTA"
	case googleproxyclient.QuotaRulesV1betaQuotaTypeDEFAULTGROUPQUOTA:
		rule.QuotaType = "DEFAULT_GROUP_QUOTA"
	}

	// Set quota target if available
	if quotaTarget, hasQuotaTarget := clientRule.QuotaTarget.Get(); hasQuotaTarget {
		rule.QuotaTarget = quotaTarget
	}

	// Set state if available
	if state, hasState := clientRule.State.Get(); hasState {
		rule.State = string(state)
	}

	// Set state details if available
	if stateDetails, hasStateDetails := clientRule.StateDetails.Get(); hasStateDetails {
		rule.StateDetails = stateDetails
	}

	return rule
}

// CreateQuotaRulesRemote is a generic helper function that creates quota rules on a remote volume.
// It can be used in replication and other processes that need to sync quota rules between volumes.
func CreateQuotaRulesRemote(
	ctx context.Context,
	logger log.Logger,
	basePath string,
	jwtToken string,
	projectNumber string,
	locationId string,
	volumeId string,
	correlationID string,
	srcQuotaRules []*datamodel.QuotaRule,
	dstQuotaRules []*datamodel.QuotaRule,
) ([]*datamodel.QuotaRule, error) {
	// Create Google Proxy client
	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwtToken, logger)

	// Convert source quota rules to client type
	srcQuotaRulesClient := make([]googleproxyclient.QuotaRulesV1beta, 0)
	if srcQuotaRules != nil {
		srcQuotaRulesClient = make([]googleproxyclient.QuotaRulesV1beta, 0, len(srcQuotaRules))
		for _, rule := range srcQuotaRules {
			clientRule := convertDbModelToQuotaRulesV1beta(rule)
			srcQuotaRulesClient = append(srcQuotaRulesClient, clientRule)
		}
	}

	// Convert destination quota rules to client type
	dstQuotaRulesClient := make([]googleproxyclient.QuotaRulesV1beta, 0)
	if dstQuotaRules != nil {
		dstQuotaRulesClient = make([]googleproxyclient.QuotaRulesV1beta, 0, len(dstQuotaRules))
		for _, rule := range dstQuotaRules {
			clientRule := convertDbModelToQuotaRulesV1beta(rule)
			dstQuotaRulesClient = append(dstQuotaRulesClient, clientRule)
		}
	}

	// Create request body
	requestBody := &googleproxyclient.UpdateDstWithSrcQuotaRulesV1beta{
		SrcQuotaRules: srcQuotaRulesClient,
		DstQuotaRules: dstQuotaRulesClient,
	}

	// Create API parameters
	params := googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPParams{
		ProjectNumber:  projectNumber,
		LocationId:     locationId,
		VolumeId:       volumeId,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	// Call the API
	res, err := googleProxyClient.Invoker.V1betaUpdateDestinationQuotaRulesVCP(ctx, requestBody, params)
	if err != nil {
		logger.Errorf("Failed to call V1betaUpdateDestinationQuotaRulesVCP: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, err)
	}

	// Handle response
	switch r := res.(type) {
	case *googleproxyclient.UpdateDestinationQuotaRulesResponseV1beta:
		logger.Infof("Successfully synced %d source and %d destination quota rules to volume", len(srcQuotaRulesClient), len(dstQuotaRulesClient))

		// Convert the returned quota rules from the API response to datamodel.QuotaRule
		quotaRules := make([]*datamodel.QuotaRule, 0)
		if r.QuotaRules != nil {
			quotaRules = make([]*datamodel.QuotaRule, 0, len(r.QuotaRules))
			for _, clientRule := range r.QuotaRules {
				quotaRule := convertQuotaRulesV1betaToDbModel(clientRule)
				quotaRules = append(quotaRules, quotaRule)
			}
		}

		return quotaRules, nil
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPBadRequest:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnauthorized:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPForbidden:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPNotFound:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPUnprocessableEntity:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPTooManyRequests:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateDestinationQuotaRulesVCPInternalServerError:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New(r.Message)))
	default:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCreateInternalReplication, errors.New("unexpected response type from Google Proxy")))
	}
}

// HydrateQuotaRulesList hydrates a list of quota rules by calling the callback API.
// This is a common function that can be used in replication and other processes.
func HydrateQuotaRulesList(
	ctx context.Context,
	logger log.Logger,
	quotaRules []*datamodel.QuotaRule,
	volumeResourceId string,
	location string,
	projectNumber string,
) error {
	// Get callback token for hydration
	callbackToken, err := auth.GenerateCallbackToken(ctx)
	if err != nil {
		logger.Errorf("Error when getting callback token: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrHydrateVolumeCreate, err))
	}

	// Hydrate each quota rule
	for _, quotaRule := range quotaRules {
		var qRule models.QuotaRuleHydrateObject

		// Validate that the quota rule has a UUID
		if quotaRule.UUID == "" {
			logger.Errorf("Quota rule does not have UUID set: name=%s", quotaRule.Name)
			return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrHydrateVolumeCreate, errors.New("quota rule missing UUID")))
		}

		qRule.ResourceId = quotaRule.Name
		qRule.QuotaRuleId = quotaRule.UUID

		// Call the hydration function
		err := hydrateQuotaRuleCreate(ctx, logger, qRule, volumeResourceId, location, projectNumber, callbackToken)
		if err != nil {
			logger.Errorf("Failed to hydrate quota rule: resourceId=%s, quotaId=%s, error=%v", qRule.ResourceId, qRule.QuotaRuleId, err)
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrHydrateVolumeCreate, err))
		}

		logger.Infof("Successfully hydrated quota rule: resourceId=%s, quotaId=%s", qRule.ResourceId, qRule.QuotaRuleId)
	}

	logger.Infof("Successfully hydrated %d quota rules for volume: %s", len(quotaRules), volumeResourceId)
	return nil
}
