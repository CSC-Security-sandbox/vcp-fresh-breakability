package activities

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

// QuotaRuleCommonActivity contains activities shared between create and update workflows
type QuotaRuleCommonActivity struct {
	SE database.Storage
}

// QuotaRuleCreateActivity contains create-specific activities
type QuotaRuleCreateActivity struct {
	SE database.Storage
}

// QuotaRuleUpdateActivity contains update-specific activities
type QuotaRuleUpdateActivity struct {
	SE database.Storage
}

// getBasePathForQuotaRule gets the base path for a given location/region.
// This is a local helper to avoid import cycle with replicationActivities.
func getBasePathForQuotaRule(ctx context.Context, location string) (*string, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("getBasePathForQuotaRule")

	region, _, parseError := utils.ParseRegionAndZone(location)
	if parseError != nil {
		logger.Error("Parse Location Error")
		return nil, parseError
	}

	basePath, err := replication.InternalUtilGetPairedRegionURI(region)
	if err != nil {
		return nil, err
	}
	return &basePath, nil
}

const (
	// Quota type identifiers used in protocol validation
	QuotaIndividualGroup = "INDIVIDUAL_GROUP_QUOTA"
	QuotaDefaultGroup    = "DEFAULT_GROUP_QUOTA"
)

// ValidateQuotaTargetByProtocol performs protocol-specific quotaTarget validations.
// It checks SMB, NFS (v3/v4) and Dual Protocol rules and returns a user-validation
// error when the quotaTarget does not conform to the expected format for the
// volume's protocols.
func (a *QuotaRuleCreateActivity) ValidateQuotaTargetByProtocol(ctx context.Context, quotaRule *datamodel.QuotaRule, protocolTypes []string) error {
	logger := util.GetLogger(ctx)

	protocolTypesLength := len(protocolTypes)

	if HasCIFS(protocolTypes) {
		if quotaRule.QuotaType == QuotaIndividualGroup || quotaRule.QuotaType == QuotaDefaultGroup {
			logger.Errorf("Protocol type validation failed: group quota not allowed for SMB volume")
			err := customerrors.NewUserInputValidationErr("Group Quota cannot be specified for a SMB and Dual Protocol volume. To create this quota rule the quotaType to DefaultUserQuota/IndividualUserQuota type")
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		if protocolTypesLength == 1 {
			re := regexp.MustCompile(`^S-1-[0-59]-\d{2}-\d{8,10}-\d{8,10}-\d{8,10}-[1-9]\d{3}`)
			if quotaRule.QuotaTarget != "" && !re.MatchString(quotaRule.QuotaTarget) {
				logger.Errorf("SMB quota target validation failed: invalid SID format: %s", quotaRule.QuotaTarget)
				err := customerrors.NewUserInputValidationErr("quotaTarget is invalid. Please pass valid SID in quotaTarget for SMB volume")
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
	}

	if (HasNFSv3(protocolTypes) || HasNFSv4(protocolTypes)) && protocolTypesLength == 1 {
		re := regexp.MustCompile(`^[0-9]*$`)
		if !re.MatchString(quotaRule.QuotaTarget) {
			logger.Errorf("NFS quota target validation failed: non-numeric value: %s", quotaRule.QuotaTarget)
			err := customerrors.NewUserInputValidationErr("quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
			return vsaerrors.WrapAsTemporalApplicationError(err)
		} else if quotaRule.QuotaTarget != "" {
			numericQuotaTarget, err := strconv.Atoi(quotaRule.QuotaTarget)
			if err != nil {
				logger.Errorf("NFS quota target validation failed: conversion error: %s", quotaRule.QuotaTarget)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}

			if quotaRule.QuotaTarget != "" && re.MatchString(quotaRule.QuotaTarget) && (numericQuotaTarget < 0 || numericQuotaTarget > 4294967295) {
				logger.Errorf("NFS quota target validation failed: out of range: %s", quotaRule.QuotaTarget)
				err := customerrors.NewUserInputValidationErr("quotaTarget is invalid. Please pass numeric value for quotaTarget in range [0, 4294967295] for NFS volumes")
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
	}

	if HasDualProtocolForUserMapping(protocolTypes) {
		re := regexp.MustCompile(`^S-1-[0-59]-\d{2}-\d{8,10}-\d{8,10}-\d{8,10}-[1-9]\d{3}`)
		re1 := regexp.MustCompile(`^[0-9]*$`)
		if !(re.MatchString(quotaRule.QuotaTarget) || re1.MatchString(quotaRule.QuotaTarget)) {
			logger.Errorf("Dual protocol quota target validation failed: invalid format: %s", quotaRule.QuotaTarget)
			err := customerrors.NewUserInputValidationErr("quotaTarget is invalid. Please pass numeric value in range [0, 4294967295] or SID for quotaTarget for dual protocol volumes")
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

// GetVolumeByID fetches authoritative volume details from the database by numeric ID.
func (a *QuotaRuleCommonActivity) GetVolumeByID(ctx context.Context, volumeID int64, accountID int64) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	volume, err := se.GetVolumeByIDAndAccountID(ctx, volumeID, accountID)
	if err != nil {
		logger.Errorf("GetVolumeByID failed for ID %d: %v", volumeID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return volume, nil
}

// CreateQuotaRuleForDataProtectionVolume handles quota rule creation for DP (Data Protection) volumes.
// DP volumes are SnapMirror destinations where quotas are managed by the source volume.
// This activity only updates the database to set the quota rule state to AVAILABLE without any ONTAP operations.
func (a *QuotaRuleCreateActivity) CreateQuotaRuleForDataProtectionVolume(ctx context.Context, quotaRule *datamodel.QuotaRule) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Update the quota rule in database
	_, err := se.UpdateQuotaRule(ctx, quotaRule)
	if err != nil {
		logger.Errorf("Failed to create quota rule for DP volume: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// GetVolumeReplication fetches replication details for a volume.
// Returns the list of replications associated with the volume.
func (a *QuotaRuleCommonActivity) GetVolumeReplication(ctx context.Context, volumeID int64) ([]*datamodel.VolumeReplication, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Fetch replications for this volume using ListVolumeReplications with volume_id filter
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("volume_id", "=", volumeID))

	replications, err := se.ListVolumeReplications(ctx, *filter, database.QueryDepthZero)
	if err != nil {
		logger.Errorf("Failed to list volume replications for volume_id %d: %v", volumeID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return replications, nil
}

// GetSignedDstTokenForQuotaRule retrieves and stores JWT token for destination project.
// This token will be reused across all destination API calls to avoid multiple token creations.
// Following the replication workflow pattern for consistency.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - projectNumber: GCP project number for the destination
//
// Returns:
//   - jwtToken: Pointer to JWT token string for reuse
//   - error: Error if token creation fails
func (a *QuotaRuleCommonActivity) GetSignedDstTokenForQuotaRule(
	ctx context.Context,
	projectNumber string,
) (*string, error) {
	logger := util.GetLogger(ctx)
	jwtToken, err := auth.GetSignedJwtToken(projectNumber)
	if err != nil {
		logger.Errorf("Failed to get JWT token for destination project %s: %v", projectNumber, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Successfully obtained JWT token for destination project %s", projectNumber)
	return &jwtToken, nil
}

// GetOntapQuotaUUID retrieves the UUID of an existing quota rule from ONTAP.
func (a *QuotaRuleCommonActivity) GetOntapQuotaUUID(
	ctx context.Context,
	volumeDetails *datamodel.Volume,
	node *models.Node,
	quotaType string,
	target string,
) (string, error) {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Get the volume UUID from VolumeAttributes
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return "", vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no ExternalUUID"))
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	// Get SVM name - check if it's already loaded
	var svmName string
	if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
		svmName = volumeDetails.Svm.Name
	} else {
		logger.Errorf("Volume %s has no SVM details loaded", volumeDetails.UUID)
		return "", vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no SVM details"))
	}

	// Call provider to get quota UUID and type
	quotaUUID, _, err := provider.GetOntapQuotaUUIDAndType(ctx, volumeUUID, svmName, quotaType, target)
	if err != nil {
		logger.Errorf("Failed to get quota UUID and type: %v", err)
		return "", vsaerrors.WrapAsTemporalApplicationError(err)
	}
	if quotaUUID == "" {
		logger.Errorf("No quota UUID found for volume %s, quotaType %s, target %s", volumeUUID, quotaType, target)
		return "", vsaerrors.WrapAsTemporalApplicationError(customerrors.NewNotFoundErr("Quota", nil))
	}

	return quotaUUID, nil
}

// GetReplicationDetails fetches replication details from the destination region
func GetReplicationDetails(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
	logger := util.GetLogger(ctx)

	logger.Debug(
		"Fetching destination replication for quota validation",
		"basePath", basePath,
		"projectNumber", projectNumber,
		"locationID", locationID,
		"volumeReplicationID", volumeReplicationID,
	)

	googleProxyClient := googleproxyclient.GetGProxyClient(basePath, jwt, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  projectNumber,
		LocationId:     locationID,
		XCorrelationID: googleproxyclient.NewOptString(utils.GetCoRelationIDFromContext(ctx)),
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{volumeReplicationID}}

	response, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		logger.Error("Failed to fetch destination replication", "error", err)
		return nil, coreerrors.NewVCPError(coreerrors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}

	replicationResponse, ok := response.(*googleproxyclient.V1betaGetMultipleReplicationsInternalOK)
	if !ok || replicationResponse == nil || len(replicationResponse.Replications) < 1 {
		logger.Error("Destination replication not found or invalid response")
		return nil, coreerrors.NewVCPError(coreerrors.ErrGoogleProxyInternalGetMultipleReplicationsNotFound,
			fmt.Errorf("destination replication %s not found", volumeReplicationID))
	}

	// Convert response to models.VolumeReplication
	dstReplication := ConvertGoogleProxyReplicationToModel(replicationResponse)
	return dstReplication, nil
}

// ConvertGoogleProxyReplicationToModel converts google-proxy replication response to models.VolumeReplication
func ConvertGoogleProxyReplicationToModel(response *googleproxyclient.V1betaGetMultipleReplicationsInternalOK) *models.VolumeReplication {
	if response == nil || response.Replications == nil || len(response.Replications) < 1 {
		return nil
	}

	rep := response.Replications[0]
	replication := &models.VolumeReplication{}

	if rep.MirrorState.Set {
		mirrorState := string(rep.MirrorState.Value)
		replication.MirrorState = &mirrorState
	}

	if rep.RelationshipStatus.Set {
		relationshipStatus := string(rep.RelationshipStatus.Value)
		replication.RelationshipStatus = &relationshipStatus
	}

	return replication
}

// VerifyReplicationState validates that a single replication is eligible for quota rule sync.
// This activity checks replication state to ensure quota operations are safe.
//
// Parameters:
//   - ctx: Context for logging
//   - replication: Single volume replication to check
//   - locationId: Current location (zone) to check for destination sync eligibility
//
// Returns:
//   - eligible: Boolean indicating if the replication is eligible for quota sync
//   - error: Error if replication state check fails
func (a *QuotaRuleCommonActivity) VerifyReplicationState(
	ctx context.Context,
	replication *datamodel.VolumeReplication,
	locationId string,
) (bool, error) {
	logger := util.GetLogger(ctx)

	// If replication is nil or has no attributes, it's not eligible
	if replication == nil || replication.ReplicationAttributes == nil {
		logger.Debugf("Replication is nil or has no attributes, not eligible for sync")
		return false, nil
	}

	// Parse current region from locationId
	currentRegion, _, err := utils.ParseRegionAndZone(locationId)
	if err != nil {
		logger.Errorf("Failed to parse region from locationId %s: %v", locationId, err)
		return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to parse region from locationId: %w", err))
	}

	// Parse destination region from destination location to compare with current region
	destRegion, _, err := utils.ParseRegionAndZone(replication.ReplicationAttributes.DestinationLocation)
	if err != nil {
		logger.Warnf("Failed to parse destination location %s: %v", replication.ReplicationAttributes.DestinationLocation, err)
		return false, nil
	}

	// Check if current region is NOT the destination region
	// Only perform mirror state check when we're on the source side (current region != destination region)
	if destRegion != currentRegion {
		// We are on the source side - fetch destination replication to get accurate mirror state
		// Parse destination project number from RemoteUri
		destProjectNumber, err := utils.ParseProjectNumberFromURI(replication.RemoteUri)
		if err != nil {
			logger.Errorf("Failed to parse destination project number from RemoteUri: %v, remoteUri: %s", err, replication.RemoteUri)
			return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to parse destination project number: %w", err))
		}

		// Get destination region base path
		destBasePath, err := utils.GetPairedRegionURI(destRegion)
		if err != nil {
			logger.Errorf("Failed to get paired destination region URI: %v", err)
			return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
		}

		logger.Debug("Fetching destination replication details",
			"projectNumber", destProjectNumber,
			"region", destRegion,
			"location", replication.ReplicationAttributes.DestinationLocation,
			"replicationUUID", replication.ReplicationAttributes.DestinationReplicationUUID)

		// Get JWT token for destination project
		dstToken, err := auth.GetSignedJwtToken(destProjectNumber)
		if err != nil {
			logger.Errorf("Failed to get signed token: %v", err)
			return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get signed token: %w", err))
		}

		// Fetch destination replication details
		destinationReplication, err := GetReplicationDetails(ctx, destBasePath, destProjectNumber,
			replication.ReplicationAttributes.DestinationLocation,
			replication.ReplicationAttributes.DestinationReplicationUUID, dstToken)
		if err != nil {
			logger.Errorf("Failed to fetch destination replication: %v", err)
			return false, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to fetch destination replication for validation: %w", err))
		}

		// Check if this replication is eligible for destination sync using destination replication's mirror state
		// Only MIRRORED or UNINITIALIZED states are eligible
		if destinationReplication.MirrorState != nil {
			mirrorState := *destinationReplication.MirrorState
			if mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateMIRRORED) ||
				mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateUNINITIALIZED) {
				// This replication is eligible for sync
				logger.Infof("Replication eligible for sync: mirrorState=%s, destinationLocation=%s, destinationVolumeUUID=%s, projectNumber=%s",
					*destinationReplication.MirrorState, replication.ReplicationAttributes.DestinationLocation,
					replication.ReplicationAttributes.DestinationVolumeUUID, destProjectNumber)
				return true, nil
			}
		}
	}

	logger.Debugf("Replication is not eligible for sync: destinationLocation=%s, destinationVolumeUUID=%s",
		replication.ReplicationAttributes.DestinationLocation, replication.ReplicationAttributes.DestinationVolumeUUID)
	return false, nil
}

// UpdateQuotaRulesOnOntap updates a quota rule's disk limit on ONTAP.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - externalQuotaUUID: The ONTAP UUID of the quota rule to update
//   - node: Node structure for creating the provider
//   - diskLimitInKibs: The new disk limit in kibibytes (KiB)
//
// Returns:
//   - error: Error if the update fails or the job returns failure status
func (a *QuotaRuleUpdateActivity) UpdateQuotaRulesOnOntap(
	ctx context.Context,
	externalQuotaUUID string,
	node *models.Node,
	diskLimitInKibs int64,
) error {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for quota rule update: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	quotaModifyParams := &vsa.UpdateQuotaRuleParams{
		ExternalQuotaRuleUUID: externalQuotaUUID,
		DiskLimitInKibs:       diskLimitInKibs,
	}

	quotaModifyRuleResp, err := provider.UpdateQuotaRule(ctx, quotaModifyParams)
	if err != nil {
		logger.Errorf("Failed to update quota rule on ONTAP: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if quotaModifyRuleResp.IsFailure() {
		logger.Errorf("Quota update job failed: %s", quotaModifyRuleResp.Message)
		return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewBadRequestErr(
			quotaModifyRuleResp.Message))
	}

	return nil
}

// RevertQuotaRulesOnSource reverts the quota rule on the source ONTAP back to its original value.
// This is used when destination sync fails to maintain source-destination synchronization.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - externalQuotaUUID: The ONTAP UUID of the quota rule to revert
//   - node: Node structure for creating the provider
//   - originalDiskLimitInKib: The original disk limit in kibibytes (KiB) to revert to
//
// Returns:
//   - error: Error if the revert fails
func (a *QuotaRuleUpdateActivity) RevertQuotaRulesOnSource(
	ctx context.Context,
	externalQuotaUUID string,
	node *models.Node,
	originalDiskLimitInKib int64,
) error {
	logger := util.GetLogger(ctx)

	logger.Infof("Reverting quota rule on source ONTAP: quotaUUID=%s, originalDiskLimit=%d KiB",
		externalQuotaUUID, originalDiskLimitInKib)

	// Use the existing UpdateQuotaRulesOnOntap activity with the original value
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for quota rule update: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	quotaModifyParams := &vsa.UpdateQuotaRuleParams{
		ExternalQuotaRuleUUID: externalQuotaUUID,
		DiskLimitInKibs:       originalDiskLimitInKib,
	}

	quotaModifyRuleResp, err := provider.UpdateQuotaRule(ctx, quotaModifyParams)
	if err != nil {
		logger.Errorf("Failed to update quota rule on ONTAP: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if quotaModifyRuleResp.IsFailure() {
		logger.Errorf("Quota update job failed: %s", quotaModifyRuleResp.Message)
		return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewBadRequestErr(
			quotaModifyRuleResp.Message))
	}

	return nil
}

// ReplicationSyncEligibility holds the result of checking replication sync eligibility.
type ReplicationSyncEligibility struct {
	Eligible              bool
	DestinationVolumeUUID string
	DestinationLocation   string
	DestinationProjectNum string // Destination GCP project number for API calls
}

// QuotaRuleOperationResult holds the result of a quota rule operation on destination.
// When an async operation is started, OperationName will contain the operation identifier
// that can be polled for completion.
type QuotaRuleOperationResult struct {
	OperationName string // Operation name/ID for polling (empty if operation completed synchronously)
	IsDone        bool   // True if operation completed synchronously
}

// UpdateRQuotaOnSvm enables recursive quota on the SVM if required for quota rule creation.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - svmExternalUUID: External UUID of the SVM
//   - node: Node structure for creating provider
//   - rquota: Boolean flag indicating whether to enable (true) or disable (false) rquota
//
// Returns error if:
//   - SVM ExternalUUID is empty
//   - ModifyRquota API call fails
func (a *QuotaRuleCreateActivity) UpdateRQuotaOnSvm(ctx context.Context, svmExternalUUID string, node *models.Node, rquota bool) error {
	logger := util.GetLogger(ctx)

	// Validate SVM ExternalUUID
	if svmExternalUUID == "" {
		logger.Errorf("SVM ExternalUUID is empty")
		return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"SVM ExternalUUID is required"))
	}

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for SVM %s: %v", svmExternalUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Call ModifyRquota on the provider
	err = provider.ModifyRquota(ctx, svmExternalUUID, rquota)
	if err != nil {
		logger.Errorf("Failed to modify RQuota on SVM %s: %v", svmExternalUUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return nil
}

// HandleDefaultQuotaRuleUpdate checks if a default quota rule already exists for the volume
// and updates it if found. This handles the case where a default quota was auto-created
// as a side effect of creating an individual quota.
//
// Parameters:
//   - volumeDetails: Volume containing ExternalUUID and Svm.SvmDetails for SVM name
//   - node: Node structure for creating provider
//   - quotaType: Type of quota (user/group)
//   - diskLimitInKibs: Disk limit in kibibytes
//
// Returns:
//   - NotFoundErr: If the default quota doesn't exist (acceptable - workflow will proceed with creation)
//   - Error: For any other failure
func (a *QuotaRuleCreateActivity) HandleDefaultQuotaRuleUpdate(
	ctx context.Context,
	volumeDetails *datamodel.Volume,
	node *models.Node,
	quotaType string,
	diskLimitInKibs int64,
) error {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Get the volume UUID from VolumeAttributes
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no ExternalUUID"))
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	// Get SVM name - check if it's already loaded
	var svmName string
	if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
		svmName = volumeDetails.Svm.Name
	} else {
		logger.Errorf("Volume %s has no SVM details loaded", volumeDetails.UUID)
		return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no SVM details"))
	}

	// This searches through all quota rules on the volume
	// For default quotas, target is empty string
	quotaUUID, ontapQuotaType, err := provider.GetOntapQuotaUUIDAndType(ctx, volumeUUID, svmName, quotaType, "")
	if err != nil {
		logger.Errorf("Failed to get quota UUID and type: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if quotaUUID != "" {
		quotaModifyParams := &vsa.UpdateQuotaRuleParams{
			ExternalQuotaRuleUUID: quotaUUID,
			DiskLimitInKibs:       diskLimitInKibs,
		}

		quotaModifyRuleResp, err := provider.UpdateQuotaRule(ctx, quotaModifyParams)
		if err != nil {
			logger.Errorf("Failed to update default quota rule on ONTAP: %v", err)
			return vsaerrors.WrapAsTemporalApplicationError(err)
		}

		if quotaModifyRuleResp.IsFailure() {
			logger.Errorf("Quota update job failed: %s", quotaModifyRuleResp.Message)
			return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewBadRequestErr(
				quotaModifyRuleResp.Message))
		}

		// If explicit quota exists and same type of default quota rule is getting created,
		// reinitialization is not required
		convertedQuotaType := vsa.GetQuotaTypeForOntap(quotaType)
		if convertedQuotaType != ontapQuotaType {
			logger.Infof("Quota type mismatch detected: converted=%s, ontap=%s - triggering reinitialization",
				convertedQuotaType, ontapQuotaType)

			// Call helper function to perform quota reinitialization
			err = performQuotaReinitialization(ctx, provider, volumeDetails)
			if err != nil {
				logger.Errorf("Failed to reinitialize quota: %v", err)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
		}
		return nil
	}
	return customerrors.NewNotFoundErr("Default Quota", nil)
}

// CreateQuotaRuleOnONTAP creates a quota rule on ONTAP using the provider.
// This activity follows the spec pattern (Step 6 of handleQuotaCreationOnOntap):
// It directly uses the provider to call CreateQuotaRule with the necessary parameters.
//
// Parameters:
//   - node: Node structure for creating provider
//   - volumeDetails: Volume containing ExternalUUID and Svm details
//   - quotaRule: Database quota rule with all the quota configuration
//
// Returns:
//   - QuotaRuleProviderResponse: Response from ONTAP with operation state and message
//   - Error: For any failure during quota creation
func (a *QuotaRuleCreateActivity) CreateQuotaRuleOnONTAP(
	ctx context.Context,
	node *models.Node,
	volumeDetails *datamodel.Volume,
	quotaRule *datamodel.QuotaRule,
) (*vsa.QuotaRuleProviderResponse, error) {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Validate inputs
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no ExternalUUID"))
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	logger.Infof("Creating quota rule on ONTAP: volumeUUID=%s, quotaType=%s, quotaTarget=%s, diskLimit=%d KiB",
		volumeUUID, quotaRule.QuotaType, quotaRule.QuotaTarget, quotaRule.DiskLimitInKib)

	// Get SVM name from volume details
	var svmName string
	if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
		svmName = volumeDetails.Svm.Name
	} else {
		logger.Errorf("Volume %s has no SVM details", volumeDetails.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no SVM details"))
	}

	// Prepare quota rule parameters for ONTAP (following spec Step 6)
	// DiskLimitInKib is already in KiB from database, which is what CreateQuotaRule expects
	params := vsa.CreateQuotaRuleParams{
		VolumeUUID:     volumeUUID,
		SVMName:        svmName,
		QuotaTarget:    quotaRule.QuotaTarget,
		QuotaType:      quotaRule.QuotaType,
		DiskLimitInKib: quotaRule.DiskLimitInKib,
		RQuota:         quotaRule.RQuota,
	}

	jobStatus, err := provider.CreateQuotaRule(ctx, params)
	if err != nil {
		logger.Errorf("Failed to create quota rule on ONTAP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Per spec, CreateQuotaRule returns *JobStatus (not QuotaRuleProviderResponse)
	// Convert to QuotaRuleProviderResponse for backward compatibility
	res := &vsa.QuotaRuleProviderResponse{
		State:   jobStatus.State,
		Message: jobStatus.Message,
	}

	logger.Infof("Successfully created quota rule on ONTAP with State: %s", res.State)
	return res, nil
}

// GetQuotaStatus retrieves the current quota system status for a volume.
//
// Parameters:
//   - node: Node structure for creating provider
//   - volumeDetails: Volume containing ExternalUUID
//
// Returns:
//   - QuotaStatus: Current quota status with Enabled flag and State string
//   - Error: For any failure
func (a *QuotaRuleCreateActivity) GetQuotaStatus(
	ctx context.Context,
	node *models.Node,
	volumeDetails *datamodel.Volume,
) (*vsa.QuotaStatus, error) {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Validate inputs
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no ExternalUUID"))
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	logger.Infof("Getting quota status for volume: %s", volumeUUID)

	// Call provider to get quota status
	quotaStatus, err := provider.GetQuotaStatus(ctx, volumeUUID)
	if err != nil {
		logger.Errorf("Failed to get quota status from ONTAP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Quota status for volume %s: enabled=%t, state=%s",
		volumeUUID, quotaStatus.Enabled, quotaStatus.State)

	return quotaStatus, nil
}

func performQuotaReinitialization(
	ctx context.Context,
	provider vsa.Provider,
	volumeDetails *datamodel.Volume,
) error {
	logger := util.GetLogger(ctx)

	// Validate inputs
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return customerrors.NewUserInputValidationErr("Volume has no ExternalUUID")
	}

	// Get SVM name
	var svmName string
	if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
		svmName = volumeDetails.Svm.Name
	} else {
		logger.Errorf("Volume %s has no SVM details loaded", volumeDetails.UUID)
		return customerrors.NewUserInputValidationErr("Volume has no SVM details")
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	logger.Infof("Starting quota reinitialization for volume: %s (disable then enable)", volumeUUID)

	quotaDisableResp, err := provider.QuotaEnableDisable(context.TODO(), volumeUUID, svmName, false)
	if err != nil {
		logger.Errorf("Failed to disable quota: %v", err)
		return err
	}

	if quotaDisableResp.State == vsa.JobRespFailure {
		// Delete the quota rule from ONTAP
		logger.Errorf("Quota disable failed: %s", quotaDisableResp.Message)
		return customerrors.NewUserInputValidationErr(quotaDisableResp.Message)
	}

	logger.Infof("Successfully disabled quota on volume: %s", volumeUUID)

	quotaEnableResp, err := provider.QuotaEnableDisable(context.TODO(), volumeUUID, svmName, true)
	if err != nil {
		logger.Errorf("Failed to enable quota: %v", err)
		return err
	}

	if quotaEnableResp.State == vsa.JobRespFailure {
		logger.Errorf("Quota enable failed: %s", quotaEnableResp.Message)
		return customerrors.NewUserInputValidationErr(quotaEnableResp.Message)
	}

	logger.Infof("Successfully enabled quota on volume: %s (reinitialization complete)", volumeUUID)
	return nil
}

// QuotaReinitialization performs quota reinitialization (disable then enable).
// This activity matches the spec (create-quota-cvs-job-function.md, Section 3, Sub-function: quotaReinitialization).
// It reinitializes the quota system on a volume by disabling and then re-enabling quotas.
// This is required when quota rule changes cannot be applied through a simple resize operation,
// typically when changing quota types, when resize operations fail, or when activation operations fail.
//
// According to the spec (Step 7), reinitialization is needed when:
// - Resize operation failed
// - Activation operation failed
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - provider: ONTAP provider for making REST API calls
//   - volumeDetails: Volume containing ExternalUUID and Svm details
//
// Returns:
//   - Error: Wrapped error for Temporal if reinitialization fails
func (a *QuotaRuleCreateActivity) QuotaReinitialization(
	ctx context.Context,
	node *models.Node,
	volumeDetails *datamodel.Volume,
) error {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	err = performQuotaReinitialization(ctx, provider, volumeDetails)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

// HandleQuotaEnableDisable enables quota on a volume.
// This activity calls QuotaEnableDisable and returns the response for workflow-level processing.
// Response validation is done in the workflow, following the sample code pattern.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - provider: ONTAP provider for making REST API calls
//   - volumeDetails: Volume containing ExternalUUID and Svm details
//
// Returns:
//   - *vsa.JobStatus: Job status response from QuotaEnableDisable
//   - error: Error if the API call fails
func (a *QuotaRuleCreateActivity) HandleQuotaEnableDisable(
	ctx context.Context,
	node *models.Node,
	volumeDetails *datamodel.Volume,
) (*vsa.JobStatus, error) {
	logger := util.GetLogger(ctx)

	// Create provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, node)
	if err != nil {
		logger.Errorf("Failed to create provider for volume %s: %v", volumeDetails.UUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Validate inputs
	if volumeDetails.VolumeAttributes == nil || volumeDetails.VolumeAttributes.ExternalUUID == "" {
		logger.Errorf("Volume %s has no ExternalUUID in VolumeAttributes", volumeDetails.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no ExternalUUID"))
	}

	// Get SVM name
	var svmName string
	if volumeDetails.Svm != nil && volumeDetails.Svm.Name != "" {
		svmName = volumeDetails.Svm.Name
	} else {
		logger.Errorf("Volume %s has no SVM details loaded", volumeDetails.UUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
			"Volume has no SVM details"))
	}

	volumeUUID := volumeDetails.VolumeAttributes.ExternalUUID

	logger.Infof("Enabling quota for the first time on volume: %s", volumeUUID)

	// Call provider to enable quota and return response for workflow processing
	jobStatus, err := provider.QuotaEnableDisable(ctx, volumeUUID, svmName, true)
	if err != nil {
		logger.Errorf("Failed to enable quota: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return jobStatus, nil
}

// hasProtocol checks if a protocol exists in the given protocol types array
func hasProtocol(protocol string, protocolTypes []string) bool {
	for _, protocolType := range protocolTypes {
		if protocolType == protocol {
			return true
		}
	}
	return false
}

// HasCIFS checks if CIFS (SMB) protocol exists in the given protocol types array
func HasCIFS(protocolTypes []string) bool {
	return hasProtocol(utils.ProtocolSMB, protocolTypes)
}

// HasNFSv3 checks if NFSv3 protocol exists in the given protocol types array
func HasNFSv3(protocolTypes []string) bool {
	return hasProtocol(utils.ProtocolNFSv3, protocolTypes)
}

// HasNFSv4 checks if NFSv4 protocol exists in the given protocol types array
func HasNFSv4(protocolTypes []string) bool {
	return hasProtocol(utils.ProtocolNFSv4, protocolTypes)
}

// HasDualProtocolForUserMapping checks if both SMB and NFS protocols exist (dual protocol)
func HasDualProtocolForUserMapping(protocolTypes []string) bool {
	return HasCIFS(protocolTypes) && (HasNFSv3(protocolTypes) || HasNFSv4(protocolTypes))
}

// UpdateQuotaRuleState updates the quota rule state in the database.
// If the quota rule is in CREATING state, it will transition to READY state
// and update the ExternalUUID. If in UPDATING state, it transitions to READY with updated fields.
func (a *QuotaRuleCommonActivity) UpdateQuotaRuleState(ctx context.Context, quotaRule datamodel.QuotaRule) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Fetch current quota rule from DB to check its state
	currentQuotaRule, err := se.GetQuotaRuleByUUID(ctx, quotaRule.UUID, quotaRule.AccountID)
	if err != nil {
		logger.Errorf("Failed to fetch quota rule for state check: uuid=%s, error=%v", quotaRule.UUID, err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// If quotaRule.State is ERROR (set by defer blocks), set it to ERROR regardless of current state
	if quotaRule.State == models.LifeCycleStateError {
		logger.Infof("Setting quota rule state to ERROR: uuid=%s", quotaRule.UUID)
		currentQuotaRule.State = models.LifeCycleStateError
		currentQuotaRule.StateDetails = quotaRule.StateDetails
		currentQuotaRule.UpdatedAt = time.Now()
	} else if currentQuotaRule.State == models.LifeCycleStateCreating {
		// If quota rule is in CREATING state, transition to READY
		logger.Infof("Quota rule is in CREATING state, transitioning to READY: uuid=%s", quotaRule.UUID)
		currentQuotaRule.State = models.LifeCycleStateREADY
		currentQuotaRule.StateDetails = models.LifeCycleStateReadyDetails
	} else if currentQuotaRule.State == models.LifeCycleStateUpdating {
		currentQuotaRule.DiskLimitInKib = quotaRule.DiskLimitInKib
		currentQuotaRule.Description = quotaRule.Description
		currentQuotaRule.State = models.LifeCycleStateREADY
		currentQuotaRule.StateDetails = models.LifeCycleStateReadyDetails

		logger.Infof("Quota rule is in UPDATING state, transitioning to READY: uuid=%s", quotaRule.UUID)
		currentQuotaRule.UpdatedAt = time.Now()
	} else if currentQuotaRule.State == models.LifeCycleStateDeleting {
		updatedAt := time.Now()
		currentQuotaRule.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		currentQuotaRule.State = models.LifeCycleStateDeleted
		currentQuotaRule.StateDetails = models.LifeCycleStateDeletedDetails
		currentQuotaRule.UpdatedAt = updatedAt
		currentQuotaRule.DeletedAt = &gorm.DeletedAt{Time: updatedAt, Valid: true}
	}

	_, err = se.UpdateQuotaRule(ctx, currentQuotaRule)
	if err != nil {
		logger.Errorf("Failed to update quota rule state in DB: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Quota rule state: %s updated successfully in the db", quotaRule.Name)

	return nil
}

// DescribeQuotaRuleRemoteJob polls the status of an async quota rule operation on the destination VCP.
// This follows the same pattern as DescribeRemoteJob in replication activities.
// The activity will return an error if the operation is not yet complete (allowing Temporal to retry),
// return nil if completed successfully, or return a non-retryable error if the operation failed.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - operationName: The operation name/ID to poll
//   - destinationRegion: The destination region
//   - projectNumber: GCP project number for the destination
//   - jwtToken: Reused JWT token for VCP-to-VCP authentication (created once in workflow)
//
// Returns:
//   - error: Returns ErrJobNotFinished (retryable) if operation is in progress,
//     nil if operation completed successfully, or a non-retryable error if operation failed
func (a *QuotaRuleCommonActivity) DescribeQuotaRuleRemoteJob(
	ctx context.Context,
	operationName string,
	destinationRegion string,
	projectNumber string,
	jwtToken *string,
) error {
	logger := util.GetLogger(ctx)

	if operationName == "" {
		logger.Debugf("No operation name provided, skipping polling")
		return nil
	}

	logger.Infof("Polling quota rule operation on destination: operationName=%s, region=%s, projectNumber=%s",
		operationName, destinationRegion, projectNumber)

	// Get destination VCP base path
	dstBasePath, err := getBasePathForQuotaRule(ctx, destinationRegion)
	if err != nil {
		logger.Errorf("Failed to get destination base path for region %s: %v", destinationRegion, err)
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
	}

	// Validate JWT token is provided
	if jwtToken == nil || *jwtToken == "" {
		logger.Errorf("JWT token is required for destination API call")
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("JWT token is required"))
	}

	// Create Google Proxy client for the destination VCP region
	googleProxyClient := googleproxyclient.GetGProxyClient(*dstBasePath, *jwtToken, logger)

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Prepare describe operation params
	describeParams := googleproxyclient.V1betaDescribeOperationParams{
		OperationId:    operationName,
		ProjectNumber:  projectNumber,
		LocationId:     destinationRegion,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaDescribeOperation(ctx, describeParams)
	if err != nil {
		logger.Errorf("Failed to describe operation %s: %v", operationName, err)
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJobAPI, err)
	}

	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if !ok {
		logger.Errorf("Unexpected response type from V1betaDescribeOperation: %T", res)
		return vsaerrors.NewVCPError(vsaerrors.ErrDescribingJobAPI, fmt.Errorf("unexpected response type: %T", res))
	}

	if operation.Done.Value {
		// Operation completed
		if operation.Error.IsSet() {
			// Operation failed
			errorMsg := operation.Error.Value.Message.Value
			logger.Errorf("Quota rule operation %s failed: %s", operationName, errorMsg)
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrJobFailed, fmt.Errorf("quota rule operation failed: %s", errorMsg)))
		}
		// Operation succeeded
		logger.Infof("Quota rule operation %s completed successfully", operationName)
		return nil
	}

	// Operation still in progress - return retryable error so Temporal will retry
	logger.Infof("Quota rule operation %s is still in progress, will retry", operationName)
	return vsaerrors.NewVCPError(vsaerrors.ErrJobNotFinished, fmt.Errorf("operation %s not finished", operationName))
}

// CreateQuotaRuleOnDestination calls the internal VCP API to create quota rule on destination volume.
// This follows the same pattern as CreateReplicationOnDestination - calling the remote VCP region
// via google-proxy-client for VCP-to-VCP communication during replication.
//
// The endpoint is:
// POST /v1beta/internal/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}/quotaRule
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - destinationVolumeUUID: UUID of the destination volume where quota rule will be created
//   - quotaRule: The quota rule to replicate
//   - destinationRegion: The destination region
//   - projectNumber: GCP project number
//   - jwtToken: Reused JWT token for VCP-to-VCP authentication (created once in workflow)
//
// Returns:
//   - QuotaRuleOperationResult: Contains operation info if async, nil if completed synchronously
//   - error: Error if API call fails (wrapped for Temporal)
func (a *QuotaRuleCreateActivity) CreateQuotaRuleOnDestination(
	ctx context.Context,
	destinationVolumeUUID string,
	quotaRule *datamodel.QuotaRule,
	destinationRegion string,
	projectNumber string,
	jwtToken *string,
) (*QuotaRuleOperationResult, error) {
	logger := util.GetLogger(ctx)

	logger.Infof("Creating quota rule on destination volume: volumeUUID=%s, region=%s, quotaType=%s, diskLimit=%d",
		destinationVolumeUUID, destinationRegion, quotaRule.QuotaType, quotaRule.DiskLimitInKib)

	// Get destination VCP base path (similar to GetDstBasePath in replication)
	dstBasePath, err := getBasePathForQuotaRule(ctx, destinationRegion)
	if err != nil {
		logger.Errorf("Failed to get destination base path for region %s: %v", destinationRegion, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
	}

	// Validate JWT token is provided
	if jwtToken == nil || *jwtToken == "" {
		logger.Errorf("JWT token is required for destination API call")
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("JWT token is required"))
	}

	// Create Google Proxy client for the destination VCP region
	// This is the same pattern used in CreateReplicationOnDestination
	googleProxyClient := googleproxyclient.GetGProxyClient(*dstBasePath, *jwtToken, logger)

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Prepare API parameters
	params := googleproxyclient.V1betaCreateQuotaRuleVCPParams{
		ProjectNumber:  projectNumber,
		LocationId:     destinationRegion,
		VolumeId:       destinationVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	// Convert disk limit from KiB to MiB (API expects MiB)
	diskLimitInMib := quotaRule.DiskLimitInKib / 1024

	// Prepare quota rule creation request body
	body := &googleproxyclient.QuotaRuleCreateV1beta{
		ResourceId:     quotaRule.Name,
		QuotaType:      googleproxyclient.QuotaRuleCreateV1betaQuotaType(quotaRule.QuotaType),
		QuotaTarget:    googleproxyclient.NewOptString(quotaRule.QuotaTarget),
		DiskLimitInMib: diskLimitInMib,
		Description:    googleproxyclient.NewOptString(quotaRule.Description),
	}

	// Call the internal VCP API to create quota rule on destination
	logger.Infof("Calling internal VCP API to create quota rule on destination: volumeUUID=%s", destinationVolumeUUID)
	res, err := googleProxyClient.Invoker.V1betaCreateQuotaRuleVCP(ctx, body, params)
	if err != nil {
		logger.Errorf("Failed to call V1betaCreateQuotaRuleVCP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to create quota rule on destination: %w", err))
	}

	// Handle response types (following the CreateReplicationOnDestination pattern)
	switch r := res.(type) {
	case *googleproxyclient.QuotaRulesVCPV1beta:
		logger.Infof("Successfully created quota rule on destination: quotaId=%s, resourceId=%s, state=%s",
			r.QuotaId.Value, r.ResourceId, r.State.Value)

		// Check if state is CREATING - need to poll for completion
		if r.State.Value == googleproxyclient.QuotaRulesVCPV1betaStateCREATING {
			// Extract JobId from Jobs array if available for polling
			if len(r.Jobs) > 0 && r.Jobs[0].JobId.IsSet() {
				jobId := r.Jobs[0].JobId.Value
				logger.Infof("Quota rule creation in progress on destination, returning JobId for polling: %s", jobId)
				return &QuotaRuleOperationResult{OperationName: jobId, IsDone: false}, nil
			}
			// No job ID available but still creating - return as done (best effort)
			logger.Warnf("Quota rule is in CREATING state but no JobId found, assuming success")
		}
		// State is READY or other terminal state
		return &QuotaRuleOperationResult{IsDone: true}, nil
	case *googleproxyclient.V1betaCreateQuotaRuleVCPBadRequest:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPUnauthorized:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPForbidden:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPNotFound:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPConflict:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPMethodNotAllowed:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPRequestTimeout:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPUnprocessableEntity:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPTooManyRequests:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPServiceUnavailable:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPInternalServerError:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	default:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New("unexpected response type from Google Proxy")))
	}
}

// GetMatchingQuotaRuleOnDestination calls the VCP API to fetch quota rules from a destination volume in a remote region
// and returns the matching quota rule by name.
// The endpoint is:
// GET /v1beta/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}/quotaRules
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - destinationVolumeUUID: UUID of the destination volume to fetch quota rules from
//   - destinationRegion: The destination region
//   - projectNumber: GCP project number
//   - quotaRuleName: Name of the quota rule to match (ResourceId)
//   - jwtToken: Reused JWT token for VCP-to-VCP authentication (created once in workflow)
//
// Returns:
//   - matchingQuotaRule: The matching quota rule from the destination volume
//   - error: Error if API call fails or no matching quota rule found (wrapped for Temporal)
func (a *QuotaRuleUpdateActivity) GetMatchingQuotaRuleOnDestination(
	ctx context.Context,
	destinationVolumeUUID string,
	destinationRegion string,
	projectNumber string,
	quotaRuleName string,
	jwtToken *string,
) (*string, error) {
	logger := util.GetLogger(ctx)

	logger.Infof("Fetching quota rules from destination volume: volumeUUID=%s, region=%s, quotaRuleName=%s",
		destinationVolumeUUID, destinationRegion, quotaRuleName)

	// Get destination VCP base path (similar to GetDstBasePath in replication)
	dstBasePath, err := getBasePathForQuotaRule(ctx, destinationRegion)
	if err != nil {
		logger.Errorf("Failed to get destination base path for region %s: %v", destinationRegion, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
	}

	// Validate JWT token is provided
	if jwtToken == nil || *jwtToken == "" {
		logger.Errorf("JWT token is required for destination API call")
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("JWT token is required"))
	}

	// Create Google Proxy client for the destination VCP region
	// This is the same pattern used in CreateReplicationOnDestination
	googleProxyClient := googleproxyclient.GetGProxyClient(*dstBasePath, *jwtToken, logger)

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Prepare API parameters
	params := googleproxyclient.V1betaListAllQuotaRulesParams{
		ProjectNumber:  projectNumber,
		LocationId:     destinationRegion,
		VolumeId:       destinationVolumeUUID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	// Call the VCP API to list quota rules on destination
	logger.Infof("Calling VCP API to list quota rules on destination: volumeUUID=%s", destinationVolumeUUID)
	res, err := googleProxyClient.Invoker.V1betaListAllQuotaRules(ctx, params)
	if err != nil {
		logger.Errorf("Failed to call V1betaListAllQuotaRules: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to list quota rules on destination: %w", err))
	}

	// Handle response types (following the CreateQuotaRuleOnDestination pattern)
	switch r := res.(type) {
	case *googleproxyclient.V1betaListAllQuotaRulesOK:
		logger.Infof("Successfully fetched quota rules from destination: count=%d, volumeUUID=%s",
			len(r.QuotaRules), destinationVolumeUUID)

		// Find the matching quota rule by name (ResourceId)
		for _, quotaRule := range r.QuotaRules {
			if quotaRule.ResourceId == quotaRuleName {
				logger.Infof("Found matching quota rule on destination: resourceId=%s, quotaId=%s, volumeUUID=%s",
					quotaRule.ResourceId, quotaRule.QuotaId, destinationVolumeUUID)

				destinationQuotaRuleId, hasQuotaId := quotaRule.QuotaId.Get()
				if !hasQuotaId {
					logger.Errorf("Matching quota rule found but QuotaId is not set: resourceId=%s, volumeUUID=%s",
						quotaRule.ResourceId, destinationVolumeUUID)
					return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleNotFound, customerrors.NewNotFoundErr("quota rule", &quotaRuleName)))
				}

				return &destinationQuotaRuleId, nil
			}
		}

		// No matching quota rule found
		logger.Errorf("No matching quota rule found on destination for quota rule name=%s: location=%s, volumeUUID=%s",
			quotaRuleName, destinationRegion, destinationVolumeUUID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleNotFound, customerrors.NewNotFoundErr("quota rule", &quotaRuleName)))
	case *googleproxyclient.V1betaListAllQuotaRulesBadRequest:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleBadRequest, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesUnauthorized:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleUnauthorized, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesForbidden:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleForbidden, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesNotFound:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrMatchingQuotaRuleNotFoundOnDestination, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesConflict:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleConflict, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesUnprocessableEntity:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleBadRequest, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesTooManyRequests:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleTooManyRequests, errors.New(r.Message)))
	case *googleproxyclient.V1betaListAllQuotaRulesInternalServerError:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrQuotaRuleInternalServerError, errors.New(r.Message)))
	default:
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInternalServerError, errors.New("unexpected response type from Google Proxy")))
	}
}

// UpdateQuotaRuleOnDestination calls the internal VCP API to update a quota rule on a destination volume in a remote region.
// The endpoint is:
// PUT /v1beta/internal/projects/{projectNumber}/locations/{locationId}/volumes/{volumeId}/quotaRule/{quotaRuleId}
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - destinationVolumeUUID: UUID of the destination volume
//   - destinationQuotaRuleId: UUID of the quota rule on the destination to update
//   - diskLimitInKib: Updated disk limit in kibibytes
//   - destinationRegion: The destination region
//   - projectNumber: GCP project number
//   - jwtToken: Reused JWT token for VCP-to-VCP authentication (created once in workflow)
//
// Returns:
//   - QuotaRuleOperationResult: Contains operation info if async, nil if completed synchronously
//   - error: Error if API call fails (wrapped for Temporal)
func (a *QuotaRuleUpdateActivity) UpdateQuotaRuleOnDestination(
	ctx context.Context,
	destinationVolumeUUID string,
	destinationQuotaRuleId string,
	diskLimitInKib int64,
	destinationRegion string,
	projectNumber string,
	jwtToken *string,
) (*QuotaRuleOperationResult, error) {
	logger := util.GetLogger(ctx)

	logger.Infof("Updating quota rule on destination volume: volumeUUID=%s, quotaRuleId=%s, region=%s, diskLimit=%d",
		destinationVolumeUUID, destinationQuotaRuleId, destinationRegion, diskLimitInKib)

	// Get destination VCP base path (similar to GetDstBasePath in replication)
	dstBasePath, err := getBasePathForQuotaRule(ctx, destinationRegion)
	if err != nil {
		logger.Errorf("Failed to get destination base path for region %s: %v", destinationRegion, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
	}

	// Validate JWT token is provided
	if jwtToken == nil || *jwtToken == "" {
		logger.Errorf("JWT token is required for destination API call")
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("JWT token is required"))
	}

	// Create Google Proxy client for the destination VCP region
	// This is the same pattern used in CreateQuotaRuleOnDestination
	googleProxyClient := googleproxyclient.GetGProxyClient(*dstBasePath, *jwtToken, logger)

	// Get correlation ID for tracing
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	// Prepare API parameters
	params := googleproxyclient.V1betaUpdateQuotaRuleVCPParams{
		ProjectNumber:  projectNumber,
		LocationId:     destinationRegion,
		VolumeId:       destinationVolumeUUID,
		QuotaRuleId:    destinationQuotaRuleId,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	// Convert disk limit from KiB to MiB (API expects MiB)
	diskLimitInMib := diskLimitInKib / 1024

	// Prepare quota rule update request body
	body := &googleproxyclient.QuotaRulesUpdateV1beta{}

	// Set disk limit if it's being updated (non-zero)
	if diskLimitInKib > 0 {
		body.DiskLimitInMib = googleproxyclient.NewOptInt64(diskLimitInMib)
	}

	// Call the internal VCP API to update quota rule on destination
	logger.Infof("Calling internal VCP API to update quota rule on destination: volumeUUID=%s, quotaRuleId=%s",
		destinationVolumeUUID, destinationQuotaRuleId)
	res, err := googleProxyClient.Invoker.V1betaUpdateQuotaRuleVCP(ctx, body, params)
	if err != nil {
		logger.Errorf("Failed to call V1betaUpdateQuotaRuleVCP: %v", err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to update quota rule on destination: %w", err))
	}

	// Handle response types (following the CreateQuotaRuleOnDestination pattern)
	switch r := res.(type) {
	case *googleproxyclient.QuotaRulesVCPV1beta:
		logger.Infof("Successfully updated quota rule on destination: quotaId=%s, resourceId=%s, state=%s",
			r.QuotaId.Value, r.ResourceId, r.State.Value)

		// Check if state is UPDATING - need to poll for completion
		if r.State.Value == googleproxyclient.QuotaRulesVCPV1betaStateUPDATING {
			// Extract JobId from Jobs array if available for polling
			if len(r.Jobs) > 0 && r.Jobs[0].JobId.IsSet() {
				jobId := r.Jobs[0].JobId.Value
				logger.Infof("Quota rule update in progress on destination, returning JobId for polling: %s", jobId)
				return &QuotaRuleOperationResult{OperationName: jobId, IsDone: false}, nil
			}
			// No job ID available but still updating - return as done (best effort)
			logger.Warnf("Quota rule is in UPDATING state but no JobId found, assuming success")
		}
		// State is READY or other terminal state
		return &QuotaRuleOperationResult{IsDone: true}, nil
	case *googleproxyclient.OperationV1beta:
		logger.Infof("Quota rule update operation started on destination: operationName=%s", r.Name.Value)
		return &QuotaRuleOperationResult{OperationName: r.Name.Value, IsDone: false}, nil
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPBadRequest:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPUnauthorized:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPForbidden:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPNotFound:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPConflict:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPMethodNotAllowed:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPRequestTimeout:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPUnprocessableEntity:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPTooManyRequests:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPServiceUnavailable:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaUpdateQuotaRuleVCPInternalServerError:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	default:
		return nil, vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New("unexpected response type from Google Proxy")))
	}
}
