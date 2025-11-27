package activities

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

type QuotaRuleCreateActivity struct {
	SE database.Storage
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

// GetVolume fetches authoritative volume details from the database by UUID.
func (a *QuotaRuleCreateActivity) GetVolumeForQuotaRule(ctx context.Context, volumeID string) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	volume, err := se.GetVolume(ctx, volumeID)
	if err != nil {
		logger.Errorf("GetVolumeForQuotaRule failed for %s: %v", volumeID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return volume, nil
}

// GetVolumeByID fetches authoritative volume details from the database by numeric ID.
func (a *QuotaRuleCreateActivity) GetVolumeByID(ctx context.Context, volumeID int64) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Use ListVolumes with ID filter
	conditions := [][]interface{}{{"id = ?", volumeID}}
	volumes, err := se.ListVolumes(ctx, conditions)
	if err != nil {
		logger.Errorf("GetVolumeByID failed for ID %d: %v", volumeID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if len(volumes) == 0 {
		logger.Errorf("Volume not found for ID %d", volumeID)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("volume not found for ID %d", volumeID))
	}

	return volumes[0], nil
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

func (a *QuotaRuleCreateActivity) UpdateQuotaRuleDetails(ctx context.Context, dbQuotaRule *datamodel.QuotaRule, quotaRuleCreateResponse *vsa.QuotaRuleProviderResponse) error {
	se := a.SE
	if quotaRuleCreateResponse == nil {
		// nil response can mean:
		// 1. Error case: quota rule creation failed (state should be ERROR)
		// 2. Default quota update case: default quota was updated successfully (QuotaTarget == "")
		// Check if this is a default quota update scenario
		if dbQuotaRule.QuotaTarget == "" && dbQuotaRule.State == models.LifeCycleStateCreating {
			// This is a default quota that was updated (not created on ONTAP)
			// Set to AVAILABLE since the update succeeded
			dbQuotaRule.State = models.LifeCycleStateAvailable
			dbQuotaRule.StateDetails = models.LifeCycleStateAvailableDetails
		} else {
			// This is an error case - quota rule creation failed
			dbQuotaRule.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
			dbQuotaRule.State = models.LifeCycleStateError
			dbQuotaRule.StateDetails = models.LifeCycleStateCreationErrorDetails
		}
	} else {
		dbQuotaRule.State = models.LifeCycleStateAvailable
		dbQuotaRule.StateDetails = models.LifeCycleStateAvailableDetails
		dbQuotaRule.QuotaRuleAttributes.ExternalUUID = quotaRuleCreateResponse.ExternalUUID
	}
	_, err := se.UpdateQuotaRule(ctx, dbQuotaRule)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	return nil
}

// GetVolumeReplication fetches replication details for a volume.
// Returns the list of replications associated with the volume.
func (a *QuotaRuleCreateActivity) GetVolumeReplication(ctx context.Context, volumeID int64) ([]*datamodel.VolumeReplication, error) {
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

// VerifyReplicationState validates that quota rules can be created given the replication state.
// This activity checks replication states to ensure quota operations are safe.
//
// Parameters:
//   - ctx: Context for logging
//   - replications: List of volume replications
//   - volumeUUID: Volume UUID for logging
//   - locationId: Current location (zone) to check for destination sync eligibility
//
// Returns:
//   - eligibleReplications: List of replications eligible for quota sync
//   - error: Error if replication state prevents quota operations
func (a *QuotaRuleCreateActivity) VerifyReplicationState(
	ctx context.Context,
	replications []*datamodel.VolumeReplication,
	locationId string,
) ([]*ReplicationSyncEligibility, error) {
	logger := util.GetLogger(ctx)

	// If no replications exist, validation passes with an empty list
	if replications == nil || len(replications) == 0 {
		return []*ReplicationSyncEligibility{}, nil
	}

	// Parse current region from locationId
	currentRegion, _, err := utils.ParseRegionAndZone(locationId)
	if err != nil {
		logger.Errorf("Failed to parse region from locationId %s: %v", locationId, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to parse region from locationId: %w", err))
	}

	var eligibleReplications []*ReplicationSyncEligibility

	for _, replication := range replications {
		if replication.ReplicationAttributes == nil {
			continue
		}

		// Parse destination region from destination location to compare with current region
		destRegion, _, err := utils.ParseRegionAndZone(replication.ReplicationAttributes.DestinationLocation)
		if err != nil {
			logger.Warnf("Failed to parse destination location %s: %v", replication.ReplicationAttributes.DestinationLocation, err)
			continue
		}

		// Check if current region is NOT the destination region
		// Only perform mirror state check when we're on the source side (current region != destination region)
		if destRegion != currentRegion {
			// We are on the source side - fetch destination replication to get accurate mirror state
			// Parse destination project number from RemoteUri
			destProjectNumber, err := utils.ParseProjectNumberFromURI(replication.RemoteUri)
			if err != nil {
				logger.Errorf("Failed to parse destination project number from RemoteUri: %v, remoteUri: %s", err, replication.RemoteUri)
				return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to parse destination project number: %w", err))
			}

			// Get destination region base path
			destBasePath, err := utils.GetPairedRegionURI(destRegion)
			if err != nil {
				logger.Errorf("Failed to get paired destination region URI: %v", err)
				return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
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
				return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get signed token: %w", err))
			}

			// Fetch destination replication details
			destinationReplication, err := GetReplicationDetails(ctx, destBasePath, destProjectNumber,
				replication.ReplicationAttributes.DestinationLocation,
				replication.ReplicationAttributes.DestinationReplicationUUID, dstToken)
			if err != nil {
				logger.Errorf("Failed to fetch destination replication: %v", err)
				return nil, vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to fetch destination replication for validation: %w", err))
			}

			// Check if this replication is eligible for destination sync using destination replication's mirror state
			// Only MIRRORED or UNINITIALIZED states are eligible
			if destinationReplication.MirrorState != nil {
				mirrorState := *destinationReplication.MirrorState
				if mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateMIRRORED) ||
					mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateUNINITIALIZED) {
					// This replication is eligible for sync
					eligibility := &ReplicationSyncEligibility{
						Eligible:              true,
						DestinationVolumeUUID: replication.ReplicationAttributes.DestinationVolumeUUID,
						DestinationLocation:   replication.ReplicationAttributes.DestinationLocation,
						DestinationProjectNum: destProjectNumber,
					}
					eligibleReplications = append(eligibleReplications, eligibility)

					logger.Infof("Replication eligible for sync: mirrorState=%s, destinationLocation=%s, destinationVolumeUUID=%s, projectNumber=%s",
						*destinationReplication.MirrorState, replication.ReplicationAttributes.DestinationLocation,
						replication.ReplicationAttributes.DestinationVolumeUUID, destProjectNumber)
				}
			}
		}
	}

	logger.Infof("Replication state validation passed for location %s, found %d eligible replications for sync",
		locationId, len(eligibleReplications))
	return eligibleReplications, nil
}

// ReplicationSyncEligibility holds the result of checking replication sync eligibility.
type ReplicationSyncEligibility struct {
	Eligible              bool
	DestinationVolumeUUID string
	DestinationLocation   string
	DestinationProjectNum string // Destination GCP project number for API calls
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
			return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
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
//   - QuotaRuleProviderResponse: Response from ONTAP with ExternalUUID
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
	// Note: ExternalUUID is not available in JobStatus per spec
	res := &vsa.QuotaRuleProviderResponse{
		State:   jobStatus.State,
		Message: jobStatus.Message,
		// ExternalUUID is not available from JobStatus - will need to be fetched separately if needed
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

// QuotaEnableDisable enables or disables the quota system on a volume.
// This activity is part of Step 7 (Enable Quota System) in the spec.
//
// Parameters:
//   - provider: ONTAP provider for making REST API calls
//   - volumeDetails: Volume containing ExternalUUID and Svm details
//   - enable: true to enable quota, false to disable quota
//
// Returns:
//   - QuotaEnableDisableResponse: Job status with State and Message
//   - Error: For any failure
func (a *QuotaRuleCreateActivity) QuotaEnableDisable(
	ctx context.Context,
	provider vsa.Provider,
	volumeDetails *datamodel.Volume,
	enable bool,
) (*vsa.QuotaEnableDisableResponse, error) {
	logger := util.GetLogger(ctx)

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
	action := "disable"
	if enable {
		action = "enable"
	}

	logger.Infof("Quota %s operation starting for volume: %s", action, volumeUUID)

	// Call provider to enable/disable quota
	jobStatus, err := provider.QuotaEnableDisable(ctx, volumeUUID, svmName, enable)
	if err != nil {
		logger.Errorf("Failed to %s quota: %v", action, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	// Convert JobStatus to QuotaEnableDisableResponse for backward compatibility
	resp := &vsa.QuotaEnableDisableResponse{
		State:   jobStatus.State,
		Message: jobStatus.Message,
	}

	logger.Infof("Quota %s operation completed for volume %s: state=%s, message=%s",
		action, volumeUUID, resp.State, resp.Message)

	return resp, nil
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

// CheckQuotaRuleCreationFailure checks if quota rule creation failed with resize/activation errors.
// According to spec (create-quota-cvs-job-function.md, Step 7, lines 1408-1419):
// When quota is already ON, check if quotaRuleResp.State indicates failure
// and handle resize/activation failures by triggering reinitialization.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - quotaRuleResponse: Response from CreateQuotaRule (contains State and Message)
//   - provider: ONTAP provider for reinitialization
//   - volumeDetails: Volume details for reinitialization
//
// Returns:
//   - error: Error if creation failed with non-recoverable error, nil if success or recoverable
func (a *QuotaRuleCreateActivity) CheckQuotaRuleCreationFailure(
	ctx context.Context,
	quotaRuleResponse *vsa.QuotaRuleProviderResponse,
	provider vsa.Provider,
	volumeDetails *datamodel.Volume,
) error {
	logger := util.GetLogger(ctx)

	// Check if response indicates failure
	if quotaRuleResponse.IsFailure() {
		// Check for resize or activation failures that can be recovered with reinitialization
		if strings.Contains(quotaRuleResponse.Message, vsa.ResizeOperationFailed) ||
			strings.Contains(quotaRuleResponse.Message, vsa.ActivationOperationFailed) {
			logger.Infof("Quota rule creation failed with resize/activation error, triggering reinitialization")

			// Call helper function to perform quota reinitialization
			err := performQuotaReinitialization(ctx, provider, volumeDetails)
			if err != nil {
				logger.Errorf("Failed to reinitialize quota: %v", err)
				return vsaerrors.WrapAsTemporalApplicationError(err)
			}
			logger.Infof("Successfully reinitialized quota after creation failure")
			return nil
		} else {
			// Other failure - return error
			logger.Errorf("Quota rule creation failed: %s", quotaRuleResponse.Message)
			return vsaerrors.WrapAsTemporalApplicationError(customerrors.NewUserInputValidationErr(
				quotaRuleResponse.Message))
		}
	}

	logger.Infof("Quota rule creation check completed successfully")
	return nil
}

// UpdateQuotaRuleState finalizes the quota rule job in the database.
// According to spec (create-quota-cvs-job-function.md, Step 2 of createQuotaRuleAsync):
// Updates quota rule state from "creating" to "available" and job state to "success".
//
// This activity uses the database's WithTransaction method to ensure atomicity - both the
// quota rule and job are updated together or not at all. This prevents inconsistent states
// where the quota rule is updated but the job update fails.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - quotaRuleUUID: UUID of the quota rule to finalize
//   - jobUUID: Job UUID to update
//   - externalUUID: Optional ONTAP quota rule UUID (empty string if not available, e.g., default quota update)
//   - description: Optional description to update (empty string to keep existing)
//
// UpdateQuotaRuleState updates the quota rule state in the database.
// This follows the same pattern as UpdateReplicationState in replication activities.
//
// Parameters:
//   - ctx: Context for logging and cancellation
//   - quotaRule: The quota rule with updated state and state details
//
// Returns:
//   - error: Error if database update fails (wrapped for Temporal)
func (a *QuotaRuleCreateActivity) UpdateQuotaRuleState(ctx context.Context, quotaRule datamodel.QuotaRule) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	_, err := se.UpdateQuotaRule(ctx, &quotaRule)
	if err != nil {
		logger.Errorf("Failed to update quota rule state in DB: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}
	logger.Debugf("Quota rule state: %s updated successfully in the db", quotaRule.Name)

	return nil
}

func (a *QuotaRuleCreateActivity) FinishQuotaRuleJob(ctx context.Context, quotaRuleUUID string, jobUUID string, externalUUID string, description string) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	// Use WithTransaction to ensure atomicity - both quota rule and job must be updated together
	err := se.WithTransaction(ctx, func(tx dbutils.Transaction) error {
		db := tx.GORM()

		// Step 1: Get quota rule to update
		var quotaRule datamodel.QuotaRule
		err := db.Where("uuid = ?", quotaRuleUUID).First(&quotaRule).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				logger.Errorf("Quota rule not found for finalization: uuid=%s", quotaRuleUUID)
				return customerrors.NewNotFoundErr("quota rule", &quotaRuleUUID)
			}
			logger.Errorf("Failed to fetch quota rule for finalization: uuid=%s, error=%v", quotaRuleUUID, err)
			return err
		}

		// Step 2: Update quota rule state to available
		quotaRule.State = models.LifeCycleStateREADY
		quotaRule.StateDetails = models.LifeCycleStateReadyDetails
		quotaRule.UpdatedAt = time.Now()

		// Update ExternalUUID if provided (from ONTAP quota rule creation)
		if externalUUID != "" {
			if quotaRule.QuotaRuleAttributes == nil {
				quotaRule.QuotaRuleAttributes = &datamodel.QuotaRuleAttributes{}
			}
			quotaRule.QuotaRuleAttributes.ExternalUUID = externalUUID
		}

		// Handle default quota update case: if externalUUID is empty and QuotaTarget is empty,
		// this is a default quota that was updated (not created), so state is already correct
		// No additional logic needed - state will be set to AVAILABLE above

		err = db.Save(&quotaRule).Error
		if err != nil {
			logger.Errorf("Failed to update quota rule state to available: uuid=%s, error=%v", quotaRuleUUID, err)
			return err
		}

		logger.Infof("Updated quota rule state to available: uuid=%s", quotaRuleUUID)

		err = db.Model(&datamodel.Job{}).
			Where("uuid = ?", jobUUID).
			Update("state", string(models.JobsStateDONE)).Error
		if err != nil {
			logger.Errorf("Failed to update job state to done: jobUUID=%s, error=%v", jobUUID, err)
			return err
		}

		logger.Infof("Updated job state to done: jobUUID=%s", jobUUID)

		// Return nil to commit the transaction
		return nil
	})

	if err != nil {
		logger.Errorf("Failed to finalize quota rule job in transaction: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully finalized quota rule job in transaction: quotaRuleUUID=%s, jobUUID=%s",
		quotaRuleUUID, jobUUID)
	return nil
}

// GetDestinationVolume fetches the destination volume by UUID.
// This is used in the quota sync workflow to get destination volume details.
//
// Parameters:
//   - ctx: Context for logging
//   - destinationVolumeUUID: UUID of the destination volume
//
// Returns:
//   - destinationVolume: The destination volume
//   - error: Error if volume fetch fails
func (a *QuotaRuleCreateActivity) GetDestinationVolume(
	ctx context.Context,
	destinationVolumeUUID string,
) (*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	destinationVolume, err := se.GetVolume(ctx, destinationVolumeUUID)
	if err != nil {
		logger.Errorf("Failed to get destination volume: destinationVolumeUUID=%s, error=%v", destinationVolumeUUID, err)
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	return destinationVolume, nil
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
//
// Returns:
//   - error: Error if API call fails (wrapped for Temporal)
func (a *QuotaRuleCreateActivity) CreateQuotaRuleOnDestination(
	ctx context.Context,
	destinationVolumeUUID string,
	quotaRule *datamodel.QuotaRule,
	destinationRegion string,
	projectNumber string,
) error {
	logger := util.GetLogger(ctx)

	logger.Infof("Creating quota rule on destination volume: volumeUUID=%s, region=%s, quotaType=%s, diskLimit=%d",
		destinationVolumeUUID, destinationRegion, quotaRule.QuotaType, quotaRule.DiskLimitInKib)

	// Get destination VCP base path (similar to GetDstBasePath in replication)
	dstBasePath, err := GetBasePath(ctx, destinationRegion)
	if err != nil {
		logger.Errorf("Failed to get destination base path for region %s: %v", destinationRegion, err)
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get destination base path: %w", err))
	}

	// Get JWT token for VCP-to-VCP authentication
	jwtToken, err := auth.GetSignedJwtToken(projectNumber)
	if err != nil {
		logger.Errorf("Failed to get JWT token for destination API call: %v", err)
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to get JWT token: %w", err))
	}

	// Create Google Proxy client for the destination VCP region
	// This is the same pattern used in CreateReplicationOnDestination
	googleProxyClient := googleproxyclient.GetGProxyClient(*dstBasePath, jwtToken, logger)

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
		return vsaerrors.WrapAsTemporalApplicationError(fmt.Errorf("failed to create quota rule on destination: %w", err))
	}

	// Handle response types (following the CreateReplicationOnDestination pattern)
	switch r := res.(type) {
	case *googleproxyclient.QuotaRulesVCPV1beta:
		logger.Infof("Successfully created quota rule on destination: quotaId=%s, resourceId=%s, state=%s",
			r.QuotaId.Value, r.ResourceId, r.State.Value)
		return nil
	case *googleproxyclient.V1betaCreateQuotaRuleVCPBadRequest:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPUnauthorized:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPForbidden:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPNotFound:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPConflict:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPMethodNotAllowed:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPRequestTimeout:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPUnprocessableEntity:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPTooManyRequests:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPServiceUnavailable:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	case *googleproxyclient.V1betaCreateQuotaRuleVCPInternalServerError:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New(r.Message)))
	default:
		return vsaerrors.WrapAsTemporalApplicationError(coreerrors.NewVCPError(coreerrors.ErrCreateInternalQuotaRule, customerrors.New("unexpected response type from Google Proxy")))
	}
}
