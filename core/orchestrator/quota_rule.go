package orchestrator

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

const (
	// Quota type constants
	IndividualUserQuota                = "INDIVIDUAL_USER_QUOTA"
	IndividualGroupQuota               = "INDIVIDUAL_GROUP_QUOTA"
	DefaultUserQuota                   = "DEFAULT_USER_QUOTA"
	DefaultGroupQuota                  = "DEFAULT_GROUP_QUOTA"
	DiskQuotaLowerLimit          int64 = 4
	DiskQuotaUpperLimit          int64 = 1125899906842620
	VolumeQuotaRulesDefaultLimit       = 100
	kibToMibDivisor                    = 1024
	mibToKibMultiplier                 = 1024
	mibToBytesMultiplier               = 1024 * 1024
)

var (
	createQuotaRule                  = _createQuotaRule
	createQuotaRuleInternal          = _createQuotaRuleInternal
	validateQuotaRuleCreateParams    = _validateQuotaRuleCreateParams
	convertDatastoreQuotaRuleToModel = _convertDatastoreQuotaRuleToModel
	getDestinationReplication        = _getDestinationReplication
	internalUtilGetSignedToken       = auth.GetSignedJwtToken
	internalUtilGetPairedRegionURI   = utils.GetPairedRegionURI
	internalParseRegionAndZone       = utils.ParseRegionAndZone
)

// validateQuotaRuleCreateParams validates quota rule creation parameters
func _validateQuotaRuleCreateParams(params *common.CreateQuotaRulesParam) error {
	if params.Name == "" {
		return customerrors.NewUserInputValidationErr("Quota rule name is required")
	}

	diskLimitInKiB := params.DiskLimitInMib * mibToKibMultiplier
	if diskLimitInKiB < DiskQuotaLowerLimit || diskLimitInKiB > DiskQuotaUpperLimit {
		return customerrors.NewUserInputValidationErr("DiskLimit is outside the permissible range")
	}

	if params.QuotaTarget == "" && (params.QuotaType == IndividualUserQuota || params.QuotaType == IndividualGroupQuota) {
		return customerrors.NewUserInputValidationErr(
			"quotaTarget has to be specified for Individual user/group quotaType. To create this quotaRule, change quotaType to DefaultUserQuota or DefaultGroupQuota")
	}

	if params.QuotaTarget != "" && (params.QuotaType == DefaultUserQuota || params.QuotaType == DefaultGroupQuota) {
		return customerrors.NewUserInputValidationErr(
			"quotaTarget cannot be specified for Default user/group quotaType. To create this quotaRule, change quotaType to IndividualUserQuota or IndividualGroupQuota")
	}
	return nil
}

// _getDestinationReplication fetches replication details from the destination region
func _getDestinationReplication(ctx context.Context, basePath string, projectNumber string, locationID string, volumeReplicationID string, jwt string) (*models.VolumeReplication, error) {
	return activities.GetReplicationDetails(ctx, basePath, projectNumber, locationID, volumeReplicationID, jwt)
}

// validateReplicationState validates that quota rules are not created on destination volumes with active replication
// Reference: validateQuotaEvent in specification, following pattern from _verifyDstReplicationResume
func validateReplicationState(ctx context.Context, se database.Storage, volume *datamodel.Volume, locationID string) error {
	logger := util.GetLogger(ctx)

	// Fetch replications for this volume using ListVolumeReplications with volume_id filter
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("volume_id", "=", volume.ID))

	replications, err := se.ListVolumeReplications(ctx, *filter, database.QueryDepthZero)
	if err != nil {
		logger.Error("Failed to list volume replications", "error", err)
		return err
	}

	// If no replications exist, validation passes
	if replications == nil || len(replications) == 0 {
		return nil
	}

	// Check each replication
	for _, replication := range replications {
		if replication.ReplicationAttributes == nil {
			continue
		}
		// TODO : Add hybrid replicaion check
		// replication.ClusterPeer.Valid

		// Determine which replication to validate based on destination location
		var destinationReplication *models.VolumeReplication

		// Check if the current location is the destination location for this replication
		// If destination location matches the current locationID, we are on the destination side
		if replication.ReplicationAttributes.DestinationLocation == locationID {
			// Current location is the destination - use local replication data
			logger.Info("Current location matches destination location, using local replication data", "locationID", locationID)
			destinationReplication = &models.VolumeReplication{
				State:              replication.State,
				MirrorState:        replication.MirrorState,
				RelationshipStatus: replication.RelationshipStatus,
			}
		} else {
			// Current location is NOT the destination - this is the source side, fetch destination replication
			// Parse destination project number from RemoteUri
			destProjectNumber, err := utils.ParseProjectNumberFromURI(replication.RemoteUri)
			if err != nil {
				logger.Error("Failed to parse destination project number from RemoteUri", "error", err, "remoteUri", replication.RemoteUri)
				return fmt.Errorf("failed to parse destination project number: %w", err)
			}

			// Parse destination region from location
			destRegion, _, parseError := internalParseRegionAndZone(replication.ReplicationAttributes.DestinationLocation)
			if parseError != nil {
				logger.Error("Failed to parse destination location", "error", parseError)
				return coreerrors.NewVCPError(coreerrors.ErrParseDestinationLocation, fmt.Errorf("failed to parse destination location: %w", parseError))
			}

			// Get destination region base path
			destBasePath, err := internalUtilGetPairedRegionURI(destRegion)
			if err != nil {
				logger.Error("Failed to get paired destination region URI", "error", err)
				return coreerrors.NewVCPError(coreerrors.ErrGetDstBasePath, err)
			}

			logger.Debug("Destination replication details",
				"projectNumber", destProjectNumber,
				"region", destRegion,
				"location", replication.ReplicationAttributes.DestinationLocation,
				"replicationUUID", replication.ReplicationAttributes.DestinationReplicationUUID)

			// Get JWT token for destination project (same project only)
			srcProjectNumber := volume.Account.Name

			// Check for cross-project replication: quota rule sync is not allowed
			if destProjectNumber != srcProjectNumber {
				logger.Error("QuotaRule sync for cross project replication is not allowed",
					"srcProjectNumber", srcProjectNumber, "destProjectNumber", destProjectNumber)
				return customerrors.NewUserInputValidationErr("QuotaRule sync for cross project replication is not allowed")
			}

			// Same project: get token for current project
			dstToken, err := internalUtilGetSignedToken(destProjectNumber)
			if err != nil {
				logger.Error("Failed to get signed token", "error", err)
				return coreerrors.NewVCPError(coreerrors.ErrGetSignedToken, err)
			}

			// Fetch destination replication details
			destinationReplication, err = getDestinationReplication(ctx, destBasePath, destProjectNumber,
				replication.ReplicationAttributes.DestinationLocation,
				replication.ReplicationAttributes.DestinationReplicationUUID, dstToken)
			if err != nil {
				logger.Error("Failed to fetch destination replication", "error", err)
				return fmt.Errorf("failed to fetch destination replication for validation: %w", err)
			}
		}

		// Common validation logic for destination replication state
		// Check lifecycle state: Cannot modify quotas during replication state transitions
		// This check must come BEFORE mirror state checks
		if destinationReplication.State == models.LifeCycleStateCreating ||
			destinationReplication.State == models.LifeCycleStateUpdating ||
			destinationReplication.State == models.LifeCycleStateDeleting {
			logger.Errorf("Destination replication is in transitioning state: %s", destinationReplication.State)
			return customerrors.NewUserInputValidationErr(
				"User/Group Quota operations not allowed when destination replication is in transitioning state (CREATING, UPDATING, or DELETING)")
		}

		// Check mirror state: MIRRORED or UNINITIALIZED indicates active replication
		// Only perform this check if the replication creation request is received on destination
		// (i.e., current locationID matches the destination location)
		if replication.ReplicationAttributes.DestinationLocation == locationID && destinationReplication.MirrorState != nil {
			mirrorState := *destinationReplication.MirrorState
			if mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateMIRRORED) ||
				mirrorState == string(gcpgenserver.ReplicationV1betaMirrorStateUNINITIALIZED) {
				logger.Errorf("Destination replication is in %s state", mirrorState)
				return customerrors.NewUserInputValidationErr(
					fmt.Sprintf("Quota creation not allowed when destination replication is in %s state", mirrorState))
			}
		}
	}
	return nil
}

// CreateQuotaRule creates a new quota rule for a volume
func (o *Orchestrator) CreateQuotaRule(ctx context.Context, params *common.CreateQuotaRulesParam) (*models.QuotaRule, string, error) {
	return createQuotaRule(ctx, o.storage, o.temporal, params)
}

// CreateQuotaRuleInternal creates a new quota rule for a volume via internal VCP API
// This function is similar to CreateQuotaRule but skips replication state validation
// as it's intended for VCP-to-VCP communication where replication validation is handled separately
func (o *Orchestrator) CreateQuotaRuleInternal(ctx context.Context, params *common.CreateQuotaRulesParam) (*models.QuotaRule, string, error) {
	return createQuotaRuleInternal(ctx, o.storage, o.temporal, params)
}

func _createQuotaRule(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateQuotaRulesParam) (*models.QuotaRule, string, error) {
	logger := util.GetLogger(ctx)

	_, err := getOrCreateAccount(ctx, se, params.ProjectId)
	if err != nil {
		return nil, "", err
	}

	if err := validateQuotaRuleCreateParams(params); err != nil {
		logger.Errorf("Quota rule validation failed: %v", err)
		return nil, "", err
	}

	volumeDataModel, err := se.GetVolume(ctx, params.VolumeUUID)
	if err != nil {
		logger.Errorf("Failed to retrieve volume: %v", err)
		return nil, "", err
	}

	// Validate the volume type and size for quota rule creation (SAN, FlexCache, and size checks)
	if err := validateVolumeType(ctx, volumeDataModel, params); err != nil {
		logger.Errorf("Volume type validation failed: %v", err)
		return nil, "", err
	}

	// Validate replication state: quota rules are not allowed on destination volumes with active replication.
	// Perform this validation before fetching existing quota rules to avoid unnecessary DB calls.
	if err := validateReplicationState(ctx, se, volumeDataModel, params.LocationId); err != nil {
		logger.Errorf("replication state validation failed: %v", err)
		return nil, "", err
	}

	// Fetch existing quota rules for this volume after replication validation
	existingQuotaRulesData, err := se.GetQuotaRulesByVolumeID(ctx, volumeDataModel.ID)
	if err != nil {
		logger.Errorf("Failed to fetch existing quota rules: %v", err)
		return nil, "", err
	}
	// Validate Quota Rules Limit as early quota-rule related check
	if len(existingQuotaRulesData) >= VolumeQuotaRulesDefaultLimit {
		logger.Errorf("Quota rules limit validation failed: volume has %d quota rules, limit is %d",
			len(existingQuotaRulesData), VolumeQuotaRulesDefaultLimit)
		return nil, "", customerrors.NewUserInputValidationErr(
			"Volume quota rules limit reached")
	}

	// Validate name and (type,target) uniqueness using in-memory existing rules
	if err := validateQuotaRuleUniqueness(ctx, existingQuotaRulesData, params); err != nil {
		logger.Errorf("Quota rule uniqueness validation failed: %v", err)
		return nil, "", err
	}

	// Determine if RQuota is required for this quota rule
	// This checks if the volume uses NFS and if this is the first quota rule in the SVM
	rquotaRequired, err := determineRQuota(ctx, se, volumeDataModel, existingQuotaRulesData)
	if err != nil {
		logger.Errorf("RQuota determination failed: %v", err)
		return nil, "", err
	}
	logger.Debugf("RQuota determination for volume %s: required=%t", volumeDataModel.UUID, rquotaRequired)

	// Create job entry in database
	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateQuotaRule),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: volumeDataModel.AccountID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	var dbQuotaRule *datamodel.QuotaRule
	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
			if dbQuotaRule != nil && dbQuotaRule.UUID != "" {
				logger.Warnf("Error occurred, marking quota rule in DB as deleted. Quota Rule UUID: %s", dbQuotaRule.UUID)
				if _, delErr := se.DeleteQuotaRule(ctx, dbQuotaRule.UUID); delErr != nil {
					logger.Errorf("Failed to delete quota rule: %v", delErr)
				}
			}
		}
	}()

	job, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database. Error: %v", err)
		return nil, "", err
	}

	// Convert disk limit from MiB (input param) to KiB (database storage and ONTAP API)
	diskLimitInKib := params.DiskLimitInMib * mibToKibMultiplier

	// Create quota rule entry in database with CREATING state
	quotaRuleDataModel := &datamodel.QuotaRule{
		Name:           params.Name,
		VolumeID:       volumeDataModel.ID,
		AccountID:      volumeDataModel.AccountID,
		QuotaType:      params.QuotaType,
		QuotaTarget:    params.QuotaTarget,
		DiskLimitInKib: diskLimitInKib,
		State:          models.LifeCycleStateCreating,
		StateDetails:   models.LifeCycleStateCreatingDetails,
		RQuota:         rquotaRequired,
		Description:    params.Description,
	}

	dbQuotaRule, err = se.CreatingQuotaRule(ctx, quotaRuleDataModel)
	if err != nil {
		logger.Errorf("Failed to create quota rule in database. Error: %v", err)
		return nil, "", err
	}

	// Start Temporal workflow for quota rule creation
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    job.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.CreateQuotaRuleWorkflow,
		params,
		dbQuotaRule,
	)

	if err != nil {
		logger.Errorf("Failed to start create quota rule workflow. Error: %v ", err)
		return nil, "", err
	}

	dataStoreQuotaRule := convertDatastoreQuotaRuleToModel(dbQuotaRule)
	return dataStoreQuotaRule, job.UUID, nil
}

// _createQuotaRuleInternal creates a new quota rule for a volume via internal VCP API
// This function is similar to _createQuotaRule but skips replication state validation
// as it's intended for VCP-to-VCP communication where replication validation is handled separately
func _createQuotaRuleInternal(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateQuotaRulesParam) (*models.QuotaRule, string, error) {
	logger := util.GetLogger(ctx)

	if err := validateQuotaRuleCreateParams(params); err != nil {
		logger.Errorf("Quota rule validation failed: %v", err)
		return nil, "", err
	}

	volumeDataModel, err := se.GetVolume(ctx, params.VolumeUUID)
	if err != nil {
		logger.Errorf("Failed to retrieve volume: %v", err)
		return nil, "", err
	}

	// Set LocationId from volume's pool primary zone if not already set
	if params.LocationId == "" && volumeDataModel.Pool != nil && volumeDataModel.Pool.PoolAttributes != nil {
		params.LocationId = volumeDataModel.Pool.PoolAttributes.PrimaryZone
		logger.Debugf("Set LocationId from volume pool's primary zone: %s", params.LocationId)
	}

	// Note: Replication state validation is skipped for internal VCP API calls
	// Replication validation is handled separately in the calling VCP instance

	// Fetch existing quota rules for this volume
	existingQuotaRulesData, err := se.GetQuotaRulesByVolumeID(ctx, volumeDataModel.ID)
	if err != nil {
		logger.Errorf("Failed to fetch existing quota rules: %v", err)
		return nil, "", err
	}
	// Validate Quota Rules Limit as early quota-rule related check
	if len(existingQuotaRulesData) >= VolumeQuotaRulesDefaultLimit {
		logger.Errorf("Quota rules limit validation failed: volume has %d quota rules, limit is %d",
			len(existingQuotaRulesData), VolumeQuotaRulesDefaultLimit)
		return nil, "", customerrors.NewUserInputValidationErr(
			"Volume quota rules limit reached, please contact support to request for higher limit.")
	}

	// Validate the volume type and size for quota rule creation (SAN, FlexCache, and size checks)
	if err := validateVolumeType(ctx, volumeDataModel, params); err != nil {
		logger.Errorf("Volume type/size validation failed: %v", err)
		return nil, "", err
	}

	// Validate name and (type,target) uniqueness using in-memory existing rules
	if err := validateQuotaRuleUniqueness(ctx, existingQuotaRulesData, params); err != nil {
		logger.Errorf("Quota rule uniqueness validation failed: %v", err)
		return nil, "", err
	}

	// Determine if RQuota is required for this quota rule
	// This checks if the volume uses NFS and if this is the first quota rule in the SVM
	rquotaRequired, err := determineRQuota(ctx, se, volumeDataModel, existingQuotaRulesData)
	if err != nil {
		logger.Errorf("RQuota determination failed: %v", err)
		return nil, "", err
	}
	logger.Debugf("RQuota determination for volume %s: required=%t", volumeDataModel.UUID, rquotaRequired)

	// Note: Data Protection volume handling has been moved to workflow
	// See CreateQuotaRuleForDataProtectionVolume activity in quota_rule_create_workflow
	// DP volumes only need database entry, no ONTAP operations

	// Create job entry in database
	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateQuotaRule),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: volumeDataModel.AccountID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	var dbQuotaRule *datamodel.QuotaRule
	// Cleanup in case of error
	defer func() {
		if err != nil {
			if job != nil && job.UUID != "" {
				logger.Warnf("Error occurred, marking job entry in DB as deleted. Job UUID: %s", job.UUID)
				if delErr := se.DeleteJob(ctx, job.UUID, err.Error()); delErr != nil {
					logger.Errorf("Failed to delete job: %v", delErr)
				}
			}
			if dbQuotaRule != nil && dbQuotaRule.UUID != "" {
				logger.Warnf("Error occurred, marking quota rule in DB as deleted. Quota Rule UUID: %s", dbQuotaRule.UUID)
				if _, delErr := se.DeleteQuotaRule(ctx, dbQuotaRule.UUID); delErr != nil {
					logger.Errorf("Failed to delete quota rule: %v", delErr)
				}
			}
		}
	}()

	job, err = se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database. Error: %v", err)
		return nil, "", err
	}

	// Convert disk limit from MiB (input param) to KiB (database storage and ONTAP API)
	const mibToKibMultiplier = 1024
	diskLimitInKib := params.DiskLimitInMib * mibToKibMultiplier

	// Create quota rule entry in database with CREATING state
	quotaRuleDataModel := &datamodel.QuotaRule{
		Name:           params.Name,
		VolumeID:       volumeDataModel.ID,
		AccountID:      volumeDataModel.AccountID,
		QuotaType:      params.QuotaType,
		QuotaTarget:    params.QuotaTarget,
		DiskLimitInKib: diskLimitInKib,
		State:          models.LifeCycleStateCreating,
		StateDetails:   models.LifeCycleStateCreatingDetails,
		RQuota:         rquotaRequired,
		Description:    params.Description,
	}

	dbQuotaRule, err = se.CreatingQuotaRule(ctx, quotaRuleDataModel)
	if err != nil {
		logger.Errorf("Failed to create quota rule in database. Error: %v", err)
		return nil, "", err
	}

	// Start Temporal workflow for quota rule creation
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    job.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		workflows.CreateQuotaRuleWorkflow,
		params,
		dbQuotaRule,
	)

	if err != nil {
		logger.Errorf("Failed to start create quota rule workflow. Error: %v ", err)
		return nil, "", err
	}

	dataStoreQuotaRule := convertDatastoreQuotaRuleToModel(dbQuotaRule)
	return dataStoreQuotaRule, job.UUID, nil
}

// _convertDatastoreQuotaRuleToModel converts a datamodel.QuotaRule to models.QuotaRule
func _convertDatastoreQuotaRuleToModel(quotaRule *datamodel.QuotaRule) *models.QuotaRule {
	if quotaRule == nil {
		return nil
	}

	// Convert disk limit from KiB (database storage) back to MiB (API response)
	diskLimitInMib := quotaRule.DiskLimitInKib / kibToMibDivisor

	// Get VolumeUUID from preloaded Volume
	volumeUUID := ""
	if quotaRule.Volume != nil {
		volumeUUID = quotaRule.Volume.UUID
	}

	result := &models.QuotaRule{
		BaseModel: models.BaseModel{
			UUID:      quotaRule.UUID,
			CreatedAt: quotaRule.CreatedAt,
			UpdatedAt: quotaRule.UpdatedAt,
			DeletedAt: DeletedAtOrNil(quotaRule.DeletedAt),
		},
		Name:                  quotaRule.Name,
		Description:           quotaRule.Description,
		LifeCycleState:        quotaRule.State,
		LifeCycleStateDetails: quotaRule.StateDetails,
		VolumeUUID:            volumeUUID,
		QuotaType:             quotaRule.QuotaType,
		QuotaTarget:           quotaRule.QuotaTarget,
		DiskLimitInMib:        diskLimitInMib,
	}
	return result
}

// validateVolumeType performs checks related to volume protocol/type and size.
// It validates that quota rules are not created for SAN (block) volumes,
// not created for FlexCache volumes, and that the requested quota size does not
// exceed the volume size.
func validateVolumeType(ctx context.Context, volumeDataModel *datamodel.Volume, params *common.CreateQuotaRulesParam) error {
	logger := util.GetLogger(ctx)

	// Block volume check: Quota rules are not allowed on SAN/block volumes
	if utils.IsSanProtocols(volumeDataModel.VolumeAttributes.Protocols) {
		logger.Errorf("Block volume check failed: quota rule creation is not supported for block volumes")
		return customerrors.NewUserInputValidationErr("quota rule creation is not supported for block (SAN) volumes")
	}

	// FlexCache Volume Check: FlexCache volumes don't support quotas
	if volumeDataModel.CacheParameters != nil {
		logger.Errorf("FlexCache volume check failed: quota rule creation is not supported for flexcache volumes")
		return customerrors.NewNotSupportedErrWithMessage("quota rule creation is not supported for flexcache volume")
	}

	// TODO : validate onprem volume check

	// Validate volume size: quota rule size cannot exceed volume size
	diskLimitInBytes := uint64(params.DiskLimitInMib) * mibToBytesMultiplier
	if uint64(volumeDataModel.SizeInBytes) < diskLimitInBytes {
		logger.Errorf("Volume size validation failed: quota rule size (%d bytes) exceeds volume size (%d bytes)", diskLimitInBytes, volumeDataModel.SizeInBytes)
		return customerrors.NewUserInputValidationErr(
			"quota rule size can not be greater than volume size, please pass quota rule size less than volume size")
	}

	return nil
}

// validateQuotaRuleUniqueness checks uniqueness of quota rule name and the (type,target)
// combination against the existing quota rules fetched earlier. This avoids extra DB calls
// since existingQuotaRulesData is already available in the caller.
func validateQuotaRuleUniqueness(ctx context.Context, existingRules []*datamodel.QuotaRule, params *common.CreateQuotaRulesParam) error {
	logger := util.GetLogger(ctx)

	for _, ex := range existingRules {
		if ex.Name == params.Name {
			logger.Errorf("Name uniqueness validation failed: quota rule with name %s already exists for this volume", params.Name)
			return customerrors.NewConflictErr("quota rule with same name " + params.Name + " already exist for this volume")
		}
		if ex.QuotaType == params.QuotaType && ex.QuotaTarget == params.QuotaTarget {
			logger.Errorf("Type+Target uniqueness validation failed: quota rule with type %s and target %s already exists (name: %s)", params.QuotaType, params.QuotaTarget, ex.Name)
			return customerrors.NewConflictErr(
				"quota rule with same type " + params.QuotaType + " and target " + params.QuotaTarget +
					" already exist for this volume for volumeQuotaRuleName " + ex.Name)
		}
	}

	return nil
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

// hasNFSv3 checks if NFSv3 protocol exists in the given protocol types array
func hasNFSv3(protocolTypes []string) bool {
	return hasProtocol(utils.ProtocolNFSv3, protocolTypes)
}

// hasNFSv4 checks if NFSv4 protocol exists in the given protocol types array
func hasNFSv4(protocolTypes []string) bool {
	return hasProtocol(utils.ProtocolNFSv4, protocolTypes)
}

// determineRQuota determines if recursive quota (RQuota) should be enabled for this quota rule.
// This is a helper function called from _createQuotaRule and _createQuotaRuleInternal.
// RQuota is set to true if and only if:
// 1. Volume uses NFS protocol (NFSv3 or NFSv4)
// 2. This is the first quota rule on this volume
// 3. No other quota rules exist in the entire SVM
//
// This follows the ONTAP requirement that recursive quotas must be enabled at the SVM level
// when creating the first quota rule in the entire SVM.
//
// Parameters:
//   - ctx: Context for logging
//   - se: Database storage interface
//   - volumeDetails: Volume to check
//   - existingQuotaRules: Existing quota rules for the volume (already fetched by caller)
//
// Returns:
//   - bool: true if RQuota is required, false otherwise
//   - error: Error if database operations fail
func determineRQuota(ctx context.Context, se database.Storage, volumeDetails *datamodel.Volume, existingQuotaRules []*datamodel.QuotaRule) (bool, error) {
	logger := util.GetLogger(ctx)

	// Get protocols from volume attributes
	var protocols []string
	if volumeDetails.VolumeAttributes != nil && volumeDetails.VolumeAttributes.Protocols != nil {
		protocols = volumeDetails.VolumeAttributes.Protocols
	}

	// Check if volume uses NFS protocol using helper functions
	if !(hasNFSv3(protocols) || hasNFSv4(protocols)) {
		logger.Infof("Volume %s does not use NFS protocol, RQuota not required", volumeDetails.UUID)
		return false, nil
	}

	// Only check SVM-level count if this is the first quota rule on the volume
	if len(existingQuotaRules) == 0 {
		// Get quota count under the SVM using SvmID
		quotaCountUnderSVM, err := se.GetQuotaRuleCountBySvmID(ctx, volumeDetails.SvmID)
		if err != nil {
			logger.Errorf("Failed to get quota rule count for SVM ID %d: %v", volumeDetails.SvmID, err)
			return false, err
		}

		// RQuota should be enabled only if this is the very first quota rule in the entire SVM
		rquotaRequired := quotaCountUnderSVM == 0
		logger.Infof("RQuota determination for volume %s (SvmID=%d): quotaCountUnderSVM=%d, rquotaRequired=%t",
			volumeDetails.UUID, volumeDetails.SvmID, quotaCountUnderSVM, rquotaRequired)

		return rquotaRequired, nil
	}

	logger.Infof("Volume %s already has %d quota rules, RQuota not required", volumeDetails.UUID, len(existingQuotaRules))
	return false, nil
}
