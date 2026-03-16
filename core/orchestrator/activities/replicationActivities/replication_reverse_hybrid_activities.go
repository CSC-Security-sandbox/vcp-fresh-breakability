package replicationActivities

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	hydrateReplicationStateAndTypeForHybrid = HydrateReplicationStateAndType
)

type ReverseHybridReplicationActivity struct {
	SE database.Storage
}

const (
	// SnapmirrorResyncPrivilegePath is the path for snapmirror resync privilege
	SnapmirrorResyncPrivilegePath = "snapmirror resync"
	// SnapmirrorResyncPrivilegeAccess is the access level for snapmirror resync privilege
	SnapmirrorResyncPrivilegeAccess = "readonly"
)

// buildSnapmirrorQuery builds the query string for snapmirror resync privilege.
// Returns empty string if the path already exists (indicating skip update).
func buildSnapmirrorQuery(existingPrivilege *vsa.RolePrivilege, newPath string) string {
	if existingPrivilege == nil || existingPrivilege.Query == "" {
		return fmt.Sprintf("-source-path %s", newPath)
	}

	existingQuery := existingPrivilege.Query
	// Extract paths from existing query: -source-path path1|path2|path3
	pathsStr := strings.TrimPrefix(existingQuery, "-source-path ")
	existingPaths := strings.Split(pathsStr, "|")

	// Check if the new path already exists
	for _, path := range existingPaths {
		if strings.TrimSpace(path) == newPath {
			return "" // Path exists, skip update
		}
	}

	// Append new path to existing paths
	allPaths := append(existingPaths, newPath)
	return fmt.Sprintf("-source-path %s", strings.Join(allPaths, "|"))
}

// SetHybridReplicationVariablesReverse sets hybrid replication variables for reverse operations
func (a *ReverseHybridReplicationActivity) SetHybridReplicationVariablesReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	if result.DbVolReplication != nil && result.DbVolReplication.HybridReplicationAttributes != nil {
		logger.Infof("Replication is a hybrid replication")
		result.IsHybridReplicationVolume = true
		if replication.IsSrcForHybridReplication(result.DbVolReplication) {
			result.IsSrcForHybridReplication = true
		}
	}
	return result, nil
}

// CheckClusterPeerHealthForHybridReverse checks cluster peer health from ONTAP
// Uses ClusterPeer relationship from VolumeReplication to get the cluster peering row and check health
func (a *ReverseHybridReplicationActivity) CheckClusterPeerHealthForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("CheckClusterPeerHealthForHybridReverse")

	if result.DbVolReplication == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication is nil"))
	}
	dbReplication := result.DbVolReplication

	// Check if ClusterPeer is set
	if dbReplication.ClusterPeer == nil {
		return nil, errors.NewVCPError(errors.ErrClusterPeerNotFound, fmt.Errorf("cluster peer is not set in replication"))
	}

	clusterPeering := dbReplication.ClusterPeer

	if clusterPeering.OntapPeerUUID == "" {
		return nil, errors.NewVCPError(errors.ErrClusterPeerNotFound, fmt.Errorf("ontap peer UUID is missing in cluster peering row"))
	}

	result.ClusterPeeringRow = clusterPeering

	// Get provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerNotFound, fmt.Errorf("cluster peer not found in provider: %v", err)))
	}

	// Get cluster peer from ONTAP using OntapPeerUUID
	clusterPeer, err := provider.GetClusterPeer(clusterPeering.OntapPeerUUID)
	if err != nil {
		logger.Errorf("Failed to get cluster peer from ONTAP: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Check if cluster peer is available
	if clusterPeer.Availability != models.AvailabilityAvailable {
		logger.Warnf("Cluster peer is not available: Availability=%s, AuthenticationState=%s",
			clusterPeer.Availability, clusterPeer.AuthenticationState)
		return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerNotAvailable,
			fmt.Errorf("cluster peer is not available: %s", clusterPeer.Availability)))
	}

	result.ClusterPeer = clusterPeer
	logger.Infof("Cluster peer is healthy: UUID=%s, Availability=%s, AuthenticationState=%s",
		clusterPeer.ExternalUUID, clusterPeer.Availability, clusterPeer.AuthenticationState)
	return result, nil
}

// UpdateRbacRoleForHybridReverse updates RBAC role for remote SnapMirror access
func (a *ReverseHybridReplicationActivity) UpdateRbacRoleForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	if result.IsSrcForHybridReplication {
		return result, nil
	}
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateRbacRoleForHybridReverse")

	if result.DbVolReplication == nil || result.DbVolReplication.ReplicationAttributes == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication or replication attributes is nil"))
	}

	// Get provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Role name for hybrid replication
	roleName := onPremPeerRole

	// Get the role - it must exist
	// GetRoleCollection with name filter queries directly by name and returns matching roles
	roles, err := provider.GetRoleCollection(vsa.GetRoleCollectionParams{
		Name: &roleName,
	})
	if err != nil {
		logger.Errorf("Failed to get role collection: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	if len(roles) == 0 {
		logger.Errorf("Role %s not found", roleName)
		return nil, errors.WrapAsTemporalApplicationError(fmt.Errorf("role %s not found", roleName))
	}

	// Use the first matching role (name filter ensures it matches the requested name)
	targetRole := roles[0]

	// Format: svm:volume
	sourcePath := getPath(result.DbVolReplication.ReplicationAttributes.DestinationSvmName, result.DbVolReplication.ReplicationAttributes.DestinationVolumeName)

	// Check if snapmirror resync privilege already exists
	var existingSnapmirrorPrivilege *vsa.RolePrivilege
	for _, privilege := range targetRole.Privileges {
		if privilege.Path == SnapmirrorResyncPrivilegePath {
			existingSnapmirrorPrivilege = privilege
			break
		}
	}

	// Build query with source path(s)
	snapmirrorQuery := buildSnapmirrorQuery(existingSnapmirrorPrivilege, sourcePath)
	if snapmirrorQuery == "" {
		// Path already exists, skip update
		logger.Infof("Source path %s already exists in snapmirror resync privilege, skipping update", sourcePath)
		return result, nil
	}

	// Common privilege parameters
	ownerID := targetRole.OwnerID

	if existingSnapmirrorPrivilege != nil {
		// Modify existing privilege
		err = provider.ModifyRolePrivilege(vsa.ModifyRolePrivilegeParams{
			OwnerID: ownerID,
			Name:    roleName,
			Path:    SnapmirrorResyncPrivilegePath,
			Access:  SnapmirrorResyncPrivilegeAccess,
			Query:   snapmirrorQuery,
		})
		if err != nil {
			logger.Errorf("Failed to modify snapmirror resync privilege: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully modified RBAC role %s snapmirror resync privilege (%s) for source path(s): %s", roleName, SnapmirrorResyncPrivilegeAccess, snapmirrorQuery)
	} else {
		// Create new privilege
		_, err = provider.CreateRolePrivilege(vsa.CreateRolePrivilegeParams{
			OwnerID: ownerID,
			Name:    roleName,
			Path:    SnapmirrorResyncPrivilegePath,
			Access:  SnapmirrorResyncPrivilegeAccess,
			Query:   snapmirrorQuery,
		})
		if err != nil {
			logger.Errorf("Failed to create snapmirror resync privilege: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully created RBAC role %s snapmirror resync privilege (%s) for source path: %s", roleName, SnapmirrorResyncPrivilegeAccess, snapmirrorQuery)
	}

	return result, nil
}

// GenerateReverseCommandsForHybridReverse generates snapmirror reverse commands
func (a *ReverseHybridReplicationActivity) GenerateReverseCommandsForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	if result.IsSrcForHybridReplication {
		return result, nil
	}
	logger := util.GetLogger(ctx)
	logger.Debugf("GenerateReverseCommandsForHybridReverse")

	if result.DbVolReplication == nil || result.DbVolReplication.ReplicationAttributes == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication or replication attributes is nil"))
	}

	replicationSchedule := result.DbVolReplication.ReplicationAttributes.ReplicationSchedule
	cronScheduleName := fmt.Sprintf("%s-%s", result.DbVolReplication.Name, replicationSchedule)

	// For reverse: GCNV becomes source, external ONTAP becomes destination
	// gcnvPath is the source path (GCNV, previously destination, now source after reverse)
	gcnvPath := getPath(result.DbVolReplication.ReplicationAttributes.DestinationSvmName, result.DbVolReplication.ReplicationAttributes.DestinationVolumeName)
	// extOntapPath is the destination path (external ONTAP, previously source, now destination after reverse)
	extOntapPath := getPath(result.DbVolReplication.ReplicationAttributes.SourceSvmName, result.DbVolReplication.ReplicationAttributes.SourceVolumeName)

	var cronSchedule string
	switch replicationSchedule {
	case vsa.VolumeReplicationSchedule10Minutely:
		// create a cron schedule that runs every 10 minutes
		cronSchedule = "-minute \"0,10,20,30,40,50\""
	case vsa.VolumeReplicationScheduleHourly:
		// create a cron schedule that runs every 1 hour
		cronSchedule = "-hour all -minute 0"
	case vsa.VolumeReplicationScheduleDaily:
		// create a cron schedule that runs every day
		cronSchedule = "-dayofweek all -hour 0 -minute 0"
	default:
		logger.Errorf("Invalid replication schedule: %s", replicationSchedule)
		return nil, errors.NewVCPError(errors.ErrorReplicationScheduleUnspecified, fmt.Errorf("invalid replication schedule: %s", replicationSchedule))
	}

	commands := []string{
		fmt.Sprintf("job schedule cron create -name %s %s", cronScheduleName, cronSchedule),
		fmt.Sprintf("snapmirror resync -destination-path %s -source-path %s", extOntapPath, gcnvPath),
		fmt.Sprintf("snapmirror modify -destination-path %s -source-path %s -schedule %s", extOntapPath, gcnvPath, cronScheduleName),
	}

	result.HybridReplicationUserCommands = commands
	logger.Infof("Generated reverse commands for replication %s with schedule %s", result.DbVolReplication.UUID, replicationSchedule)
	return result, nil
}

// UpdateReplicationWithReverseCommandsForHybridReverse updates replication in database with reverse commands
func (a *ReverseHybridReplicationActivity) UpdateReplicationWithReverseCommandsForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateReplicationWithReverseCommandsForHybridReverse")

	if result.IsSrcForHybridReplication {
		result.DbVolReplication.HybridReplicationAttributes.StatusDetails = "Reverse in progress"
		result.DbVolReplication.HybridReplicationAttributes.HybridReplicationUserCommands = nil
	} else {
		// Ensure HybridReplicationAttributes exists
		if result.DbVolReplication.HybridReplicationAttributes == nil {
			result.DbVolReplication.HybridReplicationAttributes = &datamodel.HybridReplicationAttribute{}
		}

		// Update HybridReplicationUserCommands
		result.DbVolReplication.HybridReplicationAttributes.HybridReplicationUserCommands = result.HybridReplicationUserCommands

		// Set status to PendingReverseResume
		result.DbVolReplication.HybridReplicationAttributes.Status = models.HybridReplicationStatusPendingRemoteResync

		// Set status details
		result.DbVolReplication.HybridReplicationAttributes.StatusDetails = "Please execute the SnapMirror commands on the on-premises system to establish a new SnapMirror relationship."
	}

	// Update the replication in database
	err := a.SE.UpdateVolumeReplication(ctx, result.DbVolReplication)
	if err != nil {
		logger.Errorf("Failed to update replication in database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	logger.Infof("Successfully updated replication with reverse commands: %s", result.DbVolReplication.UUID)
	return result, nil
}

// ListSnapmirrorDestinationsForHybridReverse lists snapmirror destinations from ONTAP
func (a *ReverseHybridReplicationActivity) ListSnapmirrorDestinationsForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("ListSnapmirrorDestinationsForHybridReverse")

	if result.NodeProvider == nil {
		return nil, errors.NewVCPError(errors.ErrVSAClusterNodeNotFound, fmt.Errorf("node provider is nil"))
	}

	// Get provider from node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		logger.Errorf("Failed to get provider from node: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	// Build source and destination paths for filtering
	// For reverse: external ONTAP becomes source, GCNV becomes destination
	if result.DbVolReplication == nil || result.DbVolReplication.ReplicationAttributes == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication or replication attributes is nil"))
	}

	newDestPath := getPath(result.DbVolReplication.ReplicationAttributes.SourceSvmName, result.DbVolReplication.ReplicationAttributes.SourceVolumeName)
	newSourcePath := getPath(result.DbVolReplication.ReplicationAttributes.DestinationSvmName, result.DbVolReplication.ReplicationAttributes.DestinationVolumeName)

	// Create params to filter by source and destination paths
	params := &ontapRest.SnapmirrorRelationshipListDestinationsParams{
		SourcePath:      &newSourcePath,
		DestinationPath: &newDestPath,
	}

	// List snapmirror destinations using the provider's method with filtering params
	destinations, err := provider.ListSnapmirrorDestinations(params)
	if err != nil {
		logger.Errorf("Failed to list snapmirror destinations: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Found %d snapmirror destinations for replication %s (SourcePath=%s, DestinationPath=%s)",
		len(destinations), result.DbVolReplication.UUID, newSourcePath, newDestPath)

	// Return error if no destination found - this will trigger activity retry
	if len(destinations) == 0 {
		logger.Debugf("No snapmirror destination found for replication %s, will retry", result.DbVolReplication.UUID)
		return nil, errors.NewVCPError(errors.ErrGetRemoteReplicationJobs, fmt.Errorf("snapmirror destination not found for sourcePath=%s, destinationPath=%s", newSourcePath, newDestPath))
	}

	// Found matching destination
	logger.Infof("Found matching snapmirror destination: SourcePath=%s, DestinationPath=%s, RelationshipUUID=%s",
		destinations[0].SourcePath, destinations[0].DestinationPath, destinations[0].RelationshipUUID)

	return result, nil
}

// SetReplicationToErrorForReverseHybrid updates the replication row to error state
// when the reverse hybrid replication poll workflow fails: State, StateDetails, and HybridReplicationAttributes
// (Status, StatusDetails, HybridReplicationUserCommands, HybridReplicationType).
func (a *ReverseHybridReplicationActivity) SetReplicationToErrorForReverseHybrid(ctx context.Context, replication *datamodel.VolumeReplication, errorMessage string, isSrcForHybridReplication bool) error {
	if replication == nil {
		return nil
	}
	replicationToUpdate := *replication
	replicationToUpdate.State = models.LifeCycleStateError
	replicationToUpdate.StateDetails = fmt.Sprintf("Reverse was not successful: %s", errorMessage)

	if replicationToUpdate.HybridReplicationAttributes == nil {
		replicationToUpdate.HybridReplicationAttributes = &datamodel.HybridReplicationAttribute{}
	}
	replicationToUpdate.HybridReplicationAttributes.StatusDetails = replicationToUpdate.StateDetails
	replicationToUpdate.HybridReplicationAttributes.HybridReplicationUserCommands = nil
	if isSrcForHybridReplication {
		replicationToUpdate.HybridReplicationAttributes.Status = models.HybridReplicationStatusExternalManaged
		hybridType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
		replicationToUpdate.HybridReplicationAttributes.HybridReplicationType = &hybridType
	} else {
		replicationToUpdate.HybridReplicationAttributes.Status = models.HybridReplicationStatusPeered
		hybridType := string(models.HybridReplicationParametersReplicationTypeONPREM)
		replicationToUpdate.HybridReplicationAttributes.HybridReplicationType = &hybridType
	}

	if err := a.SE.UpdateVolumeReplication(ctx, &replicationToUpdate); err != nil {
		return err
	}
	return nil
}

// UpdateReplicationStateForHybridReverse updates replication state based on snapmirror status
func (a *ReverseHybridReplicationActivity) UpdateReplicationStateForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateReplicationStateForHybridReverse")

	if result.DbVolReplication == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication is nil"))
	}

	// Get the latest replication from database
	dbReplication, err := a.SE.GetVolumeReplication(ctx, result.DbVolReplication.UUID)
	if err != nil {
		logger.Errorf("Failed to get replication from database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, err)
	}

	// Ensure HybridReplicationAttributes exists
	if dbReplication.HybridReplicationAttributes == nil {
		dbReplication.HybridReplicationAttributes = &datamodel.HybridReplicationAttribute{}
	}

	// Set hybrid replication type
	hybridReplicationType := string(models.HybridReplicationParametersReplicationTypeREVERSE)
	dbReplication.HybridReplicationAttributes.HybridReplicationType = &hybridReplicationType

	// Set status to ExternalManaged
	dbReplication.HybridReplicationAttributes.Status = models.HybridReplicationStatusExternalManaged

	// Set status details
	dbReplication.HybridReplicationAttributes.StatusDetails = "Replication is being externally managed by the On-Prem cluster"

	// Clear user commands (set to nil)
	dbReplication.HybridReplicationAttributes.HybridReplicationUserCommands = nil

	// Update the replication in database
	err = a.SE.UpdateVolumeReplication(ctx, dbReplication)
	if err != nil {
		logger.Errorf("Failed to update replication state in database: %v", err)
		return nil, errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
	}

	result.DbVolReplication = dbReplication
	logger.Infof("Updated replication state: UUID=%s, HybridReplicationType=%s, Status=%s",
		dbReplication.UUID, hybridReplicationType, dbReplication.HybridReplicationAttributes.Status)
	return result, nil
}

// CreateJobForHybridReverse creates a job entry for the child workflow
func (a *ReverseHybridReplicationActivity) CreateJobForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult, jobType string) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("CreateJobForHybridReverse: jobType=%s", jobType)

	if result.DbVolReplication == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication is nil"))
	}

	// The job state is set to NEW here because the workflow itself will update it to PROCESSING
	job := &datamodel.Job{
		AccountID:     sql.NullInt64{Int64: result.DbVolReplication.AccountID, Valid: true},
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: result.DbVolReplication.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	// Build resource name for the job
	resourceName := fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s",
		*result.DstProjectNumber,
		result.DbVolReplication.ReplicationAttributes.DestinationLocation,
		result.DbVolReplication.Volume.Name,
		result.DbVolReplication.Name)
	job.ResourceName = resourceName

	createdJob, err := a.SE.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	logger.Infof("Successfully created job: UUID=%s, WorkflowID=%s", createdJob.UUID, createdJob.WorkflowID)
	return createdJob, nil
}

// GetNodeProviderForHybridReverse gets node provider from replication pool
func (a *ReverseHybridReplicationActivity) GetNodeProviderForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetNodeProviderForHybridReverse")

	// Get nodes for the pool
	nodes, err := a.SE.GetNodesByPoolID(ctx, result.DbVolReplication.Volume.PoolID)
	if err != nil || len(nodes) == 0 {
		logger.Errorf("Failed to get nodes for pool %d: %v", result.DbVolReplication.Volume.PoolID, err)
		return nil, errors.NewVCPError(errors.ErrVSAClusterNodeNotFound, err)
	}

	// Create node provider
	if result.DbVolReplication.Volume.Pool.PoolCredentials == nil {
		return nil, errors.NewVCPError(errors.ErrResourceNotFound, fmt.Errorf("pool credentials not found for pool %d", result.DbVolReplication.Volume.PoolID))
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            nodes,
		DeploymentName:   result.DbVolReplication.Volume.Pool.DeploymentName,
		OntapCredentials: result.DbVolReplication.Volume.Pool.PoolCredentials,
	})

	result.NodeProvider = node
	logger.Infof("Successfully created node provider for pool %d", result.DbVolReplication.Volume.PoolID)
	return result, nil
}

// GetDstBasePathForHybridReverse gets the base path for the current destination (which will become new source after reverse)
func (a *ReverseHybridReplicationActivity) GetDstBasePathForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetDstBasePathForHybridReverse")

	dstBasePath, err := GetBasePath(ctx, result.DbVolReplication.ReplicationAttributes.DestinationLocation)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetDstBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

// GetSignedDstTokenForHybridReverse gets signed token for the current destination (which will become new source after reverse)
func (a *ReverseHybridReplicationActivity) GetSignedDstTokenForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("GetSignedDstTokenForHybridReverse")

	dstJwt, err := GetSignedToken(ctx, *result.DstProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.DstJwtToken = dstJwt
	return result, nil
}

// CleanupOldReplicationForHybridReverse cleans up the old replication on the destination (new source after reverse)
func (a *ReverseHybridReplicationActivity) CleanupOldReplicationForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	// Run CleanupReplicationAfterReverse on the original destination (new source)
	logger := util.GetLogger(ctx)
	logger.Debugf("CleanupOldReplicationForHybridReverse")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	deleteReplicationParams := &googleproxyclient.V1betaInternalDeleteVolumeReplicationParams{
		ProjectNumber:       *result.DstProjectNumber,
		LocationId:          result.DbVolReplication.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.DbVolReplication.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.Event.CommonReplicationEventParams.XCorrelationID),
		CleanupAfterReverse: googleproxyclient.NewOptBool(true),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDeleteVolumeReplication(ctx, *deleteReplicationParams)
	if err != nil {
		logger.Errorf("Failed to cleanup old replication: %v", err)
		return nil, errors.NewVCPError(errors.ErrCleanupVolumeReplicationAfterReverse, err)
	}

	switch r := res.(type) {
	case *googleproxyclient.VolumeReplicationInternalV1beta:
		if len(r.Jobs) > 0 && r.Jobs[0].JobId.Set {
			jobId := r.Jobs[0].JobId.Value
			result.JobId = &jobId
			logger.Infof("Cleanup job created: %s", jobId)
		}
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

// DescribeRemoteJobOnDstForHybridReverse describes remote jobs for hybrid reverse operations
func (a *ReverseHybridReplicationActivity) DescribeRemoteJobOnDstForHybridReverse(ctx context.Context, result *replication.ReverseHybridReplicationResult) error {
	// Describe the cleanup job if it exists
	if result.JobId != nil {
		logger := util.GetLogger(ctx)
		logger.Debugf("DescribeRemoteJobOnDstForHybridReverse: JobId=%v", result.JobId)

		err := activities.DescribeJob(ctx, result.JobId, result.DstBasePath, result.DstJwtToken, result.DstProjectNumber, &result.DbVolReplication.ReplicationAttributes.DestinationLocation, result.Event.CommonReplicationEventParams.XCorrelationID)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *ReverseHybridReplicationActivity) HydrateReplicationSateAndTypeForReverseHybridReplication(ctx context.Context, result *replication.ReverseHybridReplicationResult) (*replication.ReverseHybridReplicationResult, error) {
	err := HydrateReplicationStateAndTypeForHybridReplication(ctx, result.DbVolReplication, models.VolumeReplicationHydrateStateExternalManaged, models.HybridReplicationParametersReplicationTypeREVERSE, result.Event.Location, result.Event.VolumeResourceID)
	if err != nil {
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	return result, nil
}

func HydrateReplicationStateAndTypeForHybridReplication(ctx context.Context, dbVolRep *datamodel.VolumeReplication, hydrateState models.VolumeReplicationHydrateState, hydrateType models.HybridReplicationParametersReplicationType, location, volumeResourceId string) error {
	if hydrationEnabled {
		logger := util.GetLogger(ctx)
		logger.Debugf("Hydrating volume replication for hybrid replication after reverse")
		// Convert the database volume replication to models.VolumeReplication for hydration
		volumeRepModel := models.VolumeReplication{
			Name: dbVolRep.Name,
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationRegion:     location,
				DestinationVolumeName: volumeResourceId,
			},
		}

		projectNumber, err := utils.ParseProjectNumberFromURI(dbVolRep.Uri)
		if err != nil {
			logger.Errorf("Failed to parse project number from Uri: %v, remoteUri: %s", err, dbVolRep.Uri)
			return errors.WrapAsTemporalApplicationError(fmt.Errorf("failed to parse project number: %w", err))
		}
		// Call HydrateVolumeReplication with the specified parameters
		err = hydrateReplicationStateAndTypeForHybrid(ctx, volumeRepModel, hydrateState, hydrateType, projectNumber)
		if err != nil {
			logger.Errorf("Failed to hydrate volume replication: %v", err)
			return errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully hydrated volume replication after reverse")
	}
	return nil
}
