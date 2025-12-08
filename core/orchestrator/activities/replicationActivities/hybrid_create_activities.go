package replicationActivities

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	ontaprestmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"gorm.io/gorm"
)

var (
	hydrateReplicationStateForHybrid = HydrateReplicationState
)

type HybridReplicationActivity struct {
	SE database.Storage
}

const (
	VolumeReplicationEndpointTypeDestination = "dst"
	onPremPeerRole                           = "external-peer"
	accessNone                               = "none"
	accessReadOnly                           = "readonly"
	defaultPath                              = "DEFAULT"
	RemoteRegionCustomer                     = "customer"
	ReplicationTypeExternalMigration         = "ExternalMigration"
	ReplicationTypeExternalDisasterRecovery  = "ExternalDisasterRecovery"
)

var defaultNoneRolePrivilege = []*vsa.RolePrivilege{
	{Path: defaultPath, Access: accessNone},
	{Path: "debug", Access: accessNone},
}

func (a *HybridReplicationActivity) CreateJobForHybridReplication(ctx context.Context, replicationResult replication.CreateHybridReplicationResult, jobType string) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateJobForHybridReplication")
	se := a.SE
	var resourceName string
	// The job state is set to PROCESSING here because the workflow itself is creating the job
	job := &datamodel.Job{
		AccountID:     sql.NullInt64{Int64: replicationResult.DestinationVolume.AccountID, Valid: true},
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: replicationResult.DestinationVolume.UUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	if jobType == string(models.JobTypeCreateVolume) {
		resourceName = replicationResult.DestinationVolume.Name
	} else if jobType == string(models.JobTypeHybridReplicationEstablishPeering) || jobType == string(models.JobTypeHybridReplicationInternalEstablish) {
		resourceName = fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s",
			replicationResult.DestinationProjectNumber,
			replicationResult.DestinationRegion,
			replicationResult.DestinationVolume.Name,
			replicationResult.HybridReplicationParameters.ResourceID)
	}
	job.ResourceName = resourceName
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		return nil, err
	}
	return createdJob, nil
}

func (a *HybridReplicationActivity) GetDstBasePathForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetDstBasePathForHybridReplication")
	dstBasePath, err := GetBasePath(ctx, result.DestinationRegion)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSrcBasePath, err)
	}
	result.DstBasePath = dstBasePath
	return result, nil
}

func (a *HybridReplicationActivity) GetDstSignedTokenForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetDstSignedTokenForHybridReplication")
	dstJwt, err := GetSignedToken(ctx, result.DestinationProjectNumber)
	if err != nil {
		return nil, errors.NewVCPError(errors.ErrGetSignedToken, err)
	}
	result.DstJwtToken = dstJwt
	return result, nil
}

func (a *HybridReplicationActivity) DescribeJobForHybridReplicationWorkflow(ctx context.Context, result *replication.CreateHybridReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debug("DescribeJobForHybridReplicationWorkflow")
	if result.JobId == nil {
		return nil
	}

	// Query job from database
	job, err := a.SE.GetJob(ctx, *result.JobId)
	if err != nil {
		if customerrors.IsNotFoundErr(err) {
			logger.Errorf("Job with UUID %s not found in database", *result.JobId)
			return errors.NewVCPError(errors.ErrDescribingJobNotFound, fmt.Errorf("job with UUID %s not found", *result.JobId))
		}
		logger.Errorf("Failed to get job from database: %v", err)
		return errors.WrapAsTemporalApplicationError(err)
	}

	// Check if job is finished
	if job.State == string(models.JobsStateDONE) {
		logger.Debugf("Job %s is done successfully", *result.JobId)
		return nil
	}

	// Check if job failed
	if job.State == string(models.JobsStateERROR) {
		// Get error message from tracking ID, similar to V1betaInternalDescribeOperation
		errMsg := errors.GetErrorMessageByTrackingID(job.TrackingID)
		detailedErrorMessage := errMsg.Message

		// Special case: For ErrRestoreVolumeValidation, use ErrorDetails directly
		if job.TrackingID == errors.ErrRestoreVolumeValidation {
			detailedErrorMessage = string(job.ErrorDetails)
		}

		logger.Errorf("Job %s failed with error: %s (TrackingID: %d)", *result.JobId, detailedErrorMessage, job.TrackingID)

		// Return non-retryable error with the detailed error message
		return errors.WrapAsNonRetryableTemporalApplicationError(
			errors.NewVCPError(job.TrackingID, fmt.Errorf("job failed with error: %s", detailedErrorMessage)))
	}

	// Job is still in progress (NEW, PROCESSING, or WAIT_FOR_TEMPORAL)
	logger.Debugf("Job %s is not finished yet, current state: %s", *result.JobId, job.State)
	return errors.NewVCPError(errors.ErrJobNotFinished, fmt.Errorf("job not finished, current state: %s", job.State))
}

// GetNodeForHybridReplication retrieves the node associated with the given pool ID.
func (a *HybridReplicationActivity) GetNodeForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetNodeForHybridReplication")
	se := a.SE
	nodes, err := se.GetNodesByPoolID(ctx, result.DestinationVolume.Pool.ID)
	if err != nil {
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	if len(nodes) == 0 {
		return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrUnexpectedNodeCountForPool, errors.New("Node not found for the pool")))
	}
	result.DbNodes = nodes
	return result, nil
}

func (a *HybridReplicationActivity) CreateNodesForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateNodesForHybridReplication")

	nodes := hyperscaler.NodeProviderInput{Nodes: result.DbNodes, DeploymentName: result.DestinationVolume.Pool.DeploymentName, OntapCredentials: result.DestinationVolume.Pool.PoolCredentials}
	nodeProvider := hyperscaler.CreateNodeForProvider(nodes)
	if nodeProvider == nil {
		return nil, errors.NewVCPError(errors.ErrInputValidationError, fmt.Errorf("failed to create destination node"))
	}
	result.NodeProvider = nodeProvider
	logger.Debugf("Successfully created destination node with deployment: %s", nodeProvider.DeploymentName)
	return result, nil
}

// GetOrCreateClusterPeerForHybridReplication retrieves existing cluster peer from database or creates a new one
// This combines the logic of GetClusterPeerFromDatabase and CreateClusterPeeringInDatabaseForHybridReplication
func (a *HybridReplicationActivity) GetOrCreateClusterPeerForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetOrCreateClusterPeerForHybridReplication")
	se := a.SE
	onPrempCluster := result.HybridReplicationParameters.PeerClusterName
	// First, try to get existing cluster peer from database
	clusterPeer, err := se.GetClusterPeerByAccountIDExternalClusterAndPoolID(ctx, result.DestinationVolume.AccountID, onPrempCluster, result.DestinationVolume.PoolID)
	if err != nil {
		if !customerrors.IsNotFoundErr(err) {
			logger.Errorf("Failed to get cluster peer from database: %v", err)
			return nil, err
		}
		// Cluster peer not found, we'll create a new one below
		logger.Debugf("Cluster peer not found in database, will create new one")
	} else if clusterPeer != nil {
		// Cluster peer exists, use it
		logger.Debugf("Found existing cluster peer in database")
		result.ClusterPeeringRow = clusterPeer
		return result, nil
	}

	// Cluster peer doesn't exist, create a new one
	logger.Debugf("Creating new cluster peering database entry")
	newClusterPeer := &datamodel.ClusterPeerings{
		State:          models.CvpClusterPeeringStatusCREATING,
		OnprempCluster: result.HybridReplicationParameters.PeerClusterName,
		AccountID:      result.DestinationVolume.AccountID,
		PoolID:         result.DestinationVolume.PoolID,
		ClusterPeeringAttributes: &datamodel.ClusterPeeringAttributes{
			ClusterLocation: &result.HybridReplicationParameters.ClusterLocation,
		},
	}
	newClusterPeer.UUID = utils.RandomUUID()

	createdClusterPeer, err := se.CreateClusterPeeringRow(ctx, newClusterPeer)
	if err != nil {
		logger.Errorf("Failed to create cluster peering row: %v", err)
		return nil, err
	}
	// Update the cluster peer in the result
	result.ClusterPeeringRow = createdClusterPeer
	logger.Infof("Successfully created new cluster peer")
	return result, nil
}

func (a *HybridReplicationActivity) CreateLocalHybridReplicationRow(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateLocalHybridReplicationRow")
	if result.DbVolReplication != nil {
		logger.Debugf("Volume replication row already exists in database: %s", result.DbVolReplication.Name)
		return result, nil
	}

	location := result.DestinationRegion
	if result.DestinationZone != "" {
		location = result.DestinationZone
	}

	// Build replication URI
	ccfeReplicationUri := fmt.Sprintf("projects/%s/locations/%s/volumes/%s/replications/%s",
		result.DestinationProjectNumber,
		location,
		result.DestinationVolume.Name,
		result.HybridReplicationParameters.ResourceID)

	// Try to fetch existing replication record from database first using the same logic as _validateReplicationParams
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", result.DestinationVolume.AccountID),
		utils2.NewFilterCondition("uri", "=", ccfeReplicationUri))
	existingReplications, err := a.SE.ListVolumeReplications(ctx, *filter, database.QueryDepthOne)
	if err != nil {
		logger.Debugf("Error fetching existing volume replication for URI: %s, error: %v", ccfeReplicationUri, err)
	} else if len(existingReplications) > 0 {
		logger.Debugf("Found existing volume replication: %s", existingReplications[0].Name)
		return nil, customerrors.NewConflictErr(fmt.Sprintf("Volume replication with URI '%s' already exists", ccfeReplicationUri))
	} else {
		logger.Debugf("No existing volume replication found for URI: %s", ccfeReplicationUri)
	}

	replicationType := ReplicationTypeExternalMigration
	if result.HybridReplicationParameters.ReplicationType == models.HybridReplicationParametersReplicationTypeONPREM {
		replicationType = ReplicationTypeExternalDisasterRecovery
	}
	mirrorState := models.OntapUninitialized
	emptyUUID := uuid.UUID{}.String()
	// Create replication attributes
	replicationAttributes := &datamodel.ReplicationDetails{
		EndpointType:               VolumeReplicationEndpointTypeDestination,
		ReplicationType:            replicationType,
		ReplicationSchedule:        result.HybridReplicationParameters.ReplicationSchedule,
		SourcePoolUUID:             emptyUUID,
		SourceVolumeUUID:           emptyUUID,
		SourceLocation:             RemoteRegionCustomer,
		SourceReplicationUUID:      emptyUUID,
		SourceSvmName:              result.HybridReplicationParameters.PeerSvmName,
		SourceHostName:             result.HybridReplicationParameters.PeerClusterName,
		SourceVolumeName:           result.HybridReplicationParameters.PeerVolumeName,
		DestinationPoolUUID:        result.DestinationVolume.Pool.UUID,
		DestinationVolumeUUID:      result.DestinationVolume.UUID,
		DestinationLocation:        location,
		DestinationHostName:        result.DestinationVolume.Pool.ClusterDetails.ExternalName,
		DestinationReplicationUUID: utils.RandomUUID(),
		DestinationVolumeName:      result.DestinationVolume.Name,
		DestinationSvmName:         result.DestinationVolume.Svm.Name,
	}

	// Create hybrid replication attributes
	hybridReplicationAttributes := &datamodel.HybridReplicationAttribute{
		Status:                models.HybridReplicationStatusPendingClusterPeer,
		HybridReplicationType: nillable.ToPointer(string(result.HybridReplicationParameters.ReplicationType)),
		Description:           result.HybridReplicationParameters.Description,
		Labels:                result.HybridReplicationParameters.Labels,
		PeerVolumeName:        result.HybridReplicationParameters.PeerVolumeName,
		PeerSvmName:           result.HybridReplicationParameters.PeerSvmName,
		ReplicationSchedule:   result.HybridReplicationParameters.ReplicationSchedule,
		StateDetailsCode:      models.DefaultCode,
	}

	// Create the volume replication with all data in one go
	expectedDbReplication := &datamodel.VolumeReplication{
		Uri:       ccfeReplicationUri,
		RemoteUri: "",
		BaseModel: datamodel.BaseModel{
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		Name:                        result.HybridReplicationParameters.ResourceID,
		Description:                 nillable.GetString(&result.HybridReplicationParameters.Description, ""),
		ReplicationAttributes:       replicationAttributes,
		HybridReplicationAttributes: hybridReplicationAttributes,
		AccountID:                   result.DestinationVolume.AccountID,
		VolumeID:                    result.DestinationVolume.ID,
		MirrorState:                 &mirrorState,
	}

	volumeRep, err := a.SE.CreateVolumeReplication(ctx, expectedDbReplication)
	if err != nil {
		logger.Errorf("Failed to create volume replication: %v", err)
		return nil, fmt.Errorf("failed to create volume replication with AccountID %d, VolumeID %d: %w",
			result.DestinationVolume.AccountID, result.DestinationVolume.ID, err)
	}
	result.DbVolReplication = volumeRep
	logger.Debugf("Successfully created volume replication: %s", volumeRep.Name)
	return result, nil
}

func areIPsMatching(existingIPs, newIPs []string) bool {
	ipSet := make(map[string]struct{})
	for _, ip := range existingIPs {
		ipSet[ip] = struct{}{}
	}
	for _, ip := range newIPs {
		if _, exists := ipSet[ip]; !exists {
			return false
		}
	}
	return true
}

func onPremMigrationRoleProfile() []*vsa.RolePrivilege {
	// add the 'system capability clusterset show' with 'readonly' access and query for auto SVM peering capabilities
	// Replication is not compatible with ONTAP 9.2.0 this gives the role the ability to see if we are compatible for replication
	profile := append(
		defaultNoneRolePrivilege,
		&vsa.RolePrivilege{Path: "system capability clusterset show", Access: accessReadOnly, Query: "-capability DATA_ONTAP.9.2.0"})
	return profile
}

func modifyExternalVolumeReplicationSecurityRoleIfNeeded(provider vsa.Provider, roleName string) {
	// Use RoleCollectionGet to query for roles by name without needing owner UUID
	// This is more reliable than GetRole with specific owner UUID
	roles, err := provider.GetRoleCollection(vsa.GetRoleCollectionParams{
		Name: &roleName,
	})
	if err != nil {
		return
	}

	// Find the role with the matching name
	var targetRole *vsa.Role
	for _, role := range roles {
		if role.Name == roleName {
			targetRole = role
			break
		}
	}

	if targetRole == nil {
		return
	}

	privilegeModifications := vsa.ModifyRolePrivilegeParams{
		OwnerID: targetRole.OwnerID,
		Name:    roleName,
		Path:    defaultPath,
		Access:  accessNone,
	}

	// Apply each privilege modification
	err = provider.ModifyRolePrivilege(privilegeModifications)
	if err != nil {
		return
	}
}

func (a *HybridReplicationActivity) GetOrCreateClusterPeerInOntapForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetOrCreateClusterPeerInOntapForHybridReplication")

	// Get the provider from the node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsTemporalApplicationError(err)
	}

	se := a.SE

	// Case 1: Cluster peer already exists with UUID - get it from ONTAP
	if result.ClusterPeeringRow != nil && result.ClusterPeeringRow.OntapPeerUUID != "" {
		logger.Debugf("Cluster peer already exists with UUID: %s, retrieving from ONTAP", result.ClusterPeeringRow.OntapPeerUUID)
		// Get cluster peer information from ONTAP
		clusterPeer, err := provider.GetClusterPeer(result.ClusterPeeringRow.OntapPeerUUID)
		if err != nil {
			logger.Errorf("Failed to get cluster peer from ONTAP: %v", err)
			// Handle specific error cases - if not found, create new cluster peer
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
				logger.Warnf("Cluster peer not found in ONTAP, UUID: %s. Will create new cluster peer.", result.ClusterPeeringRow.OntapPeerUUID)
				// Clear the UUID so we can create a new one
				result.ClusterPeeringRow.OntapPeerUUID = ""
				// Fall through to create new cluster peer
			} else {
				return nil, errors.WrapAsTemporalApplicationError(err)
			}
		} else {
			result.ClusterPeer = clusterPeer
			// Update the cluster peer in the result
			logger.Infof("Successfully retrieved cluster peer from ONTAP: UUID=%s, State=%s",
				clusterPeer.UUID, clusterPeer.Availability)
			return result, nil
		}
	}

	// Case 2: Cluster peer UUID is empty - update status to pending and create new one
	if result.ClusterPeeringRow != nil && result.ClusterPeeringRow.OntapPeerUUID == "" {
		logger.Debugf("Cluster peer UUID is empty, updating cluster peer status to pending")
		clusterPeering := result.ClusterPeeringRow
		// Ensure ClusterPeeringRowAttributes is initialized
		if clusterPeering.ClusterPeeringAttributes == nil {
			clusterPeering.ClusterPeeringAttributes = &datamodel.ClusterPeeringAttributes{}
		}
		clusterPeering.StateDetails = models.InitiatingClusterPeering
		clusterPeering.State = models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING
		err = se.UpdateClusterPeeringRow(ctx, clusterPeering)
		if err != nil {
			logger.Errorf("Failed to update cluster peering row: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		// Update the cluster peer in the result
		result.ClusterPeeringRow = clusterPeering
		logger.Infof("Successfully updated cluster peer status to pending: UUID=%s", clusterPeering.UUID)

		result, err = a.updateReplicationStateDetailsCode(ctx, result, models.InitiatingClusterPeeringCode)
		if err != nil {
			logger.Errorf("Failed to update cluster peering row: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
	}

	// Define role name and privileges for hybrid replication
	roleName := onPremPeerRole
	rolePrivileges := onPremMigrationRoleProfile()
	roleParams := vsa.CreateRoleParams{
		Name:       roleName,
		Privileges: rolePrivileges,
	}

	// Create role if it doesn't exist
	_, err = provider.CreateRole(roleParams)
	if err != nil {
		if strings.Contains(err.Error(), "Role already exists in legacy role table") {
			modifyExternalVolumeReplicationSecurityRoleIfNeeded(provider, roleName)
		} else {
			return nil, err
		}
	}

	// Case 3: Create new cluster peer in ONTAP
	logger.Debugf("Creating new cluster peer in ONTAP")
	// Check for existing cluster peers before creating a new one
	clusterPeers, err := provider.ListClusterPeers()
	if err != nil {
		logger.Errorf("Failed to list cluster peers: %v", err)
		return nil, err
	}

	// Look for existing cluster peer that matches our requirements
	for _, peer := range clusterPeers {
		// Check if this peer matches our cluster name and IP addresses
		if peer.PeerClusterName == result.HybridReplicationParameters.PeerClusterName &&
			areIPsMatching(peer.PeerAddresses, result.HybridReplicationParameters.PeerIPAddresses) {
			if peer.Availability == models.AvailabilityAvailable {
				// Found a matching available cluster peer - reuse it
				logger.Infof("Found existing available cluster peer: %s, reusing it", peer.ExternalUUID)
				clusterPeering := result.ClusterPeeringRow
				clusterPeering.OntapPeerUUID = peer.ExternalUUID
				// Update cluster peer in database
				err = se.UpdateClusterPeeringRow(ctx, clusterPeering)
				if err != nil {
					logger.Errorf("Failed to update cluster peering row: %v", err)
					return nil, err
				}
				result.ClusterPeer = peer
				// Update the cluster peer in the result
				result.ClusterPeeringRow = clusterPeering
				logger.Infof("Successfully reused existing cluster peer: UUID=%s", peer.ExternalUUID)
				return result, nil
			} else {
				logger.Errorf("Found matching cluster peer but it's not available (state: %s)", peer.Availability)
				return nil, errors.WrapAsNonRetryableTemporalApplicationError(
					errors.NewVCPError(errors.ErrClusterPeerNotAvailable,
						fmt.Errorf("cluster peer %s is not available (state: %s)", peer.ExternalUUID, peer.Availability)))
			}
		}
	}

	// Create cluster peer parameters
	clusterPeerParams := vsa.CreateClusterPeerParams{
		PeerName:      result.HybridReplicationParameters.PeerClusterName,
		PeerAddresses: result.HybridReplicationParameters.PeerIPAddresses,
		IPSpace:       activities.IpSpace,
	}

	clusterPeerParams.LocalRole = &roleName

	// Set expiry time
	var expiryTime *strfmt.DateTime
	expiry := time.Now().Add(time.Minute * 60) // Default expiry time of 60 mins
	expiryTime = (*strfmt.DateTime)(&expiry)
	clusterPeerParams.ExpiryTime = expiryTime
	// Create cluster peer in ONTAP
	clusterPeerFromOntap, err := provider.CreateClusterPeer(clusterPeerParams)
	if err != nil {
		if strings.Contains(err.Error(), "Error creating cluster peer - Max retries reached") ||
			strings.Contains(err.Error(), "Verify that the peer address is correct, and then try the operation again.") ||
			strings.Contains(err.Error(), "Error creating cluster peer - Retries exhausted") {
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrorCreateClusterPeerCVISourceClusterUnreachable, err))
		}
		return nil, errors.NewVCPError(errors.ErrClusterPeerError, err)
	}
	// Generate cluster peer command for source cluster
	clusterPeerCommand := fmt.Sprintf("cluster peer create -peer-addrs %s -initial-allowed-vserver-peers %s",
		strings.Join(result.DestinationVolume.Pool.ClusterDetails.InterclusterLifIPs, ","),
		result.HybridReplicationParameters.PeerSvmName)

	convertedExpiryTime := (*time.Time)(clusterPeerFromOntap.ExpiryTime)
	// Update cluster peering record with ONTAP cluster peer details
	clusterPeering := result.ClusterPeeringRow
	if clusterPeering.ClusterPeeringAttributes != nil {
		// Set cluster peer command for source cluster execution
		clusterPeering.ClusterPeeringAttributes.Command = &clusterPeerCommand
		// Set expiry time
		clusterPeering.ClusterPeeringAttributes.ExpiryTime = convertedExpiryTime
		clusterPeering.State = models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING
		clusterPeering.StateDetails = models.WaitingForClusterPeering
	}
	clusterPeering.OntapPeerUUID = clusterPeerFromOntap.ExternalUUID
	// Set passphrase from ONTAP cluster peer
	if clusterPeerFromOntap.Passphrase != nil {
		passphraseStr := string(*clusterPeerFromOntap.Passphrase)
		clusterPeering.ClusterPeeringAttributes.PassPhrase = &passphraseStr
	}
	// Update cluster peer in database
	err = se.UpdateClusterPeeringRow(ctx, clusterPeering)
	if err != nil {
		logger.Errorf("Failed to update cluster peering row: %v", err)
		return nil, err
	}
	result.ClusterPeer = clusterPeerFromOntap
	result.ClusterPeeringRow = clusterPeering

	result, err = a.updateReplicationStateDetailsCode(ctx, result, models.WaitingForClusterPeeringCode)
	if err != nil {
		logger.Errorf("Failed to update cluster peering row: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	if result.ClusterPeeringRow.State != models.CvpClusterPeeringStatusPEERED {
		result.CurrentHydrateState = models.VolumeReplicationHydrateStatePendingClusterPeering
	} else {
		result.CurrentHydrateState = models.VolumeReplicationHydrateStatePendingSvmPeering
	}
	logger.Infof("Successfully created cluster peer in ONTAP: UUID=%s", clusterPeerFromOntap.ExternalUUID)
	return result, nil
}

func (a *HybridReplicationActivity) WaitForClusterPeerActivityForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("WaitForClusterPeerActivityForHybridReplication")
	// Get the provider from the node

	if result.ClusterPeer != nil && result.ClusterPeer.Availability == models.AvailabilityAvailable {
		return result, nil
	}

	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}

	clusterPeer, err := provider.GetClusterPeer(result.ClusterPeeringRow.OntapPeerUUID)
	if err != nil {
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}

	// If authentication state is in problem state, track first occurrence and wait up to 10 mins
	if clusterPeer.AuthenticationState == models.AuthenticationStateProblem {
		result.ClusterPeer = clusterPeer
		now := time.Now()

		// Track the first time we see problem state
		if result.FirstProblemStateTime == nil {
			result.FirstProblemStateTime = &now
			logger.Warnf("Cluster peer authentication state is in problem state (first occurrence) - ClusterPeerUUID: %s, Availability: %s, will wait up to 10 minutes",
				result.ClusterPeeringRow.OntapPeerUUID, clusterPeer.Availability)
			return result, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerTimeout,
				fmt.Errorf("cluster peer authentication state is %s, waiting for recovery", clusterPeer.AuthenticationState)))
		}

		// Check if 10 minutes have passed since first seeing problem state
		elapsed := now.Sub(*result.FirstProblemStateTime)
		if elapsed >= 10*time.Minute {
			logger.Errorf("Cluster peer authentication state has been in problem state for %v (exceeded 10 minute limit) - ClusterPeerUUID: %s, Availability: %s",
				elapsed, result.ClusterPeeringRow.OntapPeerUUID, clusterPeer.Availability)
			return result, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerNotAvailable,
				fmt.Errorf("cluster peer authentication state has been %s for %v (exceeded 10 minute limit)", clusterPeer.AuthenticationState, elapsed)))
		}

		// Still within 10 minute window, continue waiting
		logger.Warnf("Cluster peer authentication state is in problem state (waiting, elapsed: %v) - ClusterPeerUUID: %s, Availability: %s",
			elapsed, result.ClusterPeeringRow.OntapPeerUUID, clusterPeer.Availability)
		return result, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerTimeout,
			fmt.Errorf("cluster peer authentication state is %s, waiting for recovery (elapsed: %v)", clusterPeer.AuthenticationState, elapsed)))
	}

	// If authentication state is not problem, reset the first problem state time (in case it recovered)
	if result.FirstProblemStateTime != nil && clusterPeer.AuthenticationState != models.AuthenticationStateProblem {
		result.FirstProblemStateTime = nil
		logger.Debugf("Cluster peer authentication state recovered from problem - ClusterPeerUUID: %s, CurrentState: %s",
			result.ClusterPeeringRow.OntapPeerUUID, clusterPeer.AuthenticationState)
	}

	// If authentication state is absent, return non-retryable error (cannot recover)
	if clusterPeer.AuthenticationState == models.AuthenticationStateAbsent {
		result.ClusterPeer = clusterPeer
		return result, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerNotAvailable,
			fmt.Errorf("cluster peer authentication state is %s", clusterPeer.AuthenticationState)))
	}

	// If cluster peer is available and authentication is ok, return success
	if clusterPeer.Availability == models.AvailabilityAvailable && clusterPeer.AuthenticationState == models.AuthenticationStateOk {
		result.ClusterPeer = clusterPeer
		logger.Infof("Cluster peer is available and authenticated - ClusterPeerUUID: %s", result.ClusterPeeringRow.OntapPeerUUID)
		return result, nil
	}

	// Otherwise, continue waiting (retryable timeout)
	result.ClusterPeer = clusterPeer
	logger.Debugf("Cluster peer is not ready yet - ClusterPeerUUID: %s, Availability: %s, AuthenticationState: %s",
		result.ClusterPeeringRow.OntapPeerUUID, clusterPeer.Availability, clusterPeer.AuthenticationState)
	return result, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrClusterPeerTimeout,
		fmt.Errorf("cluster peer is not ready yet - Availability: %s, AuthenticationState: %s", clusterPeer.Availability, clusterPeer.AuthenticationState)))
}

func (a *HybridReplicationActivity) SetClusterPeeringStatusToPeeredForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("SetClusterPeeringStatusToPeeredForHybridReplication")
	se := a.SE

	// Update cluster peering record with ONTAP cluster peer details
	clusterPeering := result.ClusterPeeringRow
	if clusterPeering.ClusterPeeringAttributes != nil {
		clusterPeering.ClusterPeeringAttributes.PassPhrase = nil
		clusterPeering.ClusterPeeringAttributes.Command = nil
		clusterPeering.ClusterPeeringAttributes.ExpiryTime = nil
	}
	// Update state and status
	clusterPeering.State = models.CvpClusterPeeringStatusPEERED
	clusterPeering.StateDetails = ""
	// Update cluster peer in database
	err := se.UpdateClusterPeeringRow(ctx, clusterPeering)
	if err != nil {
		logger.Errorf("Failed to update cluster peering row: %v", err)
		return nil, err
	}
	// Update the cluster peer in the result
	result.ClusterPeeringRow = clusterPeering
	result, err = a.updateReplicationStateDetailsCode(ctx, result, models.DefaultCode)
	if err != nil {
		logger.Errorf("Failed to update cluster peering row: %v", err)
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	return result, nil
}

func (a *HybridReplicationActivity) SetVolumeReplicationPeeringStatusToPendingSVMPeering(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("SetVolumeReplicationPeeringStatusToPendingSVMPeering")
	se := a.SE
	volumeRep := result.DbVolReplication
	if volumeRep.HybridReplicationAttributes != nil {
		volumeRep.HybridReplicationAttributes.Status = models.HybridReplicationStatusPendingSVMPeer
		volumeRep.HybridReplicationAttributes.StatusDetails = models.InitiatingSVMPeering
		volumeRep.HybridReplicationAttributes.StateDetailsCode = models.InitiatingSVMPeeringCode
	}
	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep
	logger.Debugf("Volume Replication state: %s updated successfully in the db", volumeRep.Name)
	return result, nil
}

// createSVMPeerForHybridReplication is a helper function to create SVM peer for hybrid replication
func (a *HybridReplicationActivity) createSVMPeerForHybridReplication(ctx context.Context, provider vsa.Provider, result *replication.CreateHybridReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debug("createSVMPeerForHybridReplication")
	snapmirrorApplication := ontaprestmodels.SvmPeerApplicationsSnapmirror
	params := vsa.CreateSVMPeerParams{
		LocalSVMName:    result.DestinationVolume.Svm.Name,
		PeerSVMName:     result.HybridReplicationParameters.PeerSvmName,
		PeerClusterName: result.HybridReplicationParameters.PeerClusterName,
		Applications:    []ontaprestmodels.SvmPeerApplications{snapmirrorApplication},
	}
	_, err := provider.CreateSVMPeer(params)
	if err != nil {
		logger.Errorf("Failed to create SVM peer for hybrid replication: %v", err)
		return err
	}
	logger.Infof("Successfully created SVM peer for hybrid replication between %s and %s",
		result.DestinationVolume.Svm.Name, result.HybridReplicationParameters.PeerSvmName)
	return nil
}

// deleteSVMPeerForHybridReplication is a helper function to delete SVM peer for hybrid replication
func (a *HybridReplicationActivity) deleteSVMPeerForHybridReplication(ctx context.Context, provider vsa.Provider, svmPeerUUID string) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("deleteSVMPeerForHybridReplication")
	err := provider.DeleteSVMPeer(svmPeerUUID, false)
	if err != nil {
		logger.Errorf("Failed to delete SVM peer: %v", err)
		return err
	}
	logger.Infof("Successfully deleted SVM peer: %s", svmPeerUUID)
	return nil
}

// CreateSVMPeerInOntapForHybridReplication creates an SVM peer relationship for hybrid replication
func (a *HybridReplicationActivity) CreateSVMPeerInOntapForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Creating SVM peer for hybrid replication between local SVM: %s and peer SVM: %s in cluster: %s",
		result.DestinationVolume.Svm.Name, result.HybridReplicationParameters.PeerSvmName, result.HybridReplicationParameters.PeerClusterName)
	// Get the provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsTemporalApplicationError(err)
	}
	localSVMName := result.DestinationVolume.Svm.Name
	peerSVMName := result.HybridReplicationParameters.PeerSvmName

	// Step 1: Get SVM peer from ONTAP
	svmPeer, err := provider.GetSVMPeer(&localSVMName, &peerSVMName)
	if err != nil {
		// Step 2: If error is "not found", create SVM peer
		if customerrors.IsNotFoundErr(err) {
			logger.Debugf("SVM peer not found, creating new SVM peer")
			err = a.createSVMPeerForHybridReplication(ctx, provider, result)
			if err != nil {
				return nil, errors.WrapAsTemporalApplicationError(err)
			}
			return result, nil
		} else {
			// Step 2: If error is not "not found", return error
			logger.Errorf("Failed to get SVM peer information: %v", err)
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
		}
	}

	// Check if state is initializing or pending - these are acceptable states
	if svmPeer.State == ontaprestmodels.SvmPeerStateRejected || svmPeer.State == ontaprestmodels.SvmPeerStateSuspended {
		// Step 3: For any other state, delete the SVM peer first, then create new one
		logger.Warnf("SVM peer exists with unacceptable state (%s), deleting and recreating", svmPeer.State)
		// Delete existing SVM peer
		err = a.deleteSVMPeerForHybridReplication(ctx, provider, svmPeer.UUID)
		if err != nil {
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully deleted existing SVM peer, now creating new one")
		// Create new SVM peer
		err = a.createSVMPeerForHybridReplication(ctx, provider, result)
		if err != nil {
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
	}
	return result, nil
}

func (a *HybridReplicationActivity) SetVolumeReplicationSVMPeeringDetails(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("SetVolumeReplicationSVMPeeringDetails")
	se := a.SE
	volumeRep := result.DbVolReplication
	if volumeRep.HybridReplicationAttributes != nil {
		volumeRep.HybridReplicationAttributes.Status = models.HybridReplicationStatusPendingSVMPeer
		volumeRep.HybridReplicationAttributes.StatusDetails = models.WaitingForSVMPeering
		volumeRep.HybridReplicationAttributes.StateDetailsCode = models.WaitingForSVMPeeringCode
		volumeRep.HybridReplicationAttributes.SvmPeerCommand = nillable.ToPointer(
			fmt.Sprintf("vserver peer accept -vserver %s -peer-vserver %s", result.HybridReplicationParameters.PeerSvmName, result.DestinationVolume.Svm.Name),
		)
	}
	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep
	logger.Debugf("Volume Replication state: %s updated successfully in the db", volumeRep.Name)
	return result, nil
}

func (a *HybridReplicationActivity) SetSVMPeeringToPeered(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("SetVolumeReplicationSVMPeeringDetails")
	se := a.SE
	volumeRep := result.DbVolReplication
	if volumeRep.HybridReplicationAttributes != nil {
		volumeRep.HybridReplicationAttributes.Status = models.HybridReplicationStatusSVMPeered
		volumeRep.HybridReplicationAttributes.StatusDetails = ""
		volumeRep.HybridReplicationAttributes.StateDetailsCode = models.DefaultCode
	}
	err := se.UpdateVolumeReplication(ctx, volumeRep)
	if err != nil {
		return nil, err
	}
	result.DbVolReplication = volumeRep
	logger.Debugf("Volume Replication state: %s updated successfully in the db", volumeRep.Name)
	return result, nil
}

// WaitForSVMPeerForHybridReplication retrieves SVM peer information from ONTAP
func (a *HybridReplicationActivity) WaitForSVMPeerForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Getting SVM peer information for hybrid replication between local SVM: %s and peer SVM: %s",
		result.DestinationVolume.Svm.Name, result.HybridReplicationParameters.PeerSvmName)
	// Get the provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	// Call the GetSVMPeer method to retrieve SVM peer information
	localSVMName := result.DestinationVolume.Svm.Name
	peerSVMName := result.HybridReplicationParameters.PeerSvmName
	svmPeer, err := provider.GetSVMPeer(&localSVMName, &peerSVMName)
	if err != nil && !customerrors.IsNotFoundErr(err) {
		logger.Errorf("Failed to get SVM peer information: %v", err)
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}
	if svmPeer != nil {
		if svmPeer.State == ontaprestmodels.SvmPeerStatePeered {
			return result, nil
		} else if svmPeer.State == ontaprestmodels.SvmPeerStateInitializing || svmPeer.State == ontaprestmodels.SvmPeerStateInitiated || svmPeer.State == ontaprestmodels.SvmPeerStatePending {
			return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerTimeout, fmt.Errorf("SVM peer is not ready yet")))
		} else {
			err = a.deleteSVMPeerForHybridReplication(ctx, provider, svmPeer.UUID)
			if err != nil {
				return nil, errors.WrapAsTemporalApplicationError(err)
			}
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerNotAvailable, fmt.Errorf("svm peer state is %s", svmPeer.State)))
		}
	}
	// Todo handle svm peer delete from Ontap
	return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerTimeout, fmt.Errorf("svm peer not found")))
}

func (a *HybridReplicationActivity) GetReplicationForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("GetReplicationForHybridReplication")
	if result.DbVolReplication == nil || result.DbVolReplication.ReplicationAttributes.ExternalUUID == "" {
		logger.Debugf("DestinationReplicationUUID is empty, skipping replication retrieval")
		return result, nil
	}
	// Use Google Proxy client to get replication details
	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)
	params := &googleproxyclient.V1betaGetMultipleReplicationsInternalParams{
		ProjectNumber:  result.DestinationProjectNumber,
		LocationId:     result.DestinationRegion,
		XCorrelationID: googleproxyclient.NewOptString(*result.CorrelationID),
	}
	body := googleproxyclient.ReplicationIDListV1beta{ReplicationUUIDs: []string{result.DbVolReplication.UUID}}
	res, err := googleProxyClient.Invoker.V1betaGetMultipleReplicationsInternal(ctx, &body, *params)
	if err != nil {
		logger.Error("Failed to get multiple replications from Google Proxy", "error", err)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, err)
	}
	switch r := res.(type) {
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalOK:
		if len(r.Replications) > 0 {
			result.DstReplication = &r.Replications[0]
			logger.Debugf("Successfully retrieved replication: %s", r.Replications[0].VolumeReplicationUuid.Value)
		} else {
			logger.Debugf("No replications found in response")
		}
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalBadRequest:
		logger.Errorf("Bad request when getting replication: %s", r.Message)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsBadRequest, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalInternalServerError:
		logger.Errorf("Internal server error when getting replication: %s", r.Message)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsInternalServerError, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalUnauthorized:
		logger.Errorf("Unauthorized when getting replication: %s", r.Message)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsUnauthorized, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalForbidden:
		logger.Errorf("Forbidden when getting replication: %s", r.Message)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplicationsForbidden, errors.New(r.Message))
	case *googleproxyclient.V1betaGetMultipleReplicationsInternalNotFound:
		logger.Debugf("Replication not found, continuing without replication details")
		// Not found is not an error in this context, just continue
	default:
		logger.Errorf("Unknown response type when getting replication: %T", r)
		return nil, errors.NewVCPError(errors.ErrGoogleProxyInternalGetMultipleReplications, fmt.Errorf("unknown response type: %T", r))
	}
	return result, nil
}

func (a *HybridReplicationActivity) CleanupReplicationIfNeeded(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CleanupReplicationIfNeeded - checking if replication cleanup is needed")
	// Early return if no replication to cleanup
	if result.DstReplication == nil {
		logger.Debugf("No destination replication found, skipping cleanup")
		return result, nil
	}
	// Early return if replication is not in error state
	if !result.DstReplication.LifeCycleState.IsSet() || result.DstReplication.LifeCycleState.Value != googleproxyclient.VolumeReplicationInternalV1betaLifeCycleStateError {
		logger.Debugf("Replication is not in error state (%v), skipping cleanup", result.DstReplication.LifeCycleState)
		return result, nil
	}
	logger.Infof("Replication is in error state, proceeding with cleanup")
	// Get the provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}

	SkipPeeringCleanup := true
	replication := &vsa.VolumeReplication{
		EndpointType:          result.DbVolReplication.ReplicationAttributes.EndpointType,
		SourceHostName:        result.DbVolReplication.ReplicationAttributes.SourceHostName,
		SourceSVMName:         result.DbVolReplication.ReplicationAttributes.SourceSvmName,
		SourceVolumeName:      result.DbVolReplication.ReplicationAttributes.SourceVolumeName,
		DestinationHostName:   result.DbVolReplication.ReplicationAttributes.DestinationHostName,
		DestinationSVMName:    result.DbVolReplication.ReplicationAttributes.DestinationSvmName,
		ReplicationSchedule:   result.DbVolReplication.ReplicationAttributes.ReplicationSchedule,
		DestinationVolumeName: result.DbVolReplication.ReplicationAttributes.DestinationVolumeName,
		RelationshipID:        result.DbVolReplication.ReplicationAttributes.ExternalUUID,
		SkipPeeringCleanup:    &SkipPeeringCleanup,
	}

	vsaDeleteVolumeReplicationParams := &vsa.DeleteVolumeReplicationParams{
		VolumeReplication: replication,
		DestinationOnly:   &SkipPeeringCleanup,
	}
	// Delete replication from ONTAP
	deletedReplication, err := provider.DeleteVolumeReplication(vsaDeleteVolumeReplicationParams)
	if err != nil {
		if customerrors.IsConflictErr(err) {
			logger.Error("Failed to delete volume replication due to conflict", "error", err)
			return nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrProviderDeleteVolumeReplication, err))
		}
		logger.Error("Failed to delete volume replication from ONTAP", "error", err)
		return nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrProviderDeleteVolumeReplication, err))
	}
	logger.Infof("Successfully deleted replication from ONTAP: %s", deletedReplication.RelationshipID)
	// Clear the replication from result since it's been deleted
	result.DstReplication = nil
	return result, nil
}

func (a *HybridReplicationActivity) CreateHybridVolumeReplicationInternal(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateHybridVolumeReplicationInternal")
	if result.DstReplication != nil {
		return result, nil
	}
	// Get the provider for the node
	provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
	if err != nil {
		return nil, errors.WrapAsNonRetryableTemporalApplicationError(err)
	}

	vrf := vsa.VolumeReplication{
		EndpointType:          result.DbVolReplication.ReplicationAttributes.EndpointType,
		SourceHostName:        result.DbVolReplication.ReplicationAttributes.SourceHostName,
		SourceSVMName:         result.HybridReplicationParameters.PeerSvmName,
		SourceVolumeName:      result.DbVolReplication.ReplicationAttributes.SourceVolumeName,
		DestinationHostName:   result.DbVolReplication.ReplicationAttributes.DestinationHostName,
		DestinationSVMName:    result.DestinationVolume.Svm.Name,
		ReplicationSchedule:   result.DbVolReplication.HybridReplicationAttributes.ReplicationSchedule,
		ReplicationPolicy:     string(googleproxyclient.VolumeReplicationCreateInternalV1betaReplicationPolicyMirrorAllSnapshots),
		DestinationVolumeName: result.DbVolReplication.ReplicationAttributes.DestinationVolumeName,
		Volume: &vsa.Volume{
			ExternalUUID: result.DbVolReplication.Volume.VolumeAttributes.ExternalUUID,
		},
	}
	vsaCreateVolumeReplicationParams := vsa.CreateVolumeReplicationParams{
		VolumeReplication: &vrf,
		ReverseResync:     false,
	}
	replication, err := provider.CreateVolumeReplication(&vsaCreateVolumeReplicationParams)
	if err != nil {
		logger.Error("Failed to create volume replication", "error", err)
		return nil, err
	}
	result.ReplicationCreateResponseONTAP = replication
	return result, nil
}

func (a *HybridReplicationActivity) UpdateHybridVolumeReplicationDetailsAndSetPeeringStatusToPeered(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	se := a.SE
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateHybridVolumeReplicationDetailsAndSetPeeringStatus - updating replication details and setting peering status to peered")
	replication := result.DbVolReplication
	// Update replication details from ONTAP response
	replication.State = models.LifeCycleStateAvailable
	replication.StateDetails = models.LifeCycleStateAvailableDetails
	if result.ReplicationCreateResponseONTAP != nil {
		replication.ReplicationAttributes.ExternalUUID = result.ReplicationCreateResponseONTAP.RelationshipID
		replication.ReplicationAttributes.ReplicationSchedule = result.ReplicationCreateResponseONTAP.ReplicationSchedule
		replication.MirrorState = &result.ReplicationCreateResponseONTAP.MirrorState
		replication.RelationshipStatus = &result.ReplicationCreateResponseONTAP.RelationshipStatus
		replication.TotalTransferBytes = result.ReplicationCreateResponseONTAP.TotalTransferBytes
		replication.TotalTransferTimeSecs = result.ReplicationCreateResponseONTAP.TotalTransferTimeSecs
		replication.LastTransferSize = int64(result.ReplicationCreateResponseONTAP.LastTransferSize)
		replication.LastTransferError = result.ReplicationCreateResponseONTAP.LastTransferError
		replication.LastTransferDuration = result.ReplicationCreateResponseONTAP.LastTransferDuration
		replication.LastTransferEndTime = result.ReplicationCreateResponseONTAP.LastTransferEndTime
		replication.LagTime = result.ReplicationCreateResponseONTAP.LagTime
	}
	replication.LastUpdatedFromOntap = time.Now()
	replication.ProgressLastUpdated = &replication.LastUpdatedFromOntap
	// Set hybrid replication peering status to peered
	if replication.HybridReplicationAttributes != nil {
		replication.HybridReplicationAttributes.StateDetailsCode = models.DefaultCode
		replication.HybridReplicationAttributes.Status = models.HybridReplicationStatusPeered
		replication.HybridReplicationAttributes.StatusDetails = ""
		replication.HybridReplicationAttributes.SvmPeerExpiryTime = nil
		replication.HybridReplicationAttributes.SvmPeerCommand = nil
		replication.HybridReplicationAttributes.Description = ""
		replication.HybridReplicationAttributes.Labels = nil
	}
	// Update the replication in the database
	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		logger.Errorf("Failed to update volume replication: %v", err)
		return nil, err
	}
	result.DbVolReplication = replication
	logger.Debugf("Volume Replication state: %s updated successfully in the db with peering status set to peered", replication.Name)
	return result, nil
}

func (a *HybridReplicationActivity) UpdateClusterPeeringInReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	se := a.SE
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateHybridVolumeReplicationDetailsAndSetPeeringStatus - updating replication details and setting peering status to peered")
	replication := result.DbVolReplication
	// Get cluster peer ID
	var clusterPeerID sql.NullInt64
	if result.ClusterPeeringRow != nil && result.ClusterPeeringRow.ID > 0 {
		clusterPeerID = sql.NullInt64{Int64: result.ClusterPeeringRow.ID, Valid: true}
		logger.Debugf("Using cluster peer ID: %d", result.ClusterPeeringRow.ID)
	} else {
		clusterPeerID = sql.NullInt64{Valid: false}
		logger.Debugf("No cluster peer found in result, setting ClusterPeerId as NULL")
	}
	replication.ClusterPeerId = clusterPeerID
	// Update the replication in the database
	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		logger.Errorf("Failed to update volume replication: %v", err)
		return nil, err
	}
	result.DbVolReplication = replication
	logger.Debugf("Volume Replication state: %s updated successfully in the db with peering status set to peered", replication.Name)
	return result, nil
}

// HydrateVolumeReplicationForHybridReplication hydrates the volume replication with the specified parameters
func (a *HybridReplicationActivity) HydrateVolumeReplicationForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	if hydrationEnabled {
		logger := util.GetLogger(ctx)
		logger.Debugf("Hydrating volume replication for hybrid replication: %s", result.HybridReplicationParameters.ResourceID)
		var replicationType models.HybridReplicationParametersReplicationType
		if result.DbVolReplication.HybridReplicationAttributes != nil {
			if *(result.DbVolReplication.HybridReplicationAttributes.HybridReplicationType) == string(models.HybridReplicationParametersReplicationTypeONPREM) {
				replicationType = models.HybridReplicationParametersReplicationTypeONPREM
			} else {
				replicationType = models.HybridReplicationParametersReplicationTypeMIGRATION
			}
		}
		// Convert the database volume replication to models.VolumeReplication for hydration
		volumeReplication := models.VolumeReplication{
			Name:  result.DbVolReplication.Name,
			State: string(models.VolumeReplicationHydrateStatePendingClusterPeering),
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationRegion:     result.DbVolReplication.ReplicationAttributes.DestinationLocation,
				DestinationVolumeName: result.DbVolReplication.ReplicationAttributes.DestinationVolumeName,
			},
			HybridReplicationAttributes: &models.HybridReplicationParameters{
				ReplicationType: replicationType,
				Labels:          result.DbVolReplication.HybridReplicationAttributes.Labels,
			},
		}
		// Call HydrateVolumeReplication with the specified parameters
		err := HydrateVolumeReplication(ctx, volumeReplication, result.DestinationProjectNumber)
		if err != nil {
			logger.Errorf("Failed to hydrate volume replication: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully hydrated volume replication: %s", result.HybridReplicationParameters.ResourceID)
	}
	return result, nil
}

func (a *HybridReplicationActivity) isSameAsCurrentHydrateState(result *replication.CreateHybridReplicationResult, state models.VolumeReplicationHydrateState) bool {
	return result.CurrentHydrateState == state
}

func (a *HybridReplicationActivity) setCurrentReplicationHydrateState(result *replication.CreateHybridReplicationResult, state models.VolumeReplicationHydrateState) *replication.CreateHybridReplicationResult {
	result.CurrentHydrateState = state
	return result
}

func (a *HybridReplicationActivity) updateReplicationStateDetailsCode(ctx context.Context, result *replication.CreateHybridReplicationResult, stateDetailsCode int32) (*replication.CreateHybridReplicationResult, error) {
	se := a.SE
	replication := result.DbVolReplication
	replication.HybridReplicationAttributes.StateDetailsCode = stateDetailsCode
	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		return nil, err
	}
	result.DbVolReplication = replication
	return result, nil
}

func (a *HybridReplicationActivity) HydrateReplicationStateForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	if hydrationEnabled {
		logger := util.GetLogger(ctx)
		logger.Debug("HydrateReplicationStateForHybridReplication")
		if result.ClusterPeer.Availability != models.AvailabilityAvailable {
			if !a.isSameAsCurrentHydrateState(result, models.VolumeReplicationHydrateStatePendingClusterPeering) {
				a.setCurrentReplicationHydrateState(result, models.VolumeReplicationHydrateStatePendingClusterPeering)
				logger.Debugf("hydrating to %s", models.VolumeReplicationHydrateStatePendingClusterPeering)
				return a.HydrateLocalReplicationSateForHybridReplication(ctx, result, models.VolumeReplicationHydrateStatePendingClusterPeering)
			}
		} else if result.DbVolReplication != nil && result.DbVolReplication.State == models.LifeCycleStateAvailable {
			if !a.isSameAsCurrentHydrateState(result, models.VolumeReplicationHydrateStateReady) {
				a.setCurrentReplicationHydrateState(result, models.VolumeReplicationHydrateStateReady)
				logger.Debugf("hydrating to %s", models.VolumeReplicationHydrateStateReady)
				return a.HydrateLocalReplicationSateForHybridReplication(ctx, result, models.VolumeReplicationHydrateStateReady)
			}
		} else if !a.isSameAsCurrentHydrateState(result, models.VolumeReplicationHydrateStatePendingSvmPeering) {
			a.setCurrentReplicationHydrateState(result, models.VolumeReplicationHydrateStatePendingSvmPeering)
			logger.Debugf("hydrating to %s", models.VolumeReplicationHydrateStatePendingSvmPeering)
			return a.HydrateLocalReplicationSateForHybridReplication(ctx, result, models.VolumeReplicationHydrateStatePendingSvmPeering)
		}
	}
	return result, nil
}

func (a *HybridReplicationActivity) HydrateLocalReplicationSateForHybridReplication(ctx context.Context, result *replication.CreateHybridReplicationResult, state models.VolumeReplicationHydrateState) (*replication.CreateHybridReplicationResult, error) {
	if hydrationEnabled {
		logger := util.GetLogger(ctx)
		logger.Debugf("Hydrating volume replication for hybrid replication: %s", result.HybridReplicationParameters.ResourceID)
		// Convert the database volume replication to models.VolumeReplication for hydration
		ss := models.VolumeReplication{
			Name: result.DbVolReplication.Name,
			ReplicationAttributes: &models.ReplicationDetails{
				DestinationRegion:     result.DbVolReplication.ReplicationAttributes.DestinationLocation,
				DestinationVolumeName: result.DbVolReplication.ReplicationAttributes.DestinationVolumeName,
			},
		}
		// Call HydrateVolumeReplication with the specified parameters
		err := hydrateReplicationStateForHybrid(ctx, ss, state, result.DestinationProjectNumber)
		if err != nil {
			logger.Errorf("Failed to hydrate volume replication: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully hydrated volume replication: %s", result.HybridReplicationParameters.ResourceID)
	}
	return result, nil
}

// UpdateSVMPeerOnErrorActivity gets the SVM peer and deletes it from ONTAP
func (a *HybridReplicationActivity) UpdateSVMPeerOnErrorActivity(ctx context.Context, result *replication.CreateHybridReplicationResult) error {
	if result.DbVolReplication.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer {
		logger := util.GetLogger(ctx)
		logger.Debugf("GetAndDeleteSVMPeerForHybridReplication - Getting SVM peer between local SVM: %s and peer SVM: %s",
			result.DestinationVolume.Svm.Name, result.HybridReplicationParameters.PeerSvmName)
		// Get the provider for the node
		provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
		if err != nil {
			logger.Errorf("Failed to get provider for SVM peer deletion: %v", err)
			return errors.WrapAsNonRetryableTemporalApplicationError(err)
		}
		// Get SVM peer information
		localSVMName := result.DestinationVolume.Svm.Name
		peerSVMName := result.HybridReplicationParameters.PeerSvmName
		svmPeer, err := provider.GetSVMPeer(&localSVMName, &peerSVMName)
		if err != nil {
			if customerrors.IsNotFoundErr(err) {
				logger.Debugf("SVM peer not found between %s and %s, nothing to delete", localSVMName, peerSVMName)
				return nil
			}
			logger.Errorf("Failed to get SVM peer information: %v", err)
			return errors.WrapAsNonRetryableTemporalApplicationError(err)
		}
		if svmPeer == nil {
			logger.Debugf("SVM peer is nil, nothing to delete")
			return nil
		}
		logger.Infof("Found SVM peer with UUID: %s, state: %s", svmPeer.UUID, svmPeer.State)
		// Delete the SVM peer from ONTAP
		err = provider.DeleteSVMPeer(svmPeer.UUID, false)
		if err != nil {
			logger.Errorf("Failed to delete SVM peer from ONTAP: %v", err)
			return errors.WrapAsTemporalApplicationError(err)
		}
		logger.Infof("Successfully deleted SVM peer from ONTAP: %s", svmPeer.UUID)
	}
	return nil
}

func (a *HybridReplicationActivity) UpdateClusterPeerDetailsOnErrorActivity(ctx context.Context, result *replication.CreateHybridReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateClusterPeerDetailsOnErrorActivity")
	se := a.SE
	// Update cluster peering record with ONTAP cluster peer details

	clusterPeering := result.ClusterPeeringRow
	if clusterPeering.State == models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING || clusterPeering.State == models.CvpClusterPeeringStatusCREATING {
		if clusterPeering.ClusterPeeringAttributes != nil {
			clusterPeering.ClusterPeeringAttributes.PassPhrase = nil
			clusterPeering.ClusterPeeringAttributes.Command = nil
			clusterPeering.ClusterPeeringAttributes.ExpiryTime = nil
		}
		// Update state and status
		clusterPeering.State = models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING
		// Delete cluster peer from ONTAP if it exists
		if clusterPeering.OntapPeerUUID != "" {
			logger.Infof("Attempting to delete cluster peer from ONTAP: %s", clusterPeering.OntapPeerUUID)
			// Get the provider from the node
			provider, err := hyperscaler.GetProviderByNode(ctx, result.NodeProvider)
			if err != nil {
				logger.Errorf("Failed to get provider for cluster peer deletion: %v", err)
				// Continue with database update even if provider retrieval fails
			} else {
				// Delete cluster peer from ONTAP
				err = provider.DeleteClusterPeer(clusterPeering.OntapPeerUUID)
				if err != nil {
					logger.Errorf("Failed to delete cluster peer from ONTAP: %v", err)
					// Continue with database update even if ONTAP deletion fails
				} else {
					logger.Infof("Successfully deleted cluster peer from ONTAP: %s", clusterPeering.OntapPeerUUID)
				}
			}
			// Clear the cluster peer UUID since we're deleting it
			clusterPeering.OntapPeerUUID = ""
		}
		// Set cluster peer row for deletion (soft delete)
		logger.Infof("Marking cluster peer row for deletion: %d", clusterPeering.ID)
		clusterPeering.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		clusterPeering.UpdatedAt = clusterPeering.DeletedAt.Time
		clusterPeering.State = models.LifeCycleStateDeleted
		clusterPeering.StateDetails = models.LifeCycleStateDeletedDetails
		// Update cluster peer in database (this will save the deletion state)
		err := se.UpdateClusterPeeringRow(ctx, clusterPeering)
		if err != nil {
			logger.Errorf("Failed to mark cluster peer row as deleted: %v", err)
			return errors.NewVCPError(errors.ErrDatabaseDataUpdateError, err)
		}
		logger.Infof("Successfully marked cluster peer row as deleted: %d", clusterPeering.ID)
	}
	return nil
}

func (a *HybridReplicationActivity) UpdateReplicationRowDetailsOnErrorActivity(ctx context.Context, result *replication.CreateHybridReplicationResult) error {
	logger := util.GetLogger(ctx)
	logger.Debugf("UpdateClusterPeerDetailsOnErrorActivity")
	se := a.SE
	replication := result.DbVolReplication
	// Update replication details from ONTAP response
	oldState := replication.HybridReplicationAttributes.Status

	replication.HybridReplicationAttributes.Status = models.HybridReplicationStatusPendingClusterPeer
	replication.State = models.LifeCycleStateError
	replication.StateDetails = models.LifeCycleStateCreationErrorDetails

	if oldState == models.HybridReplicationStatusSVMPeered {
		replication.HybridReplicationAttributes.Status = models.HybridReplicationStatusSVMPeered
	} else if result.ClusterPeeringRow.State == models.CvpClusterPeeringStatusPEERED && oldState == models.HybridReplicationStatusPendingSVMPeer {
		replication.HybridReplicationAttributes.Status = models.HybridReplicationStatusPendingSVMPeer
	}
	if replication.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingClusterPeer || replication.HybridReplicationAttributes.Status == models.HybridReplicationStatusPendingSVMPeer {
		// Mark replication record as deleted
		replication.State = models.LifeCycleStateDeleted
		replication.StateDetails = models.LifeCycleStateDeletedDetails
		replication.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
		logger.Infof("Marking replication record as deleted: %d", replication.ID)
	}

	// Set hybrid replication peering status to peered
	if replication.HybridReplicationAttributes != nil {
		replication.HybridReplicationAttributes.SvmPeerExpiryTime = nil
		replication.HybridReplicationAttributes.SvmPeerCommand = nil
	}
	// Always set cluster peer row ID to null in replication row
	logger.Infof("Setting ClusterPeerId to null for replication: %d", replication.ID)
	replication.ClusterPeerId = sql.NullInt64{Int64: 0, Valid: false}
	// Update the replication in the database
	if err := se.UpdateVolumeReplication(ctx, replication); err != nil {
		logger.Errorf("Failed to update volume replication: %v", err)
		return err
	}
	return nil
}

func (a *HybridReplicationActivity) MountReplicationAfterHybridReplicationCreate(ctx context.Context, result *replication.CreateHybridReplicationResult) (*replication.CreateHybridReplicationResult, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("MountReplicationAfterResume")

	googleProxyClient := googleproxyclient.GetGProxyClient(*result.DstBasePath, *result.DstJwtToken, logger)

	mountVolumeParams := &googleproxyclient.V1betaInternalMountVolumeReplicationParams{
		ProjectNumber:       result.DestinationProjectNumber,
		LocationId:          result.DbVolReplication.ReplicationAttributes.DestinationLocation,
		VolumeReplicationId: result.DbVolReplication.ReplicationAttributes.DestinationReplicationUUID,
		XCorrelationID:      googleproxyclient.NewOptString(*result.CorrelationID),
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
