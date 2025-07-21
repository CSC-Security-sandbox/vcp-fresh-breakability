package workflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeCreateWorkflow struct {
	BaseWorkflow
}

var (
	runningEnv = env.GetString("ENV", "")
)

// Volume provisioning phases
const (
	PhasePre  = "pre"  // Pre-provisioning phase
	PhasePost = "post" // Post-provisioning phase
)

// Enforcing the WorkflowInterface on volumeCreateWorkflow
var _ WorkflowInterface = &volumeCreateWorkflow{}

// PreVolumeProvisioningParams encapsulates parameters for pre-provisioning hooks
type PreVolumeProvisioningParams struct {
	Ctx      workflow.Context
	DBVolume *datamodel.Volume
	Node     *models.Node
}

// PostVolumeProvisioningParams encapsulates parameters for post-provisioning hooks
type PostVolumeProvisioningParams struct {
	Ctx                 workflow.Context
	DBVolume            *datamodel.Volume
	Node                *models.Node
	VolCreateResponse   *vsa.VolumeResponse
	IsRestoreFromBackup bool
}

// selectVolumeChildWorkflow selects the appropriate child workflow based on volume characteristics.
// Currently implements protocol-based selection (ISCSI for block, NFSv3/NFSv4/SMB for file protocols).
// This function is designed to be extensible for future volume attributes beyond protocols
// (e.g. performance tier, large volume, etc.).
//
// Parameters:
//   - protocols: Slice of protocol strings to determine workflow type
//   - phase: Provisioning phase (use PhasePre or PhasePost constants)
func selectVolumeChildWorkflow(protocols []string, phase string) (interface{}, error) {
	if utils.IsSanProtocols(protocols) {
		switch phase {
		case PhasePre:
			return PreBlockVolumeWorkflow, nil
		case PhasePost:
			return PostBlockVolumeWorkflow, nil
		default:
			return nil, fmt.Errorf("invalid phase: %s", phase)
		}
	}
	if utils.IsNasProtocols(protocols) {
		if !utils.FileProtocolSupported {
			return nil, fmt.Errorf("file protocols are not enabled")
		}
		switch phase {
		case PhasePre:
			return PreFileVolumeWorkflow, nil
		case PhasePost:
			return PostFileVolumeWorkflow, nil
		default:
			return nil, fmt.Errorf("invalid phase: %s", phase)
		}
	}
	return nil, fmt.Errorf("unsupported or unspecified protocol: %v", protocols)
}

// PreBlockVolumeWorkflow handles pre-provisioning for block volumes
func PreBlockVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node) (*datamodel.Volume, error) {
	// Additional pre-provisioning steps for block volumes if needed
	return dbVolume, nil
}

// PostBlockVolumeWorkflow handles post-provisioning for block volumes
func PostBlockVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node, volCreateResponse *vsa.VolumeResponse, isRestoreFromBackup bool) (*datamodel.Volume, error) {
	volumeActivity := &activities.VolumeCreateActivity{}
	var err error
	var hostGroups []*datamodel.HostGroup

	// Configure activity options for child workflow
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Get host groups for block volume
	err = workflow.ExecuteActivity(ctx, volumeActivity.GetHosts, &dbVolume).Get(ctx, &hostGroups)
	if err != nil {
		return nil, err
	}

	hostParams := createHostParamsFromHostGroups(hostGroups, dbVolume)

	// Create igroup for block volume
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateIgroup, &dbVolume, &hostParams, node).Get(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create igroup: %w", err)
	}

	// Create LUN for block volume
	var lun *vsa.LunResponse

	if isRestoreFromBackup {
		err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateLunName, &dbVolume, &node, volCreateResponse.AvailableSpace).Get(ctx, &lun)
		if err != nil {
			return nil, err
		}
	} else {
		err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLun, &dbVolume, &node, volCreateResponse.AvailableSpace).Get(ctx, &lun)
		if err != nil {
			return nil, err
		}
	}

	// Create LUN map for block volume
	lunMapParams := createLunMapParams(lun.Name, dbVolume.Svm.Name, hostParams)
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLunMap, &dbVolume, &lunMapParams, node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	dbVolume.VolumeAttributes.BlockProperties.LunName = utils.ExtractLunNameFromPath(lun.Name)
	dbVolume.VolumeAttributes.BlockProperties.LunSerialNumber = lun.SerialNumber
	dbVolume.VolumeAttributes.BlockProperties.LunUUID = lun.ExternalUUID
	return dbVolume, nil
}

// PreFileVolumeWorkflow handles pre-provisioning for file volumes
func PreFileVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node) (*datamodel.Volume, error) {
	// Configure activity options for child workflow
	volumeActivity := &activities.VolumeCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateExportPolicyInOntap, &dbVolume, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	log := util.GetLogger(ctx)
	log.Info("File pre-provisioning: create export policy, etc. (placeholder)")
	return dbVolume, nil
}

// PostFileVolumeWorkflow handles post-provisioning for file volumes
func PostFileVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node, volCreateResponse *vsa.VolumeResponse, isRestoreFromBackup bool) (*datamodel.Volume, error) {
	// Configure activity options for child workflow
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	log := util.GetLogger(ctx)
	log.Info("File post-provisioning: anything after volume create. (placeholder)")
	return dbVolume, nil
}

// CreateVolumeWorkflow Volume Workflow process volume related requests from a customer.
func CreateVolumeWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume, backupVault *datamodel.BackupVault, backup *datamodel.Backup) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	volumeWf := new(volumeCreateWorkflow)
	err := volumeWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = volumeWf.Run(ctx, volume, params, backupVault, backup)
	if err != nil {
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			return nil, err2
		}
		return nil, err
	}
	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *volumeCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreateVolumeParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *volumeCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	createVolumeParams := args[1].(*common.CreateVolumeParams)
	region := createVolumeParams.Region
	var snapshot *datamodel.Snapshot
	if createVolumeParams.Snapshot != nil {
		snapshot = createVolumeParams.Snapshot
	}
	backupVault := args[2].(*datamodel.BackupVault)
	backup := args[3].(*datamodel.Backup)
	isRestoreFromBackup := createVolumeParams.BackupPath != "" && backup != nil
	volumeActivity := &activities.VolumeCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeStateInDB, dbVolume.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	// Pre-provisioning child workflow
	preWorkflowFunc, err := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePre)
	if err != nil {
		return nil, err
	}
	var preUpdatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, preWorkflowFunc, dbVolume, node).Get(ctx, &preUpdatedVolume)
	if err != nil {
		return nil, err
	}

	// Update the dbVolume with any changes from the pre-workflow
	if preUpdatedVolume != nil {
		dbVolume = preUpdatedVolume
	}
	rollbackManager.AddActivity(activities.VolumeDeleteActivity.DeleteSnapshotPolicyInONTAP, getSnapshotPolicyName(dbVolume), &node) // This will delete the snapshotPolicy if exists
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateSnapshotPolicyInONTAP, &dbVolume, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var volCreateResponse *vsa.VolumeResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateVolumeInONTAP, &dbVolume, &node, &snapshot, backup).Get(ctx, &volCreateResponse)
	if err != nil {
		return nil, err
	}
	rollbackManager.AddActivity(activities.VolumeDeleteActivity.DeleteVolumeInONTAP, volCreateResponse.ExternalUUID, dbVolume.Name, &node) // This will delete the lunMap & lun if exists

	// If isRestoreFromBackup is true, we will restore the volume from the backup
	// backup path example: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"
	if isRestoreFromBackup {
		objStore := &common.CloudTarget{}
		smDestinationPath := getSmSourcePath(dbVolume)
		smSourcePath, err := getSmSourcePathForRestore(backupVault, backup)
		log.Debugf("\nsmDestinationPath: %v", smDestinationPath)
		log.Debugf("\nsmSourcePath: %v", smSourcePath)

		if err != nil {
			return nil, err
		}

		snapmirrorRelationship := &common.SnapmirrorRelationship{}
		SnapmirrorRelationshipParams := &common.SnapmirrorRelationshipParams{
			SourcePath:      smSourcePath,
			DestinationPath: smDestinationPath,
			SourceUUID:      &backup.Attributes.EndpointUUID,
			IsRestore:       true,
		}

		objStoreName, err := getObjStoreNameFromBackup(backupVault, backup)
		if err != nil {
			return nil, err
		}

		bucketDetails, err := getBucketDetailsFromBackup(backupVault, backup)
		if err != nil {
			return nil, err
		}
		bucketName := bucketDetails.BucketName
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.GetOrCreateObjectStore, node, objStoreName, bucketName).Get(ctx, &objStore)
		if err != nil {
			return nil, err
		}
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.SnapmirrorGetorCreate, node, &SnapmirrorRelationshipParams).Get(ctx, &snapmirrorRelationship)
		if err != nil {
			return nil, err
		}
		err = workflow.ExecuteActivity(ctx, activities.BackupActivity.SnapmirrorTransfer, node, snapmirrorRelationship.UUID, "").Get(ctx, nil)
		if err != nil {
			return nil, err
		}
		// TODO: Make this a backukground job
		done := false
		var status string
		for !done {
			err = workflow.ExecuteActivity(ctx, activities.BackupActivity.GetSnapmirrorTransferStatus, node, snapmirrorRelationship.UUID, "").Get(ctx, &status)
			if err != nil {
				return nil, err
			}
			switch status {
			case activities.SmStatusTransferring:
				err := workflow.Sleep(ctx, wait) // Wait before polling again
				if err != nil {
					return nil, fmt.Errorf("failed to sleep during snapmirror transfer polling: %w", err)
				}
			case activities.SmStatusSuccess:
				done = true
			case activities.SmStatusFailed:
				return nil, fmt.Errorf("snapmirror transfer failed for restore with status: %s", status)
			}
		}
	}

	// Post-provisioning child workflow
	postWorkflowFunc, err := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePost)
	if err != nil {
		return nil, err
	}
	var updatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, postWorkflowFunc, dbVolume, node, volCreateResponse, isRestoreFromBackup).Get(ctx, &updatedVolume)
	if err != nil {
		return nil, err
	}

	// Update the dbVolume with the changes from the child workflow
	if updatedVolume != nil {
		dbVolume = updatedVolume
	}

	dbVolume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID
	err = workflow.ExecuteActivity(ctx, volumeActivity.InitiateSplitForVolume, &dbVolume, &node, &snapshot).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupVaultID != "" {
		if runningEnv != "local" {
			var token string
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, createVolumeParams.AccountName).Get(ctx, &token)
			if err != nil {
				log.Errorf("Failed to get token for account %s: %v", createVolumeParams.AccountName, err)
				return nil, err
			}
			ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, token)
		}

		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.FindTenancy, dbVolume.VolumeAttributes.VendorSubnetID, dbVolume.Account.Name, &region).Get(ctx, &tenancyDetails)
		if err != nil {
			return nil, err
		}

		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckBackupVaultExistsInVCP, &dbVolume, &region).Get(ctx, nil)
		if err != nil {
			return nil, err
		}

		bucketDetails := &common.BucketDetails{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckForBucketResourceName, &dbVolume).Get(ctx, &bucketDetails)
		if err != nil {
			return nil, err
		}
		if bucketDetails.BucketName == "" && bucketDetails.ServiceAccountName == "" && bucketDetails.TenantProjectNumber == "" {
			resourceName := &common.ResourceNames{}
			err = workflow.ExecuteActivity(ctx, volumeActivity.GenerateResourceNames, &dbVolume, &tenancyDetails, region).Get(ctx, &resourceName)
			if err != nil {
				return nil, err
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBucket, &resourceName, &tenancyDetails, region).Get(ctx, &bucketDetails)
			if err != nil {
				return nil, err
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateBackupVaultWithBucketDetails, &dbVolume, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupPolicyID != "" {
		var backupPolicyExists bool
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckIfBackupPolicyExistsInVCP, dbVolume.DataProtection.BackupPolicyID, dbVolume.AccountID).Get(ctx, &backupPolicyExists)
		if err != nil {
			return nil, err
		}

		if !backupPolicyExists {
			var vcpBackupPolicy *datamodel.BackupPolicy
			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBackupPolicyFetchedFromSDE, &dbVolume, region).Get(ctx, &vcpBackupPolicy)
			if err != nil {
				return nil, err
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeDetails, &dbVolume, &volCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func createHostParamsFromHostGroups(hostGroups []*datamodel.HostGroup, volume *datamodel.Volume) []*common.HostParams {
	var hostParamsArray []*common.HostParams

	for _, hostGroup := range hostGroups {
		hostParams := &common.HostParams{
			HostName: hostGroup.Name,
			HostIQNs: hostGroup.Hosts.Hosts,
			OsType:   volume.VolumeAttributes.BlockProperties.OSType,
		}
		hostParamsArray = append(hostParamsArray, hostParams)
	}

	return hostParamsArray
}

func createLunMapParams(lunName string, svmName string, hostParams []*common.HostParams) *common.CreateLunMapParams {
	var hostNames []string

	for _, hostParam := range hostParams {
		hostNames = append(hostNames, hostParam.HostName)
	}

	lunMapParam := &common.CreateLunMapParams{
		LunName:   lunName,
		SvmName:   svmName,
		HostNames: hostNames,
	}

	return lunMapParam
}
