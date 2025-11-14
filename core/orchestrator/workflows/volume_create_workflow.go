package workflows

import (
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	thinCloneGASupport = env.GetBool("THIN_CLONE_GA_SUPPORT", false)
)

type volumeCreateWorkflow struct {
	BaseWorkflow
}

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
	IsRestoreSnapshot   bool
}

// selectVolumeChildWorkflow selects the appropriate child workflow based on volume characteristics.
// Currently implements protocol-based selection (ISCSI for block, NFSv3/NFSv4/SMB for file protocols).
// This function is designed to be extensible for future volume attributes beyond protocols
// (e.g. performance tier, large volume, etc.).
//
// Parameters:
//   - protocols: Slice of protocol strings to determine workflow type
//   - phase: Provisioning phase (use PhasePre or PhasePost constants)
func selectVolumeChildWorkflow(protocols []string, phase, accountName string) (interface{}, error) {
	if utils.IsSanProtocols(protocols) {
		switch phase {
		case PhasePre:
			return PreBlockVolumeWorkflow, nil
		case PhasePost:
			return PostBlockVolumeWorkflow, nil
		default:
			return nil, ConvertToVSAError(fmt.Errorf("invalid phase: %s", phase))
		}
	}
	if utils.IsNasProtocols(protocols) {
		if !utils.IsFileProtocolSupported(accountName) {
			return nil, ConvertToVSAError(fmt.Errorf("file protocols are not enabled"))
		}
		switch phase {
		case PhasePre:
			return PreFileVolumeWorkflow, nil
		case PhasePost:
			if utils.IsSMBProtocols(protocols) {
				return PostFileVolumeWorkflowForSMB, nil
			}
			return PostFileVolumeWorkflow, nil
		default:
			return nil, ConvertToVSAError(fmt.Errorf("invalid phase: %s", phase))
		}
	}
	return nil, ConvertToVSAError(fmt.Errorf("unsupported or unspecified protocol: %v", protocols))
}

// PreBlockVolumeWorkflow handles pre-provisioning for block volumes
func PreBlockVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node) (*datamodel.Volume, error) {
	// Additional pre-provisioning steps for block volumes if needed
	return dbVolume, nil
}

// PostBlockVolumeWorkflow handles post-provisioning for block volumes
func PostBlockVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse, isRestoreFromBackup bool, isRestoreSnapshot bool, restoreVolCreateResponse *vsa.VolumeResponse) (*datamodel.Volume, error) {
	volumeActivity := &activities.VolumeCreateActivity{}
	var err error

	// Configure activity options for child workflow
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Create LUN for block volume
	var lun *vsa.LunResponse

	if isRestoreSnapshot {
		err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateLunName, &dbVolume, &node, restoreVolCreateResponse).Get(ctx, &lun)
		if err != nil {
			return nil, err
		}
	} else if isRestoreFromBackup {
		err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateLunName, &dbVolume, &node, volCreateResponse).Get(ctx, &lun)
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

	if dbVolume.VolumeAttributes.BlockDevices != nil && len(*dbVolume.VolumeAttributes.BlockDevices) > 0 {
		blockDevices := *dbVolume.VolumeAttributes.BlockDevices
		for i := range blockDevices {
			blockDevices[i].Name = utils.ExtractLunNameFromPath(lun.Name)
			blockDevices[i].Identifier = lun.SerialNumber
			blockDevices[i].Size = lun.Size
			blockDevices[i].LunUUID = lun.ExternalUUID
		}
		// Update the slice back to the volume attributes
		dbVolume.VolumeAttributes.BlockDevices = &blockDevices
	} else if dbVolume.VolumeAttributes.BlockProperties != nil {
		dbVolume.VolumeAttributes.BlockProperties.LunName = utils.ExtractLunNameFromPath(lun.Name)
		dbVolume.VolumeAttributes.BlockProperties.LunSerialNumber = lun.SerialNumber
		dbVolume.VolumeAttributes.BlockProperties.LunUUID = lun.ExternalUUID
	}

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
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
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

// PostFileVolumeWorkflowForSMB is a Cadence workflow that handles SMB-specific post-provisioning tasks for a volume.
// It configures activity options, retrieves SVM and Active Directory information, and ensures CIFS share creation.
// The workflow also updates firewall rules and Active Directory association if necessary. Returns the updated volume.
func PostFileVolumeWorkflowForSMB(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node) (*datamodel.Volume, error) {
	// Configure activity options
	log := util.GetLogger(ctx)
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		log.Error("Failed to populate retry policy params during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, err
	}

	ao := getActivityOptionsForEnsureCIFSShareVolumeActivity(retryPolicy)
	ctx = workflow.WithActivityOptions(ctx, ao)

	var dbSvm *datamodel.Svm
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetSVM, &dbVolume.PoolID).Get(ctx, &dbSvm)
	if err != nil {
		log.Error("Failed to get SVM info during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	var activeDirectory *vsa.ActiveDirectory
	err = workflow.ExecuteActivity(ctx, active_directory_activities.ActiveDirectoryActivity.GetActiveDirectoryForPool, &dbVolume.PoolID).Get(ctx, &activeDirectory)
	if err != nil {
		log.Error("Failed to get active directory during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	if dbVolume.Pool == nil {
		err = fmt.Errorf("pool details not loaded for volume %s", dbVolume.UUID)
		log.Error("Pool details missing during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	poolClusterDetails := dbVolume.Pool.ClusterDetails
	if poolClusterDetails.SnHostProject == "" || poolClusterDetails.Network == "" {
		err = fmt.Errorf("pool %s missing SN host project or network details", dbVolume.Pool.UUID)
		log.Error("Pool network metadata missing during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	firewallParams := activities.CreateFirewallRuleParams{
		Project:          poolClusterDetails.SnHostProject,
		Network:          poolClusterDetails.Network,
		FirewallRuleName: activities.SmbFirewallName,
	}

	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.CreateFirewallRule, firewallParams).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create SMB firewall during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	firewallParams.FirewallRuleName = activities.ILBHealthCheckFirewallName
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.CreateFirewallRule, firewallParams).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create ILB firewall during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}

	var fqdn string
	// Use the new workflow instead of the single activity
	fqdn, err = EnsureCIFSShareWorkflow(ctx, dbVolume, node, activeDirectory, dbSvm.Name, dbSvm.SvmDetails.ExternalUUID)
	if err != nil {
		log.Error("Failed to create cifs share during PostFileVolumeWorkflowForSMB with error: ", err)
		return nil, ConvertToVSAError(err)
	}
	if fqdn != "" && dbVolume.VolumeAttributes != nil && dbVolume.VolumeAttributes.FileProperties != nil {
		log.Infof("Setting the fqdn: [%s] for volume:[%s]", fqdn, dbVolume.Name)
		dbVolume.VolumeAttributes.FileProperties.Fqdn = fqdn
	}

	if !dbSvm.ActiveDirectoryID.Valid {
		var updatedSvm *datamodel.Svm
		params := activities.UpdateSvmActiveDirectoryParams{Svm: dbSvm, ActiveDirectoryUUID: activeDirectory.UUID}
		err = workflow.ExecuteActivity(ctx, activities.CommonActivities.UpdateSvmActiveDirectory, params).Get(ctx, &updatedSvm)
		if err != nil {
			log.Error("Failed to update SVM Active Directory association during PostFileVolumeWorkflowForSMB with error: ", err)
			return nil, ConvertToVSAError(err)
		}
		dbSvm = updatedSvm
	}

	log.Info("SMB post-provisioning: created CIFS share for volume:", dbVolume.Name)
	return dbVolume, nil
}

// EnsureCIFSShareWorkflow orchestrates the creation of CIFS share through individual activities
func EnsureCIFSShareWorkflow(ctx workflow.Context, volume *datamodel.Volume, node *models.Node, activeDirectory *vsa.ActiveDirectory, svmName, externalSVMUUID string) (string, error) {
	log := util.GetLogger(ctx)

	// Validate inputs
	if volume.VolumeAttributes == nil || volume.VolumeAttributes.FileProperties == nil {
		log.Warnf("Skipping CIFS share creation for non-file volume %s", volume.Name)
		return "", nil
	}

	activeDirectoryActivity := newActiveDirectoryActivity()
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return "", err
	}

	// Set activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Step 1: Create or modify AD DNS
	log.Info("Step 1: Creating or modifying AD DNS")
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CreateOrModifyADDNS, &node, &activeDirectory, svmName, externalSVMUUID).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create or modify AD DNS", "error", err)
		return "", ConvertToVSAError(err)
	}

	// Step 2: Get or create CIFS service
	log.Info("Step 2: Getting or creating CIFS service")
	var cifsServiceResult *active_directory_activities.GetOrCreateCifsServiceResult
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.GetOrCreateCifsService, &node, &activeDirectory, svmName, externalSVMUUID).Get(ctx, &cifsServiceResult)
	if err != nil {
		log.Error("Failed to get or create CIFS service", "error", err)
		return "", ConvertToVSAError(err)
	}

	var fqdn string
	if cifsServiceResult.FQDN != "" {
		// Service was created, FQDN is already set
		fqdn = cifsServiceResult.FQDN
		log.Info("CIFS service created with FQDN", "fqdn", fqdn)
	} else {
		// Service already existed, check if DDNS needs to be enabled
		if cifsServiceResult.NeedsDDNS {
			// Step 3: Enable DDNS
			log.Info("Step 3: Enabling DDNS for existing CIFS service")
			fqdn = cifsServiceResult.CifsServiceName + "." + cifsServiceResult.AdDomain
			err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.DdnsModify, &node, externalSVMUUID, fqdn).Get(ctx, nil)
			if err != nil {
				log.Error("Failed to enable DDNS", "error", err, "fqdn", fqdn)
				return "", ConvertToVSAError(err)
			}
			log.Info("DDNS enabled", "fqdn", fqdn)
		} else {
			// DDNS already enabled or not needed, build FQDN if we have the info
			if cifsServiceResult.CifsServiceName != "" && cifsServiceResult.AdDomain != "" {
				fqdn = cifsServiceResult.CifsServiceName + "." + cifsServiceResult.AdDomain
			}
			log.Info("DDNS already enabled or not needed", "fqdn", fqdn)
		}
	}

	// Step 4: Create junction path for CIFS share
	log.Info("Step 4: Creating junction path for CIFS share", "junctionPath", volume.VolumeAttributes.FileProperties.JunctionPath)
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.CreateJunctionPathForCifsShare, &node, svmName, volume.VolumeAttributes.FileProperties.JunctionPath).Get(ctx, nil)
	if err != nil {
		log.Error("Failed to create junction path for CIFS share", "error", err)
		return "", ConvertToVSAError(err)
	}

	log.Info("CIFS share creation completed successfully", "fqdn", fqdn)
	return fqdn, nil
}

var (
	getActivityOptionsForEnsureCIFSShareVolumeActivity = _getActivityOptionsForEnsureCIFSShareVolumeActivity
	newActiveDirectoryActivity                         = func() *active_directory_activities.ActiveDirectoryActivity {
		return &active_directory_activities.ActiveDirectoryActivity{}
	}
)

func _getActivityOptionsForEnsureCIFSShareVolumeActivity(retryPolicy *WorkflowRetryPolicy) workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			MaximumInterval:        60 * time.Second,
			MaximumAttempts:        int32(10),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
}

// PostFileVolumeWorkflow handles post-provisioning for file volumes
func PostFileVolumeWorkflow(ctx workflow.Context, dbVolume *datamodel.Volume, node *models.Node, hostParams []*common.HostParams, volCreateResponse *vsa.VolumeResponse, isRestoreFromBackup bool, isRestoreSnapshot bool, restoreVolCreateResponse *vsa.VolumeResponse) (*datamodel.Volume, error) {
	// Configure activity options for child workflow
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	log := util.GetLogger(ctx)
	log.Info("File post-provisioning: anything after volume create. (placeholder)")
	return dbVolume, nil
}

// CreateVolumeWorkflow Volume Workflow process volume related requests from a customer.
func CreateVolumeWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume, backupVault *datamodel.BackupVault, backup *datamodel.Backup) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	log := util.GetLogger(ctx)
	volumeWf := new(volumeCreateWorkflow)
	err := volumeWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup CreateVolumeWorkflow: %v", err)
		return nil, err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for CreateVolumeWorkflow: %v", err)
		return nil, err
	}
	_, customErr := volumeWf.Run(ctx, volume, params, backupVault, backup)
	if customErr != nil {
		log.Errorf("CreateVolumeWorkflow completed with error: %v", customErr)
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for CreateVolumeWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}
	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for CreateVolumeWorkflow: %v", err)
	}
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

func (wf *volumeCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
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
	isRestoreSnapshot := createVolumeParams.SnapshotID != "" && snapshot != nil
	isRestoreFromBackup := createVolumeParams.BackupPath != "" && backup != nil
	volumeActivity := &activities.VolumeCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
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
		return nil, ConvertToVSAError(err)
	}

	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: dbVolume.Pool.PoolCredentials.Password, SecretID: dbVolume.Pool.PoolCredentials.SecretID, DeploymentName: dbVolume.Pool.DeploymentName, CertificateID: dbVolume.Pool.PoolCredentials.CertificateID, AuthType: dbVolume.Pool.PoolCredentials.AuthType})

	// Get Aggregates from ONTAP if the volume is large capacity
	var aggregateResult *models.AggregateDistributionResult
	if dbVolume.LargeVolumeAttributes != nil && dbVolume.LargeVolumeAttributes.LargeCapacity && dbVolume.LargeVolumeAttributes.LargeVolumeConstituentCount != nil {
		err = workflow.ExecuteActivity(ctx, volumeActivity.GetAggregatesFromOntap, &dbVolume, &node, len(dbNodes)).Get(ctx, &aggregateResult)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Pre-provisioning child workflow
	preWorkflowFunc, preErr := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePre, dbVolume.Account.Name)
	if preErr != nil {
		err = preErr
		return nil, ConvertToVSAError(err)
	}
	var preUpdatedVolume *datamodel.Volume
	err = workflow.ExecuteChildWorkflow(ctx, preWorkflowFunc, dbVolume, node).Get(ctx, &preUpdatedVolume)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update the dbVolume with any changes from the pre-workflow
	if preUpdatedVolume != nil {
		dbVolume = preUpdatedVolume
	}
	rollbackManager.AddActivity(activities.VolumeDeleteActivity.DeleteSnapshotPolicyInONTAP, getSnapshotPolicyName(dbVolume), &node) // This will delete the snapshotPolicy if exists
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateSnapshotPolicyInONTAP, &dbVolume, &node).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	var volCreateResponse *vsa.VolumeResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateVolumeInONTAP, &dbVolume, &node, &snapshot, backup, aggregateResult).Get(ctx, &volCreateResponse)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	rollbackManager.AddActivity(activities.VolumeDeleteActivity.DeleteVolumeInONTAP, volCreateResponse.ExternalUUID, dbVolume.Name, &node) // This will delete the lunMap & lun if exists

	// Persisting ExternalUUID in the database to ensure it is available for ONTAP volume deletion during cleanup triggered by CCFE/VCP
	dbVolume.VolumeAttributes.ExternalUUID = volCreateResponse.ExternalUUID
	err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeAttributesInDB, dbVolume.UUID, dbVolume.VolumeAttributes).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update CV count for auto-provisioned large volumes from the CreateVolume response
	// Only execute if the volume is large capacity AND the customer didn't specify the constituent count
	// (i.e., it was auto-provisioned by ONTAP) AND we got the count from the creation response
	if dbVolume.LargeVolumeAttributes != nil && dbVolume.LargeVolumeAttributes.LargeCapacity &&
		dbVolume.LargeVolumeAttributes.LargeVolumeConstituentCount == nil && volCreateResponse.ConstituentCount != nil {
		log.Debugf("Updating CV count for auto-provisioned volume %s: %d", dbVolume.UUID, *volCreateResponse.ConstituentCount)

		// Update the dbVolume struct with the actual CV count from ONTAP creation response
		dbVolume.LargeVolumeAttributes.LargeVolumeConstituentCount = volCreateResponse.ConstituentCount
		log.Debugf("Successfully updated CV count for auto-provisioned volume %s to %d", dbVolume.UUID, *volCreateResponse.ConstituentCount)
	}

	// Calculate the available LUN space by subtracting the reserved space for snapshots
	var restoreVolCreateResponse *vsa.VolumeResponse
	if isRestoreSnapshot {
		if utils.IsSanProtocols(dbVolume.VolumeAttributes.Protocols) {
			err = workflow.ExecuteActivity(ctx, volumeActivity.LunSizeUpdateValidation, &dbVolume, &node).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
		// TODO: [VSCP-1435] To remove 'Split' keywords as split operation is removed from create volume workflow
		err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateClonedVolumeBeforeSplit, &dbVolume, &node).Get(ctx, &restoreVolCreateResponse)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	var hostGroups []*datamodel.HostGroup
	var hostParams []*common.HostParams
	if utils.IsSanProtocols(dbVolume.VolumeAttributes.Protocols) {
		// Get host groups for block volume
		err = workflow.ExecuteActivity(ctx, volumeActivity.GetHosts, &dbVolume).Get(ctx, &hostGroups)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		hostParams = createHostParamsFromHostGroups(hostGroups, dbVolume)

		// Create igroup for block volume
		err = workflow.ExecuteActivity(ctx, volumeActivity.CreateIgroup, &dbVolume, &hostParams, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to create igroup: %w", err))
		}
	}
	// If isRestoreFromBackup is true, we will restore the volume from the backup
	// backup path example: "projects/123456789/locations/us-e4/backupVaults/bv1/backups/backupName"
	if isRestoreFromBackup {
		err = workflow.ExecuteActivity(ctx, volumeActivity.CreateRestoreWorkflow, createVolumeParams, &dbVolume, &hostParams, backupVault, backup, &volCreateResponse).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to create Restore Workflow: %w", err))
		}
	}

	if !isRestoreFromBackup {
		// Post-provisioning child workflow
		postWorkflowFunc, postErr := selectVolumeChildWorkflow(dbVolume.VolumeAttributes.Protocols, PhasePost, dbVolume.Account.Name)
		if postErr != nil {
			err = postErr
			return nil, ConvertToVSAError(err)
		}

		var updatedVolume *datamodel.Volume
		err = workflow.ExecuteChildWorkflow(ctx, postWorkflowFunc, dbVolume, node, hostParams, volCreateResponse, isRestoreFromBackup, isRestoreSnapshot, restoreVolCreateResponse).Get(ctx, &updatedVolume)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Update the dbVolume with the changes from the child workflow
		if updatedVolume != nil {
			dbVolume = updatedVolume
		}
	}

	if isRestoreSnapshot && thinCloneGASupport && !createVolumeParams.IsClone {
		err = workflow.ExecuteActivity(ctx, volumeActivity.InitiateSplitForVolume, &dbVolume, &node, &snapshot).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupVaultID != "" {
		if !env.IsLocalEnv() {
			var token string
			err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetAuthJWTToken, createVolumeParams.AccountName).Get(ctx, &token)
			if err != nil {
				log.Errorf("Failed to get token for account %s: %v", createVolumeParams.AccountName, err)
				return nil, ConvertToVSAError(err)
			}
			ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, token)
		}

		var backupVault *datamodel.BackupVault
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckBackupVaultExistsInVCP, &dbVolume, &region).Get(ctx, &backupVault)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		backupRegion := region
		if backupVault.BackupVaultType == activities.CrossRegionBackupType && *backupVault.BackupRegionName != "" {
			backupRegion = *backupVault.BackupRegionName
		}

		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.FindTenancy, dbVolume.VolumeAttributes.VendorSubnetID, dbVolume.Account.Name, backupRegion).Get(ctx, &tenancyDetails)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		bucketDetails := &common.BucketDetails{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckForBucketResourceName, &dbVolume).Get(ctx, &bucketDetails)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if bucketDetails.BucketName == "" && bucketDetails.ServiceAccountName == "" && bucketDetails.TenantProjectNumber == "" {
			resourceName := &common.ResourceNames{}
			err = workflow.ExecuteActivity(ctx, volumeActivity.GenerateResourceNames, &dbVolume, &tenancyDetails, region).Get(ctx, &resourceName)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBucket, &resourceName, &tenancyDetails, backupRegion).Get(ctx, &bucketDetails)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			// Setting the 'satisfiesPzi' and 'satisfiesPzs' fields in bucketDetails by fetching the latest info from GCP
			err = syncBucketDetailsWithGCP(ctx, bucketDetails)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateBackupVaultWithBucketDetails, &dbVolume, &bucketDetails).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateRemoteBackupVaultDetailsInVCP, &dbVolume, &bucketDetails, backupVault).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}

			if backupVault.BackupVaultType == activities.CrossRegionBackupType && backupVault.BackupRegionName != nil && *backupVault.BackupRegionName != "" {
				err = workflow.ExecuteActivity(ctx, volumeActivity.SetupCrossRegionBackupPermissionsActivity, backupVault, &dbVolume.Pool, &bucketDetails).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}
	}

	if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupPolicyID != "" {
		var backupPolicyExists bool
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckIfBackupPolicyExistsInVCP, dbVolume.DataProtection.BackupPolicyID, dbVolume.AccountID).Get(ctx, &backupPolicyExists)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if !backupPolicyExists {
			backupPolicyActivity := &activities.BackupPolicyActivity{}
			var vcpBackupPolicy *datamodel.BackupPolicy
			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBackupPolicyFetchedFromSDE, &dbVolume, region).Get(ctx, &vcpBackupPolicy)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(backupPolicyActivity.DeleteBackupPolicyInVCP, vcpBackupPolicy.UUID)
			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBackupPolicySchedule, &vcpBackupPolicy, createVolumeParams.BackupSchedule).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			rollbackManager.AddActivity(backupPolicyActivity.DeleteBackupPolicySchedule, vcpBackupPolicy.UUID)

			if !vcpBackupPolicy.PolicyEnabled {
				err = workflow.ExecuteActivity(ctx, backupPolicyActivity.PauseBackupPolicySchedule, vcpBackupPolicy).Get(ctx, nil)
				if err != nil {
					return nil, ConvertToVSAError(err)
				}
			}
		}
	}

	err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeDetails, &dbVolume, &volCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}

func createHostParamsFromHostGroups(hostGroups []*datamodel.HostGroup, volume *datamodel.Volume) []*common.HostParams {
	var hostParamsArray []*common.HostParams
	osType := ""
	if volume.VolumeAttributes.BlockDevices != nil && len(*volume.VolumeAttributes.BlockDevices) > 0 {
		osType = (*volume.VolumeAttributes.BlockDevices)[0].OSType
	} else {
		osType = volume.VolumeAttributes.BlockProperties.OSType
	}

	for _, hostGroup := range hostGroups {
		hostParams := &common.HostParams{
			HostName: hostGroup.Name,
			HostIQNs: hostGroup.Hosts.Hosts,
			OsType:   osType,
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

// syncBucketDetailsWithGCP syncs bucket details with GCP to get the latest ZiZs information
func syncBucketDetailsWithGCP(ctx workflow.Context, bucketDetails *common.BucketDetails) error {
	logger := util.GetLogger(ctx)
	existingBucketDetails := &datamodel.BucketDetails{
		BucketName:          bucketDetails.BucketName,
		TenantProjectNumber: bucketDetails.TenantProjectNumber,
		ServiceAccountName:  bucketDetails.ServiceAccountName,
		VendorSubnetID:      bucketDetails.VendorSubnetID,
	}
	var updatedBucketDetails *datamodel.BucketDetails
	syncBackupZiZsActivity := &backgroundactivities.SyncBackupZiZsActivity{}
	err := workflow.ExecuteActivity(ctx, syncBackupZiZsActivity.SyncBucketDetails, existingBucketDetails).Get(ctx, &updatedBucketDetails)
	if err != nil {
		// Log the error, but do not fail the workflow
		logger.Errorf("Failed to sync bucket details for bucket %s: %v", existingBucketDetails.BucketName, err)
	} else if updatedBucketDetails != nil {
		bucketDetails.SatisfiesPzi = updatedBucketDetails.SatisfiesPzi
		bucketDetails.SatisfiesPzs = updatedBucketDetails.SatisfiesPzs
	}
	return nil // Always return nil to not fail the workflow
}
