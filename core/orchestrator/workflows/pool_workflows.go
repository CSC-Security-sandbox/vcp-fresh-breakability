package workflows

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/api/iam/v1"
)

var (
	configureKmsConfigForSvmActivity     = _configureKmsConfigForSvmActivity
	isProberProject                      = utils.IsProberProject
	GetNewVSAClientWorkflowManager       = _getNewVSAClientWorkflowManager
	ExtractOntapVersion                  = utils.ExtractOntapVersion
	WaitForServiceNetworkOperationStatus = _waitForServiceNetworkOperationStatus
	WaitForGCPNetworkOperationStatus     = _waitForGCPNetworkOperationStatus
	verifyKmsConfigReachability          = _verifyKmsConfigReachability
	syncPoolZIZSDetailsWorkflow          = _syncPoolZIZSDetailsWorkflow
)

var (
	_                                  WorkflowInterface = &createPoolWorkflow{} // Enforcing the WorkflowInterface on createPoolWorkflow
	setupNwHeartbeatTimeout                              = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
	vmrsConfigPath                                       = env.GetString("VMRS_CONFIG_PATH", "/config/vmrs_gcp.yaml")
	maxNodesPerGroup                                     = env.GetInt("MAX_NODES_PER_GROUP", 200)
	enableMetrics                                        = env.GetBool("ENABLE_METRICS", false)
	enableUniqueSerialNumberGeneration                   = env.GetBool("ENABLE_UNIQUE_SERIAL_NUMBER_GENERATION", false)

	vsaImageName                 = env.GetString("VSA_IMAGE_NAME", "x-9-17-1p1-gcnv")
	mediatorImage                = env.GetString("VSA_MEDIATOR_IMAGE_NAME", "cvo-mediator-x-9-17-1p1")
	vsaFilesImageName            = env.GetString("VSA_FILES_IMAGE_NAME", "x-9-18-1rc1")
	filesMediatorImage           = env.GetString("VSA_FILES_MEDIATOR_IMAGE_NAME", "cvo-mediator-x-9-18-1rc1")
	waitTimeForGCPOperationInSec = env.GetInt("WAIT_TIME_FOR_GCP_OPERATION_IN_SEC", 10)

	disableVsaCleanupOnVLMFailure     = env.GetBool("DISABLE_VSA_CLEANUP_ON_VLM_FAILURE", false)
	enableAutoVolOfflineCronForGCPKMS = env.GetBool("ENABLE_AUTO_VOL_OFFLINE_CRON_FOR_GCP_KMS", true)
	ginLoggingFeatureFlag             = env.GetBool("GIN_LOGGING_FEATURE", false)
	enableSyncPoolZIZS                = env.GetBool("ENABLE_SYNC_POOL_ZIZS", false)
)

const (
	DefaultSvmName    = "gcnv"
	SaIdPrefix        = "vsa-sa-"
	statusDone        = "DONE"
	operationProgress = int64(100)
	ONTAPMode         = "ONTAP"
	GCNVMode          = "GCNV"
)

type createPoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

type poolDataSubnetWorkFlow struct {
	BaseWorkflow
	SE             *database.Storage
	TenancyDetails *common.TenancyInfo
}

var _ WorkflowInterface = &poolDataSubnetWorkFlow{}

// CreatePoolWorkflow processes pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) error {
	createPoolWF := new(createPoolWorkflow)
	log := util.GetLogger(ctx)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	createPoolWF.Status = WorkflowStatusRunning
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("failed to update job status to PROCESSING: %v", err)
		return err
	}
	_, errRun := createPoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in createPoolWorkflow: %v", errRun)
		createPoolWF.Status = WorkflowStatusFailed
		err2 := createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), errRun)
		if err2 != nil {
			log.Errorf("failed to update job with err and status to ERROR: %v", err2)
			return err2
		}
		return errRun
	}
	createPoolWF.Status = WorkflowStatusCompleted
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("failed to update job status to DONE: %v", err)
		return err
	}
	return nil
}

func (wf *createPoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
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

func (wf *createPoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.CreatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
	subnetActivity := SubnetActivity{}
	retryPolicy, err := PopulateRetryPolicyParams(params.LargeCapacity)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
			// add panic error as non-retriable types
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbPool := pool

	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	rollbackManager.AddActivity(poolActivity.ErroredPool, dbPool)
	rollbackManager.AddActivity(poolActivity.DeletePoolResourcesOnRollback, dbPool)

	// Verify if KMS config is reachable before proceeding with pool creation, if present
	err = verifyKmsConfigReachability(ctx, params.KmsConfigId)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	tenantProjectNumber := new(string)
	err = workflow.ExecuteActivity(ctx, poolActivity.FindTenancyProject, params).Get(ctx, tenantProjectNumber)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	createSubnetJobUUID := new(string)
	err = workflow.ExecuteActivity(ctx, subnetActivity.CreateSubnetJob, params, pool, tenantProjectNumber).Get(ctx, createSubnetJobUUID)
	if err != nil {
		wf.Logger.Errorf("Failed to start create subnet workflow for account: %s & vpc: %s, error: %v", params.AccountName, params.VendorSubNetID, err)
		return nil, ConvertToVSAError(err)
	}

	// Wait for the subnet creation job to complete using workflow.sleep.
	err = PollOnDBJob(ctx, *createSubnetJobUUID, retryPolicy.StartToCloseTimeout)
	if err != nil {
		wf.Logger.Errorf("Failed to wait for create subnet job %s to complete, error: %v", *createSubnetJobUUID, err)
		return nil, ConvertToVSAError(err)
	}

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, subnetActivity.GetTenancyDetails, createSubnetJobUUID).Get(ctx, &tenancyDetails)
	if err != nil {
		wf.Logger.Errorf("Failed to get tenancy details for job %s, error: %v", *createSubnetJobUUID, err)
		return nil, ConvertToVSAError(err)
	}
	dbPool.ClusterDetails.SubnetNames = tenancyDetails.SubnetworkNames

	// persist cluster details (tenancy details - as it's required for cleaning up the resources in case of failure)
	tenancyInfo := &datamodel.ClusterDetails{
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
		SubnetNames:           tenancyDetails.SubnetworkNames,
	}
	dbPool.SnHostProject = tenancyDetails.SnHostProject
	dbPool.ClusterDetails = *tenancyInfo
	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, dbPool, tenancyInfo).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	setupNwCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
	err = workflow.ExecuteChildWorkflow(setupNwCtx, ConfigureNetworkWorkflow, tenancyDetails).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	serviceAccount := &iam.ServiceAccount{}
	serviceAccountID := fmt.Sprintf("%s%s", SaIdPrefix, pool.DeploymentName)
	dbPool.ServiceAccountId = serviceAccountID

	rollbackManager.AddActivity(poolActivity.DeleteServiceAccount, tenancyDetails.RegionalTenantProject, serviceAccountID)

	// Get the service account specific retry policy
	saRetryPolicy, err := populateServiceAccountRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Create custom activity options for service account creation
	saActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: saRetryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        saRetryPolicy.InitialInterval,
			BackoffCoefficient:     saRetryPolicy.BackoffCoefficient,
			MaximumInterval:        saRetryPolicy.MaximumInterval,
			MaximumAttempts:        int32(saRetryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}

	// Create new context with custom activity options while preserving existing context
	saCtx := workflow.WithActivityOptions(ctx, saActivityOptions)

	// Use the new context for the service account creation activity
	err = workflow.ExecuteActivity(saCtx, poolActivity.CreateServiceAccountWithStorageRole, tenancyDetails.RegionalTenantProject, serviceAccountID, pool.Name).Get(saCtx, serviceAccount)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	AutoTierBucketName := fmt.Sprintf("%s-%s", params.Region, dbPool.UUID)

	// Update AutoTieringConfig with bucket name
	if dbPool.AutoTieringConfig == nil {
		dbPool.AutoTieringConfig = &datamodel.AutoTieringConfig{}
	}
	dbPool.AutoTieringConfig.BucketName = AutoTierBucketName

	rollbackManager.AddActivity(poolActivity.DeleteAutoTierBucket, AutoTierBucketName, dbPool.Account.Name, dbPool.ID)
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateAutoTierBucket, AutoTierBucketName, params.Region, tenancyDetails.RegionalTenantProject).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	credConfig := &vlm.OntapCredentials{}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateOnTapCredentials, pool, pool.DeploymentName, env.VCP_ADMIN).Get(ctx, &credConfig)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if !disableVsaCleanupOnVLMFailure {
		rollbackManager.AddActivity(poolActivity.DeleteOnTapCredentials, pool)
	}

	sizeInGB := utils.BytesToGigabytes(params.HotTierSizeInBytes)
	// Convert CustomPerformanceParams to CustomerRequestedPerformance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             *params.CustomPerformanceParams.Iops,
		DesiredThroughputInMiBs: params.CustomPerformanceParams.ThroughputMibps,
		DesiredCapacityInGiB:    int64(sizeInGB),
		ConfigForPoolInstanceScaling: &vmrs.PoolInstanceScalingConfig{
			CurrentVolCount:        int64(0), // new pool, so current vol count is 0
			VolLimitPerInstanceMap: nil,
			CurrentInstanceType:    "",
		},
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

	// Find the optimal VMs based on the customer requested performance.
	vlmConfig := &vlm.VLMConfig{}

	bucketName := ""
	if pool.AutoTieringConfig != nil {
		bucketName = pool.AutoTieringConfig.BucketName
	}

	if !disableVsaCleanupOnVLMFailure {
		deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
		prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, dbPool.DeploymentName, vlm.VLMCloudProvider, tenancyDetails.RegionalTenantProject)
		rollbackManager.AddWorkflow(vlm.VSALifecycleManagerQueue, vlm.DeleteVSAClusterDeploymentWorkflowName, deleteVSAClusterDeploymentRequest)
	}

	locationInfo := &common.LocationInfo{
		PrimaryZone:   params.PrimaryZone,
		SecondaryZone: params.SecondaryZone,
		Region:        params.Region,
	}

	// Use resolved zones to identify VMs and build VLM config
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, dbPool.DeploymentName, locationInfo, tenancyDetails, serviceAccount.Email, bucketName, pool.LargeCapacity).Get(ctx, vlmConfig)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	var resolvedLocationInfo *common.LocationInfo
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifySecondaryAndMediatorZone, tenancyDetails.RegionalTenantProject, locationInfo, vlmConfig.Deployment.VSAInstanceType, params.IsRegionalHA).Get(ctx, &resolvedLocationInfo)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	dbPool.PoolAttributes.SecondaryZone = resolvedLocationInfo.SecondaryZone
	dbPool.PoolAttributes.MediatorZone = resolvedLocationInfo.MediatorZone

	hostMap := make(map[string]string)

	createVSAClusterDeploymentRequest := &vlm.CreateVSAClusterDeploymentRequest{}
	prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest, *vlmConfig, *credConfig, dbPool, resolvedLocationInfo)

	// Allocate unique serial numbers in production
	// This is disabled by default (enableUniqueSerialNumberGeneration=false)
	// Serial number will only be allocated if the project is not a prober project.
	if enableUniqueSerialNumberGeneration && !isProberProject(params.AccountName) {
		err = workflow.ExecuteActivity(ctx, poolActivity.AllocateClusterSerialNumber, createVSAClusterDeploymentRequest, params.AccountName).Get(ctx, createVSAClusterDeploymentRequest)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	createVSAClusterDeploymentResponse, err := vsaClientWorkflowManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateCloudDNSRecords, createVSAClusterDeploymentResponse.VLMConfig, dbPool.DeploymentName, dbPool.PoolCredentials.AuthType).Get(ctx, &hostMap)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	rollbackManager.AddActivity(poolActivity.DeleteCloudDNSRecords, hostMap, pool.PoolCredentials.AuthType)

	err = workflow.ExecuteActivity(ctx, poolActivity.SaveVSANodeDetails, dbPool, createVSAClusterDeploymentResponse.VLMConfig, pool.DeploymentName, &hostMap).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID, AuthType: pool.PoolCredentials.AuthType})

	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Get intercluster LIF IPs from VLM config
	var interclusterLifIPs []string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetInterClusterLifsFromVLMConfig, createVSAClusterDeploymentResponse.VLMConfig).Get(ctx, &interclusterLifIPs)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Add PSC Endpoint
	if ginLoggingFeatureFlag {
		rollbackManager.AddWorkflow(workflowengine.CustomerTaskQueue, ReleasePSCEndpointWorkflow, dbPool)
		setupPSCEndpoint := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
		err = workflow.ExecuteChildWorkflow(setupPSCEndpoint, ConfigurePSCEndpointWorkflow, tenancyDetails.RegionalTenantProject, params.Region, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Create pool build info with current image details
	poolBuildInfo := &datamodel.PoolBuildInfo{
		VSABuildImage:      vsaImageName,
		MediatorBuildImage: mediatorImage,
		OntapVersion:       env.CurrentOntapVersionDetails,
		BuildTimestamp:     time.Now(),
	}
	dbPool.BuildInfo = poolBuildInfo

	clusterDetails := &datamodel.ClusterDetails{
		ExternalName:          createVSAClusterDeploymentResponse.VLMConfig.VsaCluster.ClusterName,
		OntapVersion:          ontapVersion,
		InterclusterLifIPs:    interclusterLifIPs,
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
		SubnetNames:           tenancyDetails.SubnetworkNames,
	}
	dbPool.SnHostProject = tenancyDetails.SnHostProject
	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, dbPool, clusterDetails).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	defer func() {
		// Once the cluster is deployed, IPs are reserved from the subnet. Using this defer block we will fetch and
		// update the SubnetToIPsReserved details for pools including failed pools.
		var subnetToIPsReserved *[]datamodel.SubnetToIPs
		err1 := workflow.ExecuteActivity(ctx, poolActivity.GetIPsConsumedForSubnet, dbPool, tenancyDetails, params.Region).Get(ctx, &subnetToIPsReserved)
		if err1 != nil {
			wf.Logger.Errorf("Failed to get IPs consumed by deployment in the alloted subnet, error: %v", err1)
		}

		if subnetToIPsReserved != nil {
			clusterDetails.ReservedIPsInSubnet = subnetToIPsReserved
			err1 = workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolFields, dbPool.UUID, map[string]interface{}{
				"cluster_details": clusterDetails,
			}).Get(ctx, nil)
			if err1 != nil {
				wf.Logger.Errorf("Failed to save IPs consumed by deployment in the alloted subnet in DB, error: %v", err1)
			}
		} else {
			wf.Logger.Debugf("No subnet to IPs reserved found for pool %s", dbPool.Name)
		}
	}()

	svmName := ""
	err = workflow.ExecuteActivity(ctx, poolActivity.AllocateSVMName, dbPool).Get(ctx, &svmName)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	createSVMRequest := &vlm.CreateSVMRequest{}
	prepareCreateSVMRequest(createSVMRequest, svmName, createVSAClusterDeploymentResponse.VLMConfig, *credConfig)
	createSVMResponse, err := vsaClientWorkflowManager.CreateVSASVM(ctx, createSVMRequest)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	svm := &datamodel.Svm{}
	err = workflow.ExecuteActivity(ctx, poolActivity.SaveSVMAndLifData, dbPool, createSVMResponse.VLMConfig, svmName).Get(ctx, svm)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Create QoS policy and apply it to the SVM
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateQoSPolicyAndApplyToSVM, dbPool, svm, node).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	expertCredConfig := &vlm.OntapCredentials{}
	if params.Mode == ONTAPMode {
		if len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 || pool.ExpertModeCredentials.ExpertModeCredential[0].Username == "" {
			return nil, ConvertToVSAError(vsaerrors.New("expert mode username not found in request"))
		}
		err = workflow.ExecuteActivity(ctx, poolActivity.CreateExpertModeCredentials, pool, pool.DeploymentName, pool.ExpertModeCredentials.ExpertModeCredential[0].Username).Get(ctx, &expertCredConfig)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if !disableVsaCleanupOnVLMFailure {
			rollbackManager.AddActivity(poolActivity.DeleteExpertModeCredentials, pool)
		}

		createVSAExpertModeReq := &vlm.OntapExpertModeUserConfig{}
		prepareCreateVSAExpertModeReq(createVSAExpertModeReq, createVSAClusterDeploymentResponse.VLMConfig, *credConfig, *expertCredConfig, dbPool)
		err = vsaClientWorkflowManager.CreateVSAExpertModeUser(ctx, createVSAExpertModeReq)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Enable KMS for SVM if KMS config is provided
	err = configureKmsConfigForSvmActivity(ctx, *dbPool, node, svm, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	dbPool.ClusterDetails.SubnetNames = tenancyDetails.SubnetworkNames

	err = workflow.ExecuteActivity(ctx, poolActivity.CreatedPool, dbPool, &createSVMResponse.VLMConfig).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	syncPoolZIZSDetailsWorkflow(ctx, dbPool, wf)

	// Enable billing metrics related workflow(NodeToHarvestFarmWorkflow), when enableMetrics is true
	if enableMetrics {
		registerNodeToHarvestFarmWorkflowInput := RegisterNodeToHarvestFarmWorkflowInput{
			PoolID:            dbPool.ID,
			MaxNodesPerGroup:  maxNodesPerGroup,
			CustomerProjectID: params.AccountName,
			TenantProjectID:   *tenantProjectNumber,
			PoolUUID:          dbPool.UUID,
			AccountID:         dbPool.AccountID,
			DeploymentName:    dbPool.DeploymentName,
			PoolName:          dbPool.Name,
			IsRegionalHA:      dbPool.PoolAttributes != nil && dbPool.PoolAttributes.IsRegionalHA,
		}

		ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID: "register-node-to-harvest-farm" + uuid.New().String(),
			TaskQueue:  workflowengine.CustomerTaskQueue,
		})

		// If on-boarding to harvest-farm fails log warning message,
		// TODO: Need to emit a metric to alert on pool on-boarding to harvest-farm
		if childWfErr := workflow.ExecuteChildWorkflow(ctx,
			RegisterNodeToHarvestFarmWorkflow,
			registerNodeToHarvestFarmWorkflowInput).Get(ctx, nil); childWfErr != nil {
			wf.Logger.Warnf("Failed to on-board poolId %d to harvest-farm due to error: %v", dbPool.ID, childWfErr)
		}
	}
	return nil, nil
}

func _syncPoolZIZSDetailsWorkflow(ctx workflow.Context, dbPool *datamodel.Pool, wf *createPoolWorkflow) {
	// Execute SyncPoolZIZSDetailsWorkflow asynchronously
	// If sync fails, log warning but don't fail the main pool creation workflow
	if enableSyncPoolZIZS {
		// Start SyncPoolZIZSDetailsWorkflow as child workflow after successful pool creation
		poolIdentifier := &database.PoolIdentifier{
			UUID:      dbPool.UUID,
			Name:      dbPool.Name,
			AccountID: dbPool.AccountID,
			VendorID:  dbPool.VendorID,
		}

		// Create child workflow context for SyncPoolZIZSDetailsWorkflow
		syncPoolZIZSCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:            fmt.Sprintf("sync-pool-zizs-%s-%s", dbPool.UUID, uuid.New().String()),
			TaskQueue:             workflowengine.BackgroundTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			ParentClosePolicy:     enums.PARENT_CLOSE_POLICY_ABANDON, // Continue running even if parent closes
		})

		// Add extra logger fields for better traceability
		syncPoolZIZSCtx = util.AddExtraLoggerFields(syncPoolZIZSCtx, map[string]interface{}{
			"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
			"poolUUID":         dbPool.UUID,
			"poolName":         dbPool.Name,
		})

		if syncErr := workflow.ExecuteChildWorkflow(syncPoolZIZSCtx, SyncPoolComplianceForPoolWorkflow, poolIdentifier).Get(syncPoolZIZSCtx, nil); syncErr != nil {
			wf.Logger.Warnf("Failed to sync pool ZI/ZS compliance for pool %s (UUID: %s) due to error: %v", dbPool.Name, dbPool.UUID, syncErr)
		}
	} else {
		wf.Logger.Infof("SyncPoolZIZS workflow is disabled via ENABLE_SYNC_POOL_ZIZS environment variable for pool %s (UUID: %s)", dbPool.Name, dbPool.UUID)
	}
}

type updatePoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on createPoolWorkflow
var _ WorkflowInterface = &updatePoolWorkflow{}

// const customerActionTimeout = 30 * time.Minute

// UpdatePoolWorkflow processes pool related requests from a customer.
func UpdatePoolWorkflow(ctx workflow.Context, params *common.UpdatePoolParams, pool *datamodel.Pool, autoscaleConfig *common.AutoPoolScalingParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	updatePoolWF := new(updatePoolWorkflow)
	err := updatePoolWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	updatePoolWF.Status = WorkflowStatusRunning
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, err = updatePoolWF.Run(ctx, params, pool, autoscaleConfig)
	if e, ok := err.(*vsaerrors.CustomError); ok && e != nil {
		updatePoolWF.Status = WorkflowStatusFailed
		err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		return nil, ConvertToVSAError(err)
	}
	updatePoolWF.Status = WorkflowStatusCompleted
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return nil, nil
}

func (wf *updatePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updatePoolParams := input.(*common.UpdatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = updatePoolParams.AccountName
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

func (wf *updatePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	updatePoolParams := args[0].(*common.UpdatePoolParams)
	pool := args[1].(*datamodel.Pool)
	autoScalingParams := args[2].(*common.AutoPoolScalingParams)
	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams(pool.LargeCapacity)
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
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	dbPool := pool

	// In case of any errors, rollback to the old values.
	rollbackManager.AddActivity(poolActivity.UpdatedPool, pool)

	wf.Logger.Info("Updating pool with new parameters", "params", updatePoolParams) // Update the pool with the new parameters

	// Determine the size to provision in VLM (hot tier size for AutoTiering, full size otherwise)
	toProvisionPoolSizeInBytes := updatePoolParams.SizeInBytes
	if updatePoolParams.AllowAutoTiering {
		toProvisionPoolSizeInBytes = updatePoolParams.HotTierSizeInBytes
	}

	// Get current provisioned size - what was actually provisioned in VLM
	var currentProvisionedSize int64
	if !dbPool.AllowAutoTiering {
		currentProvisionedSize = dbPool.SizeInBytes
	} else {
		currentProvisionedSize = dbPool.AutoTieringConfig.HotTierSizeInBytes
	}

	// if there is no need of vlm workflow, just perform update pool in db
	if currentProvisionedSize == int64(toProvisionPoolSizeInBytes) &&
		dbPool.PoolAttributes.ThroughputMibps == int64(updatePoolParams.TotalThroughputMibps) &&
		dbPool.PoolAttributes.Iops == *updatePoolParams.TotalIops && autoScalingParams == nil {
		if dbPool.Description != updatePoolParams.Description {
			dbPool.Description = updatePoolParams.Description
		}
		if updatePoolParams.Labels != nil {
			dbPool.PoolAttributes.Labels = updatePoolParams.Labels
		}

		// Always update SizeInBytes for metadata/billing (even with AutoTiering)
		dbPool.SizeInBytes = int64(updatePoolParams.SizeInBytes)

		// Update AutoTiering configuration
		updateAutoTieringFields(dbPool, updatePoolParams, pool)

		rollbackManager.AddActivity(poolActivity.UpdatedPool, pool)
		err = workflow.ExecuteActivity(ctx, poolActivity.UpdatedPool, dbPool).Get(ctx, nil)
		return nil, ConvertToVSAError(err)
	}

	bucketName := ""
	if pool.AutoTieringConfig != nil {
		bucketName = pool.AutoTieringConfig.BucketName
	}

	saEmail := utils.ConstructServiceAccountEmail(pool.ServiceAccountId, pool.ClusterDetails.RegionalTenantProject)

	// Retrieve the last known VLM config that was shared with us.
	currentVlmConfig := &vlm.VLMConfig{}
	if err := json.Unmarshal([]byte(pool.VLMConfig), currentVlmConfig); err != nil {
		return nil, ConvertToVSAError(err)
	}
	// Determine VM scaling direction to decide the order of operations
	currentInstanceType := currentVlmConfig.Deployment.VSAInstanceType

	var poolInstanceScalingConfig *vmrs.PoolInstanceScalingConfig = nil
	if autoScalingParams != nil {
		poolInstanceScalingConfig = &vmrs.PoolInstanceScalingConfig{
			CurrentVolCount:        autoScalingParams.CurrentVolumeCount,
			VolLimitPerInstanceMap: autoScalingParams.VolLimitPerInstanceMap,
			CurrentInstanceType:    currentInstanceType,
		}
	}

	// Find the optimal VMs based on the customer requested performance.
	// Use toProvisionPoolSizeInBytes which accounts for AutoTiering (hot tier size vs full size)
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:                  *updatePoolParams.TotalIops,
		DesiredThroughputInMiBs:      updatePoolParams.TotalThroughputMibps,
		DesiredCapacityInGiB:         int64(utils.BytesToGigabytes(toProvisionPoolSizeInBytes)),
		ConfigForPoolInstanceScaling: poolInstanceScalingConfig,
	}

	// Identify secondary and mediator zones first
	locationInfo := &common.LocationInfo{
		PrimaryZone:   pool.PoolAttributes.PrimaryZone,
		SecondaryZone: pool.PoolAttributes.SecondaryZone,
		Region:        updatePoolParams.Region,
		MediatorZone:  pool.PoolAttributes.PrimaryZone, // this will be updated later to use the mediator zone
	}

	newVlmConfig := &vlm.VLMConfig{}
	// Create tenancy info from pool cluster details
	poolTenancyInfo := &common.TenancyInfo{
		RegionalTenantProject: pool.ClusterDetails.RegionalTenantProject,
		Network:               pool.ClusterDetails.Network,
		SubnetworkNames:       pool.ClusterDetails.SubnetNames,
		SnHostProject:         pool.ClusterDetails.SnHostProject,
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, dbPool.DeploymentName, locationInfo, poolTenancyInfo, saEmail, bucketName, pool.LargeCapacity).Get(ctx, newVlmConfig)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Update the mediator zone in the VLM config
	newVlmConfig.Deployment.Zone.MediatorZone = locationInfo.MediatorZone
	newInstanceType := newVlmConfig.Deployment.VSAInstanceType

	// Only validate zones for machine type if the instance type is changing
	if currentInstanceType != newInstanceType {
		wf.Logger.Info("Instance type is changing, validating zone compatibility", "currentType", currentInstanceType, "newType", newInstanceType)
		// Validate that primary and secondary zones support the VSA instance type
		err = workflow.ExecuteActivity(ctx, poolActivity.ValidateZonesForMachineTypes, pool.ClusterDetails.RegionalTenantProject, locationInfo.PrimaryZone, locationInfo.SecondaryZone, newInstanceType).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	} else {
		wf.Logger.Info("Instance type unchanged, skipping zone validation", "instanceType", currentInstanceType)
	}

	var isScalingUp bool
	err = workflow.ExecuteActivity(ctx, poolActivity.DetermineVMScalingDirection, vmrsConfigPath, currentInstanceType, newInstanceType).Get(ctx, &isScalingUp)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	credentials := &vlm.OntapCredentials{}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOnTapCredentials, pool).Get(ctx, &credentials)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Get nodes for the pool to modify QoS policy
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID, AuthType: pool.PoolCredentials.AuthType})

	// Execute activities based on scaling direction
	wf.Logger.Info("VM scaling direction determined", "isScalingUp", isScalingUp)

	// Execute QoS modification before deployment update if scaling down
	if !isScalingUp {
		wf.Logger.Info("Scaling down detected - modifying QoS policy first")
		err = workflow.ExecuteActivity(ctx, poolActivity.ModifyQoSPolicyAndApplyToSVM, dbPool, node, updatePoolParams).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		updatedPoolAttributes := &datamodel.PoolAttributes{
			ThroughputMibps: int64(updatePoolParams.TotalThroughputMibps),
			Iops:            *updatePoolParams.TotalIops,
			PrimaryZone:     dbPool.PoolAttributes.PrimaryZone,
			SecondaryZone:   dbPool.PoolAttributes.SecondaryZone,
			MediatorZone:    dbPool.PoolAttributes.MediatorZone,
			Labels:          dbPool.PoolAttributes.Labels,
			IsRegionalHA:    dbPool.PoolAttributes.IsRegionalHA,
		}
		// Update pool in DB to reflect QoS changes
		err = workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolFields, dbPool.UUID, map[string]interface{}{
			"pool_attributes": updatedPoolAttributes,
		}).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		dbPool.PoolAttributes.ThroughputMibps = int64(updatePoolParams.TotalThroughputMibps)
		dbPool.PoolAttributes.Iops = *updatePoolParams.TotalIops
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()
	ontapVersion := ExtractOntapVersion(pool.ClusterDetails.OntapVersion)

	updateVSAClusterDeploymentRequest := &vlm.UpdateVSAClusterDeploymentRequest{}
	prepareUpdateVSAClusterDeploymentRequest(updateVSAClusterDeploymentRequest, *currentVlmConfig, *newVlmConfig, *credentials)

	// Update VSA cluster deployment
	updateVSAClusterDeploymentResponse, err := vsaClientWorkflowManager.UpdateVSAClusterDeployment(ctx, updateVSAClusterDeploymentRequest, ontapVersion)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Execute QoS modification after deployment update if scaling up
	if isScalingUp {
		wf.Logger.Info("Scaling up detected - modifying QoS policy after deployment update")
		err = workflow.ExecuteActivity(ctx, poolActivity.ModifyQoSPolicyAndApplyToSVM, dbPool, node, updatePoolParams).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Only hydrate to CCFE if this update was triggered by auto-tiering hot tier auto-resize.
	if updatePoolParams.AutoResizeTriggeredUpdate {
		err = workflow.ExecuteActivity(ctx, poolActivity.HydrateUpdatedPoolToCCFE, pool).Get(ctx, nil)
		if err != nil {
			wf.Logger.Errorf("Failed to hydrate pool to CCFE as part of auto-tiering hot tier auto-resize, error: %v", err)
			// TODO: Add error handling for hydration failure when auto-tiering feature integration is complete
			// return nil, ConvertToVSAError(err)
		}
	}

	// Update pool with VLM config
	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatedPoolWithVLMConfig, dbPool, updateVSAClusterDeploymentResponse.VLMConfig, updatePoolParams).Get(ctx, nil)
	return nil, ConvertToVSAError(err)
}

type deletePoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on deletePoolWorkflow
var _ WorkflowInterface = &deletePoolWorkflow{}

// DeletePoolWorkflow runs delete workflow for a pool.
func DeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	deletePoolWF := new(deletePoolWorkflow)
	err := deletePoolWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	deletePoolWF.Status = WorkflowStatusRunning
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, errRun := deletePoolWF.Run(ctx, params, pool)
	if errRun != nil {
		deletePoolWF.Status = WorkflowStatusFailed
		err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), errRun)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		return nil, errRun
	}
	deletePoolWF.Status = WorkflowStatusCompleted
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return nil, nil
}

// DeletePoolWorkflowInternal runs the core delete pool logic without job management.
// This is used when called as a child workflow where the parent manages job status.
func DeletePoolWorkflowInternal(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (interface{}, error) {
	deletePoolWF := new(deletePoolWorkflow)

	// Setup without job management
	info := workflow.GetInfo(ctx)
	deletePoolWF.CustomerID = params.AccountName
	deletePoolWF.Status = WorkflowStatusCreated
	deletePoolWF.ID = info.WorkflowExecution.ID
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": deletePoolWF.ID, "customerID": deletePoolWF.CustomerID})
	logger := util.GetLogger(ctx)
	deletePoolWF.Logger = logger

	err := workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         deletePoolWF.ID,
			Status:     deletePoolWF.Status,
			CustomerID: deletePoolWF.CustomerID,
		}, nil
	})
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Run the core logic without job status updates
	deletePoolWF.Status = WorkflowStatusRunning
	_, errRun := deletePoolWF.Run(ctx, params, pool)
	if errRun != nil {
		deletePoolWF.Status = WorkflowStatusFailed
		return nil, errRun
	}
	deletePoolWF.Status = WorkflowStatusCompleted
	return nil, nil
}

func (wf *deletePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deletePoolParams := input.(*common.DeletePoolParams)
	info := workflow.GetInfo(ctx)
	wf.CustomerID = deletePoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	wf.ID = info.WorkflowExecution.ID
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

func (wf *deletePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.DeletePoolParams)
	poolActivity := &activities.PoolActivity{}
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
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: params.PoolID},
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Add the cleanup / rollback activity using this rollback.AddActivity() method instead of writing multiple defer statements,
	// this rollback manager will be invoked whenever there is an error, and it will start calling clean up activities in LIFO manner ***/
	rollbackManager.AddActivity(poolActivity.FailedPool, dbPool, "Failed to delete pool")

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletingPoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	hostMap := make(map[string]string)
	err = workflow.ExecuteActivity(ctx, poolActivity.GetCloudDNSRecords, dbPool.ID, dbPool.PoolCredentials.AuthType).Get(ctx, &hostMap)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteCloudDNSRecords, hostMap, dbPool.PoolCredentials.AuthType).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

	ontapVersion := ExtractOntapVersion(dbPool.ClusterDetails.OntapVersion)
	if ontapVersion == "" {
		ontapVersion = vlm.OntapVersion
	}

	if !disableVsaCleanupOnVLMFailure {
		deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
		prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, dbPool.DeploymentName, vlm.VLMCloudProvider, dbPool.ClusterDetails.RegionalTenantProject)
		err = vsaClientWorkflowManager.DeleteVSAClusterDeployment(ctx, deleteVSAClusterDeploymentRequest, ontapVersion)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	} else if dbPool.State != models.LifeCycleStateError {
		deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
		prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, dbPool.DeploymentName, vlm.VLMCloudProvider, dbPool.ClusterDetails.RegionalTenantProject)
		err = vsaClientWorkflowManager.DeleteVSAClusterDeployment(ctx, deleteVSAClusterDeploymentRequest, ontapVersion)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	bucketName := ""
	if dbPool.AutoTieringConfig != nil {
		bucketName = dbPool.AutoTieringConfig.BucketName
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteAutoTierBucket, bucketName, dbPool.Account.Name, dbPool.ID).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteServiceAccount, dbPool.ClusterDetails.RegionalTenantProject, dbPool.ServiceAccountId).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if !disableVsaCleanupOnVLMFailure || dbPool.State != models.LifeCycleStateError {
		if dbPool.APIAccessMode == ONTAPMode {
			err = workflow.ExecuteActivity(ctx, poolActivity.DeleteExpertModeCredentials, dbPool).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
		err = workflow.ExecuteActivity(ctx, poolActivity.DeleteOnTapCredentials, dbPool).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if ginLoggingFeatureFlag {
		err = workflow.ExecuteChildWorkflow(ctx, ReleasePSCEndpointWorkflow, dbPool).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletePoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if enableMetrics {
		// Execute Child Work to start poller on harvest farm
		childWorkflowOptions := workflow.ChildWorkflowOptions{}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)
		unregisterParams := &unRegisterNodeFromHarvestFarmParams{
			PoolID: dbPool.ID,
		}
		// If off-boarding to harvest-farm fails log warning message
		// TODO: Need to emit a metric to alert on delete pool off-boarding to harvest-farm
		childWfError := workflow.ExecuteChildWorkflow(ctx, UnRegisterNodeFromHarvestFarmWorkflow, unregisterParams).Get(childCtx, nil)
		if childWfError != nil {
			wf.Logger.Warnf("Failed to off-board poolId %d to harvest-farm due to error: %v", dbPool.ID, childWfError)
		}
	}

	if dbPool.KmsConfig != nil {
		// Check if the KMS config is reachable and update the kms appropriately i.e. from in-use to created when last pool/svm is deleted
		kmsConfigActivity := &kms_activities.KmsConfigActivity{}
		err = workflow.ExecuteActivity(ctx, kmsConfigActivity.VerifyVsaKmsReachabilityActivity, dbPool.KmsConfig.UUID).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}
	return nil, nil
}

func _verifyKmsConfigReachability(ctx workflow.Context, kmsConfigId string) error {
	if kmsConfigId == "" {
		return nil // No KMS config provided, nothing to verify
	}

	kmsConfigActivity := &kms_activities.KmsConfigActivity{}
	kmsConfig := &datamodel.KmsConfig{}

	// Get KMS config from VSA database
	err := workflow.ExecuteActivity(ctx, kmsConfigActivity.GetKmsConfigActivity, kmsConfigId).Get(ctx, kmsConfig)
	if err != nil {
		return err
	}

	// Create the service account key for the KMS configuration, if required
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateVSAKmsConfigSAKeyActivity, kmsConfig).Get(ctx, kmsConfig)
	if err != nil {
		return err
	}

	// Grant the necessary roles to the service account, if required
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.GrantRoleActivity, kmsConfig).Get(ctx, nil)
	if err != nil {
		return err
	}

	// Access a crypto key using the KMS config in the VSA database to make sure key is reachable and update the kms config state based on the reachability
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.VerifyVsaKmsReachabilityActivity, kmsConfig.UUID).Get(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

func _configureKmsConfigForSvmActivity(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
	if params.KmsConfigId == "" {
		return nil // No KMS config provided, nothing to configure
	}

	kmsConfigActivity := &kms_activities.KmsConfigActivity{}
	kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}

	err := workflow.ExecuteActivity(ctx, kmsConfigActivity.GetKmsConfigActivity, params.KmsConfigId).Get(ctx, kmsConfig)
	if err != nil {
		return err
	}

	// Creates DNS to reach google KMS from the VSA cluster
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateDnsActivity, node).Get(ctx, nil)
	if err != nil {
		return err
	}

	// Enable the ontap scheduler to take the volume offline in case the KMS key is not reachable/disabled.
	if enableAutoVolOfflineCronForGCPKMS {
		err = workflow.ExecuteActivity(ctx, kmsConfigActivity.EnableAutoVolOfflineCronForGCPKMSActivity, node).Get(ctx, nil)
		if err != nil {
			return err
		}
	}

	// Configure KMS for SVM if KMS config is provided
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.ConfigureKmsForSvmActivity, svm, node, params).Get(ctx, svm)
	if err != nil {
		return err
	}

	// Check if the KMS config is reachable from the VSA cluster
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CheckVsaKmsConfigReachableActivity, svm, node).Get(ctx, nil)
	if err != nil {
		return err
	}

	return nil
}

func _getNewVSAClientWorkflowManager() vlm.VlmWorkflowClient {
	return vlm.NewVSAClientWorkflowManager()
}

type subnetWorkflowResult struct {
	WorkflowStatus *WorkflowStatus
	TenancyDetails *common.TenancyInfo
}

// PoolDataSubnetWorkFlow processes get pr create subnet for the pool related requests from a customer.
func PoolDataSubnetWorkFlow(ctx workflow.Context, params *common.CreatePoolParams, poolUUID, tenantProjectNumber string) (gcpgenserver.V1betaDescribePoolRes, error) {
	CreateOrGetSubnetworkWF := new(poolDataSubnetWorkFlow)
	err := CreateOrGetSubnetworkWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	CreateOrGetSubnetworkWF.Status = WorkflowStatusRunning
	err = CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, err = CreateOrGetSubnetworkWF.Run(ctx, params, poolUUID, tenantProjectNumber)
	if e, ok := err.(*vsaerrors.CustomError); ok && e != nil {
		CreateOrGetSubnetworkWF.Status = WorkflowStatusFailed
		upErr := CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		if upErr != nil {
			return nil, upErr
		}
		return nil, ConvertToVSAError(err)
	}
	CreateOrGetSubnetworkWF.Status = WorkflowStatusCompleted
	err = CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	return nil, nil
}

func (wf *poolDataSubnetWorkFlow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	// Set up the query handler for the workflow status and tenancy details.
	// This will allow the caller to query the workflow status and fetch
	// tenancy details after the workflow is completed.
	return workflow.SetQueryHandler(ctx, StatusQueryName, func() (*subnetWorkflowResult, error) {
		return &subnetWorkflowResult{
			WorkflowStatus: &WorkflowStatus{
				ID:         wf.ID,
				Status:     wf.Status,
				CustomerID: wf.CustomerID,
			},
			TenancyDetails: wf.TenancyDetails,
		}, nil
	})
}

func (wf *poolDataSubnetWorkFlow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.CreatePoolParams)
	poolUUID := args[1].(string)
	tenantProjectNumber := args[2].(string)

	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
			// TODO: Add non-retryable errors.ErrPSAPeeringNotFoundError
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	subnet := new(hyperscalermodels.Subnet)
	err = workflow.ExecuteActivity(ctx, poolActivity.GetAvailableSubnet, params, tenantProjectNumber).Get(ctx, subnet)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if subnet.Name == "" {
		var operationName string
		err = workflow.ExecuteActivity(ctx, poolActivity.GetCreateDataSubnetOp, params, tenantProjectNumber).Get(ctx, &operationName)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		if operationName == "" {
			return nil, ConvertToVSAError(fmt.Errorf("failed to create subnet for tenant project: %s, operation name is empty", tenantProjectNumber))
		}
		// add retry only for Google timeout : strings.Contains(err.Error(), "Timeout while confirming service network google components")
		opSubnetInBytes, err := WaitForServiceNetworkOperationStatus(ctx, poolActivity, operationName, retryPolicy.StartToCloseTimeout)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to create subnet for tenant project while waiting to get operation status: %s: %w", tenantProjectNumber, err))
		}
		err = workflow.ExecuteActivity(ctx, poolActivity.GetSubnetFromOperation, opSubnetInBytes).Get(ctx, &subnet)
		if err != nil {
			return nil, ConvertToVSAError(fmt.Errorf("failed to get subnet from operation for tenant project: %s: %w", tenantProjectNumber, err))
		}
	}
	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetTenancyInfo, tenantProjectNumber, subnet).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolSubnet, poolUUID, tenancyDetails).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	// Adding the result to the workflow, which will be returned to the caller as Query after workflow completion
	wf.TenancyDetails = tenancyDetails

	return nil, nil
}

// SubnetActivity is a struct used for subnet related activities.
type SubnetActivity struct {
	SE database.Storage
}

var fetchTemporalClient = _fetchTemporalClient

func _fetchTemporalClient(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

// CreateSubnetJob is an activity that triggers PoolDataSubnetWorkFlow for the pool
// in a serialized way. Since we are using the SequenceWorkflow from the workflows pkg for queueing, we
// have kept the activity implementation here to avoid cyclic imports.
func (sa *SubnetActivity) CreateSubnetJob(ctx context.Context, params *common.CreatePoolParams, pool *datamodel.Pool, tenantProjectNumber string) (string, error) {
	logger := util.GetLogger(ctx)
	se := sa.SE
	temporalClient := fetchTemporalClient(ctx)
	vpcName := utils.GetVPCNameFromSubnetID(params.VendorSubNetID)

	// Use appropriate job type based on pool capacity
	poolCategory := models.GetPoolCategory(pool.LargeCapacity)
	jobType := models.GetResourceJobType(models.ResourceTypeSubnet, models.ResourceOperationCreate, poolCategory)

	job := &datamodel.Job{
		Type:          string(jobType),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name + "-subnet",
		AccountID:     sql.NullInt64{Int64: pool.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	// Create a job in the database to track the creation of subnet activity
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	// This control workflow will be common per same Account & same VPC level.
	controlWorkflowID := fmt.Sprintf(PoolSubnetCreate, pool.Account.ID, vpcName)
	err = ExecuteWorkflowSequentially(
		temporalClient,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		PoolDataSubnetWorkFlow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		params,
		pool.UUID,
		tenantProjectNumber,
	)
	if err != nil {
		logger.Errorf("Failed to start create subnet workflow for account: %s & vpc: %s, job: %s, error: %v", params.AccountName, vpcName, createdJob.UUID, err.Error())
		return "", err
	}

	return createdJob.WorkflowID, nil
}

// GetTenancyDetails retrieves the tenancy details from the completed subnet workflow.
func (sa *SubnetActivity) GetTenancyDetails(ctx context.Context, workflowID string) (*common.TenancyInfo, error) {
	temporalClient := fetchTemporalClient(ctx)

	var subnetWfRes subnetWorkflowResult
	// Sending runID as empty string will query the latest workflow run execution.
	encVal, err := temporalClient.QueryWorkflow(ctx, workflowID, "", StatusQueryName)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	err = encVal.Get(&subnetWfRes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if subnetWfRes.WorkflowStatus == nil {
		return nil, vsaerror.Errorf("subnet create workflow %s status is nil", workflowID)
	}
	if subnetWfRes.WorkflowStatus.Status != WorkflowStatusCompleted {
		return nil, vsaerror.Errorf("subnet create workflow %s is not completed, current status: %s", workflowID, subnetWfRes.WorkflowStatus.Status)
	}

	if subnetWfRes.TenancyDetails == nil {
		return nil, vsaerror.Errorf("subnet create workflow %s returned tenancy details as nil", workflowID)
	}

	return subnetWfRes.TenancyDetails, nil
}

func prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest *vlm.CreateVSAClusterDeploymentRequest, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials, pool *datamodel.Pool, resolvedLocationInfo *common.LocationInfo) {
	// resolve location assigment
	vlmConfig.Deployment.Zone = vlm.ZoneInfo{
		Zone1:        resolvedLocationInfo.PrimaryZone,
		Zone2:        resolvedLocationInfo.SecondaryZone,
		MediatorZone: resolvedLocationInfo.MediatorZone,
	}

	vlmConfig.Deployment.Images.VSAImageName = vsaImageName
	vlmConfig.Deployment.Images.MediatorImageName = mediatorImage

	if vlmConfig.Deployment.Labels == nil {
		vlmConfig.Deployment.Labels = make(map[string]string)
	}
	vlmConfig.Deployment.Labels["pool_name"] = pool.Name
	vlmConfig.Deployment.Labels["pool_uuid"] = pool.UUID
	if pool.Account != nil {
		vlmConfig.Deployment.Labels["account_id"] = pool.Account.Name
		if utils.IsFileProtocolSupported(pool.Account.Name) {
			// Set the NFS V3 support flag based on the file protocol support
			vlmConfig.Deployment.DevFlags.EnableIlbSupport = true
			vlmConfig.Deployment.Images.VSAImageName = vsaFilesImageName
			vlmConfig.Deployment.Images.MediatorImageName = filesMediatorImage
		}
	}
	createVSAClusterDeploymentRequest.VLMConfig = vlmConfig
	createVSAClusterDeploymentRequest.OntapCredentials = ontapCredentials

	// send empty secretUri if license secret path or project id is not provided -> VLM will use default legacy license
	secretUri := utils.GetNLFSecretPath()
	if secretUri != "" {
		createVSAClusterDeploymentRequest.OntapLicense = vlm.OntapLicense{
			SecretUri: []string{secretUri},
		}
	}
}

func prepareCreateVSAExpertModeReq(createVSAExpertModeRequest *vlm.OntapExpertModeUserConfig, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials, expertModeCredentials vlm.OntapCredentials, pool *datamodel.Pool) {
	createVSAExpertModeRequest.VLMConfig = vlmConfig
	createVSAExpertModeRequest.OntapCredentials = ontapCredentials
	createVSAExpertModeRequest.ExpertModeUserCredentials = expertModeCredentials
	if pool.PoolCredentials.AuthType == env.USER_CERTIFICATE {
		createVSAExpertModeRequest.AuthenticationType = "certificate"
	}
	createVSAExpertModeRequest.Username = env.ExpertModeUser
}

func prepareCreateSVMRequest(createSVMRequest *vlm.CreateSVMRequest, svmName string, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials) {
	createSVMRequest.Name = svmName
	createSVMRequest.VLMConfig = vlmConfig
	createSVMRequest.OntapCredentials = ontapCredentials
}

func prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest *vlm.DeleteVSAClusterDeploymentRequest, deploymentID string, cloudProvider string, projectID string) {
	deleteVSAClusterDeploymentRequest.DeploymentID = deploymentID
	deleteVSAClusterDeploymentRequest.ProjectID = projectID
	deleteVSAClusterDeploymentRequest.CloudProvider = cloudProvider
}

func prepareUpdateVSAClusterDeploymentRequest(updateVSAClusterDeploymentRequest *vlm.UpdateVSAClusterDeploymentRequest, currentVlmConfig vlm.VLMConfig, newVLMConfig vlm.VLMConfig, credentials vlm.OntapCredentials) {
	updateVSAClusterDeploymentRequest.VLMConfig = currentVlmConfig
	updateVSAClusterDeploymentRequest.NumHAPair = newVLMConfig.Deployment.NumHAPair
	updateVSAClusterDeploymentRequest.SPConfig = newVLMConfig.Deployment.SPConfig
	updateVSAClusterDeploymentRequest.OntapCredentials = credentials
	if newVLMConfig.Deployment.VSAInstanceType != currentVlmConfig.Deployment.VSAInstanceType {
		// If we set this all the time, VLM will trigger a VM rotation even if we use the same instance type.
		updateVSAClusterDeploymentRequest.NewInstanceType = newVLMConfig.Deployment.VSAInstanceType
	}
}

func _waitForServiceNetworkOperationStatus(ctx workflow.Context, poolActivity *activities.PoolActivity, op string, timeout time.Duration) ([]byte, error) {
	startTime := workflow.Now(ctx)
	for {
		// Check if the timeout has been reached.
		if workflow.Now(ctx).Sub(startTime) > timeout {
			return nil, vsaerror.Errorf("timeout while confirming compute network google components: %v", timeout)
		}

		// Get the status of the GCP Operation.
		operation := &hyperscalermodels.ComputeOperation{}
		err := workflow.ExecuteActivity(ctx, poolActivity.GetServiceNetOpStatus, op).Get(ctx, &operation)
		if err != nil && !vsaerror.IsNotReadyErr(err) && !vsaerror.IsNotFoundErr(err) {
			return nil, vsaerror.Errorf("failed to get GCP Operation %s: %w", op, err)
		}

		// check the state of the operation
		if operation.Done && string(operation.Response) != "" {
			return operation.Response, nil
		}

		// Sleep for a some duration before checking again.
		err = workflow.Sleep(ctx, time.Second*time.Duration(waitTimeForGCPOperationInSec))
		if err != nil {
			return nil, vsaerror.Errorf("failed to sleep while waiting for GCP Operation %s: %w", op, err)
		}
	}
}

func ReleasePSCEndpointWorkflow(ctx workflow.Context, pool *datamodel.Pool) error {
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return ConvertToVSAError(err)
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
	poolActivity := &activities.PoolActivity{}
	pscActivity := &activities.PSCActivity{}
	setupPscCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)

	if pool == nil || pool.ClusterDetails.RegionalTenantProject == "" {
		logger := util.GetLogger(ctx)
		logger.Errorf("Regional tenant project is not set for pool: %+v, unable to release PSC Endpoint.", pool)
		return vsaerror.Errorf("Regional tenant project is not set for pool: %+v, unable to release PSC Endpoint.", pool)
	}
	deleteForwardingRuleOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.DeleteForwardingRule, pool).Get(setupPscCtx, &deleteForwardingRuleOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if deleteForwardingRuleOperation == nil {
		util.GetLogger(ctx).Infof("Unable to delete forwarding rule.")
		return nil
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &deleteForwardingRuleOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to release PSC endpoint for tenant project: %s: %w", pool.ClusterDetails.RegionalTenantProject, err)
	}
	deleteAddressOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.DeleteAddress, pool).Get(setupPscCtx, &deleteAddressOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &deleteAddressOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to release PSC endpoint for tenant project: %s: %w", pool.ClusterDetails.RegionalTenantProject, err)
	}

	return nil
}

func ConfigurePSCEndpointWorkflow(ctx workflow.Context, projectName string, region string, node *models.Node) error {
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return ConvertToVSAError(err)
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
	// poolActivity is used for GCP network operations
	poolActivity := &activities.PoolActivity{}
	pscActivity := &activities.PSCActivity{}
	ctx = workflow.WithActivityOptions(ctx, ao)

	setupPscCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)

	// CreateInternalInfraSubnet
	subnetFirewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateInternalInfraSubnet, projectName).Get(setupPscCtx, &subnetFirewallOperations)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &subnetFirewallOperations, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create internal infra subnet for tenant project: %s: %w", projectName, err)
	}
	var forwardingRuleIpAddress string
	var addressURI string
	pscEndpointName := region + "-rg-fluent-bit-psc"
	createAddressOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateAddressForPSCEndpoint, projectName, region, pscEndpointName).Get(setupPscCtx, &createAddressOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &createAddressOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create PSC endpoint for tenant project: %s: %w", projectName, err)
	}
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.GetAddressURI, projectName, region, pscEndpointName).Get(setupPscCtx, &addressURI)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if addressURI == "" {
		return vsaerror.Errorf("failed to get IP address of PSC endpoint from create address operation in tenant project: %s: %w", projectName, err)
	}

	createForwardingRuleOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateForwardingRuleForPSCEndpoint, projectName, region, pscEndpointName, addressURI).Get(setupPscCtx, &createForwardingRuleOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &createForwardingRuleOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create forwarding rule subnet for tenant project: %s: %w", projectName, err)
	}
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.GetForwardingRuleIPAddress, projectName, region, pscEndpointName).Get(setupPscCtx, &forwardingRuleIpAddress)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if forwardingRuleIpAddress == "" {
		return vsaerror.Errorf("failed to get forwarding rule from operation for tenant project: %s: %w", projectName, err)
	}

	// Update audit log
	err = workflow.ExecuteActivity(ctx, pscActivity.UpdateSecurityAudit, node).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}

	// forward ontap logging to PSC Endpoint
	err = workflow.ExecuteActivity(ctx, pscActivity.CreateClusterLogForwarding, node, forwardingRuleIpAddress).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}
	return nil
}

func ConfigureNetworkWorkflow(ctx workflow.Context, tenancyDetails *common.TenancyInfo) (interface{}, error) {
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
	poolActivity := &activities.PoolActivity{}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()
	setupNwCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
	vpcOperations := make([]common.Operations, 0)
	tenantProjectNumber := tenancyDetails.RegionalTenantProject
	err = workflow.ExecuteActivity(setupNwCtx, poolActivity.CreateVPCs, tenantProjectNumber).Get(setupNwCtx, &vpcOperations)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &vpcOperations, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return nil, vsaerror.Errorf("failed to create VPC for tenant project while waiting to get operation status: %s: %w", tenantProjectNumber, err)
	}

	subnetFirewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupNwCtx, poolActivity.CreateSubnets, tenantProjectNumber).Get(setupNwCtx, &subnetFirewallOperations)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	firewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupNwCtx, poolActivity.CreateFirewalls, tenantProjectNumber, tenancyDetails.SnHostProject, tenancyDetails.Network).Get(setupNwCtx, &firewallOperations)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	subnetFirewallOperations = append(subnetFirewallOperations, firewallOperations...)
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &subnetFirewallOperations, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return nil, vsaerror.Errorf("failed to create firewall for tenant project while waiting to get operation status: %s: %w", tenantProjectNumber, err)
	}
	return nil, nil
}

func _waitForGCPNetworkOperationStatus(ctx workflow.Context, poolActivity *activities.PoolActivity, operations *[]common.Operations, timeout time.Duration) error {
	if operations == nil {
		return nil
	}
	startTime := workflow.Now(ctx)
	var err error
	var operationsDone int
	operation := &hyperscalermodels.ComputeOperation{}
	for {
		operationsDone = 0
		for i := 0; i < len(*operations); i++ {
			op := &(*operations)[i] // Get a pointer to the original element
			if !op.IsDone {
				// Check if the timeout has been reached.
				if workflow.Now(ctx).Sub(startTime) > timeout {
					return vsaerror.Errorf("timeout while confirming compute network google components: %v", timeout)
				}

				// Get the status of the GCP Operation.
				err = workflow.ExecuteActivity(ctx, poolActivity.GetComputeOpStatus, op.Project, op.IsRegionalResource, op.OperationName).Get(ctx, &operation)
				if err != nil && !vsaerror.IsNotReadyErr(err) {
					return vsaerror.Errorf("failed to get GCP Operation %s: %w", op.OperationName, err)
				}
			}
			if (operation.Status == statusDone && operation.Progress == operationProgress) || op.IsDone {
				operationsDone++
				op.IsDone = true // this modifies the original element in the slice
			}
		}
		// If all operations are done, exit the loop
		if operationsDone == len(*operations) {
			return nil
		}
		err = workflow.Sleep(ctx, time.Second*time.Duration(waitTimeForGCPOperationInSec))
		if err != nil {
			return vsaerror.Errorf("failed to sleep while waiting for GCP Operation %s: %w", operation.Name, err)
		}
	}
}

// updateAutoTieringFields updates the AutoTiering configuration fields in the pool
func updateAutoTieringFields(dbPool *datamodel.Pool, updatePoolParams *common.UpdatePoolParams, pool *datamodel.Pool) {
	if updatePoolParams.AllowAutoTiering {
		dbPool.AllowAutoTiering = true
		dbPool.AutoTieringConfig.HotTierSizeInBytes = int64(updatePoolParams.HotTierSizeInBytes)
		dbPool.AutoTieringConfig.EnableHotTierAutoResize = updatePoolParams.EnableHotTierAutoResize
	} else {
		// Keep HotTierSizeInBytes in sync with SizeInBytes when AutoTiering is disabled
		dbPool.AutoTieringConfig.HotTierSizeInBytes = int64(updatePoolParams.SizeInBytes)
	}
}

// SyncPoolComplianceForPoolWorkflow orchestrates the pool ZI/ZS compliance sync process for a single pool
func SyncPoolComplianceForPoolWorkflow(ctx workflow.Context, pool *database.PoolIdentifier) error {
	syncPoolCompliancePoolWF := new(syncPoolComplianceForPoolWorkflow)
	err := syncPoolCompliancePoolWF.Setup(ctx, pool)
	if err != nil {
		return err
	}
	syncPoolCompliancePoolWF.Status = WorkflowStatusRunning
	_, errRun := syncPoolCompliancePoolWF.Run(ctx, pool)
	if errRun != nil {
		syncPoolCompliancePoolWF.Status = WorkflowStatusFailed
		syncPoolCompliancePoolWF.Logger.Error("Failed to sync pool compliance for pool", "PoolName", pool.Name, "Error", errRun)
		return errRun
	}
	syncPoolCompliancePoolWF.Status = WorkflowStatusCompleted
	syncPoolCompliancePoolWF.Logger.Info("Sync pool compliance completed successfully for pool", "PoolName", pool.Name)
	return nil
}

type syncPoolComplianceForPoolWorkflow struct {
	BaseWorkflow
}

var _ WorkflowInterface = &syncPoolComplianceForPoolWorkflow{}

func (wf *syncPoolComplianceForPoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	pool := input.(*database.PoolIdentifier)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = strconv.FormatInt(pool.AccountID, 10)
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

func (wf *syncPoolComplianceForPoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := wf.Logger
	poolIdentifier := args[0].(*database.PoolIdentifier)

	retryPolicy, err := PopulateRetryPolicyParams(false) // Use default retry policy
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
	poolActivity := &activities.PoolActivity{}
	logger.Infof("Starting synchronization of pool compliance for pool: %s", poolIdentifier.Name)

	// Step 1: Fetch pool data
	fetchInput := activities.FetchPoolDataActivityInput{
		PoolUUID:  poolIdentifier.UUID,
		AccountID: poolIdentifier.AccountID,
	}

	var fetchResult activities.FetchPoolDataActivityOutput
	err = workflow.ExecuteActivity(ctx, poolActivity.FetchPoolData, fetchInput).Get(ctx, &fetchResult)
	if err != nil {
		logger.Error("FetchPoolData activity execution failed.", "Error", err, "PoolName", poolIdentifier.Name)
		return nil, ConvertToVSAError(err)
	}

	if !fetchResult.Success {
		logger.Error("Pool data fetch failed", "PoolName", poolIdentifier.Name, "Error", fetchResult.Error)
		return nil, &vsaerrors.CustomError{Message: fetchResult.Error}
	}

	logger.Info("Pool data fetched successfully", "PoolName", poolIdentifier.Name)

	// Step 2: Call VLM workflow to get compliance data
	logger.Info("Calling VLM workflow for compliance check", "PoolName", poolIdentifier.Name)

	// Create compliance request
	req := &vlm.GetResourceInfoReq{
		ProjectID:    fetchResult.VLMConfig.Deployment.GCPConfig.ProjectID,
		DeploymentID: fetchResult.VLMConfig.Deployment.DeploymentID,
	}
	ctx = workflow.WithValue(ctx, "AccountName", fetchResult.AccountName)
	// Call VLM workflow to get compliance data
	vlmClient := GetNewVSAClientWorkflowManager()
	complianceResponse, err := vlmClient.GetClusterZiZsDetails(ctx, req)
	if err != nil {
		logger.Error("Failed to get pool compliance from VLM", "PoolName", poolIdentifier.Name, "Error", err)
		return nil, ConvertToVSAError(err)
	}

	// Extract compliance data from response
	satisfyZI := true
	satisfyZS := true
	var assetMetadata *datamodel.AssetMetadata
	if complianceResponse.ResourceInfo.GCPRI != nil {
		// Maps asset types to lists of asset names (grouped by asset_type)
		mapsByType := make(map[string][]string)
		for _, resources := range complianceResponse.ResourceInfo.GCPRI {
			for _, resource := range resources {
				if !resource.SatisfiesPzi {
					satisfyZI = false
				}
				if !resource.SatisfiesPzs {
					satisfyZS = false
				}
				// Accumulate asset_names grouped by asset_type
				if resource.AssetType != "" && len(resource.AssetLink) > 0 {
					mapsByType[resource.AssetType] = append(mapsByType[resource.AssetType], resource.AssetLink)
				}
			}
		}
		// Construct AssetMetadata
		if len(mapsByType) > 0 {
			childAssets := make([]datamodel.ChildAsset, 0)
			for assetType, assetNames := range mapsByType {
				childAssets = append(childAssets, datamodel.ChildAsset{AssetType: assetType, AssetNames: assetNames})
			}
			assetMetadata = &datamodel.AssetMetadata{
				ChildAssets: childAssets,
			}
		}
	}

	logger.Info("VLM compliance check completed",
		"PoolName", poolIdentifier.Name,
		"satisfyZI", satisfyZI,
		"satisfyZS", satisfyZS)

	// Step 3: If AutoTiering feature is enabled as well its enabled for the pool,
	// then only fetch AT bucket compliance from GCP & include in pool compliance calculation
	if utils.AutoTieringEnabled && fetchResult.AutoTieringEnabled {
		var bucketComplianceResult datamodel.BucketDetails
		err = workflow.ExecuteActivity(ctx, poolActivity.GetBucketCompliance, fetchResult.AutoTieringBucketName).Get(ctx, &bucketComplianceResult)
		if err != nil {
			logger.Error("GetBucketCompliance for auto tiering bucket activity execution failed.", "Error", err, "PoolName", poolIdentifier.Name)
			return nil, ConvertToVSAError(err)
		}

		logger.Info("Auto tiering bucket compliance fetched,",
			"PoolName", poolIdentifier.Name,
			"BucketName", fetchResult.AutoTieringBucketName,
			"satisfyZI", bucketComplianceResult.SatisfiesPzi,
			"satisfyZS", bucketComplianceResult.SatisfiesPzs,
		)

		// Logical AND of bucket compliance with cluster compliance
		// Pool is ZI/ZS compliant only if both cluster and bucket are compliant
		satisfyZI = satisfyZI && bucketComplianceResult.SatisfiesPzi
		satisfyZS = satisfyZS && bucketComplianceResult.SatisfiesPzs
	}

	// Step 4: Update pool with compliance data
	updateInput := activities.UpdatePoolComplianceActivityInput{
		PoolUUID:      poolIdentifier.UUID,
		SatisfyZI:     satisfyZI,
		SatisfyZS:     satisfyZS,
		AssetMetadata: assetMetadata,
	}

	var updateResult activities.UpdatePoolComplianceActivityOutput
	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolCompliance, updateInput).Get(ctx, &updateResult)
	if err != nil {
		logger.Error("UpdatePoolCompliance activity execution failed.", "Error", err, "PoolName", poolIdentifier.Name)
		return nil, ConvertToVSAError(err)
	}

	if !updateResult.Success {
		logger.Error("Pool compliance update failed", "PoolName", poolIdentifier.Name, "Error", updateResult.Error)
		return nil, &vsaerrors.CustomError{Message: updateResult.Error}
	}

	logger.Info("Pool compliance sync completed successfully",
		"PoolName", poolIdentifier.Name,
		"satisfyZI", satisfyZI,
		"satisfyZS", satisfyZS)

	return nil, nil
}
