package workflows

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	logger "golang.org/x/exp/slog"
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
	// ServiceAccountUpdateTimeout is the maximum time to wait for service account state to transition from Updating to Enabled
	ServiceAccountUpdateTimeout = 10 * time.Minute
	// ServiceAccountUpdateInterval is the interval between polling attempts for service account state
	ServiceAccountUpdateInterval = 20 * time.Second
)

var (
	_                                  WorkflowInterface = &createPoolWorkflow{} // Enforcing the WorkflowInterface on createPoolWorkflow
	setupNwHeartbeatTimeout                              = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
	vmrsConfigPath                                       = env.GetString("VMRS_CONFIG_PATH", "/config/vmrs_gcp.yaml")
	maxNodesPerGroup                                     = env.GetInt("MAX_NODES_PER_GROUP", 200)
	enableMetrics                                        = env.GetBool("ENABLE_METRICS", false)
	enableUniqueSerialNumberGeneration                   = env.GetBool("ENABLE_UNIQUE_SERIAL_NUMBER_GENERATION", false)
	Region                                               = env.GetString("LOCAL_REGION", "")

	vsaImageName                 = env.GetString("VSA_IMAGE_NAME", "")
	mediatorImage                = env.GetString("VSA_MEDIATOR_IMAGE_NAME", "")
	vsaFilesImageName            = env.GetString("VSA_FILES_IMAGE_NAME", "")
	filesMediatorImage           = env.GetString("VSA_FILES_MEDIATOR_IMAGE_NAME", "")
	vsaExperimentalImageName     = env.GetString("VSA_IMAGE_EXPERIMENTAL", "")
	experimentalMediatorImage    = env.GetString("VSA_MEDIATOR_IMAGE_EXPERIMENTAL", "")
	waitTimeForGCPOperationInSec = env.GetInt("WAIT_TIME_FOR_GCP_OPERATION_IN_SEC", 10)
	parallelNumberOfNodesForITC  = env.GetInt("PARALLEL_NUMBER_OF_NODES_FOR_ITC", 4) // As of now it's 4 as per the VLM design document

	disableVsaCleanupOnVLMFailure     = env.GetBool("DISABLE_VSA_CLEANUP_ON_VLM_FAILURE", false)
	enableAutoVolOfflineCronForGCPKMS = env.GetBool("ENABLE_AUTO_VOL_OFFLINE_CRON_FOR_GCP_KMS", true)
	ginLoggingFeatureFlag             = env.GetBool("GIN_LOGGING_FEATURE", false)
	enableSyncPoolZIZS                = env.GetBool("ENABLE_SYNC_POOL_ZIZS", false)
	enableLdap                        = env.GetBool("ENABLE_LDAP", false)
	maxRetryAttemptsForSDEPollJob     = env.GetInt("MAX_RETRY_ATTEMPTS_FOR_SDE_POLL_JOB", 20)
	poolSubnetSupervisorGracePeriod   = env.GetDuration("POOL_SUBNET_SUPERVISOR_GRACE_PERIOD", 30*time.Minute)
)

const (
	DefaultSvmName    = "gcnv"
	SaIdPrefix        = "vsa-sa-"
	statusDone        = "DONE"
	operationProgress = int64(100)
	CancelSignalName  = "cancel-pool-creation"
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
	if err = createPoolWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
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
	retryPolicy, err := PopulateRetryPolicyParams(params.LargeCapacity)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
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
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)
	dbPool := pool

	rollbackManager := common.NewRollbackManager()

	// Set up cancellation handler using common framework
	cancellationHandler := common.NewWorkflowCancellationHandler(ctx, CancelSignalName, dbPool.UUID, "pool")

	defer func() {
		common.ExecuteDeferredCleanup(ctx, cancellationHandler, rollbackManager, err, wf.Logger, "pool", dbPool.UUID, nil, nil, nil)
	}()

	rollbackManager.AddActivity(poolActivity.ErroredPool, dbPool)
	rollbackManager.AddActivity(poolActivity.DeletePoolResourcesOnRollback, dbPool)

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	// Verify if KMS config is reachable before proceeding with pool creation, if present
	err = verifyKmsConfigReachability(ctx, params.KmsConfigId)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if activities.ValidateImageDigestFlag {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		var verified bool
		err = workflow.ExecuteActivity(ctx, poolActivity.ValidateImageDigest).Get(ctx, &verified)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		if !verified {
			return nil, ConvertToVSAError(vsaerrors.New("image digest verification failed"))
		}
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	tenantProjectNumber := new(string)
	err = workflow.ExecuteActivity(ctx, poolActivity.FindTenancyProject, params).Get(ctx, tenantProjectNumber)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	tenancyDetails := &common.TenancyInfo{}
	rollbackManager.AddWorkflow(workflowengine.CustomerTaskQueue, DataSubnetSequentialPoller, params, pool, tenantProjectNumber, models.ResourceOperationDelete)
	// Using REQUEST_CANCEL policy so child workflow is cancelled when parent is cancelled
	dataSubnetCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	err = workflow.ExecuteChildWorkflow(dataSubnetCtx, DataSubnetSequentialPoller, params, pool, tenantProjectNumber, models.ResourceOperationCreate).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	// persist cluster details (tenancy details - as it's required for cleaning up the resources in case of failure)
	tenancyInfo := &datamodel.ClusterDetails{
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
		SubnetNames:           tenancyDetails.SubnetworkNames,
	}
	dbPool.SnHostProject = tenancyDetails.SnHostProject
	dbPool.ClusterDetails = *tenancyInfo

	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SavePoolWithClusterDetails, dbPool, tenancyInfo).Get(dbHbCtx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	setupNwCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
	// Using REQUEST_CANCEL policy so child workflow is cancelled when parent is cancelled
	setupNwCtx = workflow.WithChildOptions(setupNwCtx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	err = workflow.ExecuteChildWorkflow(setupNwCtx, ConfigureNetworkWorkflow, tenancyDetails, params.Mode).Get(ctx, nil)
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
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
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

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	// Use the new context for the service account creation activity
	err = workflow.ExecuteActivity(saCtx, poolActivity.CreateServiceAccountWithStorageRole, tenancyDetails.RegionalTenantProject, serviceAccountID, pool.Name).Get(ctx, serviceAccount)
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
	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateAutoTierBucket, AutoTierBucketName, params.Region, tenancyDetails.RegionalTenantProject).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	credConfig := &vlm.OntapCredentials{}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateOnTapCredentials, pool).Get(ctx, &credConfig)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Note: Password is now fetched from Secret Manager when needed during certificate rotation
	// No need to store it in pool_credentials as it's already stored in Secret Manager

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
	// For expert mode pools without auto-tiering, pass empty bucket name to VLM
	// to skip bucket attachment. The bucket is still created but not attached.
	if params.Mode == common.ONTAPMode && !params.AllowAutoTiering {
		bucketName = ""
	}

	// Calculate VLM worker queue once to use for both creation and rollback
	log := util.GetLogger(ctx)
	vlmWorkerQueue := vlm.GetVLMWorkerQueue(log, pool.Account.Name)

	if vlmWorkerQueue == "" {
		return nil, ConvertToVSAError(
			vsaerrors.NewVCPError(vsaerrors.ErrWorkflowTaskQueueEmpty, fmt.Errorf("VLM worker queue cannot be empty")))
	}

	if !disableVsaCleanupOnVLMFailure {
		deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
		prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, dbPool.DeploymentName, vlm.VLMCloudProvider, tenancyDetails.RegionalTenantProject)
		rollbackManager.AddWorkflow(vlmWorkerQueue, vlm.DeleteVSAClusterDeploymentWorkflowName, deleteVSAClusterDeploymentRequest)
	}

	locationInfo := &common.LocationInfo{
		PrimaryZone:   params.PrimaryZone,
		SecondaryZone: params.SecondaryZone,
		Region:        params.Region,
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	// Use resolved zones to identify VMs and build VLM config
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, dbPool.DeploymentName, locationInfo, tenancyDetails, serviceAccount.Email, bucketName, pool.LargeCapacity).Get(ctx, vlmConfig)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
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
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		err = workflow.ExecuteActivity(ctx, poolActivity.AllocateClusterSerialNumber, createVSAClusterDeploymentRequest, params.AccountName).Get(ctx, createVSAClusterDeploymentRequest)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	// Use the pre-calculated queue to ensure consistency between creation and rollback
	createVSAClusterDeploymentResponse, err := vsaClientWorkflowManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest, vlmWorkerQueue)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateCloudDNSRecords, createVSAClusterDeploymentResponse.VLMConfig, dbPool.DeploymentName, dbPool.PoolCredentials.AuthType).Get(ctx, &hostMap)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	rollbackManager.AddActivity(poolActivity.DeleteCloudDNSRecords, hostMap, pool.PoolCredentials.AuthType)

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SaveVSANodeDetails, dbPool, createVSAClusterDeploymentResponse.VLMConfig, pool.DeploymentName, &hostMap).Get(dbHbCtx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(dbHbCtx, activities.CommonActivities.GetNode, pool.ID).Get(dbHbCtx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   pool.DeploymentName,
		OntapCredentials: pool.PoolCredentials,
	})

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	// Get intercluster LIF IPs from VLM config
	var interclusterLifIPs []string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetInterClusterLifsFromVLMConfig, createVSAClusterDeploymentResponse.VLMConfig).Get(ctx, &interclusterLifIPs)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Add PSC Endpoint
	if ginLoggingFeatureFlag {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		rollbackManager.AddWorkflow(workflowengine.CustomerTaskQueue, ReleasePSCEndpointWorkflow, dbPool)
		setupPSCEndpoint := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
		// Using REQUEST_CANCEL policy so child workflow is cancelled when parent is cancelled
		setupPSCEndpoint = workflow.WithChildOptions(setupPSCEndpoint, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		err = workflow.ExecuteChildWorkflow(setupPSCEndpoint, ConfigurePSCEndpointWorkflow, tenancyDetails.RegionalTenantProject, params.Region, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Create pool build info with current image details
	poolBuildInfo := &datamodel.PoolBuildInfo{
		VSABuildImage:      vlmConfig.Deployment.Images.VSAImageName,
		MediatorBuildImage: vlmConfig.Deployment.Images.MediatorImageName,
		OntapVersion:       utils.GetOntapVersionBasedOnAllowlisting(pool.Account.Name),
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
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SavePoolWithClusterDetails, dbPool, clusterDetails).Get(dbHbCtx, nil)
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
			err1 = workflow.ExecuteActivity(dbHbCtx, poolActivity.UpdatePoolFields, dbPool.UUID, map[string]interface{}{
				"cluster_details": clusterDetails,
			}).Get(dbHbCtx, nil)
			if err1 != nil {
				wf.Logger.Errorf("Failed to save IPs consumed by deployment in the alloted subnet in DB, error: %v", err1)
			}
		} else {
			wf.Logger.Debugf("No subnet to IPs reserved found for pool %s", dbPool.Name)
		}
	}()

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	svmName := ""
	err = workflow.ExecuteActivity(ctx, poolActivity.AllocateSVMName, dbPool).Get(ctx, &svmName)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	createSVMRequest := &vlm.CreateSVMRequest{}
	prepareCreateSVMRequest(createSVMRequest, svmName, createVSAClusterDeploymentResponse.VLMConfig, *credConfig)
	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	createSVMResponse, err := vsaClientWorkflowManager.CreateVSASVM(ctx, createSVMRequest)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}

	svm := &datamodel.Svm{}
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SaveSVMAndLifData, dbPool, createSVMResponse.VLMConfig, svmName).Get(dbHbCtx, svm)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	// Create QoS policy and apply it to the SVM if qos type is auto
	if pool.QosType == utils.QosTypeAuto {
		err = workflow.ExecuteActivity(ctx, poolActivity.CreateQoSPolicyAndApplyToSVM, dbPool, svm, node).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	expertCredConfig := &vlm.OntapCredentials{}
	if params.Mode == common.ONTAPMode {
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		rbacFileDetails := &hyperscalermodels.BucketFileDetails{}

		err = workflow.ExecuteActivity(ctx, poolActivity.GetRbacHash, dbPool.BuildInfo.OntapVersion).Get(ctx, &rbacFileDetails)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(ctx, poolActivity.ValidateRbacHash, dbPool.BuildInfo.OntapVersion, rbacFileDetails).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if len(pool.ExpertModeCredentials.ExpertModeCredential) == 0 || pool.ExpertModeCredentials.ExpertModeCredential[0].Username == "" {
			return nil, ConvertToVSAError(vsaerrors.New("expert mode username not found in request"))
		}
		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		err = workflow.ExecuteActivity(ctx, poolActivity.CreateExpertModeCredentials, pool, pool.DeploymentName, pool.ExpertModeCredentials.ExpertModeCredential[0].Username).Get(ctx, &expertCredConfig)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		if !disableVsaCleanupOnVLMFailure {
			rollbackManager.AddActivity(poolActivity.DeleteExpertModeCredentials, pool)
		}

		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		createVSAExpertModeReq := &vlm.OntapExpertModeUserConfig{}
		err = workflow.ExecuteActivity(ctx, poolActivity.PrepareCreateVSAExpertModeReq, createVSAClusterDeploymentResponse.VLMConfig, *credConfig, *expertCredConfig, dbPool, rbacFileDetails).Get(ctx, &createVSAExpertModeReq)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		ontapExpertModeUserResponse, err := vsaClientWorkflowManager.CreateVSAExpertModeUser(ctx, createVSAExpertModeReq)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
			return nil, cancelErr
		}
		rbacFileDetails.FileHashSHA256 = ontapExpertModeUserResponse.RbacFileChecksum
		err = workflow.ExecuteActivity(ctx, poolActivity.UpdateRbacCheckSumInPool, dbPool, rbacFileDetails).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	// Enable KMS for SVM if KMS config is provided
	err = configureKmsConfigForSvmActivity(ctx, *dbPool, node, svm, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	if cancelErr := cancellationHandler.CheckCancellationSignal(ctx); cancelErr != nil {
		return nil, cancelErr
	}
	// Sync Active Directory from CVP to VCP if AD exists in CVP but not in VCP
	if params.ActiveDirectoryId != "" && !params.ADExistsInVCP {
		adSyncParams := adSyncInput{
			ActiveDirectoryID: params.ActiveDirectoryId,
			AccountName:       params.AccountName,
			Region:            params.Region,
			XCorrelationID:    params.XCorrelationID,
			ActiveDirectory:   params.ActiveDirectory,
			LargeCapacity:     params.LargeCapacity,
		}

		err = syncActiveDirectoryInVcp(ctx, adSyncParams, dbPool)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.CreatedPool, dbPool, &createSVMResponse.VLMConfig).Get(dbHbCtx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Set wafl.maxvolclonehier option on the ONTAP cluster
	if thinCloneGASupport {
		setMaxVolCloneErr := workflow.ExecuteActivity(ctx, poolActivity.SetWaflMaxVolCloneHier, node, dbPool).Get(ctx, nil)
		if setMaxVolCloneErr != nil {
			wf.Logger.Errorf("Failed to set wafl.maxvolclonehier (non-critical, continuing): %v", setMaxVolCloneErr)
		}
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

		if childWfErr := _startRegisterNodeToHarvestFarmChild(ctx, dbPool, registerNodeToHarvestFarmWorkflowInput); childWfErr != nil {
			wf.Logger.Warnf("Failed to on-board poolId %d to harvest-farm due to error: %v", dbPool.ID, childWfErr)
		}
	}
	return nil, nil
}

// _startRegisterNodeToHarvestFarmChild starts the register-node-to-harvest-farm child workflow with a deterministic WorkflowID
// (register-node-to-harvest-farm-{poolUUID}-{accountID}) so that workflow replay does not fail. Non-deterministic IDs
// would cause replay non-determinism errors.
func _startRegisterNodeToHarvestFarmChild(ctx workflow.Context, dbPool *datamodel.Pool, input RegisterNodeToHarvestFarmWorkflowInput) error {
	childWorkflowID := fmt.Sprintf("register-node-to-harvest-farm-%s-%d", dbPool.UUID, dbPool.AccountID)
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: childWorkflowID,
		TaskQueue:  workflowengine.CustomerTaskQueue,
	})
	return workflow.ExecuteChildWorkflow(ctx, RegisterNodeToHarvestFarmWorkflow, input).Get(ctx, nil)
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
			WorkflowID:            fmt.Sprintf("sync-pool-zizs-%s-%d", dbPool.UUID, dbPool.AccountID),
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
	if err = updatePoolWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
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
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)
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

	// Check if expert mode pool is enabling auto-tiering (needs VLM call for bucket attachment)
	needsBucketAttachment := pool.APIAccessMode == common.ONTAPMode && updatePoolParams.AllowAutoTiering && !pool.AllowAutoTiering

	// if there is no need of vlm workflow, just perform update pool in db
	// Note: For expert mode pools enabling auto-tiering, we must call VLM to attach the bucket
	if currentProvisionedSize == int64(toProvisionPoolSizeInBytes) &&
		dbPool.PoolAttributes.ThroughputMibps == int64(updatePoolParams.TotalThroughputMibps) &&
		dbPool.PoolAttributes.Iops == *updatePoolParams.TotalIops && autoScalingParams == nil &&
		!needsBucketAttachment {
		if dbPool.Description != updatePoolParams.Description {
			dbPool.Description = updatePoolParams.Description
		}
		if updatePoolParams.Labels != nil {
			dbPool.PoolAttributes.Labels = updatePoolParams.Labels
		}

		// Always update SizeInBytes for metadata/billing (even with AutoTiering)
		dbPool.SizeInBytes = int64(updatePoolParams.SizeInBytes)

		// Update AutoTiering configuration
		updateAutoTieringFields(dbPool, updatePoolParams)

		rollbackManager.AddActivity(poolActivity.UpdatedPool, pool)
		err = workflow.ExecuteActivity(dbHbCtx, poolActivity.UpdatedPool, dbPool).Get(dbHbCtx, nil)
		return nil, ConvertToVSAError(err)
	}

	bucketName := ""
	if pool.AutoTieringConfig != nil {
		bucketName = pool.AutoTieringConfig.BucketName
	}

	// Set bucket name for VLM update if we need to attach bucket for expert mode pool enabling auto-tiering
	bucketNameForVLMUpdate := ""
	if needsBucketAttachment {
		bucketNameForVLMUpdate = bucketName
		wf.Logger.Info("Expert mode pool enabling auto-tiering - will attach bucket to ONTAP", "bucketName", bucketNameForVLMUpdate)
	}

	saEmail := utils.ConstructServiceAccountEmail(pool.ServiceAccountId, pool.ClusterDetails.RegionalTenantProject)

	// Retrieve the last known VLM config that was shared with us.
	currentVlmConfig := &vlm.VLMConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.ParseVlmConfig, pool).Get(ctx, &currentVlmConfig)
	if err != nil {
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
	err = workflow.ExecuteActivity(dbHbCtx, activities.CommonActivities.GetNode, pool.ID).Get(dbHbCtx, &dbNodes)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	node := hyperscaler.CreateNodeForProvider(hyperscaler.NodeProviderInput{
		Nodes:            dbNodes,
		DeploymentName:   pool.DeploymentName,
		OntapCredentials: pool.PoolCredentials,
	})

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
			LdapEnabled:     dbPool.PoolAttributes.LdapEnabled,
			AccountName:     getPoolAttributesAccountName(dbPool.PoolAttributes),
		}
		// Update pool in DB to reflect QoS changes
		err = workflow.ExecuteActivity(dbHbCtx, poolActivity.UpdatePoolFields, dbPool.UUID, map[string]interface{}{
			"pool_attributes": updatedPoolAttributes,
		}).Get(dbHbCtx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		dbPool.PoolAttributes.ThroughputMibps = int64(updatePoolParams.TotalThroughputMibps)
		dbPool.PoolAttributes.Iops = *updatePoolParams.TotalIops
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()
	ontapVersion := ExtractOntapVersion(pool.BuildInfo.OntapVersion)

	// Calculate batch plan using activity
	batchPlanInput := &activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  newVlmConfig.Deployment.NumHAPair,
		ParallelNumberOfNodesForITC: parallelNumberOfNodesForITC,
	}
	var batchPlan *activities.CalculateBatchPlanActivityOutput
	err = workflow.ExecuteActivity(ctx, poolActivity.CalculateBatchPlanForUpdate, batchPlanInput).Get(ctx, &batchPlan)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Execute batch updates using reusable function
	updateVSAClusterDeploymentResponse, err := executePoolBatchUpdates(ctx, batchPlan, currentVlmConfig, newVlmConfig, credentials, ontapVersion, vsaClientWorkflowManager, wf.Logger, bucketNameForVLMUpdate)
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

	// Update nodes table with new instance type if it changed
	if currentInstanceType != newInstanceType {
		wf.Logger.Info("Instance type changed - updating nodes table", "from", currentInstanceType, "to", newInstanceType)
		err = workflow.ExecuteActivity(ctx, poolActivity.UpdateNodesInstanceTypeActivity, dbPool.ID, newInstanceType).Get(ctx, nil)
		if err != nil {
			wf.Logger.Errorf("Failed to update nodes instance type in database: %v", err)
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

	if updatePoolParams.ActiveDirectoryConfigId != "" && !updatePoolParams.IfADExistsInVCP {
		adLargeCapacity := false
		if updatePoolParams.LargeCapacity != nil {
			adLargeCapacity = *updatePoolParams.LargeCapacity
		}
		adSyncParams := adSyncInput{
			ActiveDirectoryID: updatePoolParams.ActiveDirectoryConfigId,
			AccountName:       updatePoolParams.AccountName,
			Region:            updatePoolParams.Region,
			XCorrelationID:    updatePoolParams.XCorrelationID,
			ActiveDirectory:   updatePoolParams.ActiveDirectory,
			LargeCapacity:     adLargeCapacity,
		}
		if adSyncParams.ActiveDirectory == nil {
			return nil, ConvertToVSAError(vsaerrors.New("ActiveDirectory is nil, cannot sync"))
		}
		if err = syncActiveDirectoryInVcp(ctx, adSyncParams, dbPool); err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Update pool with VLM config
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.UpdatedPoolWithVLMConfig, dbPool, updateVSAClusterDeploymentResponse.VLMConfig, updatePoolParams).Get(dbHbCtx, nil)
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
	if err = deletePoolWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
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
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)
	hyperscalerCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: utils.GetStartToCloseTimeoutHyperscaler(),
		HeartbeatTimeout:    utils.GetHeartbeatTimeoutForHyperscaler(),
		RetryPolicy:         utils.GetHyperscalerLRORetryPolicy(),
	})

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
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.GetPool, dbPool).Get(dbHbCtx, &dbPool)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Handle cancellation only if pool is in CREATING state
	cancellationActivity := &activities.CancellationActivity{}
	commonActivity := &activities.CommonActivities{}
	ackTimeout, forceTimeout := common.GetCancellationTimeouts("POOL")
	if cancelErr := common.HandleCancellationForCreatingResource(ctx, wf.Logger,
		common.HandleCancellationForCreatingResourceParams{
			ResourceUUID:               dbPool.UUID,
			ResourceState:              dbPool.State,
			CreateJobType:              models.JobTypeCreatePool,
			SignalName:                 CancelSignalName,
			CancellationAckTimeout:     ackTimeout,
			ForceTerminationAckTimeout: forceTimeout,
		},
		poolActivity.GetCreateJobByResourceUUID,
		cancellationActivity,
		commonActivity,
	); cancelErr != nil {
		wf.Logger.Warnf("Error handling cancellation: %v, proceeding with deletion", cancelErr)
	}

	// Add the cleanup / rollback activity using this rollback.AddActivity() method instead of writing multiple defer statements,
	// this rollback manager will be invoked whenever there is an error, and it will start calling clean up activities in LIFO manner ***/
	rollbackManager.AddActivity(poolActivity.FailedPool, dbPool, "Failed to delete pool")

	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.DeletingPoolResources, dbPool).Get(dbHbCtx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// For pools in CREATING state, skip cleanup activities that require resources that haven't been created yet
	hasDeploymentName := dbPool.DeploymentName != ""
	hasClusterDetails := dbPool.ClusterDetails.RegionalTenantProject != ""
	hasPoolCredentials := dbPool.PoolCredentials != nil

	// Only perform DNS cleanup if pool credentials exist (indicates pool was at least partially created)
	if hasPoolCredentials {
		hostMap := make(map[string]string)
		err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.GetCloudDNSRecords, dbPool.ID, dbPool.PoolCredentials.AuthType).Get(ctx, &hostMap)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.DeleteCloudDNSRecords, hostMap, dbPool.PoolCredentials.AuthType).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Only perform VSA cluster deployment cleanup if deployment name and cluster details exist
	if hasDeploymentName && hasClusterDetails {
		vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

		var ontapVersion string
		if dbPool == nil || dbPool.BuildInfo == nil {
			ontapVersion = utils.ExtractOntapVersion(utils.GetOntapVersionBasedOnAllowlisting(dbPool.Account.Name))
		} else {
			ontapVersion = ExtractOntapVersion(dbPool.BuildInfo.OntapVersion)
		}
		if ontapVersion == "" {
			ontapVersion = utils.ExtractOntapVersion(utils.GetOntapVersionBasedOnAllowlisting(dbPool.Account.Name))
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
	}

	bucketName := ""
	if dbPool.AutoTieringConfig != nil {
		bucketName = dbPool.AutoTieringConfig.BucketName
	}

	err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.DeleteAutoTierBucket, bucketName, dbPool.Account.Name, dbPool.ID).Get(ctx, nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	// Only delete service account if cluster details exist
	if hasClusterDetails && dbPool.ServiceAccountId != "" {
		saRetryPolicy, err := populateServiceAccountRetryPolicyParams()
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// Create custom activity options for service account deletion
		saActivityOptions := workflow.ActivityOptions{
			StartToCloseTimeout: saRetryPolicy.StartToCloseTimeout,
			HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        saRetryPolicy.InitialInterval,
				BackoffCoefficient:     saRetryPolicy.BackoffCoefficient,
				MaximumInterval:        saRetryPolicy.MaximumInterval,
				MaximumAttempts:        int32(saRetryPolicy.MaximumAttempts),
				NonRetryableErrorTypes: []string{"PanicError"},
			},
		}

		saCtx := workflow.WithActivityOptions(ctx, saActivityOptions)

		err = workflow.ExecuteActivity(saCtx, poolActivity.DeleteServiceAccount, dbPool.ClusterDetails.RegionalTenantProject, dbPool.ServiceAccountId).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		// cleanup service account roles and permissions as an orphan child workflow
		// This prevents orphaned IAM policies that would otherwise persist for 60 days
		cleanupCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_ABANDON,
			WorkflowExecutionTimeout: workflowengine.GetWorkflowGlobalTimeout(),
		})

		childWorkflowFuture := workflow.ExecuteChildWorkflow(cleanupCtx, CleanupServiceAccountPermissionsWorkflow, dbPool, retryPolicy)

		if err := childWorkflowFuture.GetChildWorkflowExecution().Get(cleanupCtx, &workflow.Execution{}); err != nil {
			wf.Logger.Warnf("Failed to start cleanup IAM permissions workflow for pool %s: %v", dbPool.UUID, err)
		}
	}

	if !disableVsaCleanupOnVLMFailure || dbPool.State != models.LifeCycleStateError {
		if dbPool.APIAccessMode == common.ONTAPMode {
			err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.DeleteExpertModeCredentials, dbPool).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
		if hasPoolCredentials {
			err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.DeleteOnTapCredentials, dbPool).Get(ctx, nil)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
		}
	}

	if ginLoggingFeatureFlag {
		err = workflow.ExecuteChildWorkflow(hyperscalerCtx, ReleasePSCEndpointWorkflow, dbPool).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	// Only execute data subnet cleanup if cluster details exist
	if hasClusterDetails {
		err = workflow.ExecuteChildWorkflow(hyperscalerCtx, DataSubnetSequentialPoller, params, dbPool, dbPool.ClusterDetails.RegionalTenantProject, models.ResourceOperationDelete).Get(ctx, nil)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
	}

	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.DeletePoolResources, dbPool).Get(dbHbCtx, nil)
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
		err = workflow.ExecuteActivity(hyperscalerCtx, kmsConfigActivity.VerifyVsaKmsReachabilityActivity, dbPool.KmsConfig.UUID, false).Get(ctx, nil)
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
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.VerifyVsaKmsReachabilityActivity, kmsConfig.UUID, true).Get(ctx, nil)
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

	// Check if service account is in UPDATING state and wait for it to transition to ENABLED
	logger := workflow.GetLogger(ctx)

	// Get KMS config to check initial state
	err := workflow.ExecuteActivity(ctx, kmsConfigActivity.GetKmsConfigActivity, params.KmsConfigId).Get(ctx, kmsConfig)
	if err != nil {
		return err
	}

	if kmsConfig.ServiceAccount != nil && kmsConfig.ServiceAccount.State == models.LifeCycleStateUpdating {
		timeout, interval := ServiceAccountUpdateTimeout, ServiceAccountUpdateInterval
		deadline := workflow.Now(ctx).Add(timeout)
		serviceAccountUUID := kmsConfig.ServiceAccount.UUID

		logger.Info("Service account is in Updating state, waiting for it to transition to Enabled", "serviceAccountUUID", serviceAccountUUID)

		stateChanged := false
		for workflow.Now(ctx).Before(deadline) {
			if err = workflow.Sleep(ctx, interval); err != nil {
				return err
			}

			polledKmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}
			err := workflow.ExecuteActivity(ctx, kmsConfigActivity.GetKmsConfigActivity, params.KmsConfigId).Get(ctx, polledKmsConfig)
			if err != nil {
				logger.Warn("Failed to fetch KMS config while waiting for service account state change", "error", err)
				continue
			}

			if polledKmsConfig.ServiceAccount != nil {
				if polledKmsConfig.ServiceAccount.State == models.AccountStateEnabled {
					logger.Info("Service account has transitioned to Enabled state", "serviceAccountUUID", serviceAccountUUID)
					kmsConfig.ServiceAccount.State = models.AccountStateEnabled
					stateChanged = true
					break
				}
				logger.Debug("Service account still in state, waiting...", "serviceAccountUUID", serviceAccountUUID, "state", polledKmsConfig.ServiceAccount.State)
			}
		}

		if !stateChanged {
			logger.Error("Service account for KMS Config did not transition to Enabled state during pool creation within timeout period, continuing with pool creation")
		}
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
func PoolDataSubnetWorkFlow(ctx workflow.Context, params *common.CreatePoolParams, poolUUID, tenantProjectNumber string, accountID int64, actionType models.ResourceOperation) (gcpgenserver.V1betaDescribePoolRes, error) {
	CreateOrGetSubnetworkWF := new(poolDataSubnetWorkFlow)
	err := CreateOrGetSubnetworkWF.Setup(ctx, params)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	if err = CreateOrGetSubnetworkWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return nil, ConvertToVSAError(err)
	}
	CreateOrGetSubnetworkWF.Status = WorkflowStatusRunning
	err = CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	_, err = CreateOrGetSubnetworkWF.Run(ctx, params, poolUUID, tenantProjectNumber, accountID, actionType)
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

func getStartToCloseTimeoutDataSubnet(actionType models.ResourceOperation) string {
	if actionType == models.ResourceOperationCreate {
		return StartToCloseTimeoutDataSubnetCreate
	}
	return StartToCloseTimeoutDataSubnetDelete
}

func (wf *poolDataSubnetWorkFlow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.CreatePoolParams)
	poolUUID := args[1].(string)
	tenantProjectNumber := args[2].(string)
	accountID := args[3].(int64)
	actionType := args[4].(models.ResourceOperation)

	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	// Parse the configurable timeout for GetCreateDataSubnetOp (long-running subnet creation)
	subnetOpsTimeout, err := time.ParseDuration(getStartToCloseTimeoutDataSubnet(actionType))
	if err != nil {
		// Fallback to default 20 minutes if parsing fails
		subnetOpsTimeout = 20 * time.Minute
	}
	defaultActivityTimeout, err := time.ParseDuration(StartToCloseTimeoutDataSubnetActivities)
	if err != nil {
		// Fallback to default 5 minutes if parsing fails
		defaultActivityTimeout = 5 * time.Minute
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: defaultActivityTimeout,
		HeartbeatTimeout:    utils.GetHeartbeatTimeoutForHyperscaler(),
		RetryPolicy:         utils.GetHyperscalerRetryPolicy(),
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	lroRetryPolicy := utils.GetHyperscalerLRORetryPolicy()
	subnetCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: subnetOpsTimeout,
		HeartbeatTimeout:    subnetOpsTimeout / 2,
		RetryPolicy:         lroRetryPolicy,
	})
	switch actionType {
	case models.ResourceOperationCreate:
		subnet := new(hyperscalermodels.Subnet)
		err = workflow.ExecuteActivity(ctx, poolActivity.GetAvailableSubnet, params, tenantProjectNumber).Get(ctx, subnet)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}

		if subnet.Name == "" {
			var operationName string
			err = workflow.ExecuteActivity(subnetCtx, poolActivity.GetCreateDataSubnetOp, params, tenantProjectNumber).Get(ctx, &operationName)
			if err != nil {
				return nil, ConvertToVSAError(err)
			}
			if operationName == "" {
				return nil, ConvertToVSAError(fmt.Errorf("failed to create subnet for tenant project: %s, operation name is empty", tenantProjectNumber))
			}
			// add retry only for Google timeout : strings.Contains(err.Error(), "Timeout while confirming service network google components")
			opSubnetInBytes, err := WaitForServiceNetworkOperationStatus(subnetCtx, poolActivity, operationName, retryPolicy.StartToCloseTimeout)
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

	case models.ResourceOperationDelete:
		// check the cases thoroughly when the accountID is empty like in case of delete pool
		dbPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: poolUUID}, AccountID: accountID}
		err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		deleteSubnetOp := make([]common.Operations, 0)
		err = workflow.ExecuteActivity(subnetCtx, poolActivity.ReleaseDataSubnetOp, dbPool).Get(ctx, &deleteSubnetOp)
		if err != nil {
			return nil, ConvertToVSAError(err)
		}
		err = WaitForGCPNetworkOperationStatus(subnetCtx, poolActivity, &deleteSubnetOp, retryPolicy.StartToCloseTimeout)
		if err != nil {
			return nil, ConvertToVSAError(vsaerror.Errorf("failed to release data subnet for pool: %s project: %s with error : %w", dbPool.Name, dbPool.Account.Name, err))
		}
	default:
		// throw error for invalid action type
		return nil, ConvertToVSAError(fmt.Errorf("invalid action type for pool data subnet workflow. Please send either Create or Delete. Current actionType: %s. poolUUID: %s", actionType, poolUUID))
	}
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

// DataSubnetSequentialPoller is a workflow that polls for the completion of subnet creation or deletion jobs. Hence making sure only one subnet operation
// is in progress for a given account and VPC at any given time.
// This is important because concurrent subnet creation or deletion requests for the same VPC causes race conditions and failures.
// This workflow is invoked as a child workflow from Pool creation or deletion workflows.
func DataSubnetSequentialPoller(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool, tenantProjectNumber string, actionType models.ResourceOperation) (*common.TenancyInfo, error) {
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: utils.GetStartToCloseTimeoutHyperscaler(),
		HeartbeatTimeout:    utils.GetHeartbeatTimeoutForHyperscaler(),
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	subnetActivity := SubnetActivity{}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	logger := util.GetLogger(ctx)
	createDeleteSubnetJobUUID := new(string)
	err = workflow.ExecuteActivity(ctx, subnetActivity.CreateDeleteDataSubnetJob, params, pool, tenantProjectNumber, actionType).Get(ctx, createDeleteSubnetJobUUID)
	if err != nil {
		logger.Errorf("Failed to start %s subnet workflow for account: %s, pool name: %s vpc: %s, error: %v", actionType, params.AccountName, params.Name, params.VendorSubNetID, err)
		return nil, ConvertToVSAError(err)
	}

	// Wait for the subnet creation job to complete using workflow.sleep.
	err = PollOnDBJob(ctx, *createDeleteSubnetJobUUID, retryPolicy.StartToCloseTimeout)
	if err != nil {
		logger.Errorf("Failed to wait for create subnet job %s to complete, error: %s", *createDeleteSubnetJobUUID, err.Error())
		return nil, vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if actionType == models.ResourceOperationCreate {
		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, subnetActivity.GetTenancyDetails, createDeleteSubnetJobUUID).Get(ctx, &tenancyDetails)
		if err != nil {
			logger.Errorf("Failed to get tenancy details for job %s, error: %v", *createDeleteSubnetJobUUID, err)
			return nil, ConvertToVSAError(err)
		}
		return tenancyDetails, nil
	}
	return nil, nil
}

// CreateDeleteDataSubnetJob is an activity that triggers PoolDataSubnetWorkFlow for the pool
// in a serialized way. Since we are using the SequenceWorkflow from the workflows pkg for queueing, we
// have kept the activity implementation here to avoid cyclic imports.
func (sa *SubnetActivity) CreateDeleteDataSubnetJob(ctx context.Context, params *common.CreatePoolParams, pool *datamodel.Pool, tenantProjectNumber string, actionType models.ResourceOperation) (string, error) {
	logger := util.GetLogger(ctx)
	se := sa.SE
	temporalClient := fetchTemporalClient(ctx)
	vpcName := utils.GetVPCNameFromSubnetID(params.VendorSubNetID)

	// Use appropriate job type based on pool capacity
	poolCategory := models.GetPoolCategory(pool.LargeCapacity)
	jobType := models.GetResourceJobType(models.ResourceTypeSubnet, actionType, poolCategory)

	activity.RecordHeartbeat(ctx, "Creating subnet job in database")
	job := &datamodel.Job{
		Type:          string(jobType),
		State:         string(models.JobsStateNEW),
		ResourceName:  pool.Name + "-subnet",
		AccountID:     sql.NullInt64{Int64: pool.Account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
		JobAttributes: &datamodel.JobAttributes{
			SupervisorAttributes: &datamodel.SupervisorAttributes{
				OverrideGracePeriod: poolSubnetSupervisorGracePeriod,
			},
		},
	}
	// Create a job in the database to track the creation of subnet activity
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}
	logger.Infof("correlationID: %s, requestID: %s - Creating subnet job for account: %s & vpc: %s, pool name : %s", job.CorrelationID, job.RequestID, params.AccountName, vpcName, params.Name)

	// controlWorkflowID defines the workflow ID for the control workflow
	// This control workflow will be common per same Account & same VPC level.
	controlWorkflowID := fmt.Sprintf(PoolDataSubnetCreateDelete, pool.Account.ID, vpcName)
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Executing subnet workflow sequentially for account: %s, vpc: %s", params.AccountName, vpcName))
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
		pool.Account.ID,
		actionType,
	)
	if err != nil {
		logger.Errorf("Failed to start create subnet workflow for account: %s & vpc: %s, job: %s, error: %v", params.AccountName, vpcName, createdJob.UUID, err.Error())
		return "", err
	}

	activity.RecordHeartbeat(ctx, "CreateDeleteDataSubnetJob activity completed successfully")
	return createdJob.WorkflowID, nil
}

// GetTenancyDetails retrieves the tenancy details from the completed subnet workflow.
func (sa *SubnetActivity) GetTenancyDetails(ctx context.Context, workflowID string) (*common.TenancyInfo, error) {
	temporalClient := fetchTemporalClient(ctx)

	var subnetWfRes subnetWorkflowResult
	// Sending runID as empty string will query the latest workflow run execution.
	activity.RecordHeartbeat(ctx, fmt.Sprintf("Querying workflow %s for tenancy details", workflowID))
	encVal, err := temporalClient.QueryWorkflow(ctx, workflowID, "", StatusQueryName)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	activity.RecordHeartbeat(ctx, "Decoding workflow query result")
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

	activity.RecordHeartbeat(ctx, "GetTenancyDetails activity completed successfully")
	return subnetWfRes.TenancyDetails, nil
}

func prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest *vlm.CreateVSAClusterDeploymentRequest, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials, pool *datamodel.Pool, resolvedLocationInfo *common.LocationInfo) {
	log := util.GetLogger(context.Background())
	// resolve location assigment
	vlmConfig.Deployment.Zone = vlm.ZoneInfo{
		Zone1:        resolvedLocationInfo.PrimaryZone,
		Zone2:        resolvedLocationInfo.SecondaryZone,
		MediatorZone: resolvedLocationInfo.MediatorZone,
	}

	// Set default images
	vlmConfig.Deployment.Images.VSAImageName = vsaImageName
	vlmConfig.Deployment.Images.MediatorImageName = mediatorImage

	if vlmConfig.Deployment.Labels == nil {
		vlmConfig.Deployment.Labels = make(map[string]string)
	}
	vlmConfig.Deployment.Labels["pool_name"] = pool.Name
	vlmConfig.Deployment.Labels["pool_uuid"] = pool.UUID
	if pool.Account != nil {
		vlmConfig.Deployment.Labels["account_id"] = pool.Account.Name
		ontapVersion := ExtractOntapVersion(utils.GetOntapVersionBasedOnAllowlisting(pool.Account.Name))
		if utils.IsOntapVersionGreaterOrEqual(ontapVersion, env.FileSupportOntapVersion) && (pool.APIAccessMode == common.ONTAPMode) || (utils.IsFileProtocolSupportedV2(ontapVersion) && pool.LargeCapacity) {
			// Set the NFS V3 support flag based on file support
			vlmConfig.Deployment.DevFlags.EnableIlbSupport = true
			vlmConfig.Deployment.DeploymentConfigFlags.EnableNfsV364BitIdentifier = "true"
		} else {
			log.Debugf("File support is disabled for pool: %s", pool.Name)
		}

		// 2. Image selection is based on account allowlisting (independent of file support)
		if utils.IsAccountAllowlisted(pool.Account.Name) {
			// Use experimental images if account is allowlisted
			if vsaExperimentalImageName != "" {
				vlmConfig.Deployment.Images.VSAImageName = vsaExperimentalImageName
				log.Debugf("Using experimental VSA image for allowlisted account: %s", pool.Account.Name)
			}
			if experimentalMediatorImage != "" {
				vlmConfig.Deployment.Images.MediatorImageName = experimentalMediatorImage
				log.Debugf("Using experimental mediator image for allowlisted account: %s", pool.Account.Name)
			}
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

func prepareUpdateVSAClusterDeploymentRequest(updateVSAClusterDeploymentRequest *vlm.UpdateVSAClusterDeploymentRequest, currentVlmConfig vlm.VLMConfig, newVLMConfig vlm.VLMConfig, credentials vlm.OntapCredentials, bucketName string) {
	updateVSAClusterDeploymentRequest.VLMConfig = currentVlmConfig
	updateVSAClusterDeploymentRequest.NumHAPair = newVLMConfig.Deployment.NumHAPair
	updateVSAClusterDeploymentRequest.SPConfig = newVLMConfig.Deployment.SPConfig
	updateVSAClusterDeploymentRequest.OntapCredentials = credentials
	if newVLMConfig.Deployment.VSAInstanceType != currentVlmConfig.Deployment.VSAInstanceType {
		// If we set this all the time, VLM will trigger a VM rotation even if we use the same instance type.
		updateVSAClusterDeploymentRequest.NewInstanceType = newVLMConfig.Deployment.VSAInstanceType
	}
	// Set bucket name for auto-tiering attachment (used when enabling auto-tiering on expert mode pools)
	updateVSAClusterDeploymentRequest.BucketName = bucketName
	// Set AutoTierThreshold to -1 to signal VLM to skip auto-tiering threshold update.
	// This is a workaround until VLM properly handles the case where object store doesn't exist.
	// Valid threshold values are 0-100, so -1 is used as a sentinel value meaning "do not update".
	updateVSAClusterDeploymentRequest.AutoTierThreshold = -1
	// Note: HAPairIndices should be set by the caller based on the update sequence
}

// executePoolBatchUpdates processes HA pair updates in batches sequentially
func executePoolBatchUpdates(ctx workflow.Context, batchPlan *activities.CalculateBatchPlanActivityOutput, currentVlmConfig *vlm.VLMConfig, newVlmConfig *vlm.VLMConfig, credentials *vlm.OntapCredentials, ontapVersion string, vsaClientWorkflowManager vlm.VlmWorkflowClient, logger log.Logger, bucketName string) (*vlm.UpdateVSAClusterDeploymentResponse, error) {
	currentConfig := currentVlmConfig
	var updateVSAClusterDeploymentResponse *vlm.UpdateVSAClusterDeploymentResponse

	// Track completed batches for error reporting in case of partial completion
	completedBatches := make([]int, 0, batchPlan.NumWorkflowCalls)

	for batchNum := 0; batchNum < batchPlan.NumWorkflowCalls; batchNum++ {
		// Get batch indices from the pre-calculated batch plan
		batchIndices := batchPlan.BatchIndices[batchNum]

		// Prepare update request
		updateRequest := &vlm.UpdateVSAClusterDeploymentRequest{}
		prepareUpdateVSAClusterDeploymentRequest(updateRequest, *currentConfig, *newVlmConfig, *credentials, bucketName)
		updateRequest.HAPairIndices = batchIndices

		logger.Info("Starting update batch", "batchNumber", batchNum+1, "totalBatches", batchPlan.NumWorkflowCalls, "indices", batchIndices, "totalHAPairs", batchPlan.NumHAPairs, "batchSize", batchPlan.BatchSize)

		// Execute the update
		response, err := vsaClientWorkflowManager.UpdateVSAClusterDeployment(ctx, updateRequest, ontapVersion)
		if err != nil {
			// Log detailed partial completion state
			logger.Errorf(
				"Pool update failed at batch %d of %d. Partial completion detected - cluster is in mixed-version state. "+
					"Completed batches: %v. Original error: %v",
				batchNum+1, batchPlan.NumWorkflowCalls, completedBatches, err)

			return nil, err
		}

		// Track successful batch completion
		completedBatches = append(completedBatches, batchNum+1)

		// Use the updated VLM config from this response as the current config for the next batch
		currentConfig = &response.VLMConfig
		updateVSAClusterDeploymentResponse = response

		logger.Info("Completed update batch", "batchNumber", batchNum+1, "totalBatches", batchPlan.NumWorkflowCalls, "indices", batchIndices)
	}

	return updateVSAClusterDeploymentResponse, nil
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
		HeartbeatTimeout:    time.Duration(setupNwHeartbeatTimeout) * time.Second,
		RetryPolicy:         utils.GetHyperscalerLRORetryPolicy(),
	}
	poolActivity := &activities.PoolActivity{}
	pscActivity := &activities.PSCActivity{}
	setupPscCtx := workflow.WithActivityOptions(ctx, ao)

	if pool == nil {
		logger.Warn("pool is nil, unable to release PSC Endpoint")
		return nil
	}
	if pool.ClusterDetails.RegionalTenantProject == "" {
		logger := util.GetLogger(ctx)
		logger.Warnf("Regional tenant project is not set for pool: %s, fetching tenanct project number", pool.UUID)

		tenantProjectNumber := new(string)
		params := &common.CreatePoolParams{
			AccountName:    pool.Account.Name,
			VendorSubNetID: pool.Network,
			Region:         Region,
		}
		err = workflow.ExecuteActivity(setupPscCtx, poolActivity.FindTenancyProject, params).Get(ctx, tenantProjectNumber)
		if err != nil {
			logger.Warnf("Failed to fetch tenancy project number for pool: %s, error: %v", pool.UUID, err)
			return nil
		}
		pool.ClusterDetails.RegionalTenantProject = *tenantProjectNumber
	}
	deleteForwardingRuleOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.DeleteForwardingRule, pool).Get(ctx, &deleteForwardingRuleOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if deleteForwardingRuleOperation == nil {
		util.GetLogger(ctx).Infof("Unable to delete forwarding rule.")
		return nil
	}
	err = WaitForGCPNetworkOperationStatus(setupPscCtx, poolActivity, &deleteForwardingRuleOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to release PSC endpoint for tenant project: %s: %w", pool.ClusterDetails.RegionalTenantProject, err)
	}
	deleteAddressOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.DeleteAddress, pool).Get(ctx, &deleteAddressOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(setupPscCtx, poolActivity, &deleteAddressOperation, retryPolicy.StartToCloseTimeout)
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
		StartToCloseTimeout: utils.GetStartToCloseTimeoutHyperscaler(),
		HeartbeatTimeout:    time.Duration(setupNwHeartbeatTimeout) * time.Second,
		RetryPolicy:         utils.GetHyperscalerRetryPolicy(),
	}
	// poolActivity is used for GCP network operations
	poolActivity := &activities.PoolActivity{}
	pscActivity := &activities.PSCActivity{}
	ctx = workflow.WithActivityOptions(ctx, ao)

	setupPscCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)

	// CreateInternalInfraSubnet
	subnetFirewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateInternalInfraSubnet, projectName).Get(ctx, &subnetFirewallOperations)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(setupPscCtx, poolActivity, &subnetFirewallOperations, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create internal infra subnet for tenant project: %s: %w", projectName, err)
	}
	var forwardingRuleIpAddress string
	var addressURI string
	pscEndpointName := region + "-rg-fluent-bit-psc"
	createAddressOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateAddressForPSCEndpoint, projectName, region, pscEndpointName).Get(ctx, &createAddressOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(setupPscCtx, poolActivity, &createAddressOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create PSC endpoint for tenant project: %s: %w", projectName, err)
	}
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.GetAddressURI, projectName, region, pscEndpointName).Get(ctx, &addressURI)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if addressURI == "" {
		return vsaerror.Errorf("failed to get IP address of PSC endpoint from create address operation in tenant project: %s: %w", projectName, err)
	}

	createForwardingRuleOperation := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateForwardingRuleForPSCEndpoint, projectName, region, pscEndpointName, addressURI).Get(ctx, &createForwardingRuleOperation)
	if err != nil {
		return ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(setupPscCtx, poolActivity, &createForwardingRuleOperation, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return vsaerror.Errorf("failed to create forwarding rule subnet for tenant project: %s: %w", projectName, err)
	}
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.GetForwardingRuleIPAddress, projectName, region, pscEndpointName).Get(ctx, &forwardingRuleIpAddress)
	if err != nil {
		return ConvertToVSAError(err)
	}
	if forwardingRuleIpAddress == "" {
		return vsaerror.Errorf("failed to get forwarding rule from operation for tenant project: %s: %w", projectName, err)
	}

	// Update audit log
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.UpdateSecurityAudit, node).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}

	// forward ontap logging to PSC Endpoint
	err = workflow.ExecuteActivity(setupPscCtx, pscActivity.CreateClusterLogForwarding, node, forwardingRuleIpAddress).Get(ctx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}
	return nil
}

func ConfigureNetworkWorkflow(ctx workflow.Context, tenancyDetails *common.TenancyInfo, poolMode string) (interface{}, error) {
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: utils.GetStartToCloseTimeoutHyperscaler(),
		HeartbeatTimeout:    time.Duration(setupNwHeartbeatTimeout) * time.Second,
		RetryPolicy:         utils.GetHyperscalerRetryPolicy(),
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
	vpcOperations := make([]common.Operations, 0)
	tenantProjectNumber := tenancyDetails.RegionalTenantProject
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVPCs, tenantProjectNumber).Get(ctx, &vpcOperations)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	err = WaitForGCPNetworkOperationStatus(ctx, poolActivity, &vpcOperations, retryPolicy.StartToCloseTimeout)
	if err != nil {
		return nil, vsaerror.Errorf("failed to create VPC for tenant project while waiting to get operation status: %s: %w", tenantProjectNumber, err)
	}

	subnetFirewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateSubnets, tenantProjectNumber).Get(ctx, &subnetFirewallOperations)
	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	firewallOperations := make([]common.Operations, 0)
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateFirewalls, tenantProjectNumber, tenancyDetails.SnHostProject, tenancyDetails.Network, poolMode).Get(ctx, &firewallOperations)
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
func updateAutoTieringFields(dbPool *datamodel.Pool, updatePoolParams *common.UpdatePoolParams) {
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

// getPoolAttributesAccountName safely gets the account name from pool attributes, returning empty string if pool attributes is nil
func getPoolAttributesAccountName(poolAttributes *datamodel.PoolAttributes) string {
	if poolAttributes == nil {
		return ""
	}
	return poolAttributes.AccountName
}

type adSyncInput struct {
	ActiveDirectoryID string
	AccountName       string
	Region            string
	XCorrelationID    string
	ActiveDirectory   *models.ActiveDirectory
	LargeCapacity     bool
}

// syncActiveDirectoryInVcp syncs Active Directory from CVP to VCP when AD exists in CVP but not in VCP
func syncActiveDirectoryInVcp(ctx workflow.Context, input adSyncInput, pool *datamodel.Pool) error {
	logger := util.GetLogger(ctx)
	logger.Infof("Syncing Active Directory from CVP to VCP for pool: %s, AD ID: %s", pool.Name, input.ActiveDirectoryID)

	if input.ActiveDirectory == nil {
		return vsaerrors.New("ActiveDirectory is nil, cannot sync")
	}

	retryPolicy, err := PopulateRetryPolicyParams(input.LargeCapacity)
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
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	ctx, err = FetchAndSetAuthToken(ctx, input.AccountName, logger)
	if err != nil {
		return ConvertToVSAError(err)
	}

	adSyncActivity := &active_directory_activities.ActiveDirectorySyncActivity{}

	// Prepare sync parameters
	syncParams := &active_directory_activities.SyncActiveDirectoryParams{
		ActiveDirectoryID: input.ActiveDirectoryID,
		AccountName:       input.AccountName,
		LocationID:        input.Region,
		XCorrelationID:    input.XCorrelationID,
		PoolUUID:          pool.UUID,
		ActiveDirectory:   input.ActiveDirectory,
	}

	// Step 1: Call CVP API V1betaPushActiveDirectoryPassword
	var pushPasswordResult *active_directory_activities.PushActiveDirectoryPasswordResult
	err = workflow.ExecuteActivity(ctx, adSyncActivity.PushActiveDirectoryPasswordActivity, syncParams).Get(ctx, &pushPasswordResult)
	if err != nil {
		logger.Errorf("Failed to push Active Directory password to CVP: %v", err)
		return ConvertToVSAError(err)
	}

	if pushPasswordResult == nil || pushPasswordResult.Operation == nil {
		logger.Errorf("Failed to push Active Directory password to cvp: %v", pushPasswordResult)
		return ConvertToVSAError(fmt.Errorf("failed to push Active Directory password to cvp: %v", pushPasswordResult))
	}

	// Step 2: Poll for job to complete
	if pushPasswordResult != nil && pushPasswordResult.Operation != nil {
		// Prepare polling options
		pollingOptions := workflow.ActivityOptions{
			StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        retryPolicy.InitialInterval,
				BackoffCoefficient:     retryPolicy.BackoffCoefficient,
				MaximumInterval:        retryPolicy.MaximumInterval,
				MaximumAttempts:        int32(maxRetryAttemptsForSDEPollJob),
				NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
			},
		}
		pollingCtx := workflow.WithActivityOptions(ctx, pollingOptions)

		// Poll the push password operation until completion
		err = workflow.ExecuteActivity(pollingCtx, adSyncActivity.PollPushPasswordOperationActivity, syncParams, pushPasswordResult.Operation).Get(pollingCtx, nil)
		if err != nil {
			logger.Errorf("Failed to poll push password operation: %v", err)
			return ConvertToVSAError(err)
		}
		logger.Info("Push password operation completed successfully")
	}

	// Step 3: Create ActiveDirectory entry in VCP
	var createdAD *datamodel.ActiveDirectory
	err = workflow.ExecuteActivity(ctx, adSyncActivity.CreateActiveDirectoryInVCPActivity, syncParams, pushPasswordResult.SecretName).Get(ctx, &createdAD)
	if err != nil {
		logger.Errorf("Failed to create Active Directory in VCP: %v", err)
		return ConvertToVSAError(err)
	}

	if createdAD == nil {
		return vsaerrors.New("Created ActiveDirectory is nil")
	}

	// Step 4: Update pool table's activedirectoryID with newly created AD Int ID
	err = workflow.ExecuteActivity(ctx, adSyncActivity.UpdatePoolActiveDirectoryIDActivity, syncParams, createdAD.ID).Get(ctx, nil)
	if err != nil {
		logger.Errorf("Failed to update pool ActiveDirectory ID: %v", err)
		return ConvertToVSAError(err)
	}

	logger.Infof("Successfully synced Active Directory from CVP to VCP for pool: %s", pool.Name)
	return nil
}

// CleanupServiceAccountPermissionsWorkflow is an orphan child workflow that cleans up
// service account IAM permissions from tenant projects.
func CleanupServiceAccountPermissionsWorkflow(ctx workflow.Context, pool *datamodel.Pool, retryPolicy *WorkflowRetryPolicy) error {
	logger := util.GetLogger(ctx)
	info := workflow.GetInfo(ctx)

	logger.Infof("Starting CleanupServiceAccountPermissionsWorkflow for pool %s (UUID: %s), WorkflowID: %s",
		pool.Name, pool.UUID, info.WorkflowExecution.ID)

	// Get the service-account-specific retry policy
	saRetryPolicy, err := populateServiceAccountRetryPolicyParams()
	if err != nil {
		return ConvertToVSAError(err)
	}

	saActivityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: saRetryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        saRetryPolicy.InitialInterval,
			BackoffCoefficient:     saRetryPolicy.BackoffCoefficient,
			MaximumInterval:        saRetryPolicy.MaximumInterval,
			MaximumAttempts:        int32(saRetryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}

	activityCtx := workflow.WithActivityOptions(ctx, saActivityOptions)
	poolActivity := &activities.PoolActivity{}

	err = workflow.ExecuteActivity(activityCtx, poolActivity.CleanupServiceAccountPermissionsInTenantProjects, pool).Get(activityCtx, nil)
	if err != nil {
		return ConvertToVSAError(err)
	}
	return nil
}
