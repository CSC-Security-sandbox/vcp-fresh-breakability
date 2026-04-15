package oci

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultSerialNumberPrefix = "12345"
	ociVSAUserBootargs        = "bootarg.use.cam.nda=true;bootarg.vm.vnvram_jswap_enable=false;bootarg.vm.nvramdevice=/dev/da2"
)

var (
	ociVSAFlexOcpus               = float32(env.GetFloat64("OCI_VSA_FLEX_OCPUS", 8))
	ociVSAFlexMemoryInGBs         = float32(env.GetFloat64("OCI_VSA_FLEX_MEMORY_IN_GBS", 96))
	dbHeartbeatTimeoutSec         = env.GetUint64("DATABASE_HEARTBEAT_TIMEOUT_SEC", 10)
	numHAPair                     = env.GetIntNotNegative("OCI_VSA_NUM_HA_PAIR", 1)
	dataDiskCount                 = env.GetIntNotNegative("OCI_VSA_DATA_DISK_COUNT", 2)
	extIPForNodeMgmt              = env.GetBool("OCI_VSA_EXT_IP_FOR_NODE_MGMT", false)
	allowNonDenseShapeForVSA      = env.GetBool("OCI_VSA_ALLOW_NON_DENSE_SHAPE_FOR_VSA", true)
	disableVsaCleanupOnVLMFailure = env.GetBool("DISABLE_VSA_CLEANUP_ON_VLM_FAILURE", false)
)

// ociVSAImageOCIDs returns trimmed VSA and mediator image OCIDs from the environment (no defaults).
func ociVSAImageOCIDs() (vsa string, mediator string) {
	return strings.TrimSpace(env.GetString("VSA_IMAGE_NAME", "")),
		strings.TrimSpace(env.GetString("VSA_MEDIATOR_IMAGE_NAME", ""))
}

func ociVSAImageEnvError() error {
	v, m := ociVSAImageOCIDs()
	if v == "" {
		return errors.New("VSA_IMAGE_NAME environment variable must be set")
	}
	if m == "" {
		return errors.New("VSA_MEDIATOR_IMAGE_NAME environment variable must be set")
	}
	return nil
}

// ValidateOCIVSAImageEnv ensures OCI VSA and mediator image OCIDs are configured (required for pool creation).
func ValidateOCIVSAImageEnv() error {
	if err := ociVSAImageEnvError(); err != nil {
		return utilserrors.NewUserInputValidationErr(err.Error())
	}
	return nil
}

type ociCreatePoolWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ociCreatePoolWorkflow{}

// OCICreatePoolWorkflow processes pool related requests from a customer for OCI.
func OCICreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) error {
	createPoolWF := new(ociCreatePoolWorkflow)
	log := util.GetLogger(ctx)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		return err
	}

	createPoolWF.Status = workflows.WorkflowStatusRunning
	_, errRun := createPoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociCreatePoolWorkflow: %v", errRun)
		createPoolWF.Status = workflows.WorkflowStatusFailed
		return errRun
	}
	createPoolWF.Status = workflows.WorkflowStatusCompleted
	return nil
}

func (wf *ociCreatePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *ociCreatePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.CreatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
	rollbackManager := common.NewRollbackManager()
	rollbackManager.AddActivity(poolActivity.ErroredPool, pool)
	var err error

	logger := util.GetLogger(ctx)
	logger.Infof("OCI Create Pool Workflow Run method called for pool: %s, account: %s, deploymentName: %s", pool.Name, params.AccountName, pool.DeploymentName)

	// Set up activity options once and reuse for both normal and rollback paths.
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		logger.Errorf("Failed to populate retry policy params: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
		},
	}

	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackCtx := workflow.WithActivityOptions(disconnectedCtx, ao)
			rollbackManager.ExecuteRollback(rollbackCtx, err)
		}
	}()

	// Prepare VLM config for VSA cluster deployment
	// Note: Deployment name is already generated from OCID in the factory layer
	vlmConfig, err := prepareVLMConfig(params, pool)
	if err != nil {
		logger.Errorf("Failed to prepare VLM config: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// TODO: Create ONTAP credentials for OCI
	// The current CreateOnTapCredentials activity implementation is GCP-specific.
	// For OCI, we need to implement OCI-specific credential creation logic.
	// For now, creating a simple struct with password from environment variable.
	credConfig := &vlm.OntapCredentials{
		AdminPassword: env.GetString("OCI_ONTAP_ADMIN_PASSWORD", ""),
		Certificate:   vlm.OntapCertificate{},
	}

	// Get VLM worker queue
	vlmWorkerQueue := vlm.GetVLMWorkerQueue(logger, pool.Account.Name)
	if vlmWorkerQueue == "" {
		return nil, workflows.ConvertToVSAError(
			vsaerrors.NewVCPError(vsaerrors.ErrWorkflowTaskQueueEmpty, fmt.Errorf("VLM worker queue cannot be empty")))
	}

	// Get VSA client workflow manager
	vsaClientWorkflowManager := workflows.GetNewVSAClientWorkflowManager()

	// Prepare CreateVSAClusterDeploymentRequest
	createVSAClusterDeploymentRequest := &vlm.CreateVSAClusterDeploymentRequest{}
	prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest, *vlmConfig, *credConfig, pool)

	if !disableVsaCleanupOnVLMFailure {
		req := &vlm.DeleteVSAClusterDeploymentRequest{}
		prepareOCIDeleteVSAClusterDeploymentRequest(req, pool, params.AccountName)
		rollbackManager.AddWorkflow(vlmWorkerQueue, vlm.DeleteVSAClusterDeploymentWorkflowName, req)
	}
	// Call CreateVSAClusterDeployment (runs on VLM worker as child workflow).
	// VLM must branch on Deployment.Provider: for provider "oci" it must NOT call GCP APIs
	// (e.g. compute.v1.RegionAddressesService.Get). Calling GCP with empty/wrong project yields
	// RESOURCE_PROJECT_INVALID and "failed to get address".
	createVSAClusterDeploymentResponse, err := vsaClientWorkflowManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest, vlmWorkerQueue)
	if err != nil {
		logger.Errorf("Failed to create VSA cluster deployment: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// Apply shared activity options for DB operations.
	ctx = workflow.WithActivityOptions(ctx, ao)

	// Create database heartbeat context for long-running DB operations
	// This inherits StartToCloseTimeout from parent context but uses shorter heartbeat timeout
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)

	// Save VSA node details to database
	// This persists the node information (VM1, VM2, mediator) from the VLM config response
	hostMap := make(map[string]string) // Empty hostMap for OCI (DNS handled differently than GCP)
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SaveVSANodeDetails, pool, createVSAClusterDeploymentResponse.VLMConfig, pool.DeploymentName, &hostMap).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to save VSA node details to database: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// Mark pool as ready and persist VLM config for future workflows.
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.CreatedPool, pool, &createVSAClusterDeploymentResponse.VLMConfig).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to mark pool as created: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}

// ociDefinedTags returns the OCI defined tags map (netapp-tags with deployment_id).
func ociDefinedTags(deploymentName string) map[string]map[string]interface{} {
	definedTags := make(map[string]map[string]interface{})
	definedTagNamespace := env.GetString("OCI_DEFINED_TAG_NAMESPACE", "netapp_tags")
	definedTags[definedTagNamespace] = map[string]interface{}{
		"deployment_id": deploymentName,
	}
	return definedTags
}

// ociDeploymentConfig builds the VLM DeploymentConfig for OCI (provider, deployment ID, images, OCI config, SP config, etc.).
func ociDeploymentConfig(params *common.CreatePoolParams, pool *datamodel.Pool, sizeStr string, throughputMibps, iops int64, ociConfig vlm.OCIConfig) vlm.DeploymentConfig {
	vsaInstanceType := env.GetString("OCI_VSA_INSTANCE_TYPE", "VM.DenseIO.E5.Flex")
	mediatorInstanceType := env.GetString("OCI_MEDIATOR_INSTANCE_TYPE", "VM.Standard3.Flex")
	region := env.GetString("LOCAL_REGION", "")
	deploymentType := vlm.DeploymentTypeNonSharedHA
	serialNumberPrefix := params.SerialNumberPrefix
	if serialNumberPrefix == "" {
		serialNumberPrefix = defaultSerialNumberPrefix
	}

	vsaImg, mediatorImg := ociVSAImageOCIDs()

	return vlm.DeploymentConfig{
		Provider:           vlm.OCICloud,
		DeploymentID:       pool.DeploymentName,
		SerialNumberPrefix: serialNumberPrefix,
		Region:             region,
		Images: vlm.ImageConfig{
			VSAImageName:      vsaImg,
			MediatorImageName: mediatorImg,
		},
		UserBootargs: ociVSAUserBootargs,
		Labels: map[string]string{
			"pool_name":  pool.Name,
			"pool_uuid":  pool.UUID,
			"account_id": params.AccountName,
		},
		DeploymentType:       deploymentType,
		NumHAPair:            numHAPair,
		VSAInstanceType:      vsaInstanceType,
		MediatorInstanceType: mediatorInstanceType,
		DataDiskCount:        dataDiskCount,
		OCIConfig:            ociConfig,
		SPConfig: vlm.SPConfig{
			Size:       sizeStr,
			IOps:       iops,
			Throughput: throughputMibps,
		},
		DevFlags: vlm.DevFlags{
			ExtIPForNodeMgmt:         extIPForNodeMgmt,
			AllowNonDenseShapeForVsa: allowNonDenseShapeForVSA,
		},
	}
}

// prepareVLMConfig prepares the VLM configuration for OCI pool creation.
func prepareVLMConfig(params *common.CreatePoolParams, pool *datamodel.Pool) (*vlm.VLMConfig, error) {
	if err := ociVSAImageEnvError(); err != nil {
		return nil, err
	}

	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)
	sizeStr := fmt.Sprintf("%dGi", sizeInGB)

	throughputMibps := int64(0)
	iops := int64(0)
	if params.CustomPerformanceParams != nil {
		throughputMibps = params.CustomPerformanceParams.ThroughputMibps
		if params.CustomPerformanceParams.Iops != nil {
			iops = *params.CustomPerformanceParams.Iops
		}
	}

	definedTags := ociDefinedTags(pool.DeploymentName)

	creator := env.GetString("OCI_CREATOR", "vcp")
	ociConfig := vlm.OCIConfig{
		CompartmentID: params.CompartmentOCID,
		SubnetID:      params.VendorSubNetID,
		AvailabilityDomain: vlm.AvailabilityDomainInfo{
			AvailabilityDomain1:        params.PrimaryZone,
			AvailabilityDomain2:        params.SecondaryZone,
			MediatorAvailabilityDomain: params.MediatorZone,
		},
		VSAInstanceShape:   env.GetString("OCI_VSA_INSTANCE_TYPE", "VM.DenseIO.E5.Flex"),
		VSAFlexOcpus:       ociVSAFlexOcpus,
		VSAFlexMemoryInGBs: ociVSAFlexMemoryInGBs,
		Creator:            creator,
		DefinedTags:        definedTags,
	}

	deployment := ociDeploymentConfig(params, pool, sizeStr, throughputMibps, iops, ociConfig)
	vlmConfig := &vlm.VLMConfig{Deployment: deployment}
	return vlmConfig, nil
}

// prepareCreateVSAClusterDeploymentRequest prepares the CreateVSAClusterDeploymentRequest for OCI
func prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest *vlm.CreateVSAClusterDeploymentRequest, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials, pool *datamodel.Pool) {
	// Ensure labels are set
	if vlmConfig.Deployment.Labels == nil {
		vlmConfig.Deployment.Labels = make(map[string]string)
	}
	vlmConfig.Deployment.Labels["pool_name"] = pool.Name
	vlmConfig.Deployment.Labels["pool_ocid"] = pool.PoolOCID
	if pool.Account != nil {
		vlmConfig.Deployment.Labels["account_id"] = pool.Account.Name
	}

	// Set images (already set in prepareVLMConfig, but ensure they're correct)
	vsaImg, mediatorImg := ociVSAImageOCIDs()
	vlmConfig.Deployment.Images.VSAImageName = vsaImg
	vlmConfig.Deployment.Images.MediatorImageName = mediatorImg

	createVSAClusterDeploymentRequest.VLMConfig = vlmConfig
	createVSAClusterDeploymentRequest.OntapCredentials = ontapCredentials

	createVSAClusterDeploymentRequest.OntapLicense = vlm.OntapLicense{
		SecretUri: []string{env.GetString("SECRET_URI", "")},
	}
}

func prepareOCIDeleteVSAClusterDeploymentRequest(req *vlm.DeleteVSAClusterDeploymentRequest, pool *datamodel.Pool, tenancyOCID string) {
	req.CloudProvider = vlm.OCICloud
	req.DeploymentID = pool.DeploymentName
	req.ProjectID = tenancyOCID
	req.HyperScalerConfig = &vlm.HyperScalerConfig{
		OCIConfig: vlm.OCIConfig{
			CompartmentID: pool.ClusterDetails.CompartmentOCID,
			DefinedTags:   ociDefinedTags(pool.DeploymentName),
		},
	}
}

type ociDeletePoolWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ociDeletePoolWorkflow{}

// OCIDeletePoolWorkflow processes pool deletion requests for OCI.
func OCIDeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) error {
	deletePoolWF := new(ociDeletePoolWorkflow)
	log := util.GetLogger(ctx)
	err := deletePoolWF.Setup(ctx, params)
	if err != nil {
		return err
	}

	deletePoolWF.Status = workflows.WorkflowStatusRunning
	_, errRun := deletePoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociDeletePoolWorkflow: %v", errRun)
		deletePoolWF.Status = workflows.WorkflowStatusFailed
		return errRun
	}
	deletePoolWF.Status = workflows.WorkflowStatusCompleted
	return nil
}

func (wf *ociDeletePoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	deletePoolParams := input.(*common.DeletePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = deletePoolParams.AccountName
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *ociDeletePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	params := args[0].(*common.DeletePoolParams)
	pool := args[1].(*datamodel.Pool)
	// Set up activity options
	poolActivity := &activities.PoolActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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
	dbHbCtx := workflow.WithActivityOptions(ctx, ao)
	dbHbCtx = workflow.WithHeartbeatTimeout(dbHbCtx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)
	hyperscalerCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: utils.GetStartToCloseTimeoutHyperscaler(),
		HeartbeatTimeout:    utils.GetHeartbeatTimeoutForHyperscaler(),
		RetryPolicy:         utils.GetHyperscalerLRORetryPolicy(),
	})
	// Call DeleteVSAClusterDeployment
	vsaClientWorkflowManager := workflows.GetNewVSAClientWorkflowManager()
	deleteRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
	prepareOCIDeleteVSAClusterDeploymentRequest(deleteRequest, pool, params.AccountName)
	ontapVersion := utils.ExtractOntapVersion(utils.GetOntapVersionBasedOnAllowlisting(params.AccountName))
	err = vsaClientWorkflowManager.DeleteVSAClusterDeployment(hyperscalerCtx, deleteRequest, ontapVersion)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	// Mark pool as deleted
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.DeletePoolResources, pool).Get(dbHbCtx, nil)
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}

	return nil, nil
}
