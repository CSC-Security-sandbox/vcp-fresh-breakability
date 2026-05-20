package oci

import (
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/validators"
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
	defaultOCIVSAUserBootargs = "bootarg.use.cam.nda=true;bootarg.vm.vnvram_jswap_enable=false;bootarg.vm.nvramdevice=/dev/da2;bootarg.vm.cluster_ports=e0a.pv1;bootarg.vm.vnvram.ephemeral=true;bootarg.vm.nvme.lssd_size_for_ec=4294967296;"
	password                  = "password"
)

var (
	ociVSAFlexOcpus               = float32(env.GetFloat64("OCI_VSA_FLEX_OCPUS", 8))
	ociVSAFlexMemoryInGBs         = float32(env.GetFloat64("OCI_VSA_FLEX_MEMORY_IN_GBS", 96))
	vsaImageName                  = strings.TrimSpace(env.GetString("VSA_IMAGE_NAME", ""))
	vsaMediatorImageName          = strings.TrimSpace(env.GetString("VSA_MEDIATOR_IMAGE_NAME", ""))
	ociOntapAdminPassword         = env.GetString("OCI_ONTAP_ADMIN_PASSWORD", "")
	ociDefinedTagNamespace        = env.GetString("OCI_DEFINED_TAG_NAMESPACE", "netapp_tags")
	ociVSAInstanceType            = env.GetString("OCI_VSA_INSTANCE_TYPE", "VM.DenseIO.E5.Flex")
	ociMediatorInstanceType       = env.GetString("OCI_MEDIATOR_INSTANCE_TYPE", "VM.Standard3.Flex")
	ociVSAUserBootargs            = env.GetString("OCI_VSA_USER_BOOTARGS", defaultOCIVSAUserBootargs)
	localRegion                   = env.GetString("LOCAL_REGION", "")
	ociCreator                    = env.GetString("OCI_CREATOR", "vcp")
	secretURI                     = env.GetString("SECRET_URI", "")
	dbHeartbeatTimeoutSec         = env.GetUint64("DATABASE_HEARTBEAT_TIMEOUT_SEC", 10)
	dataDiskCount                 = env.GetIntNotNegative("OCI_VSA_DATA_DISK_COUNT", 2)
	extIPForNodeMgmt              = env.GetBool("OCI_VSA_EXT_IP_FOR_NODE_MGMT", false)
	allowNonDenseShapeForVSA      = env.GetBool("OCI_VSA_ALLOW_NON_DENSE_SHAPE_FOR_VSA", true)
	disableVsaCleanupOnVLMFailure = env.GetBool("DISABLE_VSA_CLEANUP_ON_VLM_FAILURE", false)
	ociExpertModeRbacURL          = env.GetString("OCI_EXPERT_MODE_RBAC_FILE_URL", "")
	ociExpertModeRbacHash         = env.GetString("OCI_EXPERT_MODE_RBAC_FILE_CHECKSUM", "")
	ociExpertModeUsername         = env.GetString("OCI_EXPERT_MODE_USERNAME", "ociadmin")
	ociExpertModePassword         = env.GetString("OCI_EXPERT_MODE_PASSWORD", "")
	ociSerialNumberLeadingPrefix  = "955"
	ociSerialNumberPrefix         = "000000000000000"
)

// ValidateOCIWorkerStartupEnv ensures OCI worker startup has all required environment variables.
// This is called from worker/main.go when HYPERSCALER=oci to fail fast before polling workflows.
func ValidateOCIWorkerStartupEnv() error {
	missing := make([]string, 0, 5)
	if strings.TrimSpace(vsaImageName) == "" {
		missing = append(missing, "VSA_IMAGE_NAME")
	}
	if strings.TrimSpace(vsaMediatorImageName) == "" {
		missing = append(missing, "VSA_MEDIATOR_IMAGE_NAME")
	}
	if strings.TrimSpace(ociOntapAdminPassword) == "" {
		missing = append(missing, "OCI_ONTAP_ADMIN_PASSWORD")
	}
	if strings.TrimSpace(localRegion) == "" {
		missing = append(missing, "LOCAL_REGION")
	}
	if strings.TrimSpace(secretURI) == "" {
		missing = append(missing, "SECRET_URI")
	}
	if len(missing) > 0 {
		return utilserrors.NewUserInputValidationErr(
			fmt.Sprintf("missing required OCI startup env vars: %s", strings.Join(missing, ", ")),
		)
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
	start := workflow.Now(ctx)
	createPoolWF := new(ociCreatePoolWorkflow)
	log := util.GetLogger(ctx)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		emitDuration(ctx, wfCreatePool, queueCustomer, start)
		return err
	}

	createPoolWF.Status = workflows.WorkflowStatusRunning
	_, errRun := createPoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociCreatePoolWorkflow: %v", errRun)
		createPoolWF.Status = workflows.WorkflowStatusFailed
		emitDuration(ctx, wfCreatePool, queueCustomer, start)
		return errRun
	}
	createPoolWF.Status = workflows.WorkflowStatusCompleted
	emitDuration(ctx, wfCreatePool, queueCustomer, start)
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
	if len(args) < 2 {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreatePoolWorkflow.Run: expected 2 args, got %d", len(args)))
	}
	params, ok := args[0].(*common.CreatePoolParams)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreatePoolWorkflow.Run: args[0] has unexpected type %T, want *common.CreatePoolParams", args[0]))
	}
	if params == nil {
		return nil, workflows.ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("OCICreatePoolWorkflow.Run: args[0] (*common.CreatePoolParams) must not be nil")))
	}
	pool, ok := args[1].(*datamodel.Pool)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCICreatePoolWorkflow.Run: args[1] has unexpected type %T, want *datamodel.Pool", args[1]))
	}
	if pool == nil {
		return nil, workflows.ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("OCICreatePoolWorkflow.Run: args[1] (*datamodel.Pool) must not be nil")))
	}

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
	ctx = workflow.WithActivityOptions(ctx, ao)

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

	if pool.PoolCredentials == nil {
		err = fmt.Errorf("pool credentials are required to create ONTAP admin credentials for pool %q", pool.Name)
		return nil, workflows.ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, err))
	}
	credConfig := &activities.OCICreatePoolCredentials{}
	var ociSecret *datamodel.ExternalCredRef
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateOnTapCredentialsForOCI, pool).Get(ctx, credConfig)
	if err != nil {
		logger.Errorf("Failed to create ONTAP credentials for OCI pool: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	if !disableVsaCleanupOnVLMFailure {
		rollbackManager.AddActivity(poolActivity.DeleteOnTapCredentialsForOCI, pool)
	}
	if credConfig.Secret != nil {
		ociSecret = &datamodel.ExternalCredRef{
			Name:               credConfig.Secret.Name,
			Version:            credConfig.Secret.Version,
			ExternalIdentifier: credConfig.Secret.ExternalIdentifier,
		}
		//   pool.PoolCredentials.ExternalCertificate = ociCertificate
		pool.PoolCredentials.ExternalSecret = ociSecret
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
	prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest, *vlmConfig, credConfig.OntapCredentials, pool)

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
		emitStage(ctx, wfCreatePool, queueCustomer, stageVLMDeploy, resultFailure)
		return nil, workflows.ConvertToVSAError(err)
	}
	emitStage(ctx, wfCreatePool, queueCustomer, stageVLMDeploy, resultSuccess)

	expertModeAdminPassword := ociExpertModePassword
	if expertModeAdminPassword == "" {
		if params.OciAdminPassword == nil || params.OciAdminPassword.Ocid == "" {
			return nil, workflows.ConvertToVSAError(
				vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("OCI admin password config is required when OCI_EXPERT_MODE_PASSWORD env var is not set")))
		}
		expertModeConfig := &vlm.OntapCredentials{}
		err = workflow.ExecuteActivity(ctx, poolActivity.GetExpertModeCredentialsForOCI, pool, params.OciAdminPassword).Get(ctx, &expertModeConfig)
		if err != nil {
			logger.Errorf("Failed to get expert mode credentials for OCI pool: %v", err)
			return nil, workflows.ConvertToVSAError(err)
		}
		expertModeAdminPassword = expertModeConfig.AdminPassword
	}

	expertModeReq := &vlm.OntapExpertModeUserConfig{
		VLMConfig:          createVSAClusterDeploymentResponse.VLMConfig,
		OntapCredentials:   credConfig.OntapCredentials,
		RbacFileURL:        ociExpertModeRbacURL,
		RbacFileChecksum:   ociExpertModeRbacHash,
		Username:           ociExpertModeUsername,
		AuthenticationType: password,
		ExpertModeUserCredentials: vlm.OntapCredentials{
			AdminPassword: expertModeAdminPassword,
			Certificate:   vlm.OntapCertificate{},
		},
	}
	// TODO: change AuthenticationType once the certs are implemented for OCI expert mode users
	// if pool.PoolCredentials.AuthType == env.USER_CERTIFICATE {
	// 	expertModeReq.AuthenticationType = certificate
	// } else {
	// 	expertModeReq.AuthenticationType = password
	// }

	if _, err = vsaClientWorkflowManager.CreateVSAExpertModeUser(ctx, expertModeReq); err != nil {
		logger.Errorf("Failed to create expert mode user for OCI pool: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}

	// Create database heartbeat context for long-running DB operations
	// This inherits StartToCloseTimeout from parent context but uses shorter heartbeat timeout
	dbHbCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(dbHeartbeatTimeoutSec)*time.Second)

	// Save VSA node details to database.
	// This persists the node information (VM1, VM2, mediator) from the VLM config response.
	hostMap := make(map[string]string) // Empty hostMap for OCI (DNS handled differently than GCP)
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.SaveVSANodeDetails, pool, createVSAClusterDeploymentResponse.VLMConfig, pool.DeploymentName, &hostMap).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to save VSA node details to database: %v", err)
		emitStage(ctx, wfCreatePool, queueCustomer, stageSaveNodeDetails, resultFailure)
		return nil, workflows.ConvertToVSAError(err)
	}
	emitStage(ctx, wfCreatePool, queueCustomer, stageSaveNodeDetails, resultSuccess)

	// Mark pool as ready and persist VLM config for future workflows.
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.CreatedPool, pool, &createVSAClusterDeploymentResponse.VLMConfig).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to mark pool as created: %v", err)
		emitStage(ctx, wfCreatePool, queueCustomer, stageMarkReady, resultFailure)
		return nil, workflows.ConvertToVSAError(err)
	}
	emitStage(ctx, wfCreatePool, queueCustomer, stageMarkReady, resultSuccess)

	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.UpdatePoolFields, pool.UUID, map[string]interface{}{
		"build_info": NewPoolBuildInfo(workflow.Now(ctx), params.AccountName),
	}).Get(dbHbCtx, nil)
	if err != nil {
		logger.Errorf("Failed to persist pool build info for pool %q: %v", pool.UUID, err)
		err = nil
	}

	return nil, nil
}

func NewPoolBuildInfo(now time.Time, accountName string) *datamodel.PoolBuildInfo {
	return &datamodel.PoolBuildInfo{
		VSABuildImage:      vsaImageName,
		MediatorBuildImage: vsaMediatorImageName,
		OntapVersion:       utils.ExtractOntapVersion(utils.GetOntapVersionBasedOnAllowlisting(accountName)),
		BuildTimestamp:     now,
	}
}

// ociDefinedTags returns the OCI defined tags map (netapp-tags with deployment_id).
func ociDefinedTags(deploymentName string) map[string]map[string]interface{} {
	definedTags := make(map[string]map[string]interface{})
	definedTags[ociDefinedTagNamespace] = map[string]interface{}{
		"deployment_id": deploymentName,
	}
	return definedTags
}

// ociDeploymentConfig builds the VLM DeploymentConfig for OCI (provider, deployment ID, images, OCI config, SP config, etc.).
func ociDeploymentConfig(params *common.CreatePoolParams, pool *datamodel.Pool, sizeStr string, throughputMibps, iops int64, ociConfig vlm.OCIConfig) vlm.DeploymentConfig {
	var deploymentType string
	var enableAAConfig bool
	if params.IsRegionalHA {
		deploymentType = vlm.DeploymentTypeNonSharedHA
		enableAAConfig = false
	} else {
		deploymentType = vlm.DeploymentTypeSharedHA
		enableAAConfig = true
	}

	return vlm.DeploymentConfig{
		Provider:           vlm.OCICloud,
		DeploymentID:       pool.DeploymentName,
		SerialNumberPrefix: ociSerialNumberLeadingPrefix + ociSerialNumberPrefix,
		Region:             localRegion,
		Images: vlm.ImageConfig{
			VSAImageName:      vsaImageName,
			MediatorImageName: vsaMediatorImageName,
		},
		UserBootargs: ociVSAUserBootargs,
		Labels: map[string]string{
			"pool_name":  pool.Name,
			"pool_uuid":  pool.UUID,
			"account_id": params.AccountName,
		},
		DeploymentType:       deploymentType,
		NumHAPair:            int(params.HAPairs),
		VSAInstanceType:      ociVSAInstanceType,
		MediatorInstanceType: ociMediatorInstanceType,
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
		DeploymentConfigFlags: vlm.DeploymentConfigFlags{
			EnableAAConfig: enableAAConfig,
		},
	}
}

// prepareVLMConfig prepares the VLM configuration for OCI pool creation.
func prepareVLMConfig(params *common.CreatePoolParams, pool *datamodel.Pool) (*vlm.VLMConfig, error) {
	if params.HAPairs == 0 {
		return nil, utilserrors.NewUserInputValidationErr(
			"haPairs must be greater than 0; OCI pool creation requires CreatePoolParams.HAPairs to be set",
		)
	}

	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)
	sizeStr := fmt.Sprintf("%dGi", sizeInGB)

	throughputMibps := int64(0)
	iops := int64(0)
	if params.CustomPerformanceParams != nil {
		throughputMibps = params.CustomPerformanceParams.ThroughputMibps
		perf := &validators.CustomPerformance{
			ThroughputMibps: throughputMibps,
			Iops:            params.CustomPerformanceParams.Iops,
		}
		if err := validators.NewPoolValidator(false).ValidateIops(perf); err != nil {
			return nil, fmt.Errorf("derive iops from throughput: %w", err)
		}
		if perf.Iops != nil {
			iops = *perf.Iops
		}
	}

	definedTags := ociDefinedTags(pool.DeploymentName)

	ociConfig := vlm.OCIConfig{
		CompartmentID:   params.CompartmentOCID,
		SubnetID:        params.VendorSubNetID,
		DataNICSubnetID: params.DataNICSubnetID,
		AvailabilityDomain: vlm.AvailabilityDomainInfo{
			AvailabilityDomain1:        params.PrimaryZone,
			AvailabilityDomain2:        params.SecondaryZone,
			MediatorAvailabilityDomain: params.MediatorZone,
		},
		VSAInstanceShape:   ociVSAInstanceType,
		VSAFlexOcpus:       ociVSAFlexOcpus,
		VSAFlexMemoryInGBs: ociVSAFlexMemoryInGBs,
		Creator:            ociCreator,
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
	vlmConfig.Deployment.Labels["pool_ocid"] = pool.PoolExternalIdentifier
	if pool.Account != nil {
		vlmConfig.Deployment.Labels["account_id"] = pool.Account.Name
	}

	// Set images (already set in prepareVLMConfig, but ensure they're correct)
	vlmConfig.Deployment.Images.VSAImageName = vsaImageName
	vlmConfig.Deployment.Images.MediatorImageName = vsaMediatorImageName

	createVSAClusterDeploymentRequest.VLMConfig = vlmConfig
	createVSAClusterDeploymentRequest.OntapCredentials = ontapCredentials

	createVSAClusterDeploymentRequest.OntapLicense = vlm.OntapLicense{
		SecretUri: []string{secretURI},
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
	start := workflow.Now(ctx)
	deletePoolWF := new(ociDeletePoolWorkflow)
	log := util.GetLogger(ctx)
	err := deletePoolWF.Setup(ctx, params)
	if err != nil {
		emitDuration(ctx, wfDeletePool, queueCustomer, start)
		return err
	}

	deletePoolWF.Status = workflows.WorkflowStatusRunning
	_, errRun := deletePoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociDeletePoolWorkflow: %v", errRun)
		deletePoolWF.Status = workflows.WorkflowStatusFailed
		emitDuration(ctx, wfDeletePool, queueCustomer, start)
		return errRun
	}
	deletePoolWF.Status = workflows.WorkflowStatusCompleted
	emitDuration(ctx, wfDeletePool, queueCustomer, start)
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
	if len(args) < 2 {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeletePoolWorkflow.Run: expected 2 args, got %d", len(args)))
	}
	params, ok := args[0].(*common.DeletePoolParams)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeletePoolWorkflow.Run: args[0] has unexpected type %T, want *common.DeletePoolParams", args[0]))
	}
	if params == nil {
		return nil, workflows.ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("OCIDeletePoolWorkflow.Run: args[0] (*common.DeletePoolParams) must not be nil")))
	}
	pool, ok := args[1].(*datamodel.Pool)
	if !ok {
		return nil, workflows.ConvertToVSAError(fmt.Errorf("OCIDeletePoolWorkflow.Run: args[1] has unexpected type %T, want *datamodel.Pool", args[1]))
	}
	if pool == nil {
		return nil, workflows.ConvertToVSAError(vsaerrors.NewVCPError(vsaerrors.ErrResourceEmptyError, fmt.Errorf("OCIDeletePoolWorkflow.Run: args[1] (*datamodel.Pool) must not be nil")))
	}

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
		emitStage(ctx, wfDeletePool, queueCustomer, stageVLMDelete, resultFailure)
		return nil, workflows.ConvertToVSAError(err)
	}
	emitStage(ctx, wfDeletePool, queueCustomer, stageVLMDelete, resultSuccess)

	// Delete the ONTAP admin password secret from OCI Vault after the VSA cluster is gone.
	// Secret name is derived from DeploymentName; the activity is idempotent if no secret exists
	// (e.g. pools created with AuthType=USERNAME_PWD where no vault secret was ever provisioned).
	// TODO(oci-cert): once OCI expert-mode certificates are introduced, the activity will
	// additionally revoke the cert; the same compound guard then makes revocation skippable
	// for debug sessions, matching GCP exactly.
	if pool.DeploymentName != "" && (!disableVsaCleanupOnVLMFailure || pool.State != models.LifeCycleStateError) {
		err = workflow.ExecuteActivity(hyperscalerCtx, poolActivity.DeleteOnTapCredentialsForOCI, pool).Get(hyperscalerCtx, nil)
		if err != nil {
			return nil, workflows.ConvertToVSAError(err)
		}
	}

	// Mark pool as deleted
	err = workflow.ExecuteActivity(dbHbCtx, poolActivity.DeletePoolResources, pool).Get(dbHbCtx, nil)
	if err != nil {
		emitStage(ctx, wfDeletePool, queueCustomer, stageDBCleanup, resultFailure)
		return nil, workflows.ConvertToVSAError(err)
	}
	emitStage(ctx, wfDeletePool, queueCustomer, stageDBCleanup, resultSuccess)

	return nil, nil
}

// OCIUpdatePoolWorkflow processes pool update requests for OCI.
// TODO(VSCP-5929): Full implementation in the workflow-layer PR.
func OCIUpdatePoolWorkflow(ctx workflow.Context, params *common.UpdatePoolParams, pool *datamodel.Pool) error {
	return fmt.Errorf("OCIUpdatePoolWorkflow not yet implemented")
}
