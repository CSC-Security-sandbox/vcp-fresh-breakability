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
	vmrs_oci "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/oci"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	defaultOCIVSAUserBootargs    = "bootarg.use.cam.nda=true;bootarg.vm.vnvram_jswap_enable=false;bootarg.vm.nvramdevice=/dev/da2;bootarg.vm.cluster_ports=e0a.pv1;bootarg.vm.vnvram.ephemeral=true;bootarg.vm.nvme.lssd_size_for_ec=4294967296;"
	password                     = "password"
	ociSerialNumberLeadingPrefix = "955"
	ociSerialNumberPrefix        = "000000000000000"
  ociSerialCounterMax          = int64(10_000_000_000_000)
	ociSerialCounterWidth        = 13
	ociSerialPrefixLen           = 7
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
	ociCellNumber                 = env.GetString("LOCAL_CELL", "00")
	ociVSASerialAllocationEnabled = env.GetBool("OCI_VSA_SERIAL_NUMBER_ALLOCATION_ENABLED", false)
	ociVMRSEnabled                = env.GetBool("OCI_VMRS_ENABLED", false)
)

func buildOCISerialPrefix(regionCode, cellCode string) (string, error) {
	if err := validateNumericCode("region", regionCode, 2); err != nil {
		return "", err
	}
	if err := validateNumericCode("cell", cellCode, 2); err != nil {
		return "", err
	}
	return ociSerialNumberLeadingPrefix + regionCode + cellCode, nil
}

func validateNumericCode(name, s string, want int) error {
	if len(s) != want {
		return utilserrors.NewUserInputValidationErr(
			fmt.Sprintf("invalid %s code %q: expected %d digits, got %d", name, s, want, len(s)),
		)
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return utilserrors.NewUserInputValidationErr(
				fmt.Sprintf("invalid %s code %q: must contain only digits", name, s),
			)
		}
	}
	return nil
}

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

	// Run OCI VMRS (when enabled) to derive VM shape, OCPUs, and per-data-disk
	// VPU from the pool's (capacity, throughput, AA-vs-AP topology). The
	// Decision is then handed to prepareVLMConfig which projects it onto
	// OCIConfig.{VSAInstanceShape,VSAFlexOcpus} and the per-disk Vpus in
	// Cloud.HAPairs. When the toggle is false the activity is skipped
	// entirely and prepareVLMConfig falls back to the env-driven path.
	//
	// The OCI_VMRS_ENABLED toggle is read via workflow.SideEffect rather
	// than directly off the ociVMRSEnabled package var so this workflow
	// stays replay-deterministic. ociVMRSEnabled is initialized from the
	// worker's process environment at startup and can change between an
	// in-flight workflow's original execution and a later replay (e.g. a
	// redeploy mid-flight flips the toggle). SideEffect records the
	// value-at-decision-time in workflow history; replays read it back
	// from history instead of re-evaluating the package var, so a
	// workflow always sees the same toggle it started with regardless of
	// any redeploys. See .cursor/agents/workflow-builder.mdc for the
	// repo's "os.Getenv() in workflows" guidance.
	var ociDecision *vmrs_oci.Decision
	var vmrsEnabledForRun bool
	if err = workflow.SideEffect(ctx, func(_ workflow.Context) interface{} {
		return ociVMRSEnabled
	}).Get(&vmrsEnabledForRun); err != nil {
		logger.Errorf("Failed to read OCI VMRS toggle via SideEffect: %v", err)
		return nil, workflows.ConvertToVSAError(err)
	}
	if vmrsEnabledForRun {
		perDiskCapacityTB, perVMThroughputGBs, mErr := computeOCIVMRSInput(params)
		if mErr != nil {
			logger.Errorf("Failed to compute OCI VMRS input: %v", mErr)
			err = mErr
			return nil, workflows.ConvertToVSAError(mErr)
		}
		// VMRS is a deterministic, in-process operation: parse the local
		// VMRS YAML (mounted at /config/vmrs_oci.yaml, ~10 KB) and run a
		// pure-function selector over the catalogue. Sub-second wall-clock
		// in practice. The workflow-wide ao is tuned for OCI cloud
		// provisioning activities (minutes-long, heartbeated, retried
		// against transient cloud-provider errors) and is wildly
		// inappropriate for VMRS:
		//   - Tight StartToCloseTimeout so a hung config read fails the
		//     pool-create in ~1 minute instead of inheriting the
		//     provisioning-scale timeout.
		//   - No HeartbeatTimeout — VMRS completes faster than any
		//     reasonable heartbeat interval, and the activity does not
		//     run long enough to need Temporal's heartbeat enforcement.
		//   - MaximumAttempts=1 — every error the activity exposes
		//     (bad input, malformed YAML, no-feasible-selection) is
		//     already marked non-retryable in the activity, so retrying
		//     changes nothing and would only burn the retry budget.
		vmrsAO := workflow.ActivityOptions{
			StartToCloseTimeout: 1 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts:        1,
				NonRetryableErrorTypes: []string{"PanicError", "NonRetryableErr"},
			},
		}
		vmrsCtx := workflow.WithActivityOptions(ctx, vmrsAO)
		vmrsReq := activities.IdentifyOCIResourcesRequest{
			PoolUUID:           pool.UUID,
			PerDiskCapacityTB:  perDiskCapacityTB,
			PerVMThroughputGBs: perVMThroughputGBs,
		}
		err = workflow.ExecuteActivity(vmrsCtx, poolActivity.IdentifyOCIResources, vmrsReq).Get(vmrsCtx, &ociDecision)
		if err != nil {
			logger.Errorf("OCI VMRS selection failed for pool %q: %v", pool.UUID, err)
			return nil, workflows.ConvertToVSAError(err)
		}
		logger.Infof("OCI VMRS decision for pool %q: shape=%s ocpus=%d vpu=%d iops=%d",
			pool.UUID, ociDecision.VMShape, ociDecision.OCPUs, ociDecision.VPU, ociDecision.IOPS)
	}

	// Prepare VLM config for VSA cluster deployment
	// Note: Deployment name is already generated from OCID in the factory layer
	vlmConfig, err := prepareVLMConfig(params, pool, ociDecision)
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

	if ociVSASerialAllocationEnabled {
		if err = allocateOCIVMSerialNumbers(ctx, poolActivity, createVSAClusterDeploymentRequest); err != nil {
			logger.Errorf("Failed to allocate OCI VM serial numbers: %v", err)
			return nil, workflows.ConvertToVSAError(err)
		}
	}

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

func allocateOCIVMSerialNumbers(ctx workflow.Context, poolActivity *activities.PoolActivity, req *vlm.CreateVSAClusterDeploymentRequest) error {
	if activities.RegionNumber == "" {
		return utilserrors.NewUserInputValidationErr(
			"region number is not set; ensure LOCAL_REGION and REGION_NUMBER_MAP are configured on the OCI worker")
	}
	if ociCellNumber == "" {
		return utilserrors.NewUserInputValidationErr(
			"cell number is not set; ensure LOCAL_CELL is configured on the OCI worker as a 2-digit code")
	}
	numHAPair := req.VLMConfig.Deployment.NumHAPair
	if numHAPair < 1 {
		return utilserrors.NewUserInputValidationErr(
			fmt.Sprintf("invalid VM count for serial allocation: NumHAPair=%d (must be >= 1)", numHAPair))
	}

	prefix, err := buildOCISerialPrefix(activities.RegionNumber, ociCellNumber)
	if err != nil {
		return err
	}

	numVMs := numHAPair * activities.VMsPerHAPair
	serials := make([]string, 0, numVMs)
	for range numVMs {
		var counter int64
		if err := workflow.ExecuteActivity(ctx, poolActivity.GetNextSerialNumber).Get(ctx, &counter); err != nil {
			return err
		}
		if counter < 0 || counter >= ociSerialCounterMax {
			return vsaerrors.NewVCPError(
				vsaerrors.ErrGeneratingUniqueSerialNumber,
				fmt.Errorf("OCI serial counter %d overflows %d-digit width (max %d)", counter, ociSerialCounterWidth, ociSerialCounterMax-1),
			)
		}
		serials = append(serials, fmt.Sprintf("%s%0*d", prefix, ociSerialCounterWidth, counter))
	}

	req.VLMConfig.Deployment.SerialNumberPrefix = ""
	req.VLMConfig.Deployment.VMSerialNumbers = serials
	return nil
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
		DeploymentType: deploymentType,
		NumHAPair:      int(params.HAPairs),
		// Drive the deployment-level VSA instance type from the resolved
		// OCIConfig so the VMRS-selected shape (set in prepareVLMConfig)
		// flows here as well; otherwise this field would silently keep
		// the env default and override the VMRS choice downstream.
		VSAInstanceType:      ociConfig.VSAInstanceShape,
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

func prepareVLMConfig(params *common.CreatePoolParams, pool *datamodel.Pool, decision *vmrs_oci.Decision) (*vlm.VLMConfig, error) {
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

	// Start with the env-driven defaults; a single VMRS branch below
	// then flips all four catalogue-driven fields together. All four
	// come straight from the YAML catalogue
	// (config/vmrs_oci.yaml) — memory lives alongside ocpus on each
	// (tier, shape) entry, so no per-OCPU derivation happens here.
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
	if decision != nil {
		ociConfig.VSAInstanceShape = decision.VMShape
		ociConfig.VSAFlexOcpus = float32(decision.OCPUs)
		ociConfig.VSAFlexMemoryInGBs = float32(decision.MemoryGBs)
		ociConfig.DataDiskVpus = int64(decision.VPU)
	}

	deployment := ociDeploymentConfig(params, pool, sizeStr, throughputMibps, iops, ociConfig)
	return &vlm.VLMConfig{Deployment: deployment}, nil
}

// computeOCIVMRSInput converts the pool's total (capacity, throughput) into
// the per-VM throughput and per-disk capacity that the OCI VMRS catalogue
// expects.
//
// Topology assumptions (matches OCI pool create today):
//   - params.HAPairs HA pairs, each with 2 VSA VMs (VM1, VM2). The
//     mediator is excluded from sizing — it doesn't carry data disks.
//   - dataDiskCount data disks per VM, all sized identically.
//   - Active-Active (params.IsRegionalHA == false → shared HA →
//     EnableAAConfig=true): both VMs in a pair serve I/O concurrently,
//     so per-VM throughput = total / (2 * HAPairs).
//   - Active-Passive (params.IsRegionalHA == true → non-shared HA →
//     EnableAAConfig=false): only the primary VM in a pair serves I/O,
//     so per-VM throughput = total / HAPairs.
//   - Capacity is striped across HA pairs and per-VM data disks in both
//     modes: per-disk capacity = total / (HAPairs * dataDiskCount).
//
// Unit conversions match the OCI VMRS YAML catalogue:
//   - bytes → decimal TB (catalogue uses 10^12 bytes)
//   - MiB/s → decimal GB/s (catalogue uses 10^9 bytes/s buckets)
//     1 MiB = 1.048576 MB, so MiB/s → GB/s ≈ MiB/s * 1.048576 / 1000.
//     The selector ceils throughput to the next integer GB/s bucket so
//     float noise is absorbed.
func computeOCIVMRSInput(params *common.CreatePoolParams) (perDiskCapacityTB, perVMThroughputGBs float64, err error) {
	if err := validateOCIVMRSInput(params); err != nil {
		return 0, 0, err
	}

	activeVMsPerPair := 2 // Active-Active: both VMs serve
	if params.IsRegionalHA {
		activeVMsPerPair = 1 // Active-Passive: only primary serves
	}
	totalActiveVMs := int(params.HAPairs) * activeVMsPerPair

	totalCapacityTB := float64(params.SizeInBytes) / 1e12
	totalThroughputGBs := float64(params.CustomPerformanceParams.ThroughputMibps) * 1.048576 / 1000.0

	perVMThroughputGBs = totalThroughputGBs / float64(totalActiveVMs)
	perDiskCapacityTB = totalCapacityTB / float64(int(params.HAPairs)*dataDiskCount)
	return perDiskCapacityTB, perVMThroughputGBs, nil
}

// validateOCIVMRSInput verifies that the pool params and worker
// configuration carry every value computeOCIVMRSInput needs before it
// does any arithmetic. Split out from computeOCIVMRSInput so:
//
//  1. The math half of computeOCIVMRSInput stays readable (formulas,
//     not five `if … return` blocks at the top).
//  2. The check order is the single source of truth — any caller that
//     wants to pre-flight inputs before reaching the workflow can call
//     this directly instead of duplicating the conditions.
//  3. Each check returns a *UserInputValidationErr so the API surfaces
//     the failure as a 4xx and Temporal does not burn retries on
//     unrecoverable workflow-author / API-caller bugs.
//
// Order matters: topology fields first (HAPairs, dataDiskCount, Size),
// then performance fields (CustomPerformanceParams existence and
// ThroughputMibps). Performance validation must come last because it
// dereferences params.CustomPerformanceParams — the nil check guards
// the subsequent ThroughputMibps read.
func validateOCIVMRSInput(params *common.CreatePoolParams) error {
	if params.HAPairs <= 0 {
		return utilserrors.NewUserInputValidationErr(
			"haPairs must be > 0 for OCI VMRS",
		)
	}
	if dataDiskCount <= 0 {
		return utilserrors.NewUserInputValidationErr(
			"OCI_VSA_DATA_DISK_COUNT must be > 0 for OCI VMRS",
		)
	}
	if params.SizeInBytes <= 0 {
		return utilserrors.NewUserInputValidationErr(
			"size is required for OCI VMRS; CreatePoolParams.SizeInBytes must be > 0",
		)
	}
	if params.CustomPerformanceParams == nil {
		return utilserrors.NewUserInputValidationErr(
			"customPerformanceParams is required for OCI VMRS; throughput cannot be inferred",
		)
	}
	if params.CustomPerformanceParams.ThroughputMibps <= 0 {
		return utilserrors.NewUserInputValidationErr(
			"customPerformanceParams.throughputMibps must be > 0 for OCI VMRS",
		)
	}
	return nil
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
