package workflows

import (
	"context"
	"database/sql"
	"fmt"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"time"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	vsaerror "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/api/iam/v1"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

var (
	_                                WorkflowInterface = &createPoolWorkflow{} // Enforcing the WorkflowInterface on createPoolWorkflow
	setupNwHeartbeatTimeout                            = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
	vmrsConfigPath                                     = env.GetString("VMRS_CONFIG_PATH", "config/vmrs_gcp.yaml")
	configureKmsConfigForSvmActivity                   = _configureKmsConfigForSvmActivity
	getSignedJwtToken                                  = auth.GetSignedJwtToken
	GetNewVSAClientWorkflowManager                     = _getNewVSAClientWorkflowManager
	enableMetrics                                      = env.GetBool("ENABLE_METRICS", true)
)

const (
	DefaultSvmName   = "gcnv"
	VLMCloudProvider = "gcp"
)

const (
	TimestampLayout = "20060102150405"
	SAIDPrefix      = "vsa-sa-"
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

// const customerActionTimeout = 30 * time.Minute

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
	_, err = createPoolWF.Run(ctx, params, pool)
	if err != nil {
		log.Errorf("error in createPoolWorkflow: %v", err)
		createPoolWF.Status = WorkflowStatusFailed
		err2 := createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			log.Errorf("failed to update job with err and status to DONE: %v", err2)
			return err2
		}
		return err
	}
	createPoolWF.Status = WorkflowStatusCompleted
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("failed to update job status to DONE: %v", err)
	}
	return err
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

func (wf *createPoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.CreatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
	subnetActivity := SubnetActivity{}
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

	tenantProjectNumber := new(string)
	err = workflow.ExecuteActivity(ctx, poolActivity.FindTenancyProject, params).Get(ctx, tenantProjectNumber)
	if err != nil {
		return nil, err
	}

	createSubnetJobUUID := new(string)
	err = workflow.ExecuteActivity(ctx, subnetActivity.CreateSubnetJob, params, pool, tenantProjectNumber).Get(ctx, createSubnetJobUUID)
	if err != nil {
		wf.Logger.Errorf("Failed to start create subnet workflow for account: %s & vpc: %s, error: %v", params.AccountName, params.VendorSubNetID, err)
		return nil, err
	}

	// Wait for the subnet creation job to complete using workflow.sleep.
	err = PollOnDBJob(ctx, *createSubnetJobUUID, retryPolicy.StartToCloseTimeout)
	if err != nil {
		wf.Logger.Errorf("Failed to wait for create subnet job %s to complete, error: %v", *createSubnetJobUUID, err)
		return nil, err
	}

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, subnetActivity.GetTenancyDetails, createSubnetJobUUID).Get(ctx, &tenancyDetails)
	if err != nil {
		wf.Logger.Errorf("Failed to get tenancy details for job %s, error: %v", *createSubnetJobUUID, err)
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatePoolSubnet, dbPool.UUID, tenancyDetails).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	dbPool.ClusterDetails.SubnetNames = tenancyDetails.SubnetworkNames

	rollbackManager.AddActivity(poolActivity.ReleaseSubnet, dbPool)
	setupNwCtx := workflow.WithHeartbeatTimeout(ctx, time.Duration(setupNwHeartbeatTimeout)*time.Second)
	err = workflow.ExecuteActivity(setupNwCtx, poolActivity.SetupNetwork, params.Region, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, tenancyDetails.Network).Get(setupNwCtx, nil)
	if err != nil {
		return nil, err
	}

	serviceAccount := &iam.ServiceAccount{}
	saTimestamp := time.Now().Format(TimestampLayout)
	serviceAccountID := fmt.Sprintf("%s%s", SAIDPrefix, saTimestamp)

	rollbackManager.AddActivity(poolActivity.DeleteServiceAccount, tenancyDetails.RegionalTenantProject, serviceAccountID)
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateServiceAccountWithStorageRole, tenancyDetails.RegionalTenantProject, serviceAccountID, pool.Name).Get(ctx, serviceAccount)
	if err != nil {
		return nil, err
	}
	dbPool.ServiceAccountId = serviceAccountID

	AutoTierBucketName := fmt.Sprintf("%s-%s", params.Region, dbPool.UUID)
	rollbackManager.AddActivity(poolActivity.DeleteAutoTierBucket, AutoTierBucketName)
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateAutoTierBucket, AutoTierBucketName, params.Region, tenancyDetails.RegionalTenantProject).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	dbPool.AutoTierBucketName = AutoTierBucketName
	credConfig := &vlm.OntapCredentials{}
	// Generate a deterministic, unique cluster name (Deployment ID) for the pool using pool name, account name, and primary zone.
	// This avoids collisions when the same pool name is used in different zones or accounts.
	// The generated ID is limited to 20 characters to comply with resource naming constraints.
	clusterName := utils.GenerateDeterministicID(params.Name+"-"+params.AccountName+"-"+params.PrimaryZone, 20)

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateOnTapCredentials, pool, params.Region, pool.DeploymentName).Get(ctx, &credConfig)
	if err != nil {
		return nil, err
	}

	rollbackManager.AddActivity(poolActivity.DeleteOnTapCredentials, pool)

	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)
	// Convert CustomPerformanceParams to CustomerRequestedPerformance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             params.CustomPerformanceParams.Iops,
		DesiredThroughputInMiBs: params.CustomPerformanceParams.ThroughputMibps,
		DesiredCapacityInGiB:    int64(sizeInGB),
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

	// Find the optimal VMs based on the customer requested performance.
	vlmConfig := &vlm.VLMConfig{}

	deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
	prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, clusterName, VLMCloudProvider, tenancyDetails.RegionalTenantProject)
	rollbackManager.AddWorkflow(vlm.VSALifecycleManagerQueue, vlm.DeleteVSAClusterDeploymentWorkflowName, deleteVSAClusterDeploymentRequest)

	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, clusterName, params.Region, params.PrimaryZone, params.SecondaryZone, tenancyDetails.Network, tenancyDetails.SubnetworkNames, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, serviceAccount.Email, pool.AutoTierBucketName).Get(ctx, vlmConfig)
	if err != nil {
		return nil, err
	}
	hostMap := make(map[string]string)
	createVSAClusterDeploymentRequest := &vlm.CreateVSAClusterDeploymentRequest{}
	prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest, *vlmConfig, *credConfig)
	createVSAClusterDeploymentResponse, err := vsaClientWorkflowManager.CreateVSAClusterDeployment(ctx, createVSAClusterDeploymentRequest)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateCloudDNSRecords, vlmConfig, pool.DeploymentName).Get(ctx, &hostMap)
	if err != nil {
		return nil, err
	}
	rollbackManager.AddActivity(poolActivity.DeleteCloudDNSRecords, hostMap)

	err = workflow.ExecuteActivity(ctx, poolActivity.SaveVSANodeDetails, dbPool, createVSAClusterDeploymentResponse.VLMConfig, pool.DeploymentName, hostMap).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}
	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Password: pool.PoolCredentials.Password, SecretID: pool.PoolCredentials.SecretID, DeploymentName: pool.DeploymentName, CertificateID: pool.PoolCredentials.CertificateID})

	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, err
	}

	clusterDetails := &datamodel.ClusterDetails{
		ExternalName:          clusterName,
		OntapVersion:          ontapVersion,
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
		SubnetNames:           tenancyDetails.SubnetworkNames,
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, dbPool, clusterDetails).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	createSVMRequest := &vlm.CreateSVMRequest{}
	prepareCreateSVMRequest(createSVMRequest, DefaultSvmName, createVSAClusterDeploymentResponse.VLMConfig, *credConfig)
	createSVMResponse, err := vsaClientWorkflowManager.CreateVSASVM(ctx, createSVMRequest)
	if err != nil {
		return nil, err
	}

	svm := &datamodel.Svm{}
	err = workflow.ExecuteActivity(ctx, poolActivity.SaveSVMAndLifData, dbPool, createSVMResponse.VLMConfig).Get(ctx, svm)
	if err != nil {
		return nil, err
	}

	// Create QoS policy and apply it to the SVM
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateQoSPolicyAndApplyToSVM, dbPool, svm, node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Enable KMS for SVM if KMS config is provided
	err = configureKmsConfigForSvmActivity(ctx, *dbPool, node, svm, params)
	if err != nil {
		return nil, err
	}
	dbPool.ClusterDetails.SubnetNames = tenancyDetails.SubnetworkNames

	err = workflow.ExecuteActivity(ctx, poolActivity.CreatedPool, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func prepareCreateVSAClusterDeploymentRequest(createVSAClusterDeploymentRequest *vlm.CreateVSAClusterDeploymentRequest, vlmConfig vlm.VLMConfig, ontapCredentials vlm.OntapCredentials) {
	createVSAClusterDeploymentRequest.VLMConfig = vlmConfig
	createVSAClusterDeploymentRequest.OntapCredentials = ontapCredentials
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

type updatePoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// Enforcing the WorkflowInterface on createPoolWorkflow
var _ WorkflowInterface = &updatePoolWorkflow{}

// const customerActionTimeout = 30 * time.Minute

// UpdatePoolWorkflow processes pool related requests from a customer.
func UpdatePoolWorkflow(ctx workflow.Context, params *common.UpdatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	updatePoolWF := new(updatePoolWorkflow)
	err := updatePoolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	updatePoolWF.Status = WorkflowStatusRunning
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = updatePoolWF.Run(ctx, params, pool)
	if err != nil {
		updatePoolWF.Status = WorkflowStatusFailed
		err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	updatePoolWF.Status = WorkflowStatusCompleted
	err = updatePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
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

func (wf *updatePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	updatePoolParams := args[0].(*common.UpdatePoolParams)
	pool := args[1].(*datamodel.Pool)
	poolActivity := &activities.PoolActivity{}
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
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	dbPool := pool
	wf.Logger.Info("Updating pool with new parameters", "params", updatePoolParams) // Update the pool with the new parameters

	// Reconstruct the existing VLM config.
	dsc := &vmrs.Decision{
		ChosenVMs: []string{""}, // Doesn't matter for retrieving existing VLM config
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             int64(pool.PoolAttributes.Iops),
			DesiredThroughputInMiBs: int64(pool.PoolAttributes.ThroughputMibps),
			DesiredCapacityInGiB:    int64(utils.BytesToGigabytes(uint64(pool.SizeInBytes))),
		},
	}
	currentVlmConfig := &vlmconfig.VLMConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVlmConfig, pool.ClusterDetails.ExternalName, updatePoolParams.Region, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, pool.ClusterDetails.SubnetNames, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject, dsc, pool.KmsConfig.ServiceAccount.ServiceAccountEmail, pool.AutoTierBucketName).Get(ctx, currentVlmConfig)
	if err != nil {
		return nil, err
	}

	// Find the optimal VMs based on the customer requested performance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             int64(updatePoolParams.TotalIops),
		DesiredThroughputInMiBs: int64(updatePoolParams.TotalThroughputMibps),
		DesiredCapacityInGiB:    int64(utils.BytesToGigabytes(updatePoolParams.SizeInBytes)),
	}

	newVlmConfig := &vlm.VLMConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, pool.ClusterDetails.ExternalName, updatePoolParams.Region, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, pool.ClusterDetails.SubnetNames, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject, pool.KmsConfig.ServiceAccount.ServiceAccountEmail, pool.AutoTierBucketName).Get(ctx, newVlmConfig)
	if err != nil {
		return nil, err
	}
	credentials := &vlm.OntapCredentials{}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOnTapCredentials, pool).Get(ctx, &credentials)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.UpdateVSACluster, credentials, currentVlmConfig, newVlmConfig).Get(ctx, newVlmConfig)
	if err != nil {
		return nil, err
	}

	poolObj := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: dbPool.UUID,
		},
		SizeInBytes: int64(updatePoolParams.SizeInBytes),
		Description: updatePoolParams.Description,
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: int64(updatePoolParams.TotalThroughputMibps),
			Iops:            int64(updatePoolParams.TotalIops),
			Labels:          updatePoolParams.Labels,
		},
	}

	rollbackManager.AddActivity(poolActivity.UpdatedPool, pool)
	err = workflow.ExecuteActivity(ctx, poolActivity.UpdatedPool, poolObj).Get(ctx, nil) // replace with the actual activity to update the pool
	return nil, err
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
		return nil, err
	}
	deletePoolWF.Status = WorkflowStatusRunning
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = deletePoolWF.Run(ctx, params, pool)
	if err != nil {
		deletePoolWF.Status = WorkflowStatusFailed
		err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	deletePoolWF.Status = WorkflowStatusCompleted
	err = deletePoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, err
	}
	return nil, err
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

func (wf *deletePoolWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.DeletePoolParams)
	poolActivity := &activities.PoolActivity{}
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
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: params.PoolID},
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
	if err != nil {
		return nil, err
	}

	// Add the cleanup / rollback activity using this rollback.AddActivity() method instead of writing multiple defer statements,
	// this rollback manager will be invoked whenever there is an error, and it will start calling clean up activities in LIFO manner ***/
	rollbackManager.AddActivity(poolActivity.FailedPool, dbPool, "Failed to delete pool")

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletingPoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	hostMap := make(map[string]string)
	err = workflow.ExecuteActivity(ctx, poolActivity.GetCloudDNSRecords, dbPool.ID).Get(ctx, &hostMap)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteCloudDNSRecords, hostMap).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	vsaClientWorkflowManager := GetNewVSAClientWorkflowManager()

	deleteVSAClusterDeploymentRequest := &vlm.DeleteVSAClusterDeploymentRequest{}
	prepareDeleteVSAClusterDeployment(deleteVSAClusterDeploymentRequest, dbPool.ClusterDetails.ExternalName, VLMCloudProvider, dbPool.ClusterDetails.RegionalTenantProject)
	err = vsaClientWorkflowManager.DeleteVSAClusterDeployment(ctx, deleteVSAClusterDeploymentRequest)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteAutoTierBucket, dbPool.AutoTierBucketName).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteServiceAccount, dbPool.ClusterDetails.RegionalTenantProject, dbPool.ServiceAccountId).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.ReleaseSubnet, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteOnTapCredentials, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.DeletePoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	if enableMetrics {
		// Execute Child Work to start poller on harvest farm
		childWorkflowOptions := workflow.ChildWorkflowOptions{}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)
		unregisterParams := &unRegisterNodeFromHarvestFarmParams{
			PoolID: dbPool.ID,
		}
		err = workflow.ExecuteChildWorkflow(ctx, UnRegisterNodeFromHarvestFarmWorkflow, unregisterParams).Get(childCtx, nil)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func _configureKmsConfigForSvmActivity(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
	if params.KmsConfigId == "" {
		return nil // No KMS config provided, nothing to configure
	}

	kmsConfigActivity := &kms_activities.KmsConfigActivity{}
	kmsConfig := &datamodel.KmsConfig{KmsAttributes: &datamodel.KmsAttributes{}}

	// Check if KMS config is present in the VSA database
	// In case Kms config is not present in the VSA database, will create a new KMS configuration using the SDE KMS configuration
	err := workflow.ExecuteActivity(ctx, kmsConfigActivity.GetKmsConfigActivity, params.KmsConfigId).Get(ctx, kmsConfig)
	if err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.NonRetryable() && appErr.Type() == kms_activities.ErrTypeKmsConfigNotFound {
			if runningEnv != "local" {
				// get the JWT token for authorization; this function needs GCP_AUTH_SERVICE_ACCOUNT and GCP_SERVICE_URL to be set for the environment
				jwtToken, err := getSignedJwtToken(params.AccountName)
				if err != nil {
					return err
				}
				ctx = workflow.WithValue(ctx, middleware.AuthorizationToken, jwtToken)
			}

			// Prepare the KMS configuration object with the SDE KMS configuration details
			getKmsConfigParams := &common.GetKmsConfigParams{
				UUID:          params.KmsConfigId,
				LocationID:    params.Region,
				ProjectNumber: params.AccountName,
			}

			var cvpKmsConfig cvpmodels.KmsConfigV1beta
			// Describe KMS configurations to get the created KMS configuration; this must be called after polling the operation
			err = workflow.ExecuteActivity(ctx, kmsConfigActivity.DescribeSDEKmsConfigurationActivity, getKmsConfigParams).Get(ctx, &cvpKmsConfig)
			if err != nil {
				return err
			}

			// create and sync the KMS configuration with the SDE KMS configuration
			createKmsConfigParams := ConvertToCreateKmsConfigParams(cvpKmsConfig, params)
			err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateAndSyncKmsConfigActivity, createKmsConfigParams).Get(ctx, kmsConfig)
			if err != nil {
				return err
			}

			// Create the service account key for the KMS configuration
			err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateVSAKmsConfigSAKeyActivity, kmsConfig).Get(ctx, kmsConfig)
			if err != nil {
				return err
			}

			// Grant the necessary roles to the service account
			err = workflow.ExecuteActivity(ctx, kmsConfigActivity.GrantRoleActivity, kmsConfig).Get(ctx, nil)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	// Access a crypto key using the KMS config in the VSA database to make sure key is reachable
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.AccessCryptoKeyWithImpersonationActivity, kmsConfig).Get(ctx, kmsConfig)
	if err != nil {
		return err
	}

	// Creates DNS to reach google KMS from the VSA cluster
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateDnsActivity, node).Get(ctx, nil)
	if err != nil {
		return err
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

	// Update the Pool with the KMS config IDs
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.UpdatePoolWithKmsConfigActivity, pool, kmsConfig.UUID).Get(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

// ConvertToCreateKmsConfigParams transforms from CVP datamodel to VSA datamodel
func ConvertToCreateKmsConfigParams(params cvpmodels.KmsConfigV1beta, createPoolParams *common.CreatePoolParams) *common.CreateKmsConfigParams {
	createConfigParams := &common.CreateKmsConfigParams{}

	createConfigParams.ProjectNumber = createPoolParams.AccountName
	createConfigParams.UUID = params.UUID
	createConfigParams.KmsState = params.KmsState
	createConfigParams.KmsStateDetails = params.KmsStateDetails
	createConfigParams.ServiceAccountEmail = params.ServiceAccountEmail
	createConfigParams.Instructions = params.Instructions
	createConfigParams.LocationID = createPoolParams.Region

	if params.Description != nil {
		createConfigParams.Description = *params.Description
	}
	if params.KeyFullPath != nil {
		createConfigParams.KeyFullPath = *params.KeyFullPath
	}
	if params.ResourceID != nil {
		createConfigParams.ResourceID = *params.ResourceID
	}
	return createConfigParams
}

func _getNewVSAClientWorkflowManager() vlm.VlmWorkflowClient {
	return vlm.NewVSAClientWorkflowManager()
}

type subnetWorkflowResult struct {
	WorkflowStatus *WorkflowStatus
	TenancyDetails *common.TenancyInfo
}

// PoolDataSubnetWorkFlow processes get pr create subnet for the pool related requests from a customer.
func PoolDataSubnetWorkFlow(ctx workflow.Context, params *common.CreatePoolParams, tenantProjectNumber string) (gcpgenserver.V1betaDescribePoolRes, error) {
	CreateOrGetSubnetworkWF := new(poolDataSubnetWorkFlow)
	err := CreateOrGetSubnetworkWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	CreateOrGetSubnetworkWF.Status = WorkflowStatusRunning
	err = CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = CreateOrGetSubnetworkWF.Run(ctx, params, tenantProjectNumber)
	if err != nil {
		CreateOrGetSubnetworkWF.Status = WorkflowStatusFailed
		upErr := CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if upErr != nil {
			return nil, upErr
		}
		return nil, err
	}
	CreateOrGetSubnetworkWF.Status = WorkflowStatusCompleted
	err = CreateOrGetSubnetworkWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
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

func (wf *poolDataSubnetWorkFlow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.CreatePoolParams)
	tenantProjectNumber := args[1].(string)
	poolActivity := &activities.PoolActivity{}
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
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateOrGetSubnetwork, params, tenantProjectNumber).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, err
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

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateSubnet),
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
		},
		params,
		tenantProjectNumber,
	)
	if err != nil {
		logger.Errorf("Failed to start create subnet workflow for account: %s & vpc: %s, job: %s, error: %v", params.AccountName, vpcName, createdJob.UUID)
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
		return nil, err
	}
	err = encVal.Get(&subnetWfRes)
	if err != nil {
		return nil, err
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
