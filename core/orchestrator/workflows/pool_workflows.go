package workflows

import (
	"fmt"
	"time"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/api/iam/v1"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

var (
	_                                WorkflowInterface = &createPoolWorkflow{} // Enforcing the WorkflowInterface on createPoolWorkflow
	secretManagerEnabled                               = env.GetBool("SECRET_MANAGER_ENABLED", false)
	setupNwHeartbeatTimeout                            = env.GetUint64("SETUP_NW_HEARTBEAT_TIMEOUT_SEC", 300)
	vmrsConfigPath                                     = env.GetString("VMRS_CONFIG_PATH", "config/vmrs_gcp.yaml")
	configureKmsConfigForSvmActivity                   = _configureKmsConfigForSvmActivity
	getSignedJwtToken                                  = auth.GetSignedJwtToken
)

const (
	TimestampLayout = "20060102150405"
	SAIDPrefix      = "vsa-sa-"
)

type createPoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

// const customerActionTimeout = 30 * time.Minute

// CreatePoolWorkflow processes pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	createPoolWF := new(createPoolWorkflow)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	createPoolWF.Status = WorkflowStatusRunning
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = createPoolWF.Run(ctx, params, pool)
	if err != nil {
		createPoolWF.Status = WorkflowStatusFailed
		err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	createPoolWF.Status = WorkflowStatusCompleted
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
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

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateTenancy, params, dbPool.UUID).Get(ctx, &tenancyDetails)
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

	secret := &hyperscalermodels.CustomSecret{}
	var vsaClusterPassword string
	if common.AuthType == common.USERNAME_PWD_SEC_MGR {
		err = workflow.ExecuteActivity(ctx, poolActivity.CreateSecret, params.Region, pool.SecretID).Get(ctx, secret)
		if err != nil {
			return nil, err
		}
		vsaClusterPassword = secret.SecretVersion.Value
		rollbackManager.AddActivity(poolActivity.DeleteSecret, pool.SecretID)
	} else {
		vsaClusterPassword = pool.Password
	}

	// Use deployment name as cluster name
	deploymentName := dbPool.DeploymentName
	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)
	// Convert CustomPerformanceParams to CustomerRequestedPerformance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             params.CustomPerformanceParams.Iops,
		DesiredThroughputInMiBs: params.CustomPerformanceParams.ThroughputMibps,
		DesiredCapacityInGiB:    int64(sizeInGB),
	}

	// Find the optimal VMs based on the customer requested performance.
	vlmConfig := &vlmconfig.VLMConfig{}

	poolWithClusterDetails := dbPool
	poolWithClusterDetails.ClusterDetails = datamodel.ClusterDetails{
		ExternalName:          "",
		RegionalTenantProject: tenancyDetails.RegionalTenantProject,
		SnHostProject:         tenancyDetails.SnHostProject,
		Network:               tenancyDetails.Network,
		SubnetNames:           tenancyDetails.SubnetworkNames}
	rollbackManager.AddActivity(poolActivity.DeleteVSADeployment, poolWithClusterDetails)

	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, deploymentName, params.Region, params.PrimaryZone, params.SecondaryZone, tenancyDetails.Network, tenancyDetails.SubnetworkNames, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, vsaClusterPassword, serviceAccount.Email, pool.AutoTierBucketName).Get(ctx, vlmConfig)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSACluster, vlmConfig).Get(ctx, vlmConfig)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SaveVSANodeDetails, dbPool, vlmConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	var dbNodes []*datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, pool.ID).Get(ctx, &dbNodes)
	if err != nil {
		return nil, err
	}

	node := common.CreateNodeForProvider(common.NodeProviderInput{Nodes: dbNodes, Username: pool.Username, Password: pool.Password, SecretID: pool.SecretID})

	node.Username = pool.Username
	if common.AuthType == common.USERNAME_PWD_SEC_MGR {
		node.SecretID = pool.SecretID
	} else {
		node.Password = pool.Password
	}

	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, err
	}

	clusterDetails := &datamodel.ClusterDetails{
		ExternalName:          vlmConfig.Deployment.DeploymentName + "-cluster", // FIXME: Replace with cluster name from VLM config instead of deployment name
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

	svm := &datamodel.Svm{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSASVM, dbPool, vlmConfig).Get(ctx, svm)
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
	return nil, err
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
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVlmConfig, pool.ClusterDetails.ExternalName, updatePoolParams.Region, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, pool.ClusterDetails.SubnetNames, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject, dsc, pool.Password, pool.KmsConfig.ServiceAccount.ServiceAccountEmail, pool.AutoTierBucketName).Get(ctx, currentVlmConfig)
	if err != nil {
		return nil, err
	}

	// Find the optimal VMs based on the customer requested performance.
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             int64(updatePoolParams.TotalIops),
		DesiredThroughputInMiBs: int64(updatePoolParams.TotalThroughputMibps),
		DesiredCapacityInGiB:    int64(utils.BytesToGigabytes(updatePoolParams.SizeInBytes)),
	}
	newVlmConfig := &vlmconfig.VLMConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.IdentifyVMs, vmrsConfigPath, customerRequestedPerformance, pool.ClusterDetails.ExternalName, updatePoolParams.Region, pool.PoolAttributes.PrimaryZone, pool.PoolAttributes.SecondaryZone, pool.ClusterDetails.Network, pool.ClusterDetails.SubnetNames, pool.ClusterDetails.RegionalTenantProject, pool.ClusterDetails.SnHostProject, pool.Password, pool.KmsConfig.ServiceAccount.ServiceAccountEmail, pool.AutoTierBucketName).Get(ctx, newVlmConfig)
	if err != nil {
		return nil, err
	}

	// Now it's time to invoke VLM.
	dup := &vlmconfig.DeploymentUpdateParams{
		VlmConfig: currentVlmConfig,
		NumHAPair: newVlmConfig.Deployment.NumHAPair,
		SPConfig:  newVlmConfig.Deployment.SPConfig,
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.UpdateVSACluster, dup).Get(ctx, newVlmConfig)
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

	// Add the cleanup / rollback activity using this rollback.Add() method instead of writing multiple defer statements,
	// this rollback manager will be invoked whenever there is an error, and it will start calling clean up activities in LIFO manner ***/
	rollbackManager.AddActivity(poolActivity.FailedPool, dbPool, "Failed to delete pool")

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletingPoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteVSADeployment, dbPool).Get(ctx, nil)
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

	if common.AuthType == common.USERNAME_PWD_SEC_MGR {
		err = workflow.ExecuteActivity(ctx, poolActivity.DeleteSecret, dbPool.SecretID).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.DeletePoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
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
			createKmsConfigParams := convertToCreateKmsConfigParams(cvpKmsConfig, params)
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
func convertToCreateKmsConfigParams(params cvpmodels.KmsConfigV1beta, createPoolParams *common.CreatePoolParams) *common.CreateKmsConfigParams {
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
