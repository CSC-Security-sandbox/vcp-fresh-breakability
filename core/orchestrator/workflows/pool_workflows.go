package workflows

import (

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type PoolWorkflow struct {
	BaseWorkflow
	SE *database.Storage
}

type poolWorkflowStatus struct {
	ID         string
	customerID string
	status     string
}

// const customerActionTimeout = 30 * time.Minute

// Pool Workflow process pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	poolWF := new(PoolWorkflow)
	err := poolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	poolWF.Status = WorkflowStatusRunning
	err = poolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = poolWF.Run(ctx, params, pool)
	if err != nil {
		poolWF.Status = WorkflowStatusFailed
		err = poolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	poolWF.Status = WorkflowStatusCompleted
	err = poolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *PoolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = "created"
	wf.Logger = log.With(
		workflow.GetLogger(ctx),
		"workflowID", wf.ID,
		"customerID", wf.CustomerID,
	)

	return workflow.SetQueryHandler(ctx, "status", func() (*poolWorkflowStatus, error) {
		return &poolWorkflowStatus{
			ID:         wf.ID,
			status:     wf.Status,
			customerID: wf.CustomerID,
		}, nil
	})
}

func (wf *PoolWorkflow) Run(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (interface{}, error) {
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

	clusterName := params.Name + "-vsa"
	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateTenancy, params).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, err
	}
	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)

	dbPool := &datamodel.Pool{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreatingPool, &pool).Get(ctx, &dbPool)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, poolActivity.FailedPool, dbPool, err.Error()).Get(ctx, nil)
		}
	}()
	var vsaCluster *[]map[string]string
	err = workflow.ExecuteActivity(ctx, poolActivity.DeployDeploymentManager, clusterName, params.Region, params.CurrentZone, tenancyDetails.Network, tenancyDetails.SubnetworkName, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, sizeInGB).Get(ctx, &vsaCluster)
	if err != nil {
		return nil, err
	}
	node := &models.Node{
		Name:            (*vsaCluster)[0]["Name"],
		EndpointAddress: (*vsaCluster)[0]["NodeIp"],
		Username:        pool.Username,
		Password:        pool.Password,
		Zone:            (*vsaCluster)[0]["Zone"],
		InstanceType:    (*vsaCluster)[0]["MachineType"],
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.WaitForNodes, node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.WaitForAggr, node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var ontapVersion string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetOntapVersion, node).Get(ctx, &ontapVersion)
	if err != nil {
		return nil, err
	}

	clusterDetails := &datamodel.ClusterDetails{
		ExternalName: clusterName,
		OntapVersion: ontapVersion,
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, params.Name, params.AccountName, clusterDetails).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SaveNodeDetails, dbPool, vsaCluster).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var svm datamodel.Svm
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateSvmForPool, dbPool, node).Get(ctx, &svm)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateLifForSvm, node, *vsaCluster, dbPool, svm).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateNetworkIpRoute, node, svm.Name, tenancyDetails.Gateway).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreatedPool, dbPool).Get(ctx, nil)
	return nil, err
}

func (poolWF *PoolWorkflow) Revert(ctx workflow.Context) error {
	// Implement the revert logic for pool workflows
	// This might involve rolling back any changes made during the workflow execution
	return nil
}
