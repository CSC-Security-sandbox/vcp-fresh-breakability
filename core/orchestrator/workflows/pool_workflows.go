package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type poolWorkflow struct {
	// add fields needed for pool workflow
	ID         string
	customerID string
	status     string
	logger     log.Logger
}

type poolWorkflowStatus struct {
	ID         string
	customerID string
	status     string
}

// const customerActionTimeout = 30 * time.Minute

// Pool Workflow process pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	poolWF := new(poolWorkflow)
	err := poolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	poolWF.status = WorkflowStatusRunning
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStatePROCESSING), "")
	// if err != nil {
	//	return nil, err
	// }
	_, err = poolWF.Run(ctx, params, pool)
	if err != nil {
		poolWF.status = WorkflowStatusFailed
	}
	// poolWF.status = WorkflowStatusCompleted
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStateDONE), "")
	// if err != nil {
	//	return nil, err
	// }
	return nil, err
}

func (wf *poolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	wf.customerID = createPoolParams.AccountName
	wf.status = "created"
	wf.logger = log.With(
		workflow.GetLogger(ctx),
		"workflowID", wf.ID,
		"customerID", wf.customerID,
	)

	return workflow.SetQueryHandler(ctx, "status", func() (*poolWorkflowStatus, error) {
		return &poolWorkflowStatus{
			ID:         wf.ID,
			status:     wf.status,
			customerID: wf.customerID,
		}, nil
	})
}

func (wf *poolWorkflow) Run(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (interface{}, error) {
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
	err = workflow.ExecuteActivity(ctx, poolActivity.CreatePool, &pool).Get(ctx, &dbPool)
	if err != nil {
		return nil, err
	}

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

	var gateway string
	err = workflow.ExecuteActivity(ctx, poolActivity.GetProxyIP, strings.Split((*vsaCluster)[0]["dataLif"], "/")[0]).Get(ctx, &gateway)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateNetworkIpRoute, node, svm.Name, gateway).Get(ctx, nil)

	return nil, err
}

func (poolWF *poolWorkflow) UpdateStatus(ctx workflow.Context, status string, error string) error {
	updatedJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: poolWF.ID},
		State:     status,
	}
	if error != "" {
		updatedJob.ErrorDetails = []byte(error)
	}

	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Second,
	})
	return workflow.ExecuteLocalActivity(ctx, activities.CommonActivities.UpdateJobStatus, updatedJob).Get(ctx, nil)
}

func (poolWF *poolWorkflow) Revert(ctx workflow.Context) error {
	// Implement the revert logic for pool workflows
	// This might involve rolling back any changes made during the workflow execution
	return nil
}
