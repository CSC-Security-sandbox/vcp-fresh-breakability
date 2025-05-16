package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
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

// CreatePoolWorkflow process pool related requests from a customer.
func CreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	poolWF := new(PoolWorkflow)
	err := poolWF.SetupCreateWorkflow(ctx, params)
	if err != nil {
		return nil, err
	}
	poolWF.Status = WorkflowStatusRunning
	err = poolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = poolWF.RunCreatePoolWorkflow(ctx, params, pool)
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

func (wf *PoolWorkflow) SetupCreateWorkflow(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreatePoolParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = "created"
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return err
	}
	wf.Logger = logger.With(log.Fields{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})

	return workflow.SetQueryHandler(ctx, "status", func() (*poolWorkflowStatus, error) {
		return &poolWorkflowStatus{
			ID:         wf.ID,
			status:     wf.Status,
			customerID: wf.CustomerID,
		}, nil
	})
}

func (wf *PoolWorkflow) RunCreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) (interface{}, error) {
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

	tenancyDetails := &common.TenancyInfo{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateTenancy, params).Get(ctx, &tenancyDetails)
	if err != nil {
		return nil, err
	}

	clusterName := params.Name + "-vsa"
	sizeInGB := utils.BytesToGigabytes(params.SizeInBytes)

	err = workflow.ExecuteActivity(ctx, poolActivity.SetupNetwork, params.Region, tenancyDetails.Network, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	cfg := &vlmconfig.VLMConfig{}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSACluster, clusterName, params.Region, params.CurrentZone, tenancyDetails.Network, tenancyDetails.SubnetworkName, tenancyDetails.RegionalTenantProject, tenancyDetails.SnHostProject, sizeInGB).Get(ctx, cfg)
	if err != nil {
		return nil, err
	}

	node := &models.Node{}
	err = workflow.ExecuteActivity(ctx, poolActivity.SaveVSANodeDetails, dbPool, cfg).Get(ctx, node)
	if err != nil {
		return nil, err
	}
	node.Username = pool.Username
	node.Password = pool.Password
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
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.SavePoolWithClusterDetails, dbPool, clusterDetails).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.CreateVSASVM, dbPool, cfg).Get(ctx, nil)
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

// DeletePoolWorkflow runs delete workflow for a pool.
func DeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (gcpgenserver.V1betaDescribePoolRes, error) {
	poolWF := new(PoolWorkflow)
	err := poolWF.SetupDeleteWorkflow(ctx, params)
	if err != nil {
		return nil, err
	}
	poolWF.Status = WorkflowStatusRunning
	err = poolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = poolWF.RunDeletePoolWorkflow(ctx, params, pool)
	if err != nil {
		poolWF.Status = WorkflowStatusFailed
		err = poolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err != nil {
			return nil, err
		}
	}
	poolWF.Status = WorkflowStatusCompleted
	err = poolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (wf *PoolWorkflow) SetupDeleteWorkflow(ctx workflow.Context, input interface{}) error {
	deletePoolParams := input.(*common.DeletePoolParams)
	wf.CustomerID = deletePoolParams.AccountName
	wf.Status = "created"
	logger, err := util.GetLogger(ctx)
	if err != nil {
		return err
	}
	wf.Logger = logger.With(log.Fields{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})

	return workflow.SetQueryHandler(ctx, "status", func() (*poolWorkflowStatus, error) {
		return &poolWorkflowStatus{
			ID:         wf.ID,
			status:     wf.Status,
			customerID: wf.CustomerID,
		}, nil
	})
}

func (wf *PoolWorkflow) RunDeletePoolWorkflow(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (interface{}, error) {
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

	dbPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: params.PoolID},
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.GetPool, dbPool).Get(ctx, &dbPool)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = workflow.ExecuteActivity(ctx, poolActivity.FailedPool, dbPool, err.Error()).Get(ctx, nil)
		}
	}()

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletingPoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeleteVSADeployment, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.ReleaseSubnet, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, poolActivity.DeletePoolResources, dbPool).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}
