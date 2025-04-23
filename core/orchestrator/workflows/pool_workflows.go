package workflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

type poolWorkflow struct {
	// add fields needed for pool workflow
	ID string
	// customerID  string
	status string
	Logger log.Logger
}

// const customerActionTimeout = 30 * time.Minute

// Pool Workflow process pool related requests from a customer.
func CreatePool(ctx workflow.Context, params gcpgenserver.V1betaDescribePoolParams) (gcpgenserver.V1betaDescribePoolRes, error) {
	poolWF := new(poolWorkflow)
	err := poolWF.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	poolWF.status = WorkflowStatusRunning
	err = poolWF.UpdateStatus(ctx)
	if err != nil {
		return nil, err
	}
	res, err := poolWF.Run(ctx, params)
	if err != nil {
		poolWF.status = WorkflowStatusFailed
	}
	poolWF.status = WorkflowStatusCompleted
	err = poolWF.UpdateStatus(ctx)
	if err != nil {
		return nil, err
	}
	return res.(gcpgenserver.V1betaDescribePoolRes), err
}

func (wf *poolWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	// Implement the setup logic for pool workflows
	return nil
}

func (wf *poolWorkflow) Run(ctx workflow.Context, input interface{}) (interface{}, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	poolActivity := activities.PoolActivity{}

	var choices []string
	err := workflow.ExecuteActivity(ctx, poolActivity.CreatePoolActivity1).Get(ctx, &choices)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreatePoolActivity2).Get(ctx, &choices)
	if err != nil {
		return nil, err
	}
	err = workflow.ExecuteActivity(ctx, poolActivity.CreatePoolActivity3).Get(ctx, &choices)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (poolWF *poolWorkflow) UpdateStatus(ctx workflow.Context) error {
	updateJobStatus := &datamodel.Job{
		ID:     poolWF.ID,
		Status: poolWF.status,
	}

	ctx = workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		ScheduleToCloseTimeout: 5 * time.Second,
	})
	return workflow.ExecuteLocalActivity(ctx, activities.CommonActivities.UpdateJobStatus, updateJobStatus).Get(ctx, nil)
}

func (poolWF *poolWorkflow) Revert(ctx workflow.Context) error {
	poolWF.Logger.Info("Reverting workflow", "ID", poolWF.ID)
	// Add workflow logic here
	return nil
}
