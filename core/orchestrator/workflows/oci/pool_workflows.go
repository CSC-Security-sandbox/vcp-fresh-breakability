package oci

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

type ociCreatePoolWorkflow struct {
	workflows.BaseWorkflow
	SE *database.Storage
}

var _ workflows.WorkflowInterface = &ociCreatePoolWorkflow{}

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

	logger := util.GetLogger(ctx)
	logger.Infof("OCI Create Pool Workflow Run method called for pool: %s, account: %s", pool.Name, params.AccountName)

	// TODO: Implement OCI-specific pool creation logic
	return nil, nil
}

// OCICreatePoolWorkflow processes pool related requests from a customer for OCI.
func OCICreatePoolWorkflow(ctx workflow.Context, params *common.CreatePoolParams, pool *datamodel.Pool) error {
	createPoolWF := new(ociCreatePoolWorkflow)
	log := util.GetLogger(ctx)
	err := createPoolWF.Setup(ctx, params)
	if err != nil {
		return err
	}
	if err = createPoolWF.EnsureJobState(ctx, models.JobsStateNEW); err != nil {
		return err
	}

	createPoolWF.Status = workflows.WorkflowStatusRunning
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("failed to update job status to PROCESSING: %v", err)
		return err
	}
	_, errRun := createPoolWF.Run(ctx, params, pool)
	if errRun != nil {
		log.Errorf("error in ociCreatePoolWorkflow: %v", errRun)
		createPoolWF.Status = workflows.WorkflowStatusFailed
		err2 := createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), errRun)
		if err2 != nil {
			log.Errorf("failed to update job with err and status to ERROR: %v", err2)
			return err2
		}
		return errRun
	}
	createPoolWF.Status = workflows.WorkflowStatusCompleted
	err = createPoolWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("failed to update job status to DONE: %v", err)
		return err
	}
	return nil
}
