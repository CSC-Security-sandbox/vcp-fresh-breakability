package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/active_directory_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type ActiveDirectoryCreateWorkflow struct {
	BaseWorkflow
}

func CreateActiveDirectoryWorkflow(
	ctx workflow.Context,
	params *common.CreateActiveDirectoryParams,
	adRecord *datamodel.ActiveDirectory,
) (interface{}, error) {
	log := util.GetLogger(ctx)
	activeDirectoryWf := new(ActiveDirectoryCreateWorkflow)

	err := activeDirectoryWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup ActiveDirectoryCreateWorkflow: %v", err)
		return nil, err
	}

	activeDirectoryWf.Status = WorkflowStatusRunning
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for ActiveDirectoryCreateWorkflow: %v", err)
		return nil, err
	}

	_, customErr := activeDirectoryWf.Run(ctx, params, adRecord)
	if customErr != nil {
		log.Errorf("ActiveDirectoryCreateWorkflow completed with error: %v", customErr)
		activeDirectoryWf.Status = WorkflowStatusFailed
		err2 := activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for ActiveDirectoryCreateWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}

	activeDirectoryWf.Status = WorkflowStatusCompleted
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for ActiveDirectoryCreateWorkflow: %v", err)
	}
	return nil, err
}

func (wf *ActiveDirectoryCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createAdParams := input.(*common.CreateActiveDirectoryParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createAdParams.AccountId
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})
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

func (wf *ActiveDirectoryCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	activeDirectoryActivity := &active_directory_activities.ActiveDirectoryCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	params := args[0].(*common.CreateActiveDirectoryParams)
	adRecord := args[1].(*datamodel.ActiveDirectory)

	rollbackManager := common.NewRollbackManager()
	rollbackManager.AddActivity(activeDirectoryActivity.RollbackActiveDirectory, adRecord)
	defer func() {
		// Trigger the rollback only if there was an error, and we are not in SDE mode
		if err != nil && cvp.CVP_HOST == "" {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	if cvp.CVP_HOST == "" {
		logger.Info("CVP_HOST environment variable is not set, creating AD in VCP")
		err = workflow.ExecuteActivity(
			ctx,
			activeDirectoryActivity.CreateVcpActiveDirectory,
			adRecord,
		).Get(ctx, nil)
	} else {
		err = workflow.ExecuteActivity(
			ctx,
			activeDirectoryActivity.CreateSdeActiveDirectory,
			params,
		).Get(ctx, nil)
	}

	if err != nil {
		return nil, ConvertToVSAError(err)
	}

	return nil, nil
}

type ActiveDirectoryUpdateWorkflow struct {
	BaseWorkflow
}

func UpdateActiveDirectoryWorkflow(
	ctx workflow.Context,
	params *common.UpdateActiveDirectoryParams,
	adRecord *models.ActiveDirectory,
) (interface{}, error) {
	log := util.GetLogger(ctx)
	activeDirectoryWf := new(ActiveDirectoryUpdateWorkflow)

	err := activeDirectoryWf.Setup(ctx, params)
	if err != nil {
		log.Errorf("Failed to setup ActiveDirectoryUpdateWorkflow: %v", err)
		return nil, err
	}

	activeDirectoryWf.Status = WorkflowStatusRunning
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Processing for ActiveDirectoryUpdateWorkflow: %v", err)
		return nil, err
	}

	_, customErr := activeDirectoryWf.Run(ctx, params, adRecord)
	if customErr != nil {
		log.Errorf("ActiveDirectoryUpdateWorkflow completed with error: %v", customErr)
		activeDirectoryWf.Status = WorkflowStatusFailed
		err2 := activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		if err2 != nil {
			log.Errorf("Failed to update job status to Done with error for ActiveDirectoryUpdateWorkflow: %v", err2)
			return nil, err2
		}
		return nil, customErr
	}

	activeDirectoryWf.Status = WorkflowStatusCompleted
	err = activeDirectoryWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	if err != nil {
		log.Errorf("Failed to update job status to Done for ActiveDirectoryUpdateWorkflow: %v", err)
	}
	return nil, err
}

func (wf *ActiveDirectoryUpdateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	updateAdParams := input.(*common.UpdateActiveDirectoryParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = updateAdParams.AccountId
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": wf.ID,
		"customerID": wf.CustomerID,
	})
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

func (wf *ActiveDirectoryUpdateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)
	activeDirectoryActivity := &active_directory_activities.ActiveDirectoryUpdateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	if len(args) < 2 {
		return nil, ConvertToVSAError(vsaerrors.New("insufficient arguments provided to workflow"))
	}
	params := args[0].(*common.UpdateActiveDirectoryParams)
	oldAd := args[1].(*models.ActiveDirectory)

	if cvp.CVP_HOST == "" {
		logger.Info("CVP_HOST environment variable is not set, Updating AD in VCP only")
		err = wf.handleVcpUpdate(ctx, activeDirectoryActivity, params, oldAd)
		if err != nil {
			logger.Errorf("Failed to update Active Directory in VCP: %v", err)
			return nil, ConvertToVSAError(err)
		}
	} else {
		logger.Info("CVP_HOST environment variable is set, Updating AD in SDE first, then VCP (if applicable)")

		var sdeResult *cvpModels.OperationV1beta
		// Trigger SDE update first
		err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.UpdateSdeActiveDirectory, params).Get(ctx, &sdeResult)
		if err != nil {
			logger.Errorf("Failed to update Active Directory in SDE: %v", err)
			return nil, ConvertToVSAError(err)
		}

		if sdeResult == nil {
			logger.Errorf("Failed to update Active Directory in SDE: SDE result is nil")
			return nil, ConvertToVSAError(vsaerrors.New("SDE Result is nil"))
		}

		// Prepare to Poll the SDE update status
		pollingOptions := workflow.ActivityOptions{
			StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        retryPolicy.InitialInterval,
				BackoffCoefficient:     retryPolicy.BackoffCoefficient,
				MaximumInterval:        retryPolicy.MaximumInterval,
				MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
				NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
			},
		}
		pollingCtx := workflow.WithActivityOptions(ctx, pollingOptions)

		// Poll the SDE Update Operation until completion
		err = workflow.ExecuteActivity(pollingCtx, activeDirectoryActivity.PollSdeUpdateActivity, params, sdeResult).Get(pollingCtx, nil)
		if err != nil {
			logger.Errorf("SDE polling failed: %v, Skipping next steps", err)
			return nil, ConvertToVSAError(err)
		}
		logger.Info("SDE AD Update operation completed successfully")

		// Only proceed to VCP update if SDE update succeeded
		logger.Info("SDE update successful, proceeding with VCP update")
		// In SDE flow, VCP AD Db record is not marked to updating earlier, thus marking as updating here.
		err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.MarkVcpAdToUpdatingActivity, params, oldAd).Get(ctx, nil)
		if err != nil {
			logger.Errorf("Failed to mark Active Directory in VCP to Updating State: %v", err)
			return nil, ConvertToVSAError(err)
		}

		err = wf.handleVcpUpdate(ctx, activeDirectoryActivity, params, oldAd)
		if err != nil {
			logger.Errorf("Failed to update Active Directory in VCP: %v", err)
			return nil, ConvertToVSAError(err)
		}
	}

	return nil, nil
}

func (wf *ActiveDirectoryUpdateWorkflow) handleVcpUpdate(
	ctx workflow.Context,
	activeDirectoryActivity *active_directory_activities.ActiveDirectoryUpdateActivity,
	params *common.UpdateActiveDirectoryParams,
	oldAd *models.ActiveDirectory,
) error {
	logger := util.GetLogger(ctx)

	rollbackManager := common.NewRollbackManager()
	rollbackManager.AddActivity(activeDirectoryActivity.MarkVcpAdToErrorActivity, params, oldAd)

	var err error
	defer func() {
		// Trigger the rollback only if there was an error
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	// Change ID to be populated both at AD table and pool table
	vcpActiveDirectoryChangeId := utils.RandomUUID()

	// Push updates to pool and SVMs first
	logger.Info("Pushing updates to pool and SVMs before VCP update")
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.PushUpdatesDownstreamActivity, oldAd, vcpActiveDirectoryChangeId).Get(ctx, nil)

	if err != nil {
		logger.Errorf("Failed to push Active Directory updates to pool and SVMs: %v", err)
		return err
	}

	// Then update VCP
	logger.Info("Updating VCP Active Directory")
	err = workflow.ExecuteActivity(ctx, activeDirectoryActivity.UpdateVcpActiveDirectory, params, oldAd, vcpActiveDirectoryChangeId).Get(ctx, nil)

	if err != nil {
		logger.Errorf("Failed to update Active Directory in VCP: %v", err)
		return err
	}

	return nil
}
