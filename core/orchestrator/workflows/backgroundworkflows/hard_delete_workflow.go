package backgroundworkflows

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/resource_events_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var fetchTemporalClient = _fetchTemporalClient

func _fetchTemporalClient(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

type SyncHardDeleteWF struct {
	workflows.BaseWorkflow
}

var scheduleHardDelete = env.GetBool("SCHEDULED_HARD_DELETE", false)

var _ workflows.WorkflowInterface = &SyncHardDeleteWF{}

func HardDeleteResourcesAndAccountWorkflow(ctx workflow.Context) error {
	logger := util.GetLogger(ctx)
	if scheduleHardDelete {
		ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
			"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
			// Adding a unique request ID for tracking purposes
			"requestID": utils.RandomUUID(),
		})

		retryPolicy, err := workflows.PopulateRetryPolicyParams()
		if err != nil {
			return err
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

		hardDeleteWF := HardDeleteResourcesAndAccountworkflow{}
		ctxWithParentWFID := util.AddExtraLoggerFields(ctx, map[string]interface{}{
			"parentWorkflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		})

		err = workflow.ExecuteActivity(ctxWithParentWFID, hardDeleteWF.HardDeleteResourcesAndAccountFunc).Get(ctx, nil)
		if err != nil {
			// Logging and not returning error to ensure that the other pools can still be processed.
			logger.Warnf("Failed to start hard delete, error: %v", err)
		}
		return nil
	} else {
		logger.Info("Scheduled hard delete is disabled. Exiting workflow.")
		return nil
	}
}

type HardDeleteResourcesAndAccountworkflow struct {
}

func (h *HardDeleteResourcesAndAccountworkflow) HardDeleteResourcesAndAccountFunc(ctx context.Context) error {
	temporalClient := fetchTemporalClient(ctx)
	logger := util.GetLogger(ctx)

	logger.Info("Starting hard Deleting of existing data")

	_, err := temporalClient.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                temporalutils.BackgroundTaskQueue,
			ID:                       utils.RandomUUID(),
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowRunTimeout:       temporalutils.GetWorkflowGlobalTimeout(),
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		SyncForHardDeleteWorkflow,
	)
	if err != nil {
		logger.Errorf("Failed to start workflow for scheduled hard delete, error: %v", err)
		return err
	}

	logger.Infof("Started sync workflow for scheduled hard delete")
	return nil
}

func SyncForHardDeleteWorkflow(ctx workflow.Context) error {
	syncHardDeletewf := new(SyncHardDeleteWF)
	err := syncHardDeletewf.Setup(ctx, nil)
	if err != nil {
		return err
	}
	syncHardDeletewf.Status = workflows.WorkflowStatusRunning
	_, errRun := syncHardDeletewf.Run(ctx, nil)
	if errRun != nil {
		syncHardDeletewf.Status = workflows.WorkflowStatusFailed
		syncHardDeletewf.Logger.Error("Failed to hard delete", "Error", errRun)
		return errRun
	}
	syncHardDeletewf.Status = workflows.WorkflowStatusCompleted
	syncHardDeletewf.Logger.Info("hard delete has been completed successfully")
	return nil
}

func (wf *SyncHardDeleteWF) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = utils.RandomUUID()
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, workflows.StatusQueryName, func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *SyncHardDeleteWF) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := wf.Logger

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, vsaerrors.ExtractCustomError(err)
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

	finishProjectEventActivity := &resource_events_activities.FinishProjectEventActivity{}
	hardDeleteActivity := &backgroundactivities.HardDeleteResourcesAndAccountActivity{}

	var accountList []*datamodel.Account
	err = workflow.ExecuteActivity(ctx, hardDeleteActivity.AccountAudit, ctx).Get(ctx, &accountList)
	if err != nil {
		logger.Error("Account audit activity failed.", "Error", err)
		return err, vsaerrors.ExtractCustomError(err)
	}

	logger.Info("Account audit activity completed.", "AccountList", accountList)

	for _, account := range accountList {
		var canHardDelete bool
		err = workflow.ExecuteActivity(ctx, finishProjectEventActivity.VerifySoftDeletedResourcesForAccount, account.Name).Get(ctx, &canHardDelete)
		if err != nil {
			logger.Error("Verify soft deleted resources activity execution failed.", "Error", err, "accountName", account.Name)
			return nil, vsaerrors.ExtractCustomError(err)
		}

		if canHardDelete {
			err = workflow.ExecuteActivity(ctx, finishProjectEventActivity.HardDeleteResourcesInOrder, account.Name).Get(ctx, nil)
			if err != nil {
				logger.Error("hard Delete activity execution failed.", "Error", err, "accountName", account.Name)
				return nil, vsaerrors.ExtractCustomError(err)
			}
		}
		logger.Info("hard Delete activity completed.", "accountName", account.Name)
	}
	return nil, nil
}
