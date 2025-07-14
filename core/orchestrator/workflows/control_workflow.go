package workflows

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"time"

	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

const (
	// VolumeCreateDeleteSnapshotDeleteSeq is a placeholder used for sequence workflow instance that runs all
	// volume CREATE & DELETE operation and snapshot DELETE calls for a specific pool sequentially.
	VolumeCreateDeleteSnapshotDeleteSeq = "Account_%d_Location_%s_Pool_%s_Ops_Volume-CD-Snapshot-D"

	// PoolSubnetCreate is a placeholder used for sequence workflow instance that runs all
	// subnet create operation for a specific account and VPC sequentially.
	PoolSubnetCreate = "Account_%d_VPC_%s_Ops_PoolSubnet-C"

	// Signal is the name of the signal used to call sequential workflows.
	Signal = "req"
)

// SignalWorkflowParams holds the parameters for the child workflow to be executed upon receiving a signal.
type SignalWorkflowParams struct {
	Function string
	Args     []interface{}
	Options  workflow.ChildWorkflowOptions
}

// SequenceWorkflow is a workflow that listens for signals and executes child workflows sequentially.
func SequenceWorkflow(ctx workflow.Context) error {
	logger := workflow.GetLogger(ctx)

	exitFlag := false
	signalChan := workflow.GetSignalChannel(ctx, Signal)

	// This timer is used to check if the workflow should exit, if sitting idle.
	timeout := workflow.NewTimer(ctx, 3*time.Second)

	for {
		selector := workflow.NewSelector(ctx)

		selector.AddReceive(signalChan, func(c workflow.ReceiveChannel, more bool) {
			var signalWf SignalWorkflowParams
			c.Receive(ctx, &signalWf)

			ctx = workflow.WithChildOptions(ctx, signalWf.Options)
			if err := workflow.ExecuteChildWorkflow(ctx, signalWf.Function, signalWf.Args...).Get(ctx, nil); err != nil {
				logger.Error("Failed to execute child workflow", "error", err)
				return
			}
		})

		selector.AddFuture(timeout, func(f workflow.Future) {
			exitFlag = true
		})

		selector.Select(ctx)
		if exitFlag {
			// If the exit flag is set, we check if there are any pending signals.
			// The Selector selects any case randomly if multiple cases are ready.
			// Hence, we need to check if there are any pending signals before exiting to handle the random case.
			if selector.HasPending() {
				continue
			}
			// Exit the workflow.
			// This will ensure the workflow is not continuously waiting for signals.
			// This will also stop the workflow from exceeding the maximum event history size.
			logger.Info("Current value reached threshold, exiting workflow")
			break
		}
	}

	return nil
}

// ExecuteWorkflowSequentially sends a signal to the sequence workflow to execute a child workflow sequentially.
// If the sequence workflow is not running, it starts a new instance with the provided options and signals it.
//
//   - temporal: Temporal client instance.
//
//   - ctx: Go context.
//
//   - sequenceWfOptions: Workflow start options for the sequence workflow.
//     This includes options like WorkflowID, TaskQueue, WorkflowIDReusePolicy.
//     Sequence WorkflowID needs to have resource and its operations that we want to sequence post-fixed in below format
//
//     (Sequence-Uniqueness-ID)_Ops_(Resource1-C/U/D)-(Resource2-C/U/D)-...-(ResourceN-C/U/D)
//     , where C=CREATE, U=UPDATE, D=DELETE.
//
//   - wfFunction: The child workflow function to execute.
//
//   - wfOptions: Workflow options for the child workflow.
//     This includes options like TaskQueue, WorkflowID, and WorkflowIDReusePolicy.
//
//   - wfArgs: Arguments to pass to the child workflow function.
//
// Returns an error if the signal or workflow start fails.
func ExecuteWorkflowSequentially(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
	return ExecuteWorkflowSeq(temporal, ctx, sequenceWfOptions, wfFunction, wfOptions, wfArgs...)
}

// ExecuteWorkflowSeq is a variable that holds the function to execute workflows sequentially.
// This allows for easier mocking in tests.
var ExecuteWorkflowSeq = _executeWorkflowSequentially

func _executeWorkflowSequentially(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
	if err := validateWorkflowParams(sequenceWfOptions.ID, wfOptions); err != nil {
		return customerrors.New(fmt.Sprintf("Invalid parameters for sequence workflow execution, error: %v", err))
	}

	// Defaulting to customer task queue for the workflows, if not provided.
	if wfOptions.TaskQueue == "" {
		wfOptions.TaskQueue = workflowengine.CustomerTaskQueue
	}
	if sequenceWfOptions.TaskQueue == "" {
		sequenceWfOptions.TaskQueue = workflowengine.CustomerTaskQueue
	}

	// SignalWithStartWorkflow is used to signal the sequence workflow if running.
	// If the sequence workflow is not running, it will start a new instance with the provided options and signal it.
	_, err := temporal.SignalWithStartWorkflow(
		ctx,
		sequenceWfOptions.ID,
		Signal,
		SignalWorkflowParams{
			Function: getWorkflowName(wfFunction),
			Args:     wfArgs,
			Options:  wfOptions,
		},
		sequenceWfOptions,
		SequenceWorkflow,
	)
	if err != nil {
		return err
	}

	return nil
}

// getWorkflowName extracts the name of the workflow function from its pointer.
// This is used to ensure that the workflow name is correctly identified when starting workflows.
func getWorkflowName(fnx interface{}) string {
	// It uses reflection to get the function concrete value pointer, passes it to FuncForPC to get the
	// function name. It returns the name in the below format -
	// github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows.<function_name>
	// Then, we split it to get the last part, which is the actual function name.
	strs := strings.Split(runtime.FuncForPC(reflect.ValueOf(fnx).Pointer()).Name(), ".")
	return strs[len(strs)-1]
}

func validateWorkflowParams(sequenceWorkflowID string, wfOptions workflow.ChildWorkflowOptions) error {
	if sequenceWorkflowID == "" {
		return customerrors.New("sequence workflow ID cannot be empty")
	}
	if wfOptions.WorkflowID == "" {
		return customerrors.New("execution workflow ID cannot be empty")
	}

	return nil
}
