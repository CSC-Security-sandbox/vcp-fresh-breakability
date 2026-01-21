package background_kms_workflows

import (
	"errors"
	"fmt"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	kmsRotationEnabled = env.GetBool("GCP_KMS_KEY_ROTATION_ENABLED", false)
)

func RotateKmsSAKeyWorkflow(ctx workflow.Context) error {
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID": workflow.GetInfo(ctx).WorkflowExecution.ID,
		// Adding a unique request ID for tracking purposes
		"requestID": utils.RandomUUID(),
	})
	logger := workflow.GetLogger(ctx)

	if !kmsRotationEnabled {
		logger.Debug("KMS key rotation disabled. Skipping workflow execution.")
		return nil
	}

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		HeartbeatTimeout:    retryPolicy.HeartBeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rotateKmsSAKeyActivity := &backgroundactivities.RotateKmsSAKeyActivity{}

	var kmsConfigs []*datamodel.KmsConfig
	err = workflow.ExecuteActivity(ctx, rotateKmsSAKeyActivity.ListKmsConfigs).Get(ctx, &kmsConfigs)
	if err != nil {
		logger.Error("ListKmsConfigs activity failed.", "Error", err)
		return err
	}

	futures := make([]workflow.Future, 0, len(kmsConfigs))
	for _, kmsConfig := range kmsConfigs {
		// Execute child workflow for key rotation
		// The child workflow orchestrates all phases: validate, create key, store key, migrate pools, complete and deletes key
		childWorkflowOptions := workflow.ChildWorkflowOptions{
			WorkflowID:          workflow.GetInfo(ctx).WorkflowExecution.ID + "-child-" + kmsConfig.UUID,
			WorkflowRunTimeout:  retryPolicy.StartToCloseTimeout * 10, // Give child workflow more time
			WorkflowTaskTimeout: retryPolicy.StartToCloseTimeout,
		}
		childCtx := workflow.WithChildOptions(ctx, childWorkflowOptions)
		future := workflow.ExecuteChildWorkflow(childCtx, RotateKmsKeyChildWorkflow, kmsConfig.ServiceAccount, kmsConfig)
		futures = append(futures, future)
	}

	keyRotationFailed := false
	skippedKmsConfigs := 0
	for index, future := range futures {
		err := future.Get(ctx, nil)
		if err != nil {
			if strings.Contains(err.Error(), utils.StoragePoolCreatingStateError) {
				skippedKmsConfigs++
				logger.Warn(fmt.Sprintf(
					"Skipping KMS config %s (service account: %s) due to pools in Creating state: %v",
					kmsConfigs[index].UUID,
					kmsConfigs[index].ServiceAccount.ServiceAccountEmail,
					err))
				continue
			}

			keyRotationFailed = true
			logger.Error(fmt.Sprintf(
				"key rotation failed for service account %s", kmsConfigs[index].ServiceAccount.ServiceAccountEmail),
				"error", err)
		}
	}

	if skippedKmsConfigs > 0 {
		logger.Info(fmt.Sprintf("Skipped %d KMS config(s) due to pools in Creating state", skippedKmsConfigs))
	}
	if keyRotationFailed {
		return errors.New("key rotation failed for one or more service accounts")
	}
	return nil
}
