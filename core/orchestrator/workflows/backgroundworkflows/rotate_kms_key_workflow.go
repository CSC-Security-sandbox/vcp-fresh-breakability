package backgroundworkflows

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
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
		future := workflow.ExecuteActivity(ctx, rotateKmsSAKeyActivity.RotateServiceAccountKey, kmsConfig.ServiceAccount, kmsConfig)
		futures = append(futures, future)
	}

	keyRotationFailed := false
	for index, future := range futures {
		err := future.Get(ctx, nil)
		if err != nil {
			keyRotationFailed = true
			logger.Error(fmt.Sprintf(
				"key rotation failed for service account %s", kmsConfigs[index].ServiceAccount.ServiceAccountEmail),
				log.Fields{"error": err})
		}
	}
	if keyRotationFailed {
		return errors.New("key rotation failed for one or more service accounts")
	}
	return nil
}
