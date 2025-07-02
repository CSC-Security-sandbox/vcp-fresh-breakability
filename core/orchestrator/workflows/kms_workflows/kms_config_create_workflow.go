package kms_workflows

import (
	"time"

	cvpmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type createKmsConfigWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeCreateWorkflow
var _ workflows.WorkflowInterface = &createKmsConfigWorkflow{}

var (
	cvpMaxPollTimeout = env.GetUint64("CVP_JOB_POLL_TIMEOUT_MIN", 20)
	cvpPollInterval   = env.GetUint64("CVP_JOB_POLL_INTERVAL_SEC", 30)
)

// CreateKmsConfigWorkflow KMS Config Workflow process pool related requests from a customer.
func CreateKmsConfigWorkflow(ctx workflow.Context, params *common.CreateKmsConfigParams, kmsConfig *datamodel.KmsConfig) (interface{}, error) {
	kmsConfigWorkflow := new(createKmsConfigWorkflow)
	err := kmsConfigWorkflow.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	kmsConfigWorkflow.Status = workflows.WorkflowStatusRunning
	err = kmsConfigWorkflow.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = kmsConfigWorkflow.Run(ctx, params, kmsConfig)

	if err != nil {
		kmsConfigWorkflow.Status = workflows.WorkflowStatusFailed
		err = kmsConfigWorkflow.UpdateJobStatus(ctx, string(models.JobsStateERROR), err)
		if err != nil {
			return nil, err
		}
	}

	kmsConfigWorkflow.Status = workflows.WorkflowStatusCompleted
	err = kmsConfigWorkflow.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (kmsConfigWorkflow *createKmsConfigWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createKmsConfigParams := input.(*common.CreateKmsConfigParams)
	info := workflow.GetInfo(ctx)
	kmsConfigWorkflow.ID = info.WorkflowExecution.ID
	kmsConfigWorkflow.CustomerID = createKmsConfigParams.AccountName
	kmsConfigWorkflow.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": kmsConfigWorkflow.ID, "customerID": kmsConfigWorkflow.CustomerID})
	logger := util.GetLogger(ctx)
	kmsConfigWorkflow.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:         kmsConfigWorkflow.ID,
			Status:     kmsConfigWorkflow.Status,
			CustomerID: kmsConfigWorkflow.CustomerID,
		}, nil
	})
}

func (kmsConfigWorkflow *createKmsConfigWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	params := args[0].(*common.CreateKmsConfigParams)
	kmsConfig := args[1].(*datamodel.KmsConfig)
	kmsConfigActivity := &kms_activities.KmsConfigActivity{}
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
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
	rollbackManager := common.NewRollbackManager()
	rollbackManager.Add(kmsConfigActivity.FailedKmsConfigCreateActivity, kmsConfig)
	defer func() {
		if err != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	// retry policy for polling the KMS configuration operation
	pollingOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(cvpMaxPollTimeout) * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			InitialInterval:    time.Duration(cvpPollInterval) * time.Second,
		},
	}

	pollingCtx := workflow.WithActivityOptions(ctx, pollingOptions)

	// Poll the KMS configuration operation until it is done
	err = workflow.ExecuteActivity(pollingCtx, kmsConfigActivity.PollKmsConfigOperationActivity, params).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Describe KMS configurations to get the created KMS configuration; this must be called after polling the operation to get the sde kms config information
	getKmsConfigParams := &common.GetKmsConfigParams{
		UUID:          kmsConfig.KmsAttributes.SdeKmsConfigUUID,
		LocationID:    params.LocationID,
		ProjectNumber: params.ProjectNumber,
	}
	var cvpKmsConfig cvpmodels.KmsConfigV1beta
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.DescribeSDEKmsConfigurationActivity, getKmsConfigParams).Get(ctx, &cvpKmsConfig)
	if err != nil {
		return nil, err
	}

	// Update the KMS configuration attributes in the database
	kmsConfig.KmsAttributes = &datamodel.KmsAttributes{
		SdeKmsConfigUUID:       cvpKmsConfig.UUID,
		SdeServiceAccountEmail: cvpKmsConfig.ServiceAccountEmail,
		SdeKmsState:            cvpKmsConfig.KmsState,
		SdeKmsStateDetails:     cvpKmsConfig.KmsStateDetails,
		Instructions:           cvpKmsConfig.Instructions,
	}
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.UpdateKmsConfigAttributesActivity, kmsConfig, kmsConfig.KmsAttributes).Get(ctx, kmsConfig)
	if err != nil {
		return nil, err
	}

	// After the KMS configuration is created, we need to perform additional steps like creating service account keys and granting roles
	// Create the service account key for the KMS configuration
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreateVSAKmsConfigSAKeyActivity, kmsConfig).Get(ctx, kmsConfig)
	if err != nil {
		return nil, err
	}

	// Grant the necessary roles to the service account
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.GrantRoleActivity, kmsConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Update the Created the KMS configuration in the database
	err = workflow.ExecuteActivity(ctx, kmsConfigActivity.CreatedKmsConfigActivity, kmsConfig).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	return kmsConfig, err
}

func (kmsConfigWorkflow *createKmsConfigWorkflow) RevertCreateKmsConfigWorkflow(ctx workflow.Context) error {
	// Implement the revert logic for kms config workflows
	// This might involve rolling back any changes made during the workflow execution
	return nil
}
