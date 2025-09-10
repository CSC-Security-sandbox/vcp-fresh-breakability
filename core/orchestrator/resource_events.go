package orchestrator

import (
	"context"
	"database/sql"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

var (
	createOrGetStartProjectEventJob = _createOrGetStartProjectEventJob
	updateResourceState             = _updateResourceState
)

func (o *Orchestrator) CreateOrGetStartProjectEventJob(ctx context.Context, params *commonparams.StartProjectEventParams) (string, error) {
	return createOrGetStartProjectEventJob(ctx, o.storage, o.temporal, params)
}

func _createOrGetStartProjectEventJob(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.StartProjectEventParams) (string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return "", err
	}

	var jobType string
	var wf func(ctx workflow.Context, params *commonparams.StartProjectEventParams) (interface{}, error)
	// For DELETE state we already returned NotImplemented error
	switch params.State {
	case models.StateOn:
		wf = workflows.StartProjectEventOnStateWorkflow
		jobType = string(models.JobTypeStartProjectEventOnState)
	case models.StateOff:
		wf = workflows.StartProjectEventOffStateWorkflow
		jobType = string(models.JobTypeStartProjectEventOffState)
	}

	jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID),
		utils2.NewFilterCondition("type", "=", jobType),
		utils2.NewFilterCondition("state", "in", jobTransitioningStates))

	jobs, err := se.GetJobsWithCondition(ctx, *filter)
	if err != nil && !errors.IsNotFoundErr(err) {
		logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter, err)
		return "", err
	}

	if len(jobs) > 0 {
		job := jobs[0]
		logger.Infof("Found ongoing startProjectEvent job for account %s with Job UUID: %s", params.ProjectNumber, job.UUID)
		return job.UUID, nil
	}

	job := &datamodel.Job{
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}

	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		wf,
		params,
	)

	if err != nil {
		logger.Error("Failed to execute workflow for Start project event", "error", err)
		return "", err
	}

	return createdJob.UUID, nil
}

func (o *Orchestrator) UpdateResourceState(ctx context.Context, params *commonparams.UpdateResourceStateParams) (string, error) {
	return updateResourceState(ctx, o.storage, o.temporal, params)
}

func _updateResourceState(ctx context.Context, se database.Storage, temporal client.Client, params *commonparams.UpdateResourceStateParams) (string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return "", err
	}

	// check if the resource is of common type (AD, KMS-Config or Backup/policy)
	params.IsCommonResource = params.ResourceType == commonparams.ResourceStateV1ResourceTypeBackupPolicy ||
		params.ResourceType == commonparams.ResourceStateV1ResourceTypeKmsConfig

	var jobType string
	var wf func(ctx workflow.Context, params *commonparams.UpdateResourceStateParams) (interface{}, error)

	// check for existence of the resource in VCP
	switch {
	case params.State == models.StateOn && params.IsCommonResource:
		wf = workflows.UpdateResourceStateCommonResourceONWorkflow
		jobType = string(models.JobTypeHandleResourceEventOnState)
	case params.State == models.StateOff && params.IsCommonResource:
		wf = workflows.UpdateResourceStateCommonResourceOFFWorkflow
		jobType = string(models.JobTypeHandleResourceEventOffState)
	case params.State == models.StateOn && !params.IsCommonResource:
		wf = workflows.UpdateResourceStateONWorkflow
		jobType = string(models.JobTypeHandleResourceEventOnState)
	case params.State == models.StateOff && !params.IsCommonResource:
		wf = workflows.UpdateResourceStateOFFWorkflow
		jobType = string(models.JobTypeHandleResourceEventOffState)
	case params.State == models.StateDelete &&
		(params.ResourceType == commonparams.ResourceStateV1ResourceTypeStoragePool ||
			params.ResourceType == commonparams.ResourceStateV1ResourceTypeVolume):
		wf = workflows.UpdateResourceStateDELETEWorkflow
		jobType = string(models.JobTypeHandleResourceEventDeleteState)
	default:
		return "", errors.NewBadRequestErr("unsupported state or resource type combination")
	}

	job := &datamodel.Job{
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowRunTimeout:    workflowengine.GetWorkflowGlobalTimeout(),
		},
		wf,
		params,
	)

	if err != nil {
		logger.Error("Failed to start handle resource event workflow: ", "error", err)
		return "", err
	}

	return createdJob.UUID, nil
}

func (o *Orchestrator) CreateOrGetFinishProjectEventJob(ctx context.Context,
	params *commonparams.FinishProjectEventParams) (string, error) {
	return _createOrGetFinishProjectEventJob(ctx, o.storage, o.temporal, params)
}

func _createOrGetFinishProjectEventJob(ctx context.Context, se database.Storage, temporal client.Client,
	params *commonparams.FinishProjectEventParams) (string, error) {
	logger := util.GetLogger(ctx)
	account, err := getOrCreateAccount(ctx, se, params.ProjectNumber)
	if err != nil {
		logger.Error("Failed to get or create account", "error", err)
		return "", err
	}

	jobTransitioningStates := []string{string(models.JobsStateNEW), string(models.JobsStatePROCESSING)}
	jobType := models.JobTypeFinishProjectEventDeleteState

	wf := workflows.FinishProjectEventDeleteStateWorkflow

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID),
		utils2.NewFilterCondition("type", "=", string(jobType)),
		utils2.NewFilterCondition("state", "in", jobTransitioningStates),
	)
	jobs, err := se.GetJobsWithCondition(ctx, *filter)
	if err != nil && !errors.IsNotFoundErr(err) {
		logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter.ToGORMQuery(), err)
		return "", err
	}

	if (len(jobs)) > 0 {
		job := jobs[0]
		logger.Infof("Found New/Ongoing finishProjectEvent job for account %s with Job UUID: %s",
			params.ProjectNumber, job.UUID)
		return job.UUID, nil
	}

	job := datamodel.Job{
		Type:          string(jobType),
		State:         string(models.JobsStateNEW),
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, &job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		wf,
		params,
	)
	if err != nil {
		logger.Error("Failed to execute workflow for finish project delete event", "error", err)
		return "", err
	}
	return createdJob.UUID, nil
}
