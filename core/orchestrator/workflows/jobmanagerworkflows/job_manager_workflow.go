package jobmanagerworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/jobmanageractivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// jobManagerWorkflow implements the WorkflowInterface for managing admin job schedules.
type jobManagerWorkflow struct {
	workflows.BaseWorkflow
}

// Enforcing the WorkflowInterface on jobManagerWorkflow
var _ workflows.WorkflowInterface = &jobManagerWorkflow{}

// JobManagerWorkflow is the entry point for the admin job manager workflow.
// It sets up the workflow, creates a job, and runs the main job management logic.
func JobManagerWorkflow(ctx workflow.Context) error {
	jobManagerWF := new(jobManagerWorkflow)
	createdJob, err := jobManagerWF.CreateJob(ctx)
	if err != nil {
		return err
	}

	err = jobManagerWF.Setup(ctx, createdJob)
	if err != nil {
		return err
	}
	jobManagerWF.Status = workflows.WorkflowStatusRunning

	_, customErr := jobManagerWF.Run(ctx)
	if customErr != nil {
		jobManagerWF.Status = workflows.WorkflowStatusFailed
		_ = jobManagerWF.UpdateJobStatus(ctx, string(models.JobsStateERROR), customErr)
		return customErr
	}
	jobManagerWF.Status = workflows.WorkflowStatusCompleted
	_ = jobManagerWF.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil
}

// Setup initializes the workflow context, logger, and query handlers.
// It must be called before running the workflow logic.
func (wf *jobManagerWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	job := input.(*datamodel.Job)
	wf.ID = job.UUID
	wf.Status = workflows.WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*workflows.WorkflowStatus, error) {
		return &workflows.WorkflowStatus{
			ID:     wf.ID,
			Status: wf.Status,
		}, nil
	})
}

// CreateJob creates a new admin job in the database and sets its initial state to PROCESSING.
func (wf *jobManagerWorkflow) CreateJob(ctx workflow.Context) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)

	// The job state is set to PROCESSING here because the workflow itself is creating the job
	job := &datamodel.Job{
		Type:       string(models.JobTypeRefreshAdminJobSpecs),
		State:      string(models.JobsStatePROCESSING),
		IsAdminJob: true,
	}

	commonActivities := activities.CommonActivities{}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	})

	var createdJob *datamodel.Job
	err := workflow.ExecuteActivity(ctx, commonActivities.CreateJob, job).Get(ctx, &createdJob)
	if err != nil {
		logger.Errorf("Failed to create job: %v", err)
		return nil, err
	}
	return createdJob, nil
}

// Run executes the main job management activities: create, update, and delete schedule activities.
// It logs errors for each activity but continues execution.
func (wf *jobManagerWorkflow) Run(ctx workflow.Context, _ ...interface{}) (interface{}, *vsaerrors.CustomError) {
	logger := util.GetLogger(ctx)

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 15,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second * 5,
			BackoffCoefficient:     2.0,
			MaximumInterval:        time.Second * 15,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	jobManagerActivity := &jobmanageractivities.JobManagerActivity{}

	err := workflow.ExecuteActivity(ctx, jobManagerActivity.CreateScheduleActivity).Get(ctx, nil)
	if err != nil {
		logger.Errorf("CreateScheduleActivity failed: %v", err)
		// TODO: Implement alerting or notification logic here if necessary
	}

	err = workflow.ExecuteActivity(ctx, jobManagerActivity.UpdateScheduleActivity).Get(ctx, nil)
	if err != nil {
		logger.Errorf("UpdateScheduleActivity failed: %v", err)
		// TODO: Implement alerting or notification logic here if necessary
	}

	err = workflow.ExecuteActivity(ctx, jobManagerActivity.DeleteScheduleActivity).Get(ctx, nil)
	if err != nil {
		logger.Errorf("DeleteScheduleActivity failed: %v", err)
		// TODO: Implement alerting or notification logic here if necessary
	}

	return nil, nil
}
