package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	maxJobsPerRun = env.GetInt("FLEXCACHE_PREPOPULATE_MAX_JOBS_PER_RUN", 20)
)

type syncFlexCachePrepopulateWorkflow struct {
	workflows.BaseWorkflow
}

var _ workflows.WorkflowInterface = &syncFlexCachePrepopulateWorkflow{}

func SyncFlexCachePrepopulateWorkflow(ctx workflow.Context) error {
	syncFlexCacheWF := new(syncFlexCachePrepopulateWorkflow)
	err := syncFlexCacheWF.Setup(ctx, nil)
	if err != nil {
		return err
	}
	syncFlexCacheWF.Status = workflows.WorkflowStatusRunning

	_, workflowErr := syncFlexCacheWF.Run(ctx)
	if workflowErr != nil {
		syncFlexCacheWF.Status = workflows.WorkflowStatusFailed
		return workflowErr
	}
	syncFlexCacheWF.Status = workflows.WorkflowStatusCompleted
	return nil
}

func (wf *syncFlexCachePrepopulateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
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

func (wf *syncFlexCachePrepopulateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, *vsaerrors.CustomError) {
	wf.Logger.Infof("Starting Sync FlexCache Prepopulate Jobs Workflow")

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
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

	rollbackManager := common.NewRollbackManager()
	flexCacheActivity := backgroundactivities.FlexCachePrepopulateActivity{}

	successCount := 0
	failureCount := 0
	var failedJobs []string

	var workflowErr error
	defer func() {
		if workflowErr != nil {
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, workflowErr)
		}
	}()

	wf.Logger.Infof("Getting active prepopulate jobs from jobs table")
	var jobs []*datamodel.Job
	workflowErr = workflow.ExecuteActivity(ctx, flexCacheActivity.GetActivePrepopulateJobs).Get(ctx, &jobs)
	if workflowErr != nil {
		wf.Logger.Errorf("Failed to get active prepopulate jobs: %v", workflowErr)
		return nil, workflows.ConvertToVSAError(workflowErr)
	}
	wf.Logger.Infof("Successfully retrieved %d active prepopulate jobs", len(jobs))

	if len(jobs) == 0 {
		wf.Logger.Infof("No active prepopulate jobs found, workflow complete")
		return nil, nil
	}

	if len(jobs) > maxJobsPerRun {
		wf.Logger.Warnf("Found %d active prepopulate jobs, limiting to %d per run",
			len(jobs), maxJobsPerRun)
		jobs = jobs[:maxJobsPerRun]
	}

	for i, job := range jobs {
		wf.Logger.Infof("Processing job %d/%d - Job UUID: %s, Resource Name: %s, ONTAP Job UUID: %s",
			i+1, len(jobs), job.UUID, job.ResourceName, job.JobAttributes.ResourceUUID)

		var volume *datamodel.Volume
		workflowErr = workflow.ExecuteActivity(ctx, flexCacheActivity.GetVolumeByResourceName,
			job.ResourceName).Get(ctx, &volume)
		if workflowErr != nil {
			wf.Logger.Errorf("Failed to get volume %s for job %s: %v",
				job.ResourceName, job.UUID, workflowErr)
			failureCount++
			failedJobs = append(failedJobs, job.UUID)
			continue
		}

		wf.Logger.Infof("Found volume %s (UUID: %s) for job %s",
			volume.Name, volume.UUID, job.UUID)

		var jobStatus *common.PrepopulateJobStatus
		workflowErr = workflow.ExecuteActivity(ctx, flexCacheActivity.PollPrepopulateJobStatus,
			volume, job).Get(ctx, &jobStatus)
		if workflowErr != nil {
			wf.Logger.Errorf("Failed to poll prepopulate job status for job %s: %v", job.UUID, workflowErr)
			failureCount++
			failedJobs = append(failedJobs, job.UUID)
			continue
		}

		if jobStatus.IsComplete() {
			wf.Logger.Infof("Prepopulate job %s completed with state: %s", job.UUID, jobStatus.State)
			workflowErr = workflow.ExecuteActivity(ctx, flexCacheActivity.UpdateJobAndVolumeStatus,
				job.UUID, volume.UUID, jobStatus).Get(ctx, nil)
			if workflowErr != nil {
				wf.Logger.Errorf("Failed to update job %s and volume %s: %v",
					job.UUID, volume.UUID, workflowErr)
				failureCount++
				failedJobs = append(failedJobs, job.UUID)
				continue
			}
			successCount++
		} else {
			wf.Logger.Infof("Job %s still in progress, state: %s", job.UUID, jobStatus.State)
		}
	}

	wf.Logger.Infof("Sync FlexCache Prepopulate Jobs workflow completed - Total: %d, Success: %d, Failed: %d",
		len(jobs), successCount, failureCount)

	if len(failedJobs) > 0 {
		wf.Logger.Warnf("Failed jobs: %v", failedJobs)
	}

	return nil, nil
}
