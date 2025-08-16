package jobmanageractivities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/backgroundworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

const (
	SyncVsaSnapshots         = "SYNC_VSA_SNAPSHOTS"
	RotateKmsServiceAccounts = "ROTATE_KMS_SERVICE_ACCOUNTS"
)

// JobTypeToWorkflow maps job types to their corresponding workflow functions.
var JobTypeToWorkflow = map[string]interface{}{
	SyncVsaSnapshots:         backgroundworkflows.SyncVSASnapshotsWorkflow,
	RotateKmsServiceAccounts: backgroundworkflows.RotateKmsSAKeyWorkflow,
}

type JobManagerActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

func (j *JobManagerActivity) CreateScheduleActivity(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	se := j.SE
	jobs, err := se.GetAdminJobSpecsByState(ctx, scheduler.JobStatusCreating)
	if err != nil {
		logger.Errorf("Failed to fetch admin jobs by state: %v", err)
		return err
	}

	for _, job := range jobs {
		workflowFunc, ok := JobTypeToWorkflow[job.JobType]
		if !ok {
			logger.Warnf("No workflow function found for job type: %s", job.JobType)
			continue
		}

		createParams := scheduler.CreateScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{
				ScheduleID: job.UUID,
			},
			TemporalScheduleOptions: scheduler.TemporalCreateScheduleParams{
				WorkflowID: job.UUID,
				Workflow:   workflowFunc,
				Spec: client.ScheduleSpec{
					CronExpressions: []string{job.CronExpression},
				},
			},
		}

		_, err := j.Scheduler.Create(ctx, createParams)
		if err != nil {
			logger.Errorf("Failed to create schedule for job type %s: %v", job.JobType, err)
			return err
		}

		err = updateJobSpecState(ctx, se, job)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *JobManagerActivity) UpdateScheduleActivity(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	se := j.SE
	jobs, err := se.GetAdminJobSpecsByState(ctx, scheduler.JobStatusUpdating)
	if err != nil {
		logger.Errorf("Failed to fetch admin jobs by state: %v", err)
		return err
	}

	for _, job := range jobs {
		updateParams := scheduler.UpdateScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{
				ScheduleID: job.UUID,
				Args:       nil,
			},
			TemporalScheduleOptions: scheduler.TemporalUpdateScheduleParams{
				Spec: client.ScheduleSpec{
					CronExpressions: []string{job.CronExpression},
				},
			},
		}

		_, err := j.Scheduler.Update(ctx, updateParams)
		if err != nil {
			logger.Errorf("Failed to update schedule for job type %s: %v", job.JobType, err)
			return err
		}

		err = updateJobSpecState(ctx, se, job)
		if err != nil {
			return err
		}
	}

	return nil
}

func (j *JobManagerActivity) DeleteScheduleActivity(ctx context.Context) error {
	logger := util.GetLogger(ctx)

	se := j.SE
	jobs, err := se.GetAdminJobSpecsByState(ctx, scheduler.JobStatusDeleting)
	if err != nil {
		logger.Errorf("Failed to fetch admin jobs by state: %v", err)
		return err
	}

	for _, job := range jobs {
		deleteParams := scheduler.DeleteScheduleParams{
			ScheduleParams: scheduler.ScheduleParams{
				ScheduleID: job.UUID,
			},
		}

		_, err := j.Scheduler.Delete(ctx, deleteParams)
		if err != nil {
			logger.Errorf("Failed to delete schedule for job type %s: %v", job.JobType, err)
			return err
		}

		err = updateJobSpecState(ctx, se, job)
		if err != nil {
			return err
		}
	}

	return nil
}

func updateJobSpecState(ctx context.Context, se database.Storage, adminJobSpec *datamodel.AdminJobSpec) error {
	logger := util.GetLogger(ctx)

	switch adminJobSpec.State {
	case scheduler.JobStatusCreating, scheduler.JobStatusUpdating:
		adminJobSpec.State = scheduler.JobStatusScheduled
	case scheduler.JobStatusDeleting:
		adminJobSpec.State = scheduler.JobStatusDeleted
		adminJobSpec.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	}

	err := se.UpdateAdminJobSpec(ctx, adminJobSpec)
	if err != nil {
		logger.Error("Failed to update admin adminJobSpec state", "Error", err)
		return err
	}

	return nil
}
