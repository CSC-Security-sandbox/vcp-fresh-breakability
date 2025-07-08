package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	replicationJobTypes = []string{
		string(models.JobTypeCreateVolumeReplication),
		string(models.JobTypeDeleteVolumeReplication),
		string(models.JobTypeUpdateVolumeReplication),
		string(models.JobTypeResumeVolumeReplication),
		string(models.JobTypeReverseResumeVolumeReplication),
		string(models.JobTypeStopVolumeReplication),
	}
	scheduledJobTypes = []string{
		string(models.JobsStateNEW),
		string(models.JobsStatePROCESSING),
	}
)

// GetJob gets the specified long-running job status
func (o *Orchestrator) GetJob(ctx context.Context, operationId string) (*models.Job, error) {
	se := o.storage
	job, err := se.GetJob(ctx, operationId)
	if err != nil {
		return nil, err
	}
	return convertDatastoreOperationToModel(job), nil
}

func (o *Orchestrator) GetReplicationJobs(ctx context.Context, projectName, poolUUID string) ([]*models.Job, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	account, err := se.GetAccount(ctx, projectName)
	if err != nil {
		return nil, err
	}
	filter := utils.CreateFilterWithConditions(
		utils.NewFilterCondition("account_id", "=", account.ID),
		utils.NewFilterCondition("type", "in", replicationJobTypes),
		utils.NewFilterCondition("state", "in", scheduledJobTypes),
	)

	dbJobs, err := se.GetJobsWithCondition(ctx, *filter)
	if err != nil {
		logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter, err)
		return nil, err
	}
	var jobs []*models.Job
	for _, job := range dbJobs {
		if job.JobAttributes != nil && job.JobAttributes.PoolUUID == poolUUID {
			jobs = append(jobs, convertDatastoreOperationToModel(job))
		}
	}
	return jobs, nil
}

func convertDatastoreOperationToModel(job *datamodel.Job) *models.Job {
	modelJob := &models.Job{
		BaseModel: models.BaseModel{
			ID:        job.ID,
			UUID:      job.UUID,
			CreatedAt: job.CreatedAt,
			UpdatedAt: job.UpdatedAt,
			DeletedAt: DeletedAtOrNil(job.DeletedAt),
		},
		CorrelationID: job.CorrelationID,
		TrackingID:    job.TrackingID,
		Type:          models.JobType(job.Type),
		State:         models.JobState(job.State),
		JobAttributes: &models.JobAttributes{},
		ResourceName:  job.ResourceName,
	}

	if job.JobAttributes != nil {
		modelJob.JobAttributes.ResourceUUID = job.JobAttributes.ResourceUUID
		modelJob.JobAttributes.PoolUUID = job.JobAttributes.PoolUUID
	}
	return modelJob
}
