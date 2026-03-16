package gcp

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	ReplicationJobTypes = []string{
		string(models.JobTypeCreateVolumeReplication),
		string(models.JobTypeDeleteVolumeReplication),
		string(models.JobTypeUpdateVolumeReplication),
		string(models.JobTypeResumeVolumeReplication),
		string(models.JobTypeReverseResumeVolumeReplication),
		string(models.JobTypeStopVolumeReplication),
		string(models.JobTypeCreateHybridReplication),
		string(models.JobTypeReverseHybridReplicationInternal),
		string(models.JobTypeReverseHybridReplicationFallbackInternal),
	}
	ScheduledJobTypes = []string{
		string(models.JobsStateNEW),
		string(models.JobsStatePROCESSING),
	}
)

// GetJob gets the specified long-running job status
func (o *GCPOrchestrator) GetJob(ctx context.Context, operationId string) (*models.Job, error) {
	se := o.storage
	job, err := se.GetJob(ctx, operationId)
	if err != nil {
		return nil, err
	}
	return common.ConvertDatastoreOperationToModel(job), nil
}

func (o *GCPOrchestrator) GetReplicationJobs(ctx context.Context, projectName, poolUUID string) ([]*models.Job, error) {
	logger := util.GetLogger(ctx)
	se := o.storage
	account, err := se.GetAccount(ctx, projectName)
	if err != nil {
		return nil, err
	}
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("account_id", "=", account.ID),
		dbutils.NewFilterCondition("type", "in", ReplicationJobTypes),
		dbutils.NewFilterCondition("state", "in", ScheduledJobTypes),
	)

	dbJobs, err := se.GetJobsWithCondition(ctx, *filter)
	if err != nil {
		logger.Errorf("Failed to get jobs with conditions: %v. Error: %v", filter, err)
		return nil, err
	}
	var jobs []*models.Job
	for _, job := range dbJobs {
		// If poolUUID is empty, return all replication jobs
		// Otherwise, filter by the specified poolUUID
		if poolUUID == "" || (job.JobAttributes != nil && job.JobAttributes.PoolUUID == poolUUID) {
			jobs = append(jobs, common.ConvertDatastoreOperationToModel(job))
		}
	}
	return jobs, nil
}

func (o *GCPOrchestrator) GetJobByResourceUUID(ctx context.Context, resourceUUID string, jobType string) (*models.Job, error) {
	job, err := o.storage.GetJobByResourceUUID(ctx, resourceUUID, jobType)
	if err != nil {
		return nil, err
	}
	return common.ConvertDatastoreOperationToModel(job), err
}
