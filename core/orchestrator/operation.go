package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
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

func convertDatastoreOperationToModel(job *datamodel.Job) *models.Job {
	return &models.Job{
		BaseModel: models.BaseModel{
			UUID:      job.UUID,
			CreatedAt: job.CreatedAt,
			UpdatedAt: job.UpdatedAt,
			DeletedAt: DeletedAtOrNil(job.DeletedAt),
		},
		CorrelationID: job.CorrelationID,
		Type:          models.JobType(job.Type),
		State:         models.JobState(job.State),
	}
}
