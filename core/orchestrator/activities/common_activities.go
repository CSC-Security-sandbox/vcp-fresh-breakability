package activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
)

type CommonActivities struct {
	SE *database.Storage
}

// CommonActivities is a struct that represents the common activities for the orchestrator.
func (j CommonActivities) UpdateJobStatus(ctx context.Context, job *models.Job) error {
	se := *j.SE
	return se.UpdateJob(ctx, job.UUID.String(), *job.State)
}
