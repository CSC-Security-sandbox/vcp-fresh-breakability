package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

// CreateJobParams captures the inputs required to create a job record in the VCP database.
type CreateJobParams struct {
	AccountName   string
	Type          models.JobType
	State         models.JobState
	ResourceName  string
	JobAttributes *datamodel.JobAttributes
	CorrelationID string
	RequestID     string
	TrackingID    int
	IsAdminJob    bool
}
