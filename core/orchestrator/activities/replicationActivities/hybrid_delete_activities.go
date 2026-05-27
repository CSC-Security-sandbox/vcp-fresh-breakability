package replicationActivities

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// HybridDeleteVolumeReplicationActivity contains activities specific to hybrid replication deletion
type HybridDeleteVolumeReplicationActivity struct {
	SE database.Storage
}

// CreateJobForHybridDeleteVolume creates a job for the hybrid delete destination volume child workflow
func (a *HybridDeleteVolumeReplicationActivity) CreateJobForHybridDeleteVolume(ctx context.Context, result *replication.DeleteReplicationResult, jobType string) (*datamodel.Job, error) {
	logger := util.GetLogger(ctx)
	logger.Debug("CreateJobForHybridDeleteVolume")
	se := a.SE

	if result.Event == nil || result.Event.ReplicationModel == nil {
		return nil, errors.NewVCPError(errors.ErrDatabaseDataReadError, fmt.Errorf("replication model is nil"))
	}

	replicationModel := result.Event.ReplicationModel
	location := replicationModel.ReplicationAttributes.DestinationLocation

	resourceName := fmt.Sprintf("projects/%s/locations/%s/volumes/%s",
		*result.DstProjectNumber,
		location,
		replicationModel.ReplicationAttributes.DestinationVolumeUUID)

	job := &datamodel.Job{
		AccountID:     sql.NullInt64{Int64: result.Event.ReplicationModel.AccountID, Valid: true},
		Type:          jobType,
		State:         string(models.JobsStateNEW),
		ResourceName:  resourceName,
		JobAttributes: &datamodel.JobAttributes{ResourceUUID: replicationModel.ReplicationAttributes.DestinationVolumeUUID},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job for hybrid delete volume: %v", err)
		return nil, err
	}

	logger.Infof("Created job %s for hybrid delete volume workflow", createdJob.UUID)
	return createdJob, nil
}
