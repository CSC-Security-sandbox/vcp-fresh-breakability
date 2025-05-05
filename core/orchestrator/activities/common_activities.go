package activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

type CommonActivities struct {
	SE *database.Storage
}

// UpdateJobStatus updates the status of a job in the database.
func (j CommonActivities) UpdateJobStatus(ctx context.Context, job *datamodel.Job) error {
	se := *j.SE
	return se.UpdateJob(ctx, job.UUID, job.State)
}

// GetNode retrieves the node associated with the given pool ID.
func (j CommonActivities) GetNode(ctx context.Context, poolId int64) (*datamodel.Node, error) {
	se := *j.SE

	nodes, err := se.GetNodesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node found for the pool")
	}

	return nodes[0], nil
}
