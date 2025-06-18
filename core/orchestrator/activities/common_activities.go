package activities

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type CommonActivities struct {
	SE database.Storage
}

// UpdateJobStatus updates the status of a job in the database.
func (ca CommonActivities) UpdateJobStatus(ctx context.Context, job *datamodel.Job) error {
	logger := util.GetLogger(ctx)
	se := ca.SE
	logger.Infof("updating job: %s with status: %s", job.UUID, job.State)
	return se.UpdateJob(ctx, job.UUID, job.State, job.TrackingID, job.ErrorDetails)
}

// DescribeJob gives the status of a job in the database.
func DescribeJob(ctx context.Context, jobId, basepath, jwtToken, projectNumber, location *string) error {
	if jobId == nil {
		return nil
	}
	logger := util.GetLogger(ctx)
	googleProxyClient := googleproxyclient.GetGProxyClient(*basepath, *jwtToken, logger)

	describeOperationParams := googleproxyclient.V1betaDescribeOperationParams{
		OperationId:   *jobId,
		ProjectNumber: *projectNumber,
		LocationId:    *location,
	}

	res, err := googleProxyClient.Invoker.V1betaDescribeOperation(ctx, describeOperationParams)
	if err != nil {
		return errors2.NewVCPError(errors2.ErrDescribingJob, err)
	}
	operation, ok := res.(*googleproxyclient.OperationV1beta)
	if ok {
		if operation.Done.Value {
			return nil
		}
	}
	return errors2.NewVCPError(errors2.ErrJobNotFinished, errors.New("job not finished"))
}

// GetNode retrieves the node associated with the given pool ID.
func (ca CommonActivities) GetNode(ctx context.Context, poolId int64) (*datamodel.Node, error) {
	se := ca.SE

	nodes, err := se.GetNodesByPoolID(ctx, poolId)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.New("no node found for the pool")
	}

	return nodes[0], nil
}
