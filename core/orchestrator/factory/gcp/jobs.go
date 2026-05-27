package gcp

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	common "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

// CreateJob persists a job record linked to the provided account and resource context.
func (o *GCPOrchestrator) CreateJob(ctx context.Context, params *common.CreateJobParams) (*datamodel.Job, error) {
	if params == nil {
		return nil, fmt.Errorf("create job params cannot be nil")
	}
	if params.AccountName == "" {
		return nil, fmt.Errorf("account name is required to create job")
	}
	if params.Type == "" {
		return nil, fmt.Errorf("job type is required to create job")
	}
	account, err := getOrCreateAccount(ctx, o.storage, params.AccountName)
	if err != nil {
		return nil, err
	}

	jobState := params.State
	if jobState == "" {
		jobState = models.JobsStateNEW
	}

	job := &datamodel.Job{
		Type:          string(params.Type),
		State:         string(jobState),
		ResourceName:  params.ResourceName,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		JobAttributes: params.JobAttributes,
		CorrelationID: params.CorrelationID,
		RequestID:     params.RequestID,
		TrackingID:    params.TrackingID,
		IsAdminJob:    params.IsAdminJob,
	}

	if job.CorrelationID == "" {
		job.CorrelationID = utils.GetCoRelationIDFromContext(ctx)
	}
	if job.RequestID == "" {
		job.RequestID = utils.GetRequestIDFromContext(ctx)
	}

	return o.storage.CreateJob(ctx, job)
}

// UpdateJobStatus updates the job state, tracking identifier, and error details in persistent storage.
func (o *GCPOrchestrator) UpdateJobStatus(ctx context.Context, jobID string, status string, trackingID int, errorDetails string) error {
	return o.storage.UpdateJob(ctx, jobID, status, trackingID, errorDetails)
}

// UpdateJobAttributes persists changes to job attributes, such as supervisor metadata.
func (o *GCPOrchestrator) UpdateJobAttributes(ctx context.Context, jobID string, jobAttributes *datamodel.JobAttributes) error {
	return o.storage.UpdateJobAttributes(ctx, jobID, jobAttributes)
}
