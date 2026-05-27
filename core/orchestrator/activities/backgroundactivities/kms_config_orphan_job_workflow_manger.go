package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
)

// CreateKmsConfigArgs implements OrphanJobWorkflowManager for creating KMS config
type CreateKmsConfigArgs struct{}

var (
	getSignedAuthToken            = auth.GetSignedJwtToken
	failedKmsConfigCreateActivity = kms_activities.FailedKmsConfigCreateActivity
)

func (c *CreateKmsConfigArgs) FailedWorkflowJob(ctx context.Context, se database.Storage, job *datamodel.Job, reason string) error {
	resourceUUID := job.JobAttributes.ResourceUUID
	kmsConfig, err := se.GetKmsConfig(ctx, resourceUUID)
	if err != nil {
		return fmt.Errorf("failed to get KMS config for UUID %s: %w", resourceUUID, err)
	}
	jwtToken, err := getSignedAuthToken(kmsConfig.CustomerProjectID)
	if err != nil {
		return err
	}
	ctx = context.WithValue(ctx, middleware.AuthorizationToken, jwtToken)
	return failedKmsConfigCreateActivity(ctx, se, kmsConfig, reason, kmsConfig.KeyRingLocation)
}

func (c *CreateKmsConfigArgs) PrepareWorkflowArgs(ctx context.Context, se database.Storage, job *datamodel.Job) ([]interface{}, error) {
	resourceUUID := job.JobAttributes.ResourceUUID
	dbKmsConfig, err := se.GetKmsConfig(ctx, resourceUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get KMS config for UUID %s: %w", resourceUUID, err)
	}

	params := &common.CreateKmsConfigParams{
		UUID:          dbKmsConfig.UUID,
		ProjectNumber: dbKmsConfig.Account.Name,
		AccountName:   dbKmsConfig.Account.Name,
		ResourceID:    dbKmsConfig.ResourceID,
		Description:   dbKmsConfig.Description,
		LocationID:    dbKmsConfig.KeyRingLocation,
	}

	if dbKmsConfig.KmsAttributes != nil {
		params.OperationUri = dbKmsConfig.KmsAttributes.SdeKmsConfigOperationURI
		params.OperationDone = dbKmsConfig.KmsAttributes.SdeKmsConfigOperationDone
	}

	return []interface{}{params, dbKmsConfig}, nil
}

// DeleteKmsConfigArgs implements OrphanJobWorkflowManager for deleting KMS config
type DeleteKmsConfigArgs struct{}

func (d *DeleteKmsConfigArgs) FailedWorkflowJob(ctx context.Context, se database.Storage, job *datamodel.Job, reason string) error {
	return nil
}

func (d *DeleteKmsConfigArgs) PrepareWorkflowArgs(ctx context.Context, se database.Storage, job *datamodel.Job) ([]interface{}, error) {
	resourceUUID := job.JobAttributes.ResourceUUID
	dbKmsConfig, err := se.GetKmsConfig(ctx, resourceUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get KMS config for UUID %s: %w", resourceUUID, err)
	}

	params := &common.DeleteKmsConfigParams{
		KmsConfigID: resourceUUID,
		Region:      dbKmsConfig.KeyRingLocation,
	}

	if dbKmsConfig.Account != nil {
		params.AccountName = dbKmsConfig.Account.Name
	}

	return []interface{}{dbKmsConfig, params}, nil
}
