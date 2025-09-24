package resource_events_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

const (
	ErrNotRetryable = "NonRetryableError"
)

type StartProjectEventActivity struct {
	SE database.Storage
}

func (j *StartProjectEventActivity) StartProjectEventForSDEActivity(ctx context.Context, params *common.StartProjectEventParams) (*common.StartProjectEventResult, error) {
	body := &models.ProjectStateUpdateV1beta{StateUpdateV1beta: models.StateUpdateV1beta{State: params.State}}
	reqParams := &resource_events.V1betaStartProjectEventParams{
		LocationID:     params.LocationId,
		ProjectNumber:  params.ProjectNumber,
		XCorrelationID: &params.XCorrelationID,
		Body:           body,
	}

	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}

	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)
	created, accepted, _, err := cvpClient.ResourceEvents.V1betaStartProjectEvent(reqParams)
	if err != nil {
		logger.Errorf("Error turning %s SDE data path: %v", params.State, err)
		// Check if this is a 404 Not Found error and make it non-retryable
		if _, tooManyRequests := err.(*resource_events.V1betaResourceStateUpdateTooManyRequests); tooManyRequests {
			logger.Infof("SDE HandleResourceEvent returned 404 (resource not found), treating as non-retryable: %v", err)
			return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err))
		}
		return nil, temporal.NewNonRetryableApplicationError(err.Error(), ErrNotRetryable, err)
	}

	if created != nil {
		pl := created.GetPayload()
		return &common.StartProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	if accepted != nil {
		pl := accepted.GetPayload()
		return &common.StartProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	return nil, nil
}

func (j *StartProjectEventActivity) PollStartProjectEventSDEOperationActivity(ctx context.Context, params *common.StartProjectEventParams, result *common.StartProjectEventResult) error {
	if result == nil || (result.Done != nil && *result.Done) {
		return nil
	}

	if result.Name == nil {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInvalidOperationName, errors.New("operation name is nil")))
	}

	logger := util.GetLogger(ctx)
	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}
	cvpClient := createClient(logger, jwtToken)

	// Extract the operation UUID
	operationUUID := utils.GetOperationUUID(*result.Name)
	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.ProjectNumber
	operationParams.LocationID = params.LocationId
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		// Check if this is a 404 Not Found error and make it non-retryable
		if _, tooManyRequests := err.(*resource_events.V1betaResourceStateUpdateTooManyRequests); tooManyRequests {
			logger.Infof("SDE HandleResourceEvent returned 404 (resource not found), treating as non-retryable: %v", err)
			return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err))
		}
		logger.Errorf("Error while polling SDE handleResourceEvent operation: %s", operationUUID)
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrNotRetryable, err)
	}

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError, errors.New(res.Error.Message)))
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func (j *StartProjectEventActivity) ListPoolsForAccount(ctx context.Context, projectNumber string, state string) ([]*datamodel.PoolView, error) {
	account, err := j.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}
	var jobTransitioningStates string
	switch state {
	case string(gcpserver.ResourceStateUpdateV1betaStateOFF):
		jobTransitioningStates = string(gcpserver.PoolV1betaStoragePoolStateREADY)
	case string(gcpserver.ResourceStateUpdateV1betaStateON):
		jobTransitioningStates = string(gcpserver.PoolV1betaStoragePoolStateDISABLED)
	}
	filter := dbutils.CreateFilterWithConditions(dbutils.NewFilterCondition("account_id", "=", account.ID),
		dbutils.NewFilterCondition("state", "=", jobTransitioningStates))
	pools, err := j.SE.ListPools(ctx, filter)
	if err != nil {
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}
	return pools, nil
}

func (j *StartProjectEventActivity) UpdateAccountStateForHandleResource(ctx context.Context, projectNumber string, newState string) error {
	account, err := j.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseListPoolsForAccount, err))
	}
	err = j.SE.UpdateAccountStateForHandleResource(ctx, account.UUID, newState)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrDatabaseUpdateAccountState, err))
	}
	return nil
}
