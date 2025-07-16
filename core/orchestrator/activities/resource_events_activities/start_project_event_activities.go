package resource_events_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type StartProjectEventActivity struct {
	SE database.Storage
}

var (
	createClient   = cvp.CreateClient
	getSignedToken = auth.GetSignedJwtToken
)

func (j *StartProjectEventActivity) StartProjectEventForSDEActivity(ctx context.Context, params *common.StartProjectEventParams) (*common.StartProjectEventResult, error) {
	body := &models.ProjectStateUpdateV1beta{StateUpdateV1beta: models.StateUpdateV1beta{State: params.State}}
	reqParams := &resource_events.V1betaStartProjectEventParams{
		LocationID:     params.LocationID,
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
		return nil, vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError, err))
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
	operationParams.LocationID = params.LocationID
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		logger.Errorf("Error while polling SDE startProjectEvent operation: %s", operationUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientStartProjectEventError, errors.New(res.Error.Message)))
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func pollCvpOperationForWorkflow(ctx context.Context, cvpClient cvpapi.Cvp, operationParams *async.V1betaDescribeOperationParams) (*models.OperationV1beta, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Polling for operation %s", operationParams.OperationID)
	operationResponse, err := cvpClient.Async.V1betaDescribeOperation(operationParams)
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDescribingSDEJob, err)
	}

	return operationResponse.Payload, nil
}
