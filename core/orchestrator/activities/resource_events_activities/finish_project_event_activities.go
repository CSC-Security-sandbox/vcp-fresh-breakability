package resource_events_activities

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

type FinishProjectEventActivity struct {
	SE database.Storage
}

func (j *FinishProjectEventActivity) FinishProjectEventForSDEActivity(ctx context.Context, params *common.FinishProjectEventParams) (*common.FinishProjectEventResult, error) {
	body := &models.ProjectStateUpdateV1beta{StateUpdateV1beta: models.StateUpdateV1beta{State: params.State}}
	reqParams := &resource_events.V1betaFinishProjectEventParams{
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
	created, accepted, _, err := cvpClient.ResourceEvents.V1betaFinishProjectEvent(reqParams)
	if err != nil {
		logger.Errorf("Error turning %s SDE data path: %v", params.State, err)
		return nil, err
	}

	if created != nil {
		pl := created.GetPayload()
		return &common.FinishProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	if accepted != nil {
		pl := accepted.GetPayload()
		return &common.FinishProjectEventResult{
			Done: pl.Done,
			Name: &pl.Name,
		}, nil
	}

	return nil, errors.New("Unexpected response from SDE")
}

func (j *FinishProjectEventActivity) PollFinishProjectEventSDEOperationActivity(ctx context.Context, params *common.FinishProjectEventParams, result *common.FinishProjectEventResult) error {
	if result.Done != nil && *result.Done {
		return nil
	}

	if result.Name == nil {
		return temporal.NewNonRetryableApplicationError("operation name is nil", "InvalidOperationNameError", nil)
	}

	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}
	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)

	// Extract the operation UUID
	operationUUID := utils.GetOperationUUID(*result.Name)
	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.ProjectNumber
	operationParams.LocationID = params.LocationId
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		logger.Errorf("Error while polling SDE finishProjectEvent operation: %s", operationUUID)
		return vsaerrors.WrapAsTemporalApplicationError(err)
	}

	if res.Done != nil && *res.Done {
		if res.Error != nil {
			return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrCVPClientFinishProjectEventError, errors.New(res.Error.Message)))
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}
