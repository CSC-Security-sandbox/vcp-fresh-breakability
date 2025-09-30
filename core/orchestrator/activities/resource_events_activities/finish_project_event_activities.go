package resource_events_activities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/async"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/resource_events"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
		switch e := err.(type) {
		case *resource_events.V1betaResourceStateUpdateBadRequest:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorBadRequest,
					fmt.Errorf("Bad request for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateUnauthorized:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorUnauthorized,
					fmt.Errorf("Unauthorized for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateForbidden:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorForbidden,
					fmt.Errorf("Forbidden for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateNotFound:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotFound,
					fmt.Errorf("Project not found for state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateConflict:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorConflict,
					fmt.Errorf("Conflict for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateInternalServerError:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorInternalServerError,
					fmt.Errorf("Internal server error for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateNotImplemented:
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotImplemented,
					fmt.Errorf("Not implemented for project state update: %s", e.Error())),
			)
		case *resource_events.V1betaResourceStateUpdateTooManyRequests:
			return nil, vsaerrors.WrapAsTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorTooManyRequests,
					fmt.Errorf("Too many requests for project state update: %s", e.Error())),
			)
		default:
			logger.Warnf("Unknown error type for project state update: %T - %s", err, err.Error())
			return nil, vsaerrors.WrapAsNonRetryableTemporalApplicationError(
				vsaerrors.NewVCPError(vsaerrors.ErrCVPClientFinishProjectEventError, err),
			)
		}
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
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrInvalidOperationName, errors.New("operation name is nil")))
	}

	jwtToken, err := getSignedToken(params.ProjectNumber)
	if err != nil {
		return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrGetSignedToken, err))
	}
	logger := util.GetLogger(ctx)
	cvpClient := createClient(logger, jwtToken)

	operationUUID := utils.GetOperationUUID(*result.Name)
	operationParams := async.NewV1betaDescribeOperationParams()
	operationParams.OperationID = operationUUID
	operationParams.ProjectNumber = params.ProjectNumber
	operationParams.LocationID = params.LocationId
	res, err := pollCvpOperationForWorkflow(ctx, cvpClient, operationParams)
	if err != nil {
		return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
			vsaerrors.NewVCPError(vsaerrors.ErrCVPClientHandleResourceEventError, err),
		)
	}
	if res.Done != nil && *res.Done {
		if res.Error != nil {
			switch int(res.Error.Code) {
			case common.HTTPStatusBadRequest:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorBadRequest,
						fmt.Errorf("Bad request while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusUnauthorized:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorUnauthorized,
						fmt.Errorf("Unauthorized while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusForbidden:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorForbidden,
						fmt.Errorf("Forbidden while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusNotFound:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorNotFound,
						fmt.Errorf("Operation %s not found while polling: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusInternalServerError:
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorInternalServerError,
						fmt.Errorf("Internal server error while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			case common.HTTPStatusTooManyRequests:
				return vsaerrors.WrapAsTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrHandleResourceEventErrorTooManyRequests,
						fmt.Errorf("Too many requests while polling operation %s: %s", operationUUID, res.Error.Message)),
				)

			default:
				logger.Warnf("Unknown error code while polling operation %s: %d - %s", operationUUID, int(res.Error.Code), res.Error.Message)
				return vsaerrors.WrapAsNonRetryableTemporalApplicationError(
					vsaerrors.NewVCPError(vsaerrors.ErrCVPClientFinishProjectEventError,
						fmt.Errorf("SDE polling failed for operation %s: %s", operationUUID, res.Error.Message)),
				)
			}
		}
		return nil
	}
	return vsaerrors.WrapAsTemporalApplicationError(vsaerrors.NewVCPError(vsaerrors.ErrSDEJobNotFinished, errors.New("job not finished")))
}

func (j *FinishProjectEventActivity) DeleteAccountActivity(ctx context.Context, projectNumber string) error {
	se := j.SE
	account, err := se.GetAccount(ctx, projectNumber)
	if err != nil {
		return err
	}
	return se.DeleteAccount(ctx, account.ID)
}

func (j *FinishProjectEventActivity) VerifySoftDeletedResourcesForAccount(ctx context.Context, projectNumber string) (bool, error) {
	var (
		softDelVolume = true
		softDelPool   = true
		softDelSvms   = true
	)
	logger := util.GetLogger(ctx)
	se := j.SE

	account, err := se.GetSoftDeleteAccount(ctx, projectNumber)
	if err != nil {
		logger.Errorf("Error getting soft-deleted account for project %s", projectNumber)
		return false, err
	}

	conditions := [][]interface{}{
		{"account_id = ?", account.ID},
	}
	softDeletedVols, err := se.ListVolumes(ctx, conditions)
	if err != nil {
		logger.Errorf("Error listing soft-deleted volumes for account %d", account.ID)
		return false, err
	}
	if len(softDeletedVols) > 0 {
		softDelVolume = false
	}

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID),
	)
	softDelPools, err := se.ListPools(ctx, filter)
	if err != nil {
		logger.Errorf("Error listing soft-deleted pools for account %d", account.ID)
		return false, err
	}
	if len(softDelPools) > 0 {
		softDelPool = false
	}

	softDeletedSvms, err := se.ListSvmsWithAccountId(ctx, account.ID)
	if err != nil {
		logger.Errorf("Error listing soft-deleted svms for account %d", account.ID)
		return false, err
	}
	if len(softDeletedSvms) > 0 {
		softDelSvms = false
	}

	if softDelVolume && softDelPool && softDelSvms {
		return true, nil
	}

	return false, errors.New("Soft-deleted")
}

func (j *FinishProjectEventActivity) RollbackAccountStateActivity(ctx context.Context, projectNumber string) error {
	se := j.SE
	logger := util.GetLogger(ctx)
	account, err := se.GetSoftDeleteAccount(ctx, projectNumber)
	if err != nil {
		logger.Errorf("Error getting soft-deleted account for project %s", projectNumber)
		return err
	}

	err = se.RollBackDeletedAccount(ctx, account.ID)
	if err != nil {
		logger.Errorf("Error rolling back soft-deleted account for project %s", projectNumber)
	}
	return nil
}

func (j *FinishProjectEventActivity) DeleteServiceAccountsFromAccountID(ctx context.Context, projectNumber string) error {
	se := j.SE
	logger := util.GetLogger(ctx)
	account, err := se.GetAccount(ctx, projectNumber)
	if err != nil {
		logger.Errorf("Error getting account for project %s", projectNumber)
		return err
	}

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("account_id", "=", account.ID))

	serviceAccounts, err := se.ListKmsServiceAccounts(ctx, filter)
	if err != nil {
		return err
	}

	for _, serviceAccount := range serviceAccounts {
		err = se.DeleteServiceAccount(ctx, serviceAccount)
		if err != nil {
			return err
		}
	}
	return nil
}
