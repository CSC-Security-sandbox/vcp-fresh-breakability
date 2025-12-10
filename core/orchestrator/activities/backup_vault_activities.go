package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
)

var (
	utilsConvertJsonToModel       = utils.ConvertJsonToModel
	ConvertToBackupVaultDataModel = _convertToBackupVaultDataModel
	cvpCreateClient               = cvp.CreateClient
	updateBackupVaultInSDE        = _updateBackupVaultInSDE
	deleteBackupVaultInSDE        = _deleteBackupVaultInSDE
	utilsGetRemoteRegionConfig    = common.GetRemoteRegionConfig
	googleProxyClientGet          = googleproxyclient.GetGProxyClient
)

type BackupVaultActivity struct {
	SE database.Storage
}

func (j *BackupVaultActivity) DeleteBackupVaultInSDE(ctx context.Context, paramz *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	return deleteBackupVaultInSDE(ctx, paramz)
}

func _deleteBackupVaultInSDE(ctx context.Context, paramz *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	vault, _, err := cvpClient.BackupVault.V1betaDeleteBackupVault(&backup_vault.V1betaDeleteBackupVaultParams{
		LocationID:     paramz.Region,
		ProjectNumber:  paramz.OwnerID,
		XCorrelationID: &xCorrelationID,
		BackupVaultID:  paramz.BackupVaultID,
	})
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaDeleteBackupVaultBadRequest:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Bad request deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultBadRequest",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultUnauthorized:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Unauthorized to delete backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultUnauthorized",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultForbidden:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Forbidden to delete backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultForbidden",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultNotFound:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Backup vault %s not found: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultNotFound",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultConflict:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Conflict deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultConflict",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultUnprocessableEntity:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Unprocessable entity deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultUnprocessableEntity",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultInternalServerError:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Internal server error deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultInternalServerError",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultTooManyRequests:
			return nil, temporal.NewApplicationError(
				fmt.Sprintf("Too many requests deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultTooManyRequests",
				err,
			)

		case *backup_vault.V1betaDeleteBackupVaultNotImplemented:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Not implemented deleting backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaDeleteBackupVaultNotImplemented",
				err,
			)

		default:
			logger.Warnf("Unknown error type for backup vault deletion %s: %T - %s", paramz.BackupVaultID, err, err.Error())
			return nil, err
		}
	}

	responseBytes, err := json.MarshalIndent(vault.Payload.Response, "", "  ")
	if err != nil {
		return nil, errors.New("failed to marshal response from SDE BackupVault Deletion")
	}
	data := models.BackupVaultV1beta{}
	err = utilsConvertJsonToModel(responseBytes, &data)
	if err != nil {
		return nil, err
	}

	model, err := ConvertToBackupVaultDataModel(&data, paramz.Region)
	if err != nil {
		return nil, err
	}
	return model, nil
}

// UpdateBackupVaultInSDE ensures idempotency by checking existing BackupVault before creation.
func (j *BackupVaultActivity) UpdateBackupVaultInSDE(ctx context.Context, paramz *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	return updateBackupVaultInSDE(ctx, paramz)
}

func convertToSDEBackupRetentionPolicy(attrs common.BackupRetentionPolicyParams) *models.BackupRetentionPolicyUpdateV1beta {
	var backupMinimumEnforcedRetentionDuration *int64
	if attrs.BackupMinimumEnforcedRetentionDuration != nil {
		backupMinimumEnforcedRetentionDuration = attrs.BackupMinimumEnforcedRetentionDuration
	}

	var dailyBackupImmutable *bool
	if attrs.IsDailyBackupImmutable != nil {
		dailyBackupImmutable = attrs.IsDailyBackupImmutable
	}
	var weeklyBackupImmutable *bool
	if attrs.IsWeeklyBackupImmutable != nil {
		weeklyBackupImmutable = attrs.IsWeeklyBackupImmutable
	}
	var monthlyBackupImmutable *bool
	if attrs.IsMonthlyBackupImmutable != nil {
		monthlyBackupImmutable = attrs.IsMonthlyBackupImmutable
	}
	var adhocBackupImmutable *bool
	if attrs.IsAdhocBackupImmutable != nil {
		adhocBackupImmutable = attrs.IsAdhocBackupImmutable
	}

	return &models.BackupRetentionPolicyUpdateV1beta{
		BackupMinimumEnforcedRetentionDays: backupMinimumEnforcedRetentionDuration,
		DailyBackupImmutable:               dailyBackupImmutable,
		ManualBackupImmutable:              adhocBackupImmutable,
		MonthlyBackupImmutable:             monthlyBackupImmutable,
		WeeklyBackupImmutable:              weeklyBackupImmutable,
	}
}

func _updateBackupVaultInSDE(ctx context.Context, paramz *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetAuthTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	body := &models.BackupVaultUpdateV1beta{
		Description: paramz.Description,
	}
	// Update the assignment
	brp := convertToSDEBackupRetentionPolicy(paramz.BackupRetentionPolicy)
	if brp != nil {
		body.BackupRetentionPolicy = brp
	}

	vault, err := cvpClient.BackupVault.V1betaUpdateBackupVault(&backup_vault.V1betaUpdateBackupVaultParams{
		LocationID:     paramz.Region,
		ProjectNumber:  paramz.OwnerID,
		XCorrelationID: &xCorrelationID,
		BackupVaultID:  paramz.BackupVaultID,
		Body:           body,
	})
	if err != nil {
		switch e := err.(type) {
		case *backup_vault.V1betaUpdateBackupVaultBadRequest:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Bad request updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultBadRequest",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultUnauthorized:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Unauthorized to update backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultUnauthorized",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultForbidden:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Forbidden to update backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultForbidden",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultConflict:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Conflict updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultConflict",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultUnprocessableEntity:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Unprocessable entity updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultUnprocessableEntity",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultInternalServerError:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Internal server error updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultInternalServerError",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultTooManyRequests:
			return nil, temporal.NewApplicationError(
				fmt.Sprintf("Too many requests updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultTooManyRequests",
				err,
			)

		case *backup_vault.V1betaUpdateBackupVaultNotImplemented:
			return nil, temporal.NewNonRetryableApplicationError(
				fmt.Sprintf("Not implemented updating backup vault %s: %s", paramz.BackupVaultID, e.Error()),
				"V1betaUpdateBackupVaultNotImplemented",
				err,
			)

		default:
			logger.Warnf("Unknown error type for backup vault Updation %s: %T - %s", paramz.BackupVaultID, err, err.Error())
			return nil, err
		}
	}

	responseBytes, err := json.MarshalIndent(vault.Payload.Response, "", "  ")
	if err != nil {
		return nil, errors.New("failed to marshal response from SDE BackupVault Updation")
	}
	data := models.BackupVaultV1beta{}
	err = utilsConvertJsonToModel(responseBytes, &data)
	if err != nil {
		return nil, err
	}

	model, err := ConvertToBackupVaultDataModel(&data, paramz.Region)
	if err != nil {
		return nil, err
	}
	return model, nil
}

func (j *BackupVaultActivity) UpdateBackupVaultInVCP(ctx context.Context, bvParams *datamodel.BackupVault, vcpBvParams *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	se := j.SE
	BackupVault, err := se.UpdateBackupVaultInVCP(ctx, bvParams, vcpBvParams)
	if err != nil {
		return nil, err
	}
	return BackupVault, nil
}

func (j *BackupVaultActivity) DeleteBackupVaultInVCP(ctx context.Context, backupVaultId string) (*datamodel.BackupVault, error) {
	se := j.SE
	BackupVault, err := se.DeleteBackupVaultInVCP(ctx, backupVaultId)
	if err != nil {
		return nil, err
	}
	return BackupVault, nil
}

func (j *BackupVaultActivity) DeleteRemoteBackupVaultInVCP(ctx context.Context, params *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	return DeleteRemoteBackupVaultInVCP(ctx, params)
}

func DeleteRemoteBackupVaultInVCP(ctx context.Context, params *common.BackupVaultParams) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)

	if params.BackupRegion == nil || *params.BackupRegion == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"BackupRegion not provided in params",
			"BackupRegionMissing",
			fmt.Errorf("backup region is required for cross-region backupVault deletion"),
		)
	}

	basePath, jwtToken, err := utilsGetRemoteRegionConfig(*params.BackupRegion, params.OwnerID)
	if err != nil {
		logger.Errorf("Failed to get remote region configuration for region %s: %v", *params.BackupRegion, err)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to get remote region configuration: %v", err),
			"InvalidRemoteRegionConfig",
			err,
		)
	}

	googleProxyClient := googleProxyClientGet(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	deleteParams := &googleproxyclient.V1betaInternalDeleteBackupVaultParams{
		ProjectNumber:  params.OwnerID,
		LocationId:     *params.BackupRegion,
		BackupVaultId:  params.BackupVaultID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalDeleteBackupVault(ctx, *deleteParams)
	if err != nil {
		logger.Errorf("Failed to call V1betaInternalDeleteBackupVault: %v", err)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to delete remote backup vault: %v", err),
			"InternalDeleteBackupVaultFailed",
			err,
		)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		isDone := r.Done.Value
		logger.Infof("Delete operation returned for remote backup vault %s in region %s. Operation: %s, Done: %v",
			params.BackupVaultID, *params.BackupRegion, r.GetName(), isDone)

		if !isDone {
			logger.Warnf("Delete operation for remote backup vault %s not marked as done, but treating as synchronous", params.BackupVaultID)
		}

		logger.Infof("Successfully deleted remote backup vault %s (external UUID) in region %s",
			params.BackupVaultID, *params.BackupRegion)
		return nil, nil

	case *googleproxyclient.V1betaInternalDeleteBackupVaultNoContent:
		logger.Infof("Successfully deleted remote backup vault %s (external UUID) in region %s - NoContent response",
			params.BackupVaultID, *params.BackupRegion)
		return nil, nil

	case *googleproxyclient.V1betaInternalDeleteBackupVaultBadRequest:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Bad request deleting remote backup vault: %s", r.Message),
			"V1betaInternalDeleteBackupVaultBadRequest",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalDeleteBackupVaultUnauthorized:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unauthorized to delete remote backup vault: %s", r.Message),
			"V1betaInternalDeleteBackupVaultUnauthorized",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalDeleteBackupVaultForbidden:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Forbidden to delete remote backup vault: %s", r.Message),
			"V1betaInternalDeleteBackupVaultForbidden",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalDeleteBackupVaultNotFound:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Remote backup vault not found: %s", r.Message),
			"V1betaInternalDeleteBackupVaultNotFound",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalDeleteBackupVaultConflict:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Conflict deleting remote backup vault: %s", r.Message),
			"V1betaInternalDeleteBackupVaultConflict",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalDeleteBackupVaultInternalServerError:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Internal server error deleting remote backup vault: %s", r.Message),
			"V1betaInternalDeleteBackupVaultInternalServerError",
			errors.New(r.Message),
		)

	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unexpected response type from internal delete backup vault endpoint: %T", r),
			"UnexpectedDeleteResponseType",
			fmt.Errorf("unexpected response type: %T", r),
		)
	}
}

func (j *BackupVaultActivity) UpdateRemoteBackupVaultInVCP(ctx context.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	return UpdateRemoteBackupVaultInVCP(ctx, params, backupVault)
}

func UpdateRemoteBackupVaultInVCP(ctx context.Context, params *common.BackupVaultParams, backupVault *datamodel.BackupVault) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)

	if params.BackupRegion == nil || *params.BackupRegion == "" {
		return nil, temporal.NewNonRetryableApplicationError(
			"BackupRegion not provided in params",
			"BackupRegionMissing",
			fmt.Errorf("backup region is required for cross-region backupVault update"),
		)
	}

	basePath, jwtToken, err := utilsGetRemoteRegionConfig(*params.BackupRegion, params.OwnerID)
	if err != nil {
		logger.Errorf("Failed to get remote region configuration for region %s: %v", *params.BackupRegion, err)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to get remote region configuration: %v", err),
			"InvalidRemoteRegionConfig",
			err,
		)
	}

	googleProxyClient := googleProxyClientGet(basePath, jwtToken, logger)
	correlationID := utils.GetCoRelationIDFromContext(ctx)

	updateBody := googleproxyclient.BackupVaultInternalUpdateV1beta{}

	if params.Description != nil {
		updateBody.Description = googleproxyclient.NewOptString(*params.Description)
	}

	if len(params.BucketDetails) > 0 {
		bucketDetailsItems := make([]googleproxyclient.BackupVaultInternalUpdateV1betaBucketDetailsItem, 0, len(params.BucketDetails))
		for _, bd := range params.BucketDetails {
			bucketDetailsItems = append(bucketDetailsItems, googleproxyclient.BackupVaultInternalUpdateV1betaBucketDetailsItem{
				BucketName:          googleproxyclient.NewOptString(bd.BucketName),
				ServiceAccountName:  googleproxyclient.NewOptString(bd.ServiceAccountName),
				VendorSubnetId:      googleproxyclient.NewOptString(bd.VendorSubnetID),
				TenantProjectNumber: googleproxyclient.NewOptString(bd.TenantProjectNumber),
			})
		}
		updateBody.BucketDetails = bucketDetailsItems
	}

	if params.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != nil ||
		params.BackupRetentionPolicy.IsDailyBackupImmutable != nil ||
		params.BackupRetentionPolicy.IsWeeklyBackupImmutable != nil ||
		params.BackupRetentionPolicy.IsMonthlyBackupImmutable != nil ||
		params.BackupRetentionPolicy.IsAdhocBackupImmutable != nil {
		brp := googleproxyclient.BackupRetentionPolicyUpdateV1beta{}

		if params.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration != nil {
			brp.BackupMinimumEnforcedRetentionDays = googleproxyclient.NewOptInt(int(*params.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration))
		}
		if params.BackupRetentionPolicy.IsDailyBackupImmutable != nil {
			brp.DailyBackupImmutable = googleproxyclient.NewOptBool(*params.BackupRetentionPolicy.IsDailyBackupImmutable)
		}
		if params.BackupRetentionPolicy.IsWeeklyBackupImmutable != nil {
			brp.WeeklyBackupImmutable = googleproxyclient.NewOptBool(*params.BackupRetentionPolicy.IsWeeklyBackupImmutable)
		}
		if params.BackupRetentionPolicy.IsMonthlyBackupImmutable != nil {
			brp.MonthlyBackupImmutable = googleproxyclient.NewOptBool(*params.BackupRetentionPolicy.IsMonthlyBackupImmutable)
		}
		if params.BackupRetentionPolicy.IsAdhocBackupImmutable != nil {
			brp.ManualBackupImmutable = googleproxyclient.NewOptBool(*params.BackupRetentionPolicy.IsAdhocBackupImmutable)
		}

		updateBody.BackupRetentionPolicy = googleproxyclient.NewOptBackupRetentionPolicyUpdateV1beta(brp)
	}

	updateParams := googleproxyclient.V1betaInternalUpdateBackupVaultParams{
		ProjectNumber:  params.OwnerID,
		LocationId:     *params.BackupRegion,
		BackupVaultId:  params.BackupVaultID,
		XCorrelationID: googleproxyclient.NewOptString(correlationID),
	}

	res, err := googleProxyClient.Invoker.V1betaInternalUpdateBackupVault(ctx, &updateBody, updateParams)
	if err != nil {
		logger.Errorf("Failed to call V1betaInternalUpdateBackupVault: %v", err)
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to update remote backup vault: %v", err),
			"InternalUpdateBackupVaultFailed",
			err,
		)
	}

	switch r := res.(type) {
	case *googleproxyclient.OperationV1beta:
		isDone := r.Done.Value
		logger.Infof("Update operation returned for remote backup vault %s (external UUID: %s) in region %s. Operation: %s, Done: %v",
			params.BackupVaultID, params.BackupVaultID, *params.BackupRegion, r.GetName(), isDone)

		if !isDone {
			logger.Warnf("Update operation for remote backup vault %s not marked as done, but treating as synchronous", params.BackupVaultID)
		}

		logger.Infof("Successfully updated remote backup vault %s (external UUID: %s) in region %s",
			params.BackupVaultID, params.BackupVaultID, *params.BackupRegion)
		return &datamodel.BackupVault{
			BaseModel: datamodel.BaseModel{
				UUID: params.BackupVaultID,
			},
		}, nil

	case *googleproxyclient.V1betaInternalUpdateBackupVaultBadRequest:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Bad request updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultBadRequest",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultUnauthorized:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unauthorized to update remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultUnauthorized",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultForbidden:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Forbidden to update remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultForbidden",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultNotFound:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Remote backup vault not found: %s", r.Message),
			"V1betaInternalUpdateBackupVaultNotFound",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultConflict:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Conflict updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultConflict",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultUnprocessableEntity:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unprocessable entity updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultUnprocessableEntity",
			errors.New(r.Message),
		)

	case *googleproxyclient.V1betaInternalUpdateBackupVaultInternalServerError:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Internal server error updating remote backup vault: %s", r.Message),
			"V1betaInternalUpdateBackupVaultInternalServerError",
			errors.New(r.Message),
		)

	default:
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Unexpected response type from internal update backup vault endpoint: %T", r),
			"UnexpectedUpdateResponseType",
			fmt.Errorf("unexpected response type: %T", r),
		)
	}
}

func _convertToBackupVaultDataModel(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
	var resourceID, backupVaultType string
	var sourceRegion, backupRegion, description *string
	if bv.ResourceID != nil {
		resourceID = *bv.ResourceID
	}
	if bv.BackupRegion != nil {
		backupRegion = bv.BackupRegion
	}
	if bv.BackupVaultType != nil {
		backupVaultType = *bv.BackupVaultType
	}
	if bv.Description != nil && *bv.Description != "" {
		description = bv.Description
	}

	// SourceRegion and BackupRegion are 'nil' in SDE for IN_REGION backup vaults. They're set only for CROSS_REGION backup vaults.
	if bv.SourceRegion != nil {
		// CROSS_REGION Backup Vault
		sourceRegion = bv.SourceRegion
	} else {
		// IN_REGION Backup Vault
		sourceRegion = nillable.ToPointer(locationId)
	}

	var minEnforcedRetentionDuration *int64
	var isDaily, isWeekly, isMonthly, isAdhoc bool
	if bv.BackupRetentionPolicy != nil {
		minEnforcedRetentionDuration = bv.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDays
		isDaily = bv.BackupRetentionPolicy.DailyBackupImmutable
		isWeekly = bv.BackupRetentionPolicy.WeeklyBackupImmutable
		isMonthly = bv.BackupRetentionPolicy.MonthlyBackupImmutable
		isAdhoc = bv.BackupRetentionPolicy.ManualBackupImmutable
	}

	immutableFields := &datamodel.ImmutableAttributes{
		BackupMinimumEnforcedRetentionDuration: minEnforcedRetentionDuration,
		IsDailyBackupImmutable:                 isDaily,
		IsWeeklyBackupImmutable:                isWeekly,
		IsMonthlyBackupImmutable:               isMonthly,
		IsAdhocBackupImmutable:                 isAdhoc,
	}

	var cmekFields *datamodel.CmekAttributes
	if bv.KmsConfigResourcePath != nil {
		cmekFields = &datamodel.CmekAttributes{
			KmsConfigResourcePath:    bv.KmsConfigResourcePath,
			EncryptionState:          bv.EncryptionState,
			BackupsPrimaryKeyVersion: bv.BackupsPrimaryKeyVersion,
		}
	}

	return &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID:      bv.BackupVaultID,
			CreatedAt: time.Time(bv.CreatedAt),
			UpdatedAt: time.Time(bv.CreatedAt),
			DeletedAt: nil,
		},
		Name:                       resourceID,
		BackupRegionName:           backupRegion,
		SourceRegionName:           sourceRegion,
		LifeCycleState:             bv.State,
		LifeCycleStateDetails:      bv.StateDetails,
		BackupVaultType:            backupVaultType,
		Description:                description,
		ImmutableAttributes:        immutableFields,
		CmekAttributes:             cmekFields,
		CrossRegionBackupVaultName: bv.DestinationBackupVault,
	}, nil
}

func (j *BackupVaultActivity) UpdateBackupVaultStateInCaseOfError(ctx context.Context, backupVault *datamodel.BackupVault, state, stateDetails string) error {
	se := j.SE

	// Update the state of the BackupVault in the database
	_, err := se.UpdateBackupVaultState(ctx, backupVault, state, stateDetails)
	if err != nil {
		return err
	}
	return nil
}

// DeleteBackupVaultBuckets deletes all buckets associated with a backup vault
func (j *BackupVaultActivity) DeleteBackupVaultBuckets(ctx context.Context, backupVault *datamodel.BackupVault) error {
	if backupVault == nil {
		return errors.New("backupVault parameter is nil")
	}

	logger := util.GetLogger(ctx)

	// Get GCP service for bucket deletion
	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	// Delete each bucket associated with the backup vault
	for _, bucketDetail := range backupVault.BucketDetails {
		if bucketDetail.BucketName != "" {
			logger.Infof("Deleting bucket %s for backup vault %s", bucketDetail.BucketName, backupVault.Name)
			_, err := DeleteGCPBucket(ctx, bucketDetail.BucketName, gcpService)
			if err != nil {
				logger.Errorf("Failed to delete bucket %s: %v", bucketDetail.BucketName, err)
				return errors.WrapAsTemporalApplicationError(err)
			}
			logger.Infof("Successfully deleted bucket %s", bucketDetail.BucketName)
		}
	}
	return nil
}

// CleanupBackupVaultsForAccount fetches all backup vaults for an account and cleans them up
func (a *BackupVaultActivity) CleanupBackupVaultsForAccount(ctx context.Context, projectNumber string) error {
	logger := util.GetLogger(ctx)

	// Get account ID from project number
	account, err := a.SE.GetAccount(ctx, projectNumber)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	// Fetch all backup vaults for the account
	backupVaults, err := a.SE.ListBackupVaults(ctx, account.ID)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	if len(backupVaults) > 0 {
		logger.Infof("Cleaning up %d backup vaults for project %s", len(backupVaults), projectNumber)
	}

	// Cleanup each backup vault
	for _, vault := range backupVaults {
		err = a.cleanupBackupVault(ctx, vault)
		if err != nil {
			logger.Errorf("Failed to cleanup backup vault %s: %v", vault.UUID, err)
			return errors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

// cleanupBackupVault handles the cleanup of a single backup vault
func (a *BackupVaultActivity) cleanupBackupVault(ctx context.Context, vault *datamodel.BackupVault) error {
	logger := util.GetLogger(ctx)

	// 1. Delete GCP buckets associated with this vault
	err := a.deleteGCPBucketsForVault(ctx, vault)
	if err != nil {
		logger.Errorf("Failed to delete GCP buckets for vault %s: %v", vault.UUID, err)
		// Continue with database deletion even if bucket deletion fails
	}

	// 2. Fetch and cleanup all backups for this vault
	err = a.cleanupBackupsForVault(ctx, vault)
	if err != nil {
		logger.Errorf("Failed to cleanup backups for vault %s: %v", vault.UUID, err)
		return errors.WrapAsTemporalApplicationError(err)
	}

	// 3. Soft delete the backup vault in database
	_, err = a.SE.DeleteBackupVaultInVCP(ctx, vault.UUID)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to soft delete backup vault %s: %v", vault.UUID, err),
			"DeleteBackupVaultError",
			err,
		)
	}

	return nil
}

// deleteGCPBucketsForVault deletes all GCP buckets associated with a backup vault
func (a *BackupVaultActivity) deleteGCPBucketsForVault(ctx context.Context, vault *datamodel.BackupVault) error {
	logger := util.GetLogger(ctx)

	// Initialize GCP service
	gcpService, err := hyperscaler.GetGCPService(ctx)
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	// Delete each bucket associated with the backup vault
	for _, bucketDetail := range vault.BucketDetails {
		if bucketDetail.BucketName != "" {
			// First empty the bucket (delete all objects)
			err := gcpService.EmptyBucket(ctx, bucketDetail.BucketName)
			if err != nil {
				logger.Errorf("Failed to empty bucket %s: %v", bucketDetail.BucketName, err)
				return errors.NewVCPError(errors.ErrGCPResourceDeprovisionError, err)
			}

			// Then delete the empty bucket
			_, err = gcpService.DeleteBucketWithLifecyclePolicy(ctx, bucketDetail.BucketName)
			if err != nil {
				logger.Errorf("Failed to delete bucket %s: %v", bucketDetail.BucketName, err)
				return errors.NewVCPError(errors.ErrGCPResourceDeprovisionError, err)
			}
		}
	}
	return nil
}

// cleanupBackupsForVault fetches and soft deletes all backups for a vault
func (a *BackupVaultActivity) cleanupBackupsForVault(ctx context.Context, vault *datamodel.BackupVault) error {
	logger := util.GetLogger(ctx)

	// Fetch all backups for this vault
	backups, err := a.SE.GetBackupsByBackupVaultOwnerIDAndFilter(ctx, vault.UUID, vault.AccountID, [][]interface{}{})
	if err != nil {
		return errors.WrapAsTemporalApplicationError(err)
	}

	// Soft delete each backup
	for _, backup := range backups {
		err = a.cleanupBackup(ctx, backup)
		if err != nil {
			logger.Errorf("Failed to cleanup backup %s: %v", backup.UUID, err)
			return errors.WrapAsTemporalApplicationError(err)
		}
	}

	return nil
}

// cleanupBackup handles the soft deletion of a single backup
func (a *BackupVaultActivity) cleanupBackup(ctx context.Context, backup *datamodel.Backup) error {
	logger := util.GetLogger(ctx)

	// Soft delete the backup in database
	_, err := a.SE.DeleteBackup(ctx, backup.UUID)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Failed to soft delete backup %s: %v", backup.UUID, err),
			"DeleteBackupError",
			err,
		)
	}

	logger.Debugf("Successfully soft deleted backup %s", backup.UUID)
	return nil
}
