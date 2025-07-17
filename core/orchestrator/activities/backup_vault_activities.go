package activities

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsConvertJsonToModel       = utils.ConvertJsonToModel
	convertToBackupVaultDataModel = _convertToBackupVaultDataModel
	cvpCreateClient               = cvp.CreateClient
	updateBackupVaultInSDE        = _updateBackupVaultInSDE
	deleteBackupVaultInSDE        = _deleteBackupVaultInSDE
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
		logger.Error("Error Deleting BackupVault : ", err)
		return nil, err
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

	model, err := convertToBackupVaultDataModel(&data, paramz.Region)
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
		ManualBackupImmutable:              weeklyBackupImmutable,
		MonthlyBackupImmutable:             monthlyBackupImmutable,
		WeeklyBackupImmutable:              adhocBackupImmutable,
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
		logger.Error("Error Updating BackupVault : ", err)
		return nil, err
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

	model, err := convertToBackupVaultDataModel(&data, paramz.Region)
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

func _convertToBackupVaultDataModel(bv *models.BackupVaultV1beta, locationId string) (*datamodel.BackupVault, error) {
	var resourceID, backupVaultType string
	var backupRegion, description *string
	if bv.ResourceID != nil {
		resourceID = *bv.ResourceID
	}
	if bv.BackupRegion != nil {
		backupRegion = bv.BackupRegion
	}
	if bv.BackupVaultType != nil {
		backupVaultType = *bv.BackupVaultType
	}
	if bv.Description != nil {
		description = bv.Description
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

	return &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{
			UUID:      bv.BackupVaultID,
			CreatedAt: time.Time(bv.CreatedAt),
			UpdatedAt: time.Time(bv.CreatedAt),
			DeletedAt: nil,
		},
		Name:                       resourceID,
		BackupRegionName:           backupRegion,
		SourceRegionName:           &locationId,
		LifeCycleState:             bv.State,
		LifeCycleStateDetails:      bv.StateDetails,
		BackupVaultType:            backupVaultType,
		Description:                description,
		ImmutableAttributes:        immutableFields,
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
