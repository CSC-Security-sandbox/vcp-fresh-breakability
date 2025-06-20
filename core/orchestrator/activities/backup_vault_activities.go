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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	utilsConvertJsonToModel                           = utils.ConvertJsonToModel
	convertImmutableAttributesToBackupRetentionPolicy = _convertImmutableAttributesToBackupRetentionPolicy
	convertToBackupVaultDataModel                     = _convertToBackupVaultDataModel
	createBvToSde                                     = _createBvToSde
	cvpCreateClient                                   = cvp.CreateClient
	checkBackupVaultExistsInSDE                       = _checkBackupVaultExistsInSDE
	createBackupVaultInSDE                            = _createBackupVaultInSDE
)

type BackupVaultActivity struct {
	SE database.Storage
}

// CreateBackupVaultInSDE ensures idempotency by checking existing BackupVault before creation.
func (j *BackupVaultActivity) CreateBackupVaultInSDE(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
	return createBackupVaultInSDE(ctx, bvParams, paramz)
}

func _createBackupVaultInSDE(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
	// for idempotency
	sdeBV, err := checkBackupVaultExistsInSDE(ctx, bvParams, paramz)
	if err != nil {
		if !errors2.IsNotFoundErr(err) {
			return nil, err
		}
	}
	if sdeBV != nil {
		return sdeBV, nil
	}
	return createBvToSde(ctx, bvParams, paramz)
}

// _checkBackupVaultExistsInSDE checks if a BackupVault exists in SDE.
func _checkBackupVaultExistsInSDE(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)
	vaults, err := cvpClient.BackupVault.V1betaListBackupVaults(&backup_vault.V1betaListBackupVaultsParams{
		LocationID:     paramz.LocationId,
		ProjectNumber:  paramz.ProjectNumber,
		XCorrelationID: &xCorrelationID,
	})
	if err != nil {
		logger.Error("Error checking backupVault : ", err)
		return nil, err
	}

	bvs := vaults.Payload.BackupVaults

	for _, bv := range bvs {
		if *bv.ResourceID == bvParams.Name {
			bvModel, err := convertToBackupVaultDataModel(bv, paramz.LocationId)
			if err != nil {
				return nil, err
			}
			return bvModel, nil
		}
	}
	logger.Info("Backup vault not found in SDE, proceeding to create a new one")
	return nil, errors2.NewNotFoundErr("Backup vault", nil)
}

func _createBvToSde(ctx context.Context, bvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
	logger := util.GetLogger(ctx)
	GetSignedJwtToken := utils.GetJWTTokenFromContext(ctx)
	cvpClient := cvpCreateClient(logger, GetSignedJwtToken)
	xCorrelationID := utils.GetCoRelationIDFromContext(ctx)

	body := &models.BackupVaultCreateV1beta{
		BackupRegion: bvParams.BackupRegionName,
		Description:  bvParams.Description,
		ResourceID:   bvParams.Name,
	}
	retentionPolicy := convertImmutableAttributesToBackupRetentionPolicy(bvParams.ImmutableAttributes)
	if retentionPolicy != nil {
		body.BackupRetentionPolicy = retentionPolicy
	}
	vault, err := cvpClient.BackupVault.V1betaCreateBackupVault(&backup_vault.V1betaCreateBackupVaultParams{
		LocationID:     paramz.LocationId,
		ProjectNumber:  paramz.ProjectNumber,
		XCorrelationID: &xCorrelationID,
		Body:           body,
	})
	if err != nil {
		logger.Error("Error creating BackupVault : ", err)
		return nil, err
	}

	responseBytes, err := json.MarshalIndent(vault.Payload.Response, "", "  ")
	if err != nil {
		return nil, errors.New("failed to marshal response from SDE BackupVault creation")
	}
	data := models.BackupVaultV1beta{}
	err = utilsConvertJsonToModel(responseBytes, &data)
	if err != nil {
		return nil, err
	}

	model, err := convertToBackupVaultDataModel(&data, paramz.LocationId)
	if err != nil {
		return nil, err
	}
	return model, nil
}

func (j *BackupVaultActivity) CreateBackupVaultInVCP(ctx context.Context, bvParams *datamodel.BackupVault, vcpBvParams *datamodel.BackupVault, paramz gcpgenserver.V1betaCreateBackupVaultParams) (*datamodel.BackupVault, error) {
	se := j.SE
	bvParams.AccountID = vcpBvParams.AccountID
	bvParams.RegionName = paramz.LocationId
	BackupVault, err := se.CreateBackupVault(ctx, bvParams, vcpBvParams)
	if err != nil {
		return nil, err
	}
	return BackupVault, nil
}

func _convertImmutableAttributesToBackupRetentionPolicy(attrs *datamodel.ImmutableAttributes) *models.BackupRetentionPolicyV1beta {
	if attrs == nil {
		return nil
	}

	minEnforcedRetentionDuration := int64(0)
	if *attrs.BackupMinimumEnforcedRetentionDuration == minEnforcedRetentionDuration && !attrs.IsDailyBackupImmutable && !attrs.IsWeeklyBackupImmutable && !attrs.IsMonthlyBackupImmutable && !attrs.IsAdhocBackupImmutable {
		return nil
	}

	return &models.BackupRetentionPolicyV1beta{
		BackupMinimumEnforcedRetentionDays: attrs.BackupMinimumEnforcedRetentionDuration,
		DailyBackupImmutable:               attrs.IsDailyBackupImmutable,
		ManualBackupImmutable:              attrs.IsAdhocBackupImmutable,
		MonthlyBackupImmutable:             attrs.IsMonthlyBackupImmutable,
		WeeklyBackupImmutable:              attrs.IsWeeklyBackupImmutable,
	}
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
