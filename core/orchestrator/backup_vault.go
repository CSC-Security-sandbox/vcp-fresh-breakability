package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
)

var (
	createBackupVault                  = _createBackupVault
	validateBackupVaultParams          = _validateBackupVaultParams
	convertDatastoreBackupVaultToModel = _convertDatastoreBackupVaultToModel
	getBackupVaultByNameAndOwnerID     = _getBackupVaultByNameAndOwnerID
)

// CreateBackupVaultParams describes parameters supplied to CreateBackupVault
type CreateBackupVaultParams struct {
	BackupVaultID              string
	Name                       string
	Description                *string
	Region                     string
	AccountVendorID            string
	BackupRegion               *string
	BackupVaultType            string
	SourceRegion               *string
	BackupRetentionPolicy      *BackupRetentionPolicyV2params
	ExternalUUID               string
	CrossRegionBackupVaultName *string
	ProjectNumber              string
}

// BackupRetentionPolicyV2params describes request parameters for BackupRetentionPolicyV2
type BackupRetentionPolicyV2params struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 bool
	IsMonthlyBackupImmutable               bool
	IsWeeklyBackupImmutable                bool
	IsAdhocBackupImmutable                 bool
}

func (o *Orchestrator) GetBackupVaultByNameAndOwnerID(ctx context.Context, bvName, ownerID string) (*models.BackupVaultV1beta, error) {
	se := o.storage
	bvDetails, err := getBackupVaultByNameAndOwnerID(ctx, se, bvName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupVaultToModel(bvDetails), nil
}

// CreateBackupVault creates a new backup vault
func (o *Orchestrator) CreateBackupVault(ctx context.Context, backupVaultParams *commonparams.BackupVaultParams, gcpParams gcpserver.V1betaCreateBackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	return createBackupVault(ctx, o.storage, o.temporal, backupVaultParams, gcpParams)
}

func _createBackupVault(ctx context.Context, se database.Storage, temporal client.Client, backupVaultParams *commonparams.BackupVaultParams, gcpParams gcpserver.V1betaCreateBackupVaultParams) (*models.BackupVaultV1beta, string, error) {
	logger := util.GetLogger(ctx)
	account, err := getAccountWithName(ctx, se, backupVaultParams.OwnerID)
	if err != nil {
		return nil, "", err
	}

	err = validateBackupVaultParams(se, backupVaultParams)
	if err != nil {
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeCreateBackupVault),
		State:         string(models.JobsStateNEW),
		ResourceName:  backupVaultParams.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}

	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Error("Failed to create job in database", "error", err)
		return nil, "", err
	}

	dbBackupVault := &datamodel.BackupVault{
		Name:        backupVaultParams.Name,
		Account:     account,
		AccountID:   account.ID,
		Description: backupVaultParams.Description,
		ImmutableAttributes: &datamodel.ImmutableAttributes{
			BackupMinimumEnforcedRetentionDuration: backupVaultParams.BackupRetentionPolicy.BackupMinimumEnforcedRetentionDuration,
			IsDailyBackupImmutable:                 *backupVaultParams.BackupRetentionPolicy.IsDailyBackupImmutable,
			IsWeeklyBackupImmutable:                *backupVaultParams.BackupRetentionPolicy.IsWeeklyBackupImmutable,
			IsMonthlyBackupImmutable:               *backupVaultParams.BackupRetentionPolicy.IsMonthlyBackupImmutable,
			IsAdhocBackupImmutable:                 *backupVaultParams.BackupRetentionPolicy.IsAdhocBackupImmutable,
		},
		SourceRegionName: backupVaultParams.SourceRegion,
		BackupRegionName: backupVaultParams.BackupRegion,
	}

	dbBV, err := se.CreatingBackupVault(ctx, dbBackupVault)
	if err != nil {
		return nil, "", err
	}
	_, err = temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			ID:                    createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		workflows.CreateBackupVault,
		backupVaultParams,
		dbBV,
		gcpParams,
	)
	if err != nil {
		logger.Error("Failed to start backup vault create workflow: ", "error", err)
		return nil, "", err
	}
	return convertDatastoreBackupVaultToModel(dbBV), createdJob.UUID, nil
}

func _validateBackupVaultParams(se database.Storage, params *commonparams.BackupVaultParams) error {
	backupVault, err := getBackupVaultByNameAndOwnerID(context.Background(), se, params.Name, params.OwnerID)
	if err != nil && !strings.Contains(err.Error(), "backup vault not found") {
		return err
	}
	if backupVault != nil && backupVault.Name == params.Name {
		return errors.New("backup vault with the same name already exists")
	}
	if params.Name == "" {
		return errors.New("backup vault name is required")
	}
	if params.Region == "" {
		return errors.New("region is required")
	}
	if params.BackupRegion != nil && params.SourceRegion != nil && strings.EqualFold(*params.SourceRegion, *params.BackupRegion) {
		return errors.New("backup region and source region cannot be the same")
	}
	return nil
}

func _getBackupVaultByNameAndOwnerID(ctx context.Context, se database.Storage, bvName, ownerID string) (*datamodel.BackupVault, error) {
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}

	bv, err := se.GetBackupVaultByNameAndOwnerID(ctx, bvName, strconv.FormatInt(account.ID, 10))
	if err != nil {
		if strings.Contains(err.Error(), "record not found") || errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.New("backup vault not found")
		}
		return nil, err
	}

	return bv, nil
}

func _convertDatastoreBackupVaultToModel(bv *datamodel.BackupVault) *models.BackupVaultV1beta {
	return &models.BackupVaultV1beta{
		ID:                    bv.ID,
		OwnerID:               bv.Account.UUID,
		BackupVaultID:         bv.UUID,
		Name:                  bv.Name,
		Description:           bv.Description,
		LifeCycleState:        bv.LifeCycleState,
		LifeCycleStateDetails: bv.LifeCycleStateDetails,
		CreatedAt:             bv.CreatedAt,
		UpdatedAt:             bv.UpdatedAt,
		BackupRegion:          bv.BackupRegionName,
		SourceRegion:          bv.SourceRegionName,
		Region:                bv.RegionName,
		AccountVendorID:       bv.AccountVendorID,
		BackupRetentionPolicy: models.BackupRetentionPolicyparams{
			BackupMinimumEnforcedRetentionDuration: bv.ImmutableAttributes.BackupMinimumEnforcedRetentionDuration,
			IsDailyBackupImmutable:                 bv.ImmutableAttributes.IsDailyBackupImmutable,
			IsMonthlyBackupImmutable:               bv.ImmutableAttributes.IsMonthlyBackupImmutable,
			IsWeeklyBackupImmutable:                bv.ImmutableAttributes.IsWeeklyBackupImmutable,
			IsAdhocBackupImmutable:                 bv.ImmutableAttributes.IsAdhocBackupImmutable,
		},
		SourceBackupVault:          &bv.Name,
		DestinationBackupVault:     bv.CrossRegionBackupVaultName,
		BackupVaultType:            &bv.BackupVaultType,
		CrossRegionBackupVaultName: bv.CrossRegionBackupVaultName,
	}
}
