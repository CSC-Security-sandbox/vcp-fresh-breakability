package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
)

var (
	convertDatastoreBackupPolicyToModel = _convertDatastoreBackupPolicyToModel
	getBackupPolicyByNameAndOwnerID     = _getBackupPolicyByNameAndOwnerID
	listBackupPolicyVolumeCount         = _listBackupPolicyVolumeCount
)

func (o *Orchestrator) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error) {
	se := o.storage
	backupPolicyDetails, err := getBackupPolicyByNameAndOwnerID(ctx, se, backupPolicyName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupPolicyToModel(backupPolicyDetails), nil
}

func (o *Orchestrator) ListBackupPolicyVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, error) {
	se := o.storage
	return listBackupPolicyVolumeCount(ctx, se, ownerID, backupPolicyUUIDs)
}

func _getBackupPolicyByNameAndOwnerID(ctx context.Context, se database.Storage, backupPolicyName, ownerID string) (*datamodel.BackupPolicy, error) {
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}

	backupPolicy, err := se.GetBackupPolicyByNameAndOwnerID(ctx, backupPolicyName, account.ID)
	if err != nil {
		return nil, err
	}
	return backupPolicy, nil
}

func _listBackupPolicyVolumeCount(ctx context.Context, se database.Storage, ownerID string, backupPolicyUUIDs []string) (map[string]int64, error) {
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, err
	}

	conditions := [][]interface{}{{"account_id = ?", account.ID}}
	if len(backupPolicyUUIDs) > 0 {
		conditions = append(conditions, []interface{}{"data_protection->>'backup_policy_id' IN ?", backupPolicyUUIDs})
	}
	backupPolicies, err := se.ListBackupPolicyVolumeCount(ctx, conditions)
	if err != nil {
		return nil, err
	}
	return backupPolicies, nil
}

func _convertDatastoreBackupPolicyToModel(backupPolicy *datamodel.BackupPolicy) *models.BackupPolicy {
	return &models.BackupPolicy{
		ResourceID:         backupPolicy.Name,
		BackupPolicyUUID:   backupPolicy.UUID,
		DailyBackupLimit:   backupPolicy.DailyBackupsToKeep,
		WeeklyBackupLimit:  backupPolicy.WeeklyBackupsToKeep,
		MonthlyBackupLimit: backupPolicy.MonthlyBackupsToKeep,
		Enabled:            backupPolicy.PolicyEnabled,
		Description:        backupPolicy.Description,
		State:              backupPolicy.LifeCycleState,
		CreatedAt:          backupPolicy.CreatedAt,
	}
}
