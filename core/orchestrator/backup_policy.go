package orchestrator

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

var (
	convertDatastoreBackupPolicyToModel = _convertDatastoreBackupPolicyToModel
	getBackupPolicyByNameAndOwnerID     = _getBackupPolicyByNameAndOwnerID
	listBackupPolicyVolumeCount         = _listBackupPolicyVolumeCount
	listBackupPolicies                  = _listBackupPolicies
)

func (o *Orchestrator) GetBackupPolicyByNameAndOwnerID(ctx context.Context, backupPolicyName, ownerID string) (*models.BackupPolicy, error) {
	se := o.storage
	backupPolicyDetails, err := getBackupPolicyByNameAndOwnerID(ctx, se, backupPolicyName, ownerID)
	if err != nil {
		return nil, err
	}
	return convertDatastoreBackupPolicyToModel(backupPolicyDetails), nil
}

func (o *Orchestrator) ListBackupPoliciesAndVolumeCount(ctx context.Context, ownerID string, backupPolicyUUIDs []string) (map[string]int64, map[string]*models.BackupPolicy, error) {
	se := o.storage
	account, err := getAccountWithName(ctx, se, ownerID)
	if err != nil {
		return nil, nil, err
	}

	backupPolicyVolumeCount, err := listBackupPolicyVolumeCount(ctx, se, account.ID, backupPolicyUUIDs)
	if err != nil {
		return nil, nil, err
	}

	backupPolicies, err := listBackupPolicies(ctx, se, account.ID, backupPolicyUUIDs)
	if err != nil {
		return nil, nil, err
	}

	backupPolicyMap := make(map[string]*models.BackupPolicy)
	for _, backupPolicy := range backupPolicies {
		backupPolicyMap[backupPolicy.UUID] = convertDatastoreBackupPolicyToModel(backupPolicy)
	}
	return backupPolicyVolumeCount, backupPolicyMap, nil
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

func _listBackupPolicyVolumeCount(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) (map[string]int64, error) {
	conditions := [][]interface{}{{"account_id = ?", accountID}}
	if len(backupPolicyUUIDs) > 0 {
		conditions = append(conditions, []interface{}{"data_protection->>'backup_policy_id' IN ?", backupPolicyUUIDs})
	}

	backupPolicies, err := se.ListBackupPolicyVolumeCount(ctx, conditions)
	if err != nil {
		return nil, err
	}
	return backupPolicies, nil
}

func _listBackupPolicies(ctx context.Context, se database.Storage, accountID int64, backupPolicyUUIDs []string) ([]*datamodel.BackupPolicy, error) {
	conditions := [][]interface{}{{"account_id = ?", accountID}}
	if len(backupPolicyUUIDs) > 0 {
		conditions = append(conditions, []interface{}{"uuid IN ?", backupPolicyUUIDs})
	}

	backupPolicies, err := se.ListBackupPolicies(ctx, conditions)
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
