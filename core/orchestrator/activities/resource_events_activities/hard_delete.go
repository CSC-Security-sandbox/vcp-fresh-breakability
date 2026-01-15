package resource_events_activities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type hardDeleteResources struct {
	tableName   string
	queryFilter string
}

var (
	// NOTE: entry in this map needs to be in the order in which resources should be hard-deleted from the tables
	resourcesToHardDelete = []hardDeleteResources{
		{"backups", dbQueryBackupID},
		{"backup_policies", dbQueryAccountID},
		{"snapshots", dbQueryAccountID},
		{"volume_replications", dbQueryAccountID},
		{"volumes", dbQueryAccountID},
		{"backup_vaults", dbQueryAccountID},
		{"svms", dbQueryAccountID},
		{"pools", dbQueryAccountID},
		{"host_groups", dbQueryAccountID},
		{"kms_configs", dbQueryAccountID},
		{"nodes", dbQueryAccountID},
		{"service_accounts", dbQueryAccountID},
		{"lifs", dbQueryAccountID},
		{"accounts", dbQueryAccountName},
		{"quota_rules", dbQueryAccountID},
	}
)

const (
	dbQueryAccountID   = "account_id = ? and deleted_at is not null"
	dbQueryBackupID    = "backup_vault_id in (select id from backup_vaults where account_id = ? and deleted_at is not null) and deleted_at is not null"
	dbQueryAccountName = "id = ?"
)

func (j *FinishProjectEventActivity) HardDeleteResourcesInOrder(ctx context.Context, projectNumber string) error {
	se := j.SE
	account, err := se.GetSoftDeleteAccount(ctx, projectNumber)
	if err != nil {
		return err
	}

	logger := util.GetLogger(ctx)
	for _, resource := range resourcesToHardDelete {
		err := se.HardDeleteResourceByTable(ctx, resource.tableName, resource.queryFilter, account.ID)
		if err != nil {
			logger.Errorf("Error hard-deleting %s resource for account %d", resource.tableName, account.ID)
			return err
		}
		logger.Infof("Successfully hard-deleted %s resource for account %d", resource.tableName, account.ID)
	}
	return nil
}
