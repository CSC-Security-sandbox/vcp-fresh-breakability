package models

import "time"

// CreateBackupVaultParams describes request parameters for CreateBackupVault
type CreateBackupVaultParams struct {
	BackupVaultID              string
	Name                       string
	Description                *string
	Region                     string
	AccountVendorID            string
	BackupRegion               *string
	SourceRegion               *string
	BackupRetentionPolicy      BackupRetentionPolicyparams
	CrossRegionBackupVaultName *string
}

// BackupRetentionPolicyparams describes request parameters for BackupRetentionPolicy
type BackupRetentionPolicyparams struct {
	BackupMinimumEnforcedRetentionDuration *int64
	IsDailyBackupImmutable                 bool
	IsMonthlyBackupImmutable               bool
	IsWeeklyBackupImmutable                bool
	IsAdhocBackupImmutable                 bool
}

type BackupVaultV1beta struct {
	ID                         int64
	OwnerID                    string
	BackupVaultID              string
	Name                       string
	Description                *string
	LifeCycleState             string
	LifeCycleStateDetails      string
	CreatedAt                  time.Time
	UpdatedAt                  time.Time
	DeletedAt                  *time.Time
	BackupRegion               *string
	SourceRegion               *string
	Region                     string
	AccountVendorID            string
	BackupRetentionPolicy      BackupRetentionPolicyparams
	SourceBackupVault          *string
	DestinationBackupVault     *string
	BackupVaultType            *string
	AccountName                string
	CrossRegionBackupVaultName *string
	ExternalUUID               *string
}
