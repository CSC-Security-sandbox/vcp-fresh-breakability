package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// BackupVaultMetricsResult holds the results from GetBackupVaultMetrics operation
type BackupVaultMetricsResult struct {
	HydratedMetrics []entity.HydratedMetric
}

const (
	// CMEK rotation state enum values
	CmekRotationStatePending    int64 = 0
	CmekRotationStateInProgress int64 = 1
	CmekRotationStateCompleted  int64 = 2
	CmekRotationStateFailed     int64 = 3

	BackupCryptoKeyVersionTag = "backup_crypto_key_version"
)

// Maps job states to CMEK rotation state enum values per schema:
// 0 = PENDING, 1 = IN_PROGRESS, 2 = COMPLETED, 3 = FAILED
// Only handles the exact job statuses from the job table: NEW, PROCESSING, ERROR, DONE
func mapJobStatusToRotationState(status string) int64 {
	switch status {
	case string(datamodel.JobsStateNEW):
		return CmekRotationStatePending
	case string(datamodel.JobsStatePROCESSING):
		return CmekRotationStateInProgress
	case string(datamodel.JobsStateDONE):
		return CmekRotationStateCompleted
	case string(datamodel.JobsStateERROR):
		return CmekRotationStateFailed
	default:
		return CmekRotationStatePending
	}
}

// GetBackupVaultMetrics retrieves CMEK key rotation metrics for backup vaults from the database and returns them in a structured result.
func GetBackupVaultMetrics(ctx context.Context, vcpDB database.Storage, config *common.TelemetryConfig, timestamp time.Time) (*BackupVaultMetricsResult, error) {
	logger := util.GetLogger(ctx)

	endTime := timestamp
	startTime := timestamp.Add(-5 * time.Minute)

	var allJobStatuses []*database.CmekRotationJobStatus
	offset := int64(0)
	limit := pageSize

	for {
		jobStatuses, err := vcpDB.GetCmekRotationJobStatuses(ctx, startTime, endTime, int(limit), int(offset))
		if err != nil {
			logger.Error("Failed to get CMEK rotation job statuses", "error", err.Error())
			return &BackupVaultMetricsResult{}, err
		}

		if len(jobStatuses) == 0 {
			break
		}

		allJobStatuses = append(allJobStatuses, jobStatuses...)

		offset += limit
	}

	logger.Info(fmt.Sprintf("Found %d CMEK rotation job statuses", len(allJobStatuses)))

	if len(allJobStatuses) == 0 {
		return &BackupVaultMetricsResult{}, nil
	}

	var metrics []entity.HydratedMetric

	for _, jobStatus := range allJobStatuses {
		if jobStatus.BackupVaultUUID == "" || jobStatus.BackupVaultName == "" {
			if jobStatus.BackupVaultUUID == "" {
				logger.Error(fmt.Sprintf("Backup vault UUID is missing for job status %d", jobStatus.ID))
			}
			if jobStatus.BackupVaultName == "" {
				logger.Error(fmt.Sprintf("Backup vault name is missing for job status %d", jobStatus.ID))
			}
			continue
		}

		backupVaultMetadata := assembleBackupVaultMetadata(jobStatus, config)

		rotationState := mapJobStatusToRotationState(jobStatus.Status)

		metric := setupHydratedMetric(timestamp, backupVaultMetadata, metadata.CMEKBackupKeyRotationState, float64(rotationState))

		// Add the backup_crypto_key_version label (this will be used as a metric label)
		// Note: Tags in ResourceMetadata are used for labels in Google Cloud Monitoring
		if metric.Metadata.Tags == nil {
			metric.Metadata.Tags = make(map[string]string)
		}
		metric.Metadata.Tags[BackupCryptoKeyVersionTag] = jobStatus.NewKmsKeyURL

		metrics = append(metrics, metric)
	}

	return &BackupVaultMetricsResult{
		HydratedMetrics: metrics,
	}, nil
}

func assembleBackupVaultMetadata(jobStatus *database.CmekRotationJobStatus, config *common.TelemetryConfig) metadata.ResourceMetadata {
	met := metadata.ResourceMetadata{}
	met.SetResourceUUID(jobStatus.BackupVaultUUID)
	met.SetResourceType(metadata.BackupVault)
	met.SetRegionName(jobStatus.Region)
	met.SetResourceName(jobStatus.BackupVaultName)
	met.SetResourceDisplayName(jobStatus.BackupVaultName)
	met.SetAccountName(jobStatus.AccountIdentifier)
	met.SetDeploymentName(jobStatus.BackupVaultName)
	return met
}
