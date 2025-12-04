package backgroundactivities

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// BackupDetail holds individual backup information.
type BackupDetail struct {
	VolName     string
	Size        int64
	AccountName string
}
type CustomerAdoptionActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

type BackupDetailsResult struct {
	Details []BackupDetail
}

// GetNonDeletedVolumesActivity fetches all non-deleted volumes from the database.
func (a *CustomerAdoptionActivity) GetActiveVolumesActivity(ctx context.Context) ([]*datamodel.Volume, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	var filtered []*datamodel.Volume
	limit := 1000
	offset := 0

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context cancelled while fetching volumes: %v", ctx.Err())
			return nil, ctx.Err()
		default:
		}

		pagination := &utils.Pagination{
			Offset: offset,
			Limit:  limit,
		}
		vols, err := se.ListAllVolumes(ctx, [][]interface{}{}, pagination)
		if err != nil {
			logger.Errorf("Failed to fetch volumes: %v", err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		if len(vols) == 0 {
			break
		}
		for _, v := range vols {
			if v.State != "deleted" {
				filtered = append(filtered, v)
			}
		}
		offset += len(vols)
	}

	metrics.EmitAutoTierEnabledMetric(filtered)
	metrics.EmitCRREnabledMetric(filtered)
	metrics.EmitLargeVolumeEnabledMetric(filtered)
	metrics.EmitCBSEnabledMetric(filtered)
	logger.Infof("Filtered %d non-deleted volumes", len(filtered))
	return filtered, nil
}

// backupDetailsResult holds the results from GetBackupDetailsActivity operation
func (a *CustomerAdoptionActivity) GetBackupDetailsActivity(ctx context.Context, timestamp time.Time) (*BackupDetailsResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE

	var allBackups []*datamodel.Backup
	offset := 0
	limit := 1000
	conditions := [][]interface{}{}

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context cancelled while fetching backups: %v", ctx.Err())
			return nil, ctx.Err()
		default:
		}

		pagination := &utils.Pagination{
			Offset: offset,
			Limit:  limit,
		}
		rawBackups, err := se.GetBackupMetrics(ctx, conditions, pagination)
		if err != nil {
			logger.Errorf("Failed to fetch backups: %v", err)
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		if len(rawBackups) == 0 {
			break
		}

		allBackups = append(allBackups, rawBackups...)
		offset += len(rawBackups)
	}

	// Create slices to hold backup details and metrics
	backupDetails := make([]BackupDetail, 0, len(allBackups))
	metricDetails := make([]metrics.BackupDetailForMetric, 0, len(allBackups))

	// Populate both slices in a single loop
	for _, b := range allBackups {
		accountIdentifier := b.Attributes.AccountIdentifier
		volumeName := b.Attributes.VolumeName
		detail := BackupDetail{
			VolName:     volumeName,
			AccountName: accountIdentifier,
			Size:        b.LatestLogicalBackupSize,
		}
		backupDetails = append(backupDetails, detail)
		metricDetails = append(metricDetails, metrics.BackupDetailForMetric{
			VolName:     detail.VolName,
			AccountName: detail.AccountName,
			Size:        detail.Size,
		})
	}

	// Emit metrics for backup details
	metrics.EmitBackupDetailsMetric(metricDetails)
	return &BackupDetailsResult{Details: backupDetails}, nil
}
