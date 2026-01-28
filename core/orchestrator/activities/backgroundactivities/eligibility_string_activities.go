package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type EligibilityStringActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
}

// Eligibility String fetches all non-deleted volumes name and state from DB and emits metrics.

func (a *EligibilityStringActivity) GetEligibilityString(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	var eligibleVolumes []*datamodel.Volume
	limit := 1000
	offset := 0

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context cancelled while fetching eligibility string volumes: %v", ctx.Err())
			return ctx.Err()
		default:
		}

		pagination := &utils.Pagination{
			Offset: offset,
			Limit:  limit,
		}
		// Use GetEligibleVolumes - optimized for eligibility string (only selects name, state)
		// Database already filters deleted_at IS NULL
		vols, err := se.GetEligibleVolumes(ctx, [][]interface{}{}, pagination)
		if err != nil {
			logger.Errorf("Failed to fetch volumes: %v", err)
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		if len(vols) == 0 {
			break
		}

		// No need to filter by state - GetEligibleVolumes already filters deleted_at IS NULL at DB level
		eligibleVolumes = append(eligibleVolumes, vols...)
		offset += len(vols)
	}

	// Emit metrics at activity level
	metrics.EmitEligibilityStringMetric(eligibleVolumes)
	logger.Infof("Fetched and emitted metrics for %d eligibility string volumes", len(eligibleVolumes))
	return nil
}
