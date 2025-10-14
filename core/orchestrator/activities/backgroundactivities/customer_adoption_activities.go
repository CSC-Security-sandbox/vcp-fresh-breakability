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

type CustomerAdoptionActivity struct {
	SE        database.Storage
	Scheduler *scheduler.TemporalScheduler
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
