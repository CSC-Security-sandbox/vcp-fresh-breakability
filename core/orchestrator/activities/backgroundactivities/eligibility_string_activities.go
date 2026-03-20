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

// GetEligibilityString fetches all non-deleted volumes (VCP and expert mode) and emits eligibility metrics.
// Both sources are fetched independently — a failure in one does not prevent the other from being collected.
// Metrics are emitted with whatever was successfully fetched; nil is safe for either slice.

func (a *EligibilityStringActivity) GetEligibilityString(ctx context.Context) error {
	logger := util.GetLogger(ctx)
	se := a.SE

	limit := 1000
	var fetchErr error

	var eligibleVolumes []*datamodel.Volume
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
		vols, err := se.GetEligibleVolumes(ctx, [][]interface{}{}, pagination)
		if err != nil {
			logger.Errorf("Failed to fetch VCP volumes for eligibility string: %v", err)
			fetchErr = vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
			break
		}
		if len(vols) == 0 {
			break
		}

		eligibleVolumes = append(eligibleVolumes, vols...)
		offset += len(vols)
	}

	var expertModeVolumes []*datamodel.ExpertModeVolumes
	offset = 0

	for {
		select {
		case <-ctx.Done():
			logger.Warnf("Context cancelled while fetching expert mode eligibility volumes: %v", ctx.Err())
			return ctx.Err()
		default:
		}

		pagination := &utils.Pagination{
			Offset: offset,
			Limit:  limit,
		}
		vols, err := se.GetEligibleExpertModeVolumes(ctx, [][]interface{}{}, pagination)
		if err != nil {
			logger.Errorf("Failed to fetch expert mode volumes for eligibility string: %v", err)
			if fetchErr == nil {
				fetchErr = vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
			}
			break
		}
		if len(vols) == 0 {
			break
		}

		expertModeVolumes = append(expertModeVolumes, vols...)
		offset += len(vols)
	}

	metrics.EmitEligibilityStringMetric(eligibleVolumes, expertModeVolumes)
	logger.Infof("Fetched and emitted metrics for %d VCP volumes and %d expert mode volumes", len(eligibleVolumes), len(expertModeVolumes))
	return fetchErr
}
