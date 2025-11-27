package supervisorhandler

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// PoolHandler implements Handler for Pool resources.
type PoolHandler struct{}

// NewPoolHandler returns the handler that cleans up pool resources in VCP.
func NewPoolHandler() *PoolHandler {
	return &PoolHandler{}
}

// JobTypes enumerates the job types supported by the pool handler.
func (h *PoolHandler) JobTypes() []models.JobType {
	return []models.JobType{
		models.JobTypeCreatePool,
		models.JobTypeCreateLargePool,
	}
}

// Handle removes pool artifacts from VCP for the job.
func (h *PoolHandler) Handle(ctx context.Context, job *datamodel.Job, event Event, storage database.Storage) error {
	if event != EventTimeout {
		return nil
	}

	logger := util.GetLogger(ctx).With(log.Fields{
		"jobUUID":                               job.UUID,
		"jobType":                               job.Type,
		string(middleware.RequestCorrelationID): utils.GetCoRelationIDFromContext(ctx),
	})

	if job.JobAttributes == nil || job.JobAttributes.ResourceUUID == "" {
		logger.Warnf("workflow-supervisor-task: job lacks pool resource UUID; skipping cleanup")
		return nil
	}

	pool, err := storage.GetPoolByUUID(ctx, job.JobAttributes.ResourceUUID)
	if err != nil {
		if vsaerrors.IsNotFoundErr(err) {
			logger.Warnf("workflow-supervisor-task: pool already deleted in VCP")
			return nil
		}
		return fmt.Errorf("load pool: %w", err)
	}

	if err := storage.DeletePool(ctx, pool); err != nil {
		return fmt.Errorf("delete pool from VCP: %w", err)
	}

	logger.Infof("workflow-supervisor-task: pool %s removed from VCP", pool.UUID)
	return nil
}
