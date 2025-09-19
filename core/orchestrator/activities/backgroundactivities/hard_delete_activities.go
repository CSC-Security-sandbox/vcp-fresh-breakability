package backgroundactivities

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

type HardDeleteResourcesAndAccountActivity struct {
	SE database.Storage
}

func (j *HardDeleteResourcesAndAccountActivity) AccountAudit(ctx context.Context) ([]*datamodel.Account, error) {
	logger := util.GetLogger(ctx)
	se := j.SE

	resultList, err := se.GetDeletedAccounts(ctx)
	if err != nil {
		logger.Errorf("Failed to list accounts: %v", err)
		return nil, err
	}
	return resultList, nil
}
