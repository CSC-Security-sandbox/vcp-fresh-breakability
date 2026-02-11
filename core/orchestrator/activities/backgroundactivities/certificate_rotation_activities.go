package backgroundactivities

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// ListPoolsBatchResult holds one page of pools and whether more pages exist (avoids Temporal activity result size limit).
type ListPoolsBatchResult struct {
	Pools   []*datamodel.Pool
	HasMore bool
}

// CertificateNeedsRotation checks if a certificate needs rotation for a specific pool
func (a *RotateVcpToVsaCertificateActivity) CertificateNeedsRotation(ctx context.Context, poolUUID string) (bool, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Checking if certificate needs rotation for pool: %s", poolUUID)

	readyStates := []string{"READY", "DEGRADED"}
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("uuid", "=", poolUUID),
		dbutils.NewFilterCondition("state", "in", readyStates),
	)
	poolViews, err := a.SE.ListPools(ctx, filter)
	if err != nil {
		logger.Errorf("Failed to get pool %s: %v", poolUUID, err)
		return false, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	if len(poolViews) == 0 {
		logger.Warnf("Pool %s not found", poolUUID)
		return false, nil
	}

	pool := ConvertPoolViewToPool(poolViews[0])

	if pool.PoolCredentials == nil || pool.PoolCredentials.CertificateID == "" {
		logger.Warnf("Pool %s has no certificate ID", poolUUID)
		return false, nil
	}

	needsRotation, err := a.certificateNeedsRotation(ctx, pool, pool.PoolCredentials.CertificateID)
	if err != nil {
		logger.Errorf("Failed to check if certificate needs rotation for pool %s: %v", poolUUID, err)
		return false, vsaerrors.NewVCPError(vsaerrors.ErrCertificateNeedsRotationCheckFailed, err)
	}

	return needsRotation, nil
}


// ListPoolsWithCertificateAuth returns one batch of pools that use certificate authentication (auth type USER_CERTIFICATE).
// Offset and limit support pagination to avoid Temporal activity result size limit.
func (a *RotateVcpToVsaCertificateActivity) ListPoolsWithCertificateAuth(ctx context.Context, offset, limit int) (*ListPoolsBatchResult, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	logger.Debugf("Listing pools with certificate authentication (offset=%d, limit=%d)", offset, limit)

	authTypeValue := fmt.Sprintf("%d", env.USER_CERTIFICATE)
	readyStates := []string{"READY", "DEGRADED"}
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("pool_credentials->>'auth_type'", "=", authTypeValue),
		dbutils.NewFilterCondition("state", "in", readyStates),
	)
	pagination := &dbutils.Pagination{Limit: limit, Offset: offset}

	poolViews, err := se.ListPoolsWithFilterAndPaginationOrderedByUUID(ctx, filter, pagination)
	if err != nil {
		logger.Errorf("Failed to list pools with certificate authentication: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	hasMore := len(poolViews) == limit
	var pools []*datamodel.Pool
	for _, poolView := range poolViews {
		pools = append(pools, ConvertPoolViewToPool(poolView))
	}
	logger.Debugf("Retrieved batch of %d certificate auth pools (hasMore=%v)", len(pools), hasMore)
	return &ListPoolsBatchResult{Pools: pools, HasMore: hasMore}, nil
}
