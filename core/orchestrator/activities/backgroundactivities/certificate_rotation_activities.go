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

// CertificateNeedsRotation checks if a certificate needs rotation for a specific pool
func (a *RotateVcpToVsaCertificateActivity) CertificateNeedsRotation(ctx context.Context, poolUUID string) (bool, error) {
	logger := util.GetLogger(ctx)
	logger.Debugf("Checking if certificate needs rotation for pool: %s", poolUUID)

	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("uuid", "=", poolUUID),
		dbutils.NewFilterCondition("state", "!=", "CREATING"),
		dbutils.NewFilterCondition("state", "!=", "DELETING"),
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


// ListPoolsWithCertificateAuth retrieves all pools that use certificate authentication (auth type USER_CERTIFICATE)
func (a *RotateVcpToVsaCertificateActivity) ListPoolsWithCertificateAuth(ctx context.Context) ([]*datamodel.Pool, error) {
	logger := util.GetLogger(ctx)
	se := a.SE
	logger.Debug("Starting to list pools with certificate authentication")

	authTypeValue := fmt.Sprintf("%d", env.USER_CERTIFICATE)
	logger.Debugf("Looking for pools with auth_type = %s (USER_CERTIFICATE = %d)", authTypeValue, env.USER_CERTIFICATE)

	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("pool_credentials->>'auth_type'", "=", authTypeValue),
		dbutils.NewFilterCondition("state", "!=", "DELETED"),
		dbutils.NewFilterCondition("state", "!=", "CREATING"),
	)
	logger.Debugf("Created filter for certificate authentication pools: %+v", filter)

	allPoolsFilter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("state", "!=", "DELETED"),
	)
	allPoolViews, err := se.ListPools(ctx, allPoolsFilter)
	if err != nil {
		logger.Errorf("Failed to list all pools: %v", err)
	} else {
		logger.Debugf("Found %d total pools in database", len(allPoolViews))
		for i, poolView := range allPoolViews {
			if poolView.PoolCredentials != nil {
				logger.Debugf("Pool %d: UUID=%s, AuthType=%v, CertificateID=%s",
					i+1, poolView.UUID, poolView.PoolCredentials.AuthType, poolView.PoolCredentials.CertificateID)
			} else {
				logger.Debugf("Pool %d: UUID=%s, PoolCredentials is nil", i+1, poolView.UUID)
			}
		}
	}

	poolViews, err := se.ListPools(ctx, filter)
	if err != nil {
		logger.Errorf("Failed to list pools with certificate authentication: %v", err)
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	logger.Debugf("Retrieved %d pool views from database with certificate authentication", len(poolViews))

	var pools []*datamodel.Pool
	for i, poolView := range poolViews {
		pool := ConvertPoolViewToPool(poolView)
		pools = append(pools, pool)
		logger.Debugf("Converted pool %d: UUID=%s, AuthType=%d, CertificateID=%s, CreatedAt=%s",
			i+1, pool.UUID, pool.PoolCredentials.AuthType, pool.PoolCredentials.CertificateID, pool.CreatedAt)
	}
	logger.Debugf("Successfully listed %d pools with certificate authentication", len(pools))
	return pools, nil
}
