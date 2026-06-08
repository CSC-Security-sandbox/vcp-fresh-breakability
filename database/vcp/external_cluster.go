package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

const (
	ExternalClusterStateCreated = "CREATED"
	ExternalClusterStateError   = "ERROR"
	ExternalClusterStateDeleted = "DELETED"
)

// CreateExternalCluster inserts a new external cluster record.
func (d *DataStoreRepository) CreateExternalCluster(ctx context.Context, cluster *datamodel.Cluster) (*datamodel.Cluster, error) {
	var count int64
	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.Cluster{}).
		Where("location_id = ? AND host_name = ? AND deleted_at IS NULL", cluster.LocationID, cluster.HostName).
		Count(&count).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	if count > 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("external cluster %q already onboarded in location %q", cluster.HostName, cluster.LocationID))
	}

	cluster.UUID = utils.RandomUUID()
	now := time.Now().UTC()
	cluster.CreatedAt = now
	cluster.UpdatedAt = now
	if cluster.LifecycleState == "" {
		cluster.LifecycleState = ExternalClusterStateCreated
	}
	if cluster.LifecycleStateDetails == "" {
		cluster.LifecycleStateDetails = "Registered"
	}

	if err := d.db.GORM().WithContext(ctx).Create(cluster).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
				fmt.Errorf("external cluster %q already onboarded in location %q", cluster.HostName, cluster.LocationID))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return cluster, nil
}

// GetExternalCluster retrieves a single external cluster by UUID.
func (d *DataStoreRepository) GetExternalCluster(ctx context.Context, externalClusterID string) (*datamodel.Cluster, error) {
	var cluster datamodel.Cluster
	err := d.db.GORM().WithContext(ctx).
		Where("uuid = ? AND deleted_at IS NULL", externalClusterID).
		First(&cluster).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError,
				customerrors.NewNotFoundErr("ExternalCluster", &externalClusterID))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &cluster, nil
}

// UpdateExternalCluster persists changes to an existing external cluster.
func (d *DataStoreRepository) UpdateExternalCluster(ctx context.Context, cluster *datamodel.Cluster) (*datamodel.Cluster, error) {
	if cluster == nil || cluster.UUID == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError,
			fmt.Errorf("external cluster uuid is required"))
	}

	cluster.UpdatedAt = time.Now().UTC()
	if err := d.db.GORM().WithContext(ctx).Save(cluster).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return cluster, nil
}

// DeleteExternalCluster soft-deletes an external cluster by UUID.
// Deleting an already-deleted cluster is idempotent and returns the existing row.
func (d *DataStoreRepository) DeleteExternalCluster(ctx context.Context, externalClusterID string) (*datamodel.Cluster, error) {
	existing, err := d.GetExternalCluster(ctx, externalClusterID)
	if err != nil {
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) && customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
			var alreadyDeleted datamodel.Cluster
			findErr := d.db.GORM().WithContext(ctx).Unscoped().
				Where("uuid = ?", externalClusterID).
				First(&alreadyDeleted).Error
			if findErr == nil && alreadyDeleted.DeletedAt != nil && alreadyDeleted.DeletedAt.Valid {
				return &alreadyDeleted, nil
			}
		}
		return nil, err
	}

	now := time.Now().UTC()
	existing.LifecycleState = ExternalClusterStateDeleted
	existing.LifecycleStateDetails = ExternalClusterStateDeleted
	existing.UpdatedAt = now
	deletedAt := gorm.DeletedAt{Time: now, Valid: true}
	existing.DeletedAt = &deletedAt

	if err := d.db.GORM().WithContext(ctx).Save(existing).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	return existing, nil
}
