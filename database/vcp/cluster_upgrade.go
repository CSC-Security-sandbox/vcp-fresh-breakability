package database

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
)

// CreateClusterUpgradeJob creates a new cluster upgrade job in the database
func (d *DataStoreRepository) CreateClusterUpgradeJob(ctx context.Context, upgradeJob *datamodel.ClusterUpgradeJob) (*datamodel.ClusterUpgradeJob, error) {
	err := d.db.GORM().WithContext(ctx).Create(upgradeJob).Error
	if err != nil {
		return nil, err
	}
	return upgradeJob, nil
}

// GetClusterUpgradeJobByUUID retrieves a cluster upgrade job by its UUID(including soft-deleted)
func (d *DataStoreRepository) GetClusterUpgradeJobByUUID(ctx context.Context, jobUUID string) (*datamodel.ClusterUpgradeJob, error) {
	var upgradeJob datamodel.ClusterUpgradeJob
	err := d.db.GORM().WithContext(ctx).Unscoped().Where("uuid = ?", jobUUID).First(&upgradeJob).Error
	if err != nil {
		return nil, err
	}
	return &upgradeJob, nil
}

// GetClusterUpgradeJobsByClusterID retrieves all cluster upgrade jobs for a given cluster ID
func (d *DataStoreRepository) GetClusterUpgradeJobsByClusterID(ctx context.Context, clusterID string) ([]*datamodel.ClusterUpgradeJob, error) {
	var upgradeJobs []*datamodel.ClusterUpgradeJob
	err := d.db.GORM().WithContext(ctx).Where("cluster_id = ?", clusterID).Find(&upgradeJobs).Error
	if err != nil {
		return nil, err
	}
	return upgradeJobs, nil
}

// UpdateClusterUpgradeJob updates an existing cluster upgrade job
func (d *DataStoreRepository) UpdateClusterUpgradeJob(ctx context.Context, upgradeJob *datamodel.ClusterUpgradeJob) error {
	err := d.db.GORM().WithContext(ctx).Save(upgradeJob).Error
	if err != nil {
		return err
	}
	return nil
}
