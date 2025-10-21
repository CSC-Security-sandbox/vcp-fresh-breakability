package database

import (
	"context"
	"errors"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"gorm.io/gorm"
)

// CreateImageVersion creates a new image version record
func (r *DataStoreRepository) CreateImageVersion(ctx context.Context, imageVersion *datamodel.ImageVersion) (*datamodel.ImageVersion, error) {
	if err := r.db.GORM().WithContext(ctx).Create(imageVersion).Error; err != nil {
		return nil, err
	}
	return imageVersion, nil
}

// GetImageVersionByOntapVersion retrieves an image version by ONTAP version
func (r *DataStoreRepository) GetImageVersionByOntapVersion(ctx context.Context, ontapVersion string) (*datamodel.ImageVersion, error) {
	var imageVersion datamodel.ImageVersion
	err := r.db.GORM().WithContext(ctx).Where("ontap_version = ?", ontapVersion).First(&imageVersion).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, gorm.ErrRecordNotFound
		}
		return nil, err
	}
	return &imageVersion, nil
}

// ListImageVersions retrieves all image versions, optionally filtering by active status
func (r *DataStoreRepository) ListImageVersions(ctx context.Context, activeOnly bool) ([]*datamodel.ImageVersion, error) {
	var imageVersions []*datamodel.ImageVersion
	db := r.db.GORM().WithContext(ctx)
	query := db.Order("ontap_version DESC")

	if activeOnly {
		query = query.Where("is_active = ?", true)
	}

	err := query.Find(&imageVersions).Error
	if err != nil {
		return nil, err
	}

	return imageVersions, nil
}

// UpdateImageVersion updates an existing image version
func (r *DataStoreRepository) UpdateImageVersion(ctx context.Context, imageVersion *datamodel.ImageVersion) error {
	return r.db.GORM().WithContext(ctx).Save(imageVersion).Error
}

// DeleteImageVersion soft deletes an image version by ONTAP version
func (r *DataStoreRepository) DeleteImageVersion(ctx context.Context, ontapVersion string) error {
	return r.db.GORM().WithContext(ctx).Where("ontap_version = ?", ontapVersion).Delete(&datamodel.ImageVersion{}).Error
}
