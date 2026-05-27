package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

// CreatePendingResourceDeletion creates a new pending resource deletion record
func (d *DataStoreRepository) CreatePendingResourceDeletion(ctx context.Context, resourceType, resourceName, errorMessage, accountName string, poolID int64) (*datamodel.PendingResourceDeletions, error) {
	if resourceType == "" || resourceName == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, errors.New("ResourceType and ResourceName cannot be empty"))
	}
	tx := d.db.GORM().WithContext(ctx)
	pendingResource := &datamodel.PendingResourceDeletions{}
	pendingResource.ResourceType = resourceType
	pendingResource.ResourceName = resourceName
	pendingResource.Error = errorMessage
	pendingResource.AccountName = accountName
	pendingResource.ResourceAttributes = &datamodel.ResourceAttributes{
		PoolID: poolID,
	}
	pendingResource.CreatedAt = time.Now()
	pendingResource.UpdatedAt = pendingResource.CreatedAt
	pendingResource.RetryCounter = 0
	err := tx.Create(pendingResource).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return pendingResource, nil
}

// UpdatePendingResourceDeletion updates a pending resource deletion based on the operation type (deletion or update)
// errorMessage is mandatory and will update the error field in the database
func (d *DataStoreRepository) UpdatePendingResourceDeletion(ctx context.Context, resourceID int64, isDeletion bool, errorMessage string) (*datamodel.PendingResourceDeletions, error) {
	var resource datamodel.PendingResourceDeletions
	tx := d.db.GORM().WithContext(ctx)

	// Fetch the resource by ID
	err := tx.First(&resource, resourceID).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, customerrors.NewNotFoundErr("resource", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}

	// Update fields based on the operation type
	resource.UpdatedAt = time.Now()
	resource.RetryCounter++
	resource.Error = errorMessage

	if isDeletion {
		resource.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	} else {
		resource.DeletedAt = nil
	}

	// Save the updated resource
	err = tx.Save(&resource).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}

	return &resource, nil
}

// ListPendingResourceDeletions retrieves all pending resource deletions with optional filtering
func (d *DataStoreRepository) ListPendingResourceDeletions(ctx context.Context, offset, limit int) ([]*datamodel.PendingResourceDeletions, error) {
	var db *gorm.DB

	db = d.db.GORM().WithContext(ctx)

	// Apply pagination if limit > 0
	if limit > 0 {
		db = db.Limit(limit)
	}
	if offset > 0 {
		db = db.Offset(offset)
	}

	var results []*datamodel.PendingResourceDeletions
	err := db.Model(&datamodel.PendingResourceDeletions{}).
		Select("id, resource_type, resource_name, retry_counter").
		Where("deleted_at IS NULL").
		Order("id ASC, created_at ASC").
		Find(&results).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return results, nil
}

// GetResourcesCount counts resources based on the provided filter
func (d *DataStoreRepository) GetResourcesCount(ctx context.Context) (int64, error) {
	var db *gorm.DB
	db = d.db.GORM().WithContext(ctx)

	var count int64
	err := db.Model(&datamodel.PendingResourceDeletions{}).Where("deleted_at IS NULL").Count(&count).Error
	if err != nil {
		return 0, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return count, nil
}
