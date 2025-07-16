package database

import (
	"context"
	"errors"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"gorm.io/gorm"
)

// CreateNodeGroup creates a new NodeGroup
func (d *DataStoreRepository) CreateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
	if group == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, errors.New("node_group is nil"))
	}
	if group.Name == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, errors.New("node_group name is empty"))
	}
	tx := d.db.GORM().WithContext(ctx)
	group.CreatedAt = time.Now()
	group.UpdatedAt = group.CreatedAt
	err := tx.Create(group).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return group, nil
}

// GetNodeGroup retrieves a NodeGroup by ID
func (d *DataStoreRepository) GetNodeGroup(ctx context.Context, id int64) (*datamodel.NodeGroup, error) {
	var group datamodel.NodeGroup
	err := d.db.GORM().WithContext(ctx).First(&group, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, customerrors.NewNotFoundErr("node_group", nil))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &group, nil
}

// UpdateNodeGroup updates an existing NodeGroup
func (d *DataStoreRepository) UpdateNodeGroup(ctx context.Context, group *datamodel.NodeGroup) (*datamodel.NodeGroup, error) {
	if group == nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, errors.New("node_group is nil"))
	}
	if group.Name == "" {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, errors.New("node_group name is empty"))
	}
	group.UpdatedAt = time.Now()
	result := d.db.GORM().WithContext(ctx).Model(&datamodel.NodeGroup{}).Where("id = ?", group.ID).Updates(group)
	if result.Error != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}
	if result.RowsAffected == 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, customerrors.NewNotFoundErr("node_group", nil))
	}
	return group, nil
}

// DeleteNodeGroup deletes a NodeGroup by ID
func (d *DataStoreRepository) DeleteNodeGroup(ctx context.Context, id int64) error {
	tx := d.db.GORM().WithContext(ctx)
	var group datamodel.NodeGroup
	err := tx.First(&group, id).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, err)
		}
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	group.DeletedAt = &gorm.DeletedAt{Time: time.Now(), Valid: true}
	err = tx.Save(&group).Error
	if err != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	return nil
}
