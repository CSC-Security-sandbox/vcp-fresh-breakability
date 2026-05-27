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
	AddressRangeStateCreated  = "CREATED"
	AddressRangeStateInUse    = "IN_USE"
	AddressRangeStateDisabled = "DISABLED"
	AddressRangeStateDeleted  = "DELETED"

	AddressRangeLifTypeDataLIF         = "dataLIF"
	AddressRangeLifTypeInterclusterLIF = "interclusterLIF"
)

// CreateAddressRange inserts a new address range record.
func (d *DataStoreRepository) CreateAddressRange(ctx context.Context, ar *datamodel.AddressRange) (*datamodel.AddressRange, error) {
	// Duplicate CIDR check
	var count int64
	err := d.db.GORM().WithContext(ctx).
		Model(&datamodel.AddressRange{}).
		Where("vpc_name = ? AND host_project_number = ? AND address_range_cidr = ? AND deleted_at IS NULL",
			ar.VpcName, ar.HostProjectNumber, ar.AddressRangeCidr).
		Count(&count).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	if count > 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("address range with CIDR %s already exists for vpc %s", ar.AddressRangeCidr, ar.VpcName))
	}

	// Duplicate name check
	err = d.db.GORM().WithContext(ctx).
		Model(&datamodel.AddressRange{}).
		Where("vpc_name = ? AND host_project_number = ? AND name = ? AND deleted_at IS NULL",
			ar.VpcName, ar.HostProjectNumber, ar.Name).
		Count(&count).Error
	if err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	if count > 0 {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("address range with name %s already exists for vpc %s", ar.Name, ar.VpcName))
	}

	// Only one interclusterLIF per VPC+host project
	if ar.LifType == AddressRangeLifTypeInterclusterLIF {
		err = d.db.GORM().WithContext(ctx).
			Model(&datamodel.AddressRange{}).
			Where("vpc_name = ? AND host_project_number = ? AND lif_type = ? AND deleted_at IS NULL",
				ar.VpcName, ar.HostProjectNumber, AddressRangeLifTypeInterclusterLIF).
			Count(&count).Error
		if err != nil {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
		}
		if count > 0 {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
				fmt.Errorf("only one interclusterLIF address range is allowed per VPC and host project"))
		}
	}

	ar.UUID = utils.RandomUUID()
	now := time.Now().UTC()
	ar.CreatedAt = now
	ar.UpdatedAt = now
	ar.AddressRangeState = AddressRangeStateCreated
	ar.AddressRangeStateDetails = AddressRangeStateCreated

	if err := d.db.GORM().WithContext(ctx).Create(ar).Error; err != nil {
		if errors.Is(err, gorm.ErrDuplicatedKey) || strings.Contains(strings.ToLower(err.Error()), "unique constraint") {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
				fmt.Errorf("address range already exists (concurrent duplicate detected)"))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataInsertError, err)
	}
	return ar, nil
}

// GetAddressRange retrieves a single address range by UUID.
func (d *DataStoreRepository) GetAddressRange(ctx context.Context, arID string) (*datamodel.AddressRange, error) {
	var ar datamodel.AddressRange
	err := d.db.GORM().WithContext(ctx).
		Where("uuid = ? AND deleted_at IS NULL", arID).
		First(&ar).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, customerrors.NewNotFoundErr("AddressRange", &arID))
		}
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return &ar, nil
}

// ListAddressRanges lists address ranges filtered by hostProjectNumber and/or vpcName.
// Optional arID and lifType narrow the result further.
func (d *DataStoreRepository) ListAddressRanges(ctx context.Context, hostProjectNumber, vpcName string, arID, lifType *string) ([]*datamodel.AddressRange, error) {
	db := d.db.GORM().WithContext(ctx).Model(&datamodel.AddressRange{}).Where("deleted_at IS NULL")
	if hostProjectNumber != "" {
		db = db.Where("host_project_number = ?", hostProjectNumber)
	}
	if vpcName != "" {
		db = db.Where("vpc_name = ?", vpcName)
	}
	if arID != nil && *arID != "" {
		db = db.Where("uuid = ?", *arID)
	}
	if lifType != nil && *lifType != "" {
		db = db.Where("lif_type = ?", *lifType)
	}

	var results []*datamodel.AddressRange
	if err := db.Find(&results).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, err)
	}
	return results, nil
}

// UpdateAddressRange updates mutable fields (applyRouteAggregation, addressRangeState→DISABLED).
// Name, AddressRangeCidr, and Network are immutable after creation.
func (d *DataStoreRepository) UpdateAddressRange(ctx context.Context, ar *datamodel.AddressRange) (*datamodel.AddressRange, error) {
	existing, err := d.GetAddressRange(ctx, ar.UUID)
	if err != nil {
		return nil, err
	}

	if existing.RouteAggregationApplied {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("route aggregation is already applied, cannot update address range"))
	}

	switch existing.AddressRangeState {
	case AddressRangeStateInUse:
		// Only applyRouteAggregation may change in IN_USE state
		existing.ApplyRouteAggregation = ar.ApplyRouteAggregation
	case AddressRangeStateCreated:
		existing.ApplyRouteAggregation = ar.ApplyRouteAggregation
		if ar.AddressRangeState == AddressRangeStateDisabled {
			existing.AddressRangeState = AddressRangeStateDisabled
			existing.AddressRangeStateDetails = AddressRangeStateDisabled
		}
	default:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("cannot update address range in %s state", existing.AddressRangeState))
	}

	existing.UpdatedAt = time.Now().UTC()
	if err := d.db.GORM().WithContext(ctx).Save(existing).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return existing, nil
}

// UpdateAddressRangeState transitions address range state between CREATED and IN_USE.
func (d *DataStoreRepository) UpdateAddressRangeState(ctx context.Context, arID, state string, routeAggregationApplied *bool) (*datamodel.AddressRange, error) {
	existing, err := d.GetAddressRange(ctx, arID)
	if err != nil {
		return nil, err
	}

	switch state {
	case AddressRangeStateInUse:
		existing.AddressRangeState = AddressRangeStateInUse
		existing.AddressRangeStateDetails = AddressRangeStateInUse
		if routeAggregationApplied != nil && *routeAggregationApplied {
			existing.RouteAggregationApplied = true
			now := time.Now().UTC()
			existing.RouteAggregationAppliedAt = &now
		}
	case AddressRangeStateCreated:
		existing.AddressRangeState = AddressRangeStateCreated
		existing.AddressRangeStateDetails = AddressRangeStateCreated
		existing.RouteAggregationApplied = false
		existing.RouteAggregationAppliedAt = nil
	default:
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError,
			fmt.Errorf("invalid address range state %s; must be CREATED or IN_USE", state))
	}

	existing.UpdatedAt = time.Now().UTC()
	if err := d.db.GORM().WithContext(ctx).Save(existing).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, err)
	}
	return existing, nil
}

// UpdateAddressRangeStateToCreatedIfLastPool atomically transitions a single IN_USE address range
// back to CREATED, but only when no other active pool on the same network has an allocated subnet
// CIDR that falls within the registered range. The check and update execute in a single
// conditional UPDATE with a NOT EXISTS subquery, eliminating the read-check-write race that
// would occur if concurrent pool deletions each read the pool list independently.
//
// Returns (true, nil) when the row was updated, (false, nil) when other pools still use the range
// (no-op), or (false, err) on failure.
func (d *DataStoreRepository) UpdateAddressRangeStateToCreatedIfLastPool(ctx context.Context, arUUID, network, excludePoolUUID, addressRangeCidr string) (bool, error) {
	now := time.Now().UTC()
	result := d.db.GORM().WithContext(ctx).Exec(`
		UPDATE address_ranges
		SET address_range_state = ?,
		    address_range_state_details = ?,
		    route_aggregation_applied = false,
		    route_aggregation_applied_at = NULL,
		    updated_at = ?
		WHERE uuid = ?
		  AND address_range_state = ?
		  AND deleted_at IS NULL
		  AND NOT EXISTS (
		    SELECT 1 FROM pools
		    WHERE network = ?
		      AND uuid != ?
		      AND deleted_at IS NULL
		      AND cluster_details->>'allocated_subnet_cidr' != ''
		      AND (cluster_details->>'allocated_subnet_cidr')::inet << ?::inet
		  )`,
		AddressRangeStateCreated, AddressRangeStateCreated, now,
		arUUID, AddressRangeStateInUse,
		network, excludePoolUUID, addressRangeCidr,
	)
	if result.Error != nil {
		return false, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}
	return result.RowsAffected > 0, nil
}

// ResetAddressRangesInUseToCreated bulk-transitions all IN_USE dataLIF address ranges
// for the given VPC to CREATED in a single UPDATE query.
func (d *DataStoreRepository) ResetAddressRangesInUseToCreated(ctx context.Context, hostProjectNumber, vpcName string) error {
	now := time.Now().UTC()
	result := d.db.GORM().WithContext(ctx).
		Model(&datamodel.AddressRange{}).
		Where("host_project_number = ? AND vpc_name = ? AND lif_type = ? AND address_range_state = ? AND deleted_at IS NULL",
			hostProjectNumber, vpcName, AddressRangeLifTypeDataLIF, AddressRangeStateInUse).
		Updates(map[string]interface{}{
			"address_range_state":          AddressRangeStateCreated,
			"address_range_state_details":  AddressRangeStateCreated,
			"route_aggregation_applied":    false,
			"route_aggregation_applied_at": nil,
			"updated_at":                   now,
		})
	if result.Error != nil {
		return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataUpdateError, result.Error)
	}
	return nil
}

// DeleteAddressRange soft-deletes an address range.
func (d *DataStoreRepository) DeleteAddressRange(ctx context.Context, arID string) (*datamodel.AddressRange, error) {
	existing, err := d.GetAddressRange(ctx, arID)
	if err != nil {
		return nil, err
	}

	if existing.RouteAggregationApplied {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("route aggregation is already applied, cannot delete address range"))
	}
	if existing.AddressRangeState == AddressRangeStateInUse {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError,
			fmt.Errorf("address range is IN_USE, cannot delete"))
	}

	now := time.Now().UTC()
	existing.AddressRangeState = AddressRangeStateDeleted
	existing.AddressRangeStateDetails = AddressRangeStateDeleted
	existing.UpdatedAt = now
	deletedAt := gorm.DeletedAt{Time: now, Valid: true}
	existing.DeletedAt = &deletedAt

	if err := d.db.GORM().WithContext(ctx).Save(existing).Error; err != nil {
		return nil, vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataDeleteError, err)
	}
	return existing, nil
}
