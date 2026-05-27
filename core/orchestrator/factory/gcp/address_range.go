package gcp

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
)

// CreateAddressRange creates a new address range in the database.
func (o *GCPOrchestrator) CreateAddressRange(ctx context.Context, ar *datamodel.AddressRange) (*datamodel.AddressRange, error) {
	return o.storage.CreateAddressRange(ctx, ar)
}

// GetAddressRange retrieves a single address range by UUID.
func (o *GCPOrchestrator) GetAddressRange(ctx context.Context, arID string) (*datamodel.AddressRange, error) {
	return o.storage.GetAddressRange(ctx, arID)
}

// ListAddressRanges lists address ranges with optional filters.
func (o *GCPOrchestrator) ListAddressRanges(ctx context.Context, hostProjectNumber, vpcName string, arID, lifType *string) ([]*datamodel.AddressRange, error) {
	return o.storage.ListAddressRanges(ctx, hostProjectNumber, vpcName, arID, lifType)
}

// UpdateAddressRange updates mutable fields of an address range.
func (o *GCPOrchestrator) UpdateAddressRange(ctx context.Context, ar *datamodel.AddressRange) (*datamodel.AddressRange, error) {
	return o.storage.UpdateAddressRange(ctx, ar)
}

// UpdateAddressRangeState transitions the lifecycle state of an address range.
func (o *GCPOrchestrator) UpdateAddressRangeState(ctx context.Context, arID, state string, routeAggregationApplied *bool) (*datamodel.AddressRange, error) {
	return o.storage.UpdateAddressRangeState(ctx, arID, state, routeAggregationApplied)
}

// DeleteAddressRange soft-deletes an address range.
func (o *GCPOrchestrator) DeleteAddressRange(ctx context.Context, arID string) (*datamodel.AddressRange, error) {
	return o.storage.DeleteAddressRange(ctx, arID)
}
