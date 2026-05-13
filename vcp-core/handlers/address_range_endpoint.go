package api

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

// V1ListAddressRanges lists address ranges with optional filters.
func (h Handler) V1ListAddressRanges(ctx context.Context, params oasgenserver.V1ListAddressRangesParams) (oasgenserver.V1ListAddressRangesRes, error) {
	var arID, lifType *string
	if params.AddressRangeId.IsSet() {
		v := params.AddressRangeId.Value
		arID = &v
	}
	if params.LifType.IsSet() {
		v := string(params.LifType.Value)
		lifType = &v
	}

	hostProjectNumber := ""
	if params.HostProjectNumber.IsSet() {
		hostProjectNumber = params.HostProjectNumber.Value
	}
	vpcName := ""
	if params.VpcName.IsSet() {
		vpcName = params.VpcName.Value
	}

	ars, err := h.Orchestrator.ListAddressRanges(ctx, hostProjectNumber, vpcName, arID, lifType)
	if err != nil {
		return &oasgenserver.V1ListAddressRangesInternalServerError{
			Message: fmt.Sprintf("Failed to list address ranges: %v", err),
			Code:    500,
		}, nil
	}

	result := make(oasgenserver.V1ListAddressRangesOKApplicationJSON, 0, len(ars))
	for _, ar := range ars {
		result = append(result, addressRangeToV1(ar))
	}
	return &result, nil
}

// V1CreateAddressRange creates a new address range.
func (h Handler) V1CreateAddressRange(ctx context.Context, req *oasgenserver.AddressRangeCreateV1, _ oasgenserver.V1CreateAddressRangeParams) (oasgenserver.V1CreateAddressRangeRes, error) {
	ar := &datamodel.AddressRange{
		Name:             req.AddressRange,
		AddressRangeCidr: req.AddressRangeCidr,
		Network:          req.Network,
		LifType:          string(req.LifType),
	}
	// Derive VpcName and HostProjectNumber from the network string "projects/{number}/global/networks/{vpc}"
	ar.VpcName, ar.HostProjectNumber = parseNetworkString(req.Network)

	created, err := h.Orchestrator.CreateAddressRange(ctx, ar)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrResourceStateConflictError) {
				return &oasgenserver.V1CreateAddressRangeConflict{
					Message: customErr.GetMessage(),
					Code:    409,
				}, nil
			}
		}
		return &oasgenserver.V1CreateAddressRangeInternalServerError{
			Message: fmt.Sprintf("Failed to create address range: %v", err),
			Code:    500,
		}, nil
	}

	result := addressRangeToV1(created)
	return &result, nil
}

// V1GetAddressRange retrieves a single address range by UUID.
func (h Handler) V1GetAddressRange(ctx context.Context, params oasgenserver.V1GetAddressRangeParams) (oasgenserver.V1GetAddressRangeRes, error) {
	ar, err := h.Orchestrator.GetAddressRange(ctx, params.AddressRangeId)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1GetAddressRangeNotFound{
					Message: "Address range not found",
					Code:    404,
				}, nil
			}
		}
		return &oasgenserver.V1GetAddressRangeInternalServerError{
			Message: fmt.Sprintf("Failed to get address range: %v", err),
			Code:    500,
		}, nil
	}

	result := addressRangeToV1(ar)
	return &result, nil
}

// V1UpdateAddressRange updates mutable fields of an address range.
// Name, AddressRangeCidr, and Network are immutable after creation — updates to these fields are silently ignored.
func (h Handler) V1UpdateAddressRange(ctx context.Context, req *oasgenserver.AddressRangeUpdateV1, params oasgenserver.V1UpdateAddressRangeParams) (oasgenserver.V1UpdateAddressRangeRes, error) {
	ar := &datamodel.AddressRange{}
	ar.UUID = params.AddressRangeId
	if req.ApplyRouteAggregation.IsSet() {
		ar.ApplyRouteAggregation = req.ApplyRouteAggregation.Value
	}
	if req.LifeCycleState.IsSet() {
		ar.AddressRangeState = string(req.LifeCycleState.Value)
	}

	updated, err := h.Orchestrator.UpdateAddressRange(ctx, ar)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1UpdateAddressRangeNotFound{
					Message: "Address range not found",
					Code:    404,
				}, nil
			}
			if customErr.IsError(vsaerrors.ErrResourceStateConflictError) {
				return &oasgenserver.V1UpdateAddressRangeConflict{
					Message: customErr.GetMessage(),
					Code:    409,
				}, nil
			}
		}
		return &oasgenserver.V1UpdateAddressRangeInternalServerError{
			Message: fmt.Sprintf("Failed to update address range: %v", err),
			Code:    500,
		}, nil
	}

	result := addressRangeToV1(updated)
	return &result, nil
}

// V1UpdateAddressRangeState transitions the lifecycle state of an address range.
func (h Handler) V1UpdateAddressRangeState(ctx context.Context, req *oasgenserver.AddressRangeCVNUpdateV1, params oasgenserver.V1UpdateAddressRangeStateParams) (oasgenserver.V1UpdateAddressRangeStateRes, error) {
	state := string(req.LifeCycleState)
	var routeAggApplied *bool
	if req.RouteAggregationApplied.IsSet() {
		v := req.RouteAggregationApplied.Value
		routeAggApplied = &v
	}

	updated, err := h.Orchestrator.UpdateAddressRangeState(ctx, params.AddressRangeId, state, routeAggApplied)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1UpdateAddressRangeStateNotFound{
					Message: "Address range not found",
					Code:    404,
				}, nil
			}
			if customErr.IsError(vsaerrors.ErrResourceStateConflictError) {
				return &oasgenserver.V1UpdateAddressRangeStateConflict{
					Message: customErr.GetMessage(),
					Code:    409,
				}, nil
			}
		}
		return &oasgenserver.V1UpdateAddressRangeStateInternalServerError{
			Message: fmt.Sprintf("Failed to update address range state: %v", err),
			Code:    500,
		}, nil
	}

	result := addressRangeToV1(updated)
	return &result, nil
}

// V1DeleteAddressRange soft-deletes an address range.
func (h Handler) V1DeleteAddressRange(ctx context.Context, params oasgenserver.V1DeleteAddressRangeParams) (oasgenserver.V1DeleteAddressRangeRes, error) {
	deleted, err := h.Orchestrator.DeleteAddressRange(ctx, params.AddressRangeId)
	if err != nil {
		if customErr := asVCPError(err); customErr != nil {
			if customErr.IsError(vsaerrors.ErrDatabaseDataNotFoundError) {
				return &oasgenserver.V1DeleteAddressRangeNotFound{
					Message: "Address range not found",
					Code:    404,
				}, nil
			}
			if customErr.IsError(vsaerrors.ErrResourceStateConflictError) {
				return &oasgenserver.V1DeleteAddressRangeUnprocessableEntity{
					Message: customErr.GetMessage(),
					Code:    422,
				}, nil
			}
		}
		return &oasgenserver.V1DeleteAddressRangeInternalServerError{
			Message: fmt.Sprintf("Failed to delete address range: %v", err),
			Code:    500,
		}, nil
	}

	result := addressRangeToV1(deleted)
	return &result, nil
}

// addressRangeToV1 converts a datamodel.AddressRange to the Ogen response type.
func addressRangeToV1(ar *datamodel.AddressRange) oasgenserver.AddressRangeV1 {
	v := oasgenserver.AddressRangeV1{
		AddressRangeId:          oasgenserver.NewOptString(ar.UUID),
		CreatedAt:               oasgenserver.NewOptDateTime(ar.CreatedAt),
		UpdatedAt:               oasgenserver.NewOptDateTime(ar.UpdatedAt),
		AddressRange:            oasgenserver.NewOptString(ar.Name),
		AddressRangeCidr:        oasgenserver.NewOptString(ar.AddressRangeCidr),
		Network:                 oasgenserver.NewOptString(ar.Network),
		VpcName:                 oasgenserver.NewOptString(ar.VpcName),
		HostProjectNumber:       oasgenserver.NewOptString(ar.HostProjectNumber),
		LifeCycleStateDetails:   oasgenserver.NewOptString(ar.AddressRangeStateDetails),
		ApplyRouteAggregation:   oasgenserver.NewOptBool(ar.ApplyRouteAggregation),
		RouteAggregationApplied: oasgenserver.NewOptBool(ar.RouteAggregationApplied),
	}

	if ar.LifType != "" {
		v.LifType = oasgenserver.NewOptAddressRangeV1LifType(oasgenserver.AddressRangeV1LifType(ar.LifType))
	}
	if ar.AddressRangeState != "" {
		v.LifeCycleState = oasgenserver.NewOptAddressRangeV1LifeCycleState(oasgenserver.AddressRangeV1LifeCycleState(ar.AddressRangeState))
	}

	if ar.DeletedAt != nil && ar.DeletedAt.Valid {
		v.DeletedAt = oasgenserver.NewOptNilDateTime(ar.DeletedAt.Time)
	}
	if ar.RouteAggregationAppliedAt != nil {
		v.RouteAggregationAppliedAt = oasgenserver.NewOptNilDateTime(*ar.RouteAggregationAppliedAt)
	}

	return v
}

// parseNetworkString extracts vpcName and hostProjectNumber from
// "projects/{number}/global/networks/{vpcName}" format.
func parseNetworkString(network string) (vpcName, hostProjectNumber string) {
	parts := splitN(network, "/", 5)
	if len(parts) == 5 {
		hostProjectNumber = parts[1]
		vpcName = parts[4]
	}
	return vpcName, hostProjectNumber
}

func splitN(s, sep string, n int) []string {
	var result []string
	start := 0
	count := 0
	for i := 0; i < len(s); i++ {
		if string(s[i]) == sep {
			result = append(result, s[start:i])
			start = i + 1
			count++
			if count == n-1 {
				break
			}
		}
	}
	result = append(result, s[start:])
	return result
}

// asVCPError attempts to cast err to *vsaerrors.CustomError.
func asVCPError(err error) *vsaerrors.CustomError {
	var customErr *vsaerrors.CustomError
	if vsaerrors.As(err, &customErr) {
		return customErr
	}
	return nil
}
