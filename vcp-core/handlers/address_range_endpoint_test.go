package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

// --- helpers ---

func newAddressRange() *datamodel.AddressRange {
	now := time.Now()
	return &datamodel.AddressRange{
		BaseModel: datamodel.BaseModel{
			UUID:      "ar-uuid-1",
			CreatedAt: now,
			UpdatedAt: now,
		},
		Name:              "my-range",
		AddressRangeCidr:  "10.0.0.0/16",
		Network:           "projects/123456/global/networks/my-vpc",
		VpcName:           "my-vpc",
		HostProjectNumber: "123456",
		LifType:           "dataLIF",
		AddressRangeState: "CREATED",
	}
}

func notFoundErr() error {
	return vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataNotFoundError, errors.New("not found"))
}

func conflictErr() error {
	return vsaerrors.NewVCPError(vsaerrors.ErrResourceStateConflictError, errors.New("conflict"))
}

// --- V1ListAddressRanges ---

func TestV1ListAddressRanges_Success_NoFilters(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	mockOrch.On("ListAddressRanges", mock.Anything, "", "", (*string)(nil), (*string)(nil)).
		Return([]*datamodel.AddressRange{ar}, nil)

	result, err := handler.V1ListAddressRanges(context.Background(), oasgenserver.V1ListAddressRangesParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1ListAddressRangesOKApplicationJSON)
	assert.True(t, ok)
	assert.Len(t, *res, 1)
	assert.Equal(t, "ar-uuid-1", (*res)[0].AddressRangeId.Value)
}

func TestV1ListAddressRanges_Success_WithFilters(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	arID := "ar-uuid-1"
	lifType := "dataLIF"
	ar := newAddressRange()

	mockOrch.On("ListAddressRanges", mock.Anything, "123456", "my-vpc", &arID, &lifType).
		Return([]*datamodel.AddressRange{ar}, nil)

	params := oasgenserver.V1ListAddressRangesParams{
		HostProjectNumber: oasgenserver.NewOptString("123456"),
		VpcName:           oasgenserver.NewOptString("my-vpc"),
		AddressRangeId:    oasgenserver.NewOptString(arID),
		LifType:           oasgenserver.NewOptLifTypeQueryParameter(oasgenserver.LifTypeQueryParameter(lifType)),
	}
	result, err := handler.V1ListAddressRanges(context.Background(), params)

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1ListAddressRangesOKApplicationJSON)
	assert.True(t, ok)
	assert.Len(t, *res, 1)
}

func TestV1ListAddressRanges_OrchestratorError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("ListAddressRanges", mock.Anything, "", "", (*string)(nil), (*string)(nil)).
		Return(nil, errors.New("db error"))

	result, err := handler.V1ListAddressRanges(context.Background(), oasgenserver.V1ListAddressRangesParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1ListAddressRangesInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- V1CreateAddressRange ---

func TestV1CreateAddressRange_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	mockOrch.On("CreateAddressRange", mock.Anything, mock.MatchedBy(func(a *datamodel.AddressRange) bool {
		return a.Name == "my-range" && a.AddressRangeCidr == "10.0.0.0/16" &&
			a.VpcName == "my-vpc" && a.HostProjectNumber == "123456"
	})).Return(ar, nil)

	req := &oasgenserver.AddressRangeCreateV1{
		AddressRange:     "my-range",
		AddressRangeCidr: "10.0.0.0/16",
		Network:          "projects/123456/global/networks/my-vpc",
		LifType:          oasgenserver.AddressRangeCreateV1LifType("dataLIF"),
	}
	result, err := handler.V1CreateAddressRange(context.Background(), req, oasgenserver.V1CreateAddressRangeParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.Equal(t, "ar-uuid-1", res.AddressRangeId.Value)
	assert.Equal(t, "my-range", res.AddressRange.Value)
	assert.Equal(t, "10.0.0.0/16", res.AddressRangeCidr.Value)
}

func TestV1CreateAddressRange_Conflict(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("CreateAddressRange", mock.Anything, mock.Anything).Return(nil, conflictErr())

	req := &oasgenserver.AddressRangeCreateV1{
		AddressRange:     "my-range",
		AddressRangeCidr: "10.0.0.0/16",
		Network:          "projects/123456/global/networks/my-vpc",
		LifType:          oasgenserver.AddressRangeCreateV1LifType("dataLIF"),
	}
	result, err := handler.V1CreateAddressRange(context.Background(), req, oasgenserver.V1CreateAddressRangeParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1CreateAddressRangeConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), res.Code)
}

func TestV1CreateAddressRange_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("CreateAddressRange", mock.Anything, mock.Anything).Return(nil, errors.New("unexpected"))

	req := &oasgenserver.AddressRangeCreateV1{
		AddressRange:     "my-range",
		AddressRangeCidr: "10.0.0.0/16",
		Network:          "projects/123456/global/networks/my-vpc",
		LifType:          oasgenserver.AddressRangeCreateV1LifType("dataLIF"),
	}
	result, err := handler.V1CreateAddressRange(context.Background(), req, oasgenserver.V1CreateAddressRangeParams{})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1CreateAddressRangeInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- V1GetAddressRange ---

func TestV1GetAddressRange_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	mockOrch.On("GetAddressRange", mock.Anything, "ar-uuid-1").Return(ar, nil)

	result, err := handler.V1GetAddressRange(context.Background(), oasgenserver.V1GetAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.Equal(t, "ar-uuid-1", res.AddressRangeId.Value)
	assert.Equal(t, "my-range", res.AddressRange.Value)
}

func TestV1GetAddressRange_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("GetAddressRange", mock.Anything, "ar-uuid-1").Return(nil, notFoundErr())

	result, err := handler.V1GetAddressRange(context.Background(), oasgenserver.V1GetAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1GetAddressRangeNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), res.Code)
}

func TestV1GetAddressRange_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("GetAddressRange", mock.Anything, "ar-uuid-1").Return(nil, errors.New("unexpected"))

	result, err := handler.V1GetAddressRange(context.Background(), oasgenserver.V1GetAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1GetAddressRangeInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- V1UpdateAddressRange ---

func TestV1UpdateAddressRange_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	ar.ApplyRouteAggregation = true
	mockOrch.On("UpdateAddressRange", mock.Anything, mock.MatchedBy(func(a *datamodel.AddressRange) bool {
		return a.UUID == "ar-uuid-1" && a.ApplyRouteAggregation == true && a.Name == ""
	})).Return(ar, nil)

	req := &oasgenserver.AddressRangeUpdateV1{
		// Name and AddressRangeCidr are intentionally omitted — they are immutable after creation.
		ApplyRouteAggregation: oasgenserver.NewOptBool(true),
	}
	result, err := handler.V1UpdateAddressRange(context.Background(), req, oasgenserver.V1UpdateAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.Equal(t, "ar-uuid-1", res.AddressRangeId.Value)
}

func TestV1UpdateAddressRange_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRange", mock.Anything, mock.Anything).Return(nil, notFoundErr())

	result, err := handler.V1UpdateAddressRange(context.Background(), &oasgenserver.AddressRangeUpdateV1{}, oasgenserver.V1UpdateAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), res.Code)
}

func TestV1UpdateAddressRange_Conflict(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRange", mock.Anything, mock.Anything).Return(nil, conflictErr())

	result, err := handler.V1UpdateAddressRange(context.Background(), &oasgenserver.AddressRangeUpdateV1{}, oasgenserver.V1UpdateAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), res.Code)
}

func TestV1UpdateAddressRange_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRange", mock.Anything, mock.Anything).Return(nil, errors.New("unexpected"))

	result, err := handler.V1UpdateAddressRange(context.Background(), &oasgenserver.AddressRangeUpdateV1{}, oasgenserver.V1UpdateAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- V1UpdateAddressRangeState ---

func TestV1UpdateAddressRangeState_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	ar.AddressRangeState = "IN_USE"
	mockOrch.On("UpdateAddressRangeState", mock.Anything, "ar-uuid-1", "IN_USE", (*bool)(nil)).
		Return(ar, nil)

	req := &oasgenserver.AddressRangeCVNUpdateV1{
		LifeCycleState: oasgenserver.AddressRangeCVNUpdateV1LifeCycleState("IN_USE"),
	}
	result, err := handler.V1UpdateAddressRangeState(context.Background(), req, oasgenserver.V1UpdateAddressRangeStateParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.Equal(t, "ar-uuid-1", res.AddressRangeId.Value)
}

func TestV1UpdateAddressRangeState_WithRouteAggregation(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	ar.AddressRangeState = "IN_USE"
	ar.RouteAggregationApplied = true

	trueVal := true
	mockOrch.On("UpdateAddressRangeState", mock.Anything, "ar-uuid-1", "IN_USE", &trueVal).
		Return(ar, nil)

	req := &oasgenserver.AddressRangeCVNUpdateV1{
		LifeCycleState:          oasgenserver.AddressRangeCVNUpdateV1LifeCycleState("IN_USE"),
		RouteAggregationApplied: oasgenserver.NewOptBool(true),
	}
	result, err := handler.V1UpdateAddressRangeState(context.Background(), req, oasgenserver.V1UpdateAddressRangeStateParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.True(t, res.RouteAggregationApplied.Value)
}

func TestV1UpdateAddressRangeState_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRangeState", mock.Anything, "ar-uuid-1", "IN_USE", (*bool)(nil)).
		Return(nil, notFoundErr())

	req := &oasgenserver.AddressRangeCVNUpdateV1{
		LifeCycleState: oasgenserver.AddressRangeCVNUpdateV1LifeCycleState("IN_USE"),
	}
	result, err := handler.V1UpdateAddressRangeState(context.Background(), req, oasgenserver.V1UpdateAddressRangeStateParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeStateNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), res.Code)
}

func TestV1UpdateAddressRangeState_Conflict(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRangeState", mock.Anything, "ar-uuid-1", "IN_USE", (*bool)(nil)).
		Return(nil, conflictErr())

	req := &oasgenserver.AddressRangeCVNUpdateV1{
		LifeCycleState: oasgenserver.AddressRangeCVNUpdateV1LifeCycleState("IN_USE"),
	}
	result, err := handler.V1UpdateAddressRangeState(context.Background(), req, oasgenserver.V1UpdateAddressRangeStateParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeStateConflict)
	assert.True(t, ok)
	assert.Equal(t, float64(409), res.Code)
}

func TestV1UpdateAddressRangeState_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("UpdateAddressRangeState", mock.Anything, "ar-uuid-1", "IN_USE", (*bool)(nil)).
		Return(nil, errors.New("unexpected"))

	req := &oasgenserver.AddressRangeCVNUpdateV1{
		LifeCycleState: oasgenserver.AddressRangeCVNUpdateV1LifeCycleState("IN_USE"),
	}
	result, err := handler.V1UpdateAddressRangeState(context.Background(), req, oasgenserver.V1UpdateAddressRangeStateParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1UpdateAddressRangeStateInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- V1DeleteAddressRange ---

func TestV1DeleteAddressRange_Success(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	ar := newAddressRange()
	mockOrch.On("DeleteAddressRange", mock.Anything, "ar-uuid-1").Return(ar, nil)

	result, err := handler.V1DeleteAddressRange(context.Background(), oasgenserver.V1DeleteAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.AddressRangeV1)
	assert.True(t, ok)
	assert.Equal(t, "ar-uuid-1", res.AddressRangeId.Value)
}

func TestV1DeleteAddressRange_NotFound(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteAddressRange", mock.Anything, "ar-uuid-1").Return(nil, notFoundErr())

	result, err := handler.V1DeleteAddressRange(context.Background(), oasgenserver.V1DeleteAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1DeleteAddressRangeNotFound)
	assert.True(t, ok)
	assert.Equal(t, float64(404), res.Code)
}

func TestV1DeleteAddressRange_UnprocessableEntity(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteAddressRange", mock.Anything, "ar-uuid-1").Return(nil, conflictErr())

	result, err := handler.V1DeleteAddressRange(context.Background(), oasgenserver.V1DeleteAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1DeleteAddressRangeUnprocessableEntity)
	assert.True(t, ok)
	assert.Equal(t, float64(422), res.Code)
}

func TestV1DeleteAddressRange_InternalServerError(t *testing.T) {
	mockOrch := factory.NewMockOrchestratorFactory(t)
	handler := NewHandler(mockOrch)

	mockOrch.On("DeleteAddressRange", mock.Anything, "ar-uuid-1").Return(nil, errors.New("unexpected"))

	result, err := handler.V1DeleteAddressRange(context.Background(), oasgenserver.V1DeleteAddressRangeParams{AddressRangeId: "ar-uuid-1"})

	assert.NoError(t, err)
	res, ok := result.(*oasgenserver.V1DeleteAddressRangeInternalServerError)
	assert.True(t, ok)
	assert.Equal(t, float64(500), res.Code)
}

// --- parseNetworkString ---

func TestParseNetworkString_Valid(t *testing.T) {
	vpcName, hostProjectNumber := parseNetworkString("projects/123456/global/networks/my-vpc")
	assert.Equal(t, "my-vpc", vpcName)
	assert.Equal(t, "123456", hostProjectNumber)
}

func TestParseNetworkString_Empty(t *testing.T) {
	vpcName, hostProjectNumber := parseNetworkString("")
	assert.Equal(t, "", vpcName)
	assert.Equal(t, "", hostProjectNumber)
}

func TestParseNetworkString_Malformed(t *testing.T) {
	vpcName, hostProjectNumber := parseNetworkString("not-a-valid-network-string")
	assert.Equal(t, "", vpcName)
	assert.Equal(t, "", hostProjectNumber)
}
