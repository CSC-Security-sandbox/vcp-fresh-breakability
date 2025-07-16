package collector

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
)

type mockStorage struct {
	mock.Mock
	database.Storage
}

func (m *mockStorage) ListPools(ctx context.Context, filter *utils.Filter) ([]*datamodel.PoolView, error) {
	args := m.Called(ctx, filter)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*datamodel.PoolView), args.Error(1)
}

func Test_GetPoolMetrics_ReturnsMetrics(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	var pools []*datamodel.PoolView
	pools = append(
		pools,
		&datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					UUID: "pool-uuid-1",
				},
				Name:        "Pool1",
				SizeInBytes: 1000,
				Account: &datamodel.Account{
					Name: "Account1",
				},
			},
		},
	)

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	metrics, err := GetPoolMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.Len(t, metrics, 1)
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func Test_GetPoolMetrics_MultiplePools(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}

	pools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-1"},
				Name:        "Pool1",
				SizeInBytes: 1000,
				Account:     &datamodel.Account{Name: "Account1"},
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-2"},
				Name:        "Pool2",
				SizeInBytes: 2000,
				Account:     &datamodel.Account{Name: "Account2"},
			},
		},
	}

	m.On("ListPools", mock.Anything, mock.Anything).Return(pools, nil)

	metrics, err := GetPoolMetrics(ctx, m, config)
	assert.NoError(t, err)
	assert.Len(t, metrics, 2)
	assert.Equal(t, float64(1000), metrics[0].Quantity)
	assert.Equal(t, "Pool1", derefString(metrics[0].Metadata.ResourceName))
	assert.Equal(t, "us-east-1", derefString(metrics[0].Metadata.RegionName))
	assert.Equal(t, "Account1", derefString(metrics[0].Metadata.AccountName))
	assert.Equal(t, float64(2000), metrics[1].Quantity)
	assert.Equal(t, "Pool2", derefString(metrics[1].Metadata.ResourceName))
	assert.Equal(t, "Account2", derefString(metrics[1].Metadata.AccountName))
}

func Test_GetPoolMetrics_EmptyPools(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	metrics, err := GetPoolMetrics(ctx, m, config)
	assert.Error(t, err)
	assert.Empty(t, metrics)
}

func Test_GetPoolMetrics_ListPoolsError(t *testing.T) {
	m := new(mockStorage)
	ctx := context.Background()
	config := &common.TelemetryConfig{RegionName: "us-east-1"}
	m.On("ListPools", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	metrics, err := GetPoolMetrics(ctx, m, config)
	assert.Error(t, err)
	assert.Empty(t, metrics)
}
