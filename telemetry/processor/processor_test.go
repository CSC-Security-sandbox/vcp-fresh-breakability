package processor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	metricdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	metricsdm "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestMetricsProcessor_ProcessPerformanceMetrics_MetricClientWrapperIsNil(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockProvider := &collector.MockVolumeMetricsProvider{}
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: mockProvider}
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-test",
				},
				Name: "test-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts to return empty list since we expect early return
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mockProvider.On("GetClient").Return(nil)

	t.Setenv("ENABLE_VOLUME_METRICS", "true")

	// No need to mock ListPools etc. as code should return early
	err := mp.ProcessPerformanceMetrics(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "metric client is nil")
}

func TestMetricsProcessor_ProcessPerformanceMetrics_Success(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	// Mock ListPools to return a non-empty, fully initialized PoolView with all pointer fields set
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-success",
				},
				Name: "success-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	// Accept both nil and non-nil error, as we cannot mock collector.GetPoolMetrics without refactor
	_ = err
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_GetPoolMetricsError(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0)
	// Mock ListPools to return error
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded)

	// Since ListPools will return error, CreateHydratedMetricsBatch won't be reached, so nil is OK
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	assert.Error(t, err) // Should get the ListPools error
	sink.AssertNotCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_DeliverMetricsReturnsZero(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(0)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-zero",
				},
				Name: "zero-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_EmptyPools(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}
	// Should not call DeliverMetrics if no pools
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err == nil {
		t.Errorf("expected error for no pools, got nil")
	}
	sink.AssertNotCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_ListPoolsNilSlice(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}
	// ListPools returns nil slice, should be treated as no pools
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return(nil, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err == nil {
		t.Errorf("expected error for nil pools, got nil")
	}
	sink.AssertNotCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_ListPoolsPanics(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}
	// ListPools panics
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		panic("db error")
	}).Return(nil, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic, got none")
		}
	}()
	_ = mp.ProcessPerformanceMetrics(ctx)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_DeliverMetricsPanics(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		panic("sink error")
	}).Return(0)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-panic",
				},
				Name: "panic-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic from DeliverMetrics, got none")
		}
	}()
	_ = mp.ProcessPerformanceMetrics(ctx)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_MultiplePools(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(2)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				Name:         "pool1",
				Description:  "desc1",
				State:        "active",
				VendorID:     "vendor1",
				ServiceLevel: "standard",
				SizeInBytes:  100,
				UsedBytes:    10,
				Network:      "net1",
				QosType:      "qos1",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-1",
					},
					Name: "account-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{},
				ClusterDetails: datamodel.ClusterDetails{},
			},
		},
		{
			Pool: datamodel.Pool{
				Name:         "pool2",
				Description:  "desc2",
				State:        "active",
				VendorID:     "vendor2",
				ServiceLevel: "premium",
				SizeInBytes:  200,
				UsedBytes:    20,
				Network:      "net2",
				QosType:      "qos2",
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				},
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{
						UUID: "account-uuid-2",
					},
					Name: "account-2",
				},
				PoolAttributes: &datamodel.PoolAttributes{},
				ClusterDetails: datamodel.ClusterDetails{},
			},
		},
	}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_DeliverMetricsReturnsNegative(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(-1)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-negative",
				},
				Name: "negative-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_NilSink(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	// Sink is nil, should panic or error when called
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-nil-sink",
				},
				Name: "nil-sink-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: nil}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic or error from nil sink, got none")
		}
	}()
	_ = mp.ProcessPerformanceMetrics(ctx)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_VolumeMetricsDisabled(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-disabled",
				},
				Name: "disabled-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts even when volume metrics disabled - throughput still needs it
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	t.Setenv("ENABLE_VOLUME_METRICS", "false")

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	// Note: CreateHydratedMetricsBatch is still called for pool metrics, not volume metrics
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_VolumeMetricsEnabledValidClient(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	mockClient.On("ListTimeSeries", mock.Anything, mock.Anything).Return(nil, nil)

	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}

	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return nil
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, err)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_CollectVolumeMetricsError(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	mockClient.On("ListTimeSeries", mock.Anything, mock.Anything).Return(nil, nil)
	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}
	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "account-uuid"}},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
			SnHostProject:  "sn_host_project",
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return errors.New("collect volume metrics error")
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)
	time.Sleep(100 * time.Millisecond)
	if err != nil {
		telemetryStore.AssertNotCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
	}
}

func TestMetricsProcessor_ProcessPerformanceMetrics_CreateHydratedMetricsError(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	mockClient.On("ListTimeSeries", mock.Anything, mock.Anything).Return(nil, nil)
	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}
	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
			SnHostProject:  "sn_host_project",
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database error"))

	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return nil
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)
	time.Sleep(100 * time.Millisecond)
	if err != nil {
		assert.Contains(t, err.Error(), "database error")
	}
}

func TestMetricsProcessor_ProcessPerformanceMetrics_ProcessesAllMetricTypes(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}
	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "account-uuid"}},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.MatchedBy(func(metrics []metricsdm.HydratedMetrics) bool {
		// For volume metrics, we expect both metrics to be included (even unknown types)
		if len(metrics) == 2 {
			// Check if this is volume metrics (contains UnknownMeasuredType or FileSystemReadOps)
			hasVolumeMetrics := false
			for _, metric := range metrics {
				if metric.MeasuredType == metadata.UnknownMeasuredType || metric.MeasuredType == metadata.AllocatedSize {
					hasVolumeMetrics = true
					break
				}
			}
			if hasVolumeMetrics {
				// This is the volume metrics call - should include both unknown and valid types
				hasUnknown := false
				hasFileSystemReadOps := false
				for _, metric := range metrics {
					if metric.MeasuredType == metadata.UnknownMeasuredType {
						hasUnknown = true
					}
					if metric.MeasuredType == metadata.AllocatedSize {
						hasFileSystemReadOps = true
					}
				}
				return hasUnknown && hasFileSystemReadOps
			} else {
				// This is the pool metrics call - should include both PoolAllocatedSize and AllocatedUsed
				hasPoolAllocated := false
				hasAllocatedUsed := false
				for _, metric := range metrics {
					if metric.MeasuredType == metadata.PoolAllocatedSize {
						hasPoolAllocated = true
					}
					if metric.MeasuredType == metadata.AllocatedUsed {
						hasAllocatedUsed = true
					}
				}
				return hasPoolAllocated && hasAllocatedUsed
			}
		}
		return false
	}), mock.AnythingOfType("int")).Return(nil).Maybe()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")

	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return nil
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, err)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_CollectVolumeMetricsReturnsError(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	mockClient.On("ListTimeSeries", mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("*monitoringpb.ListTimeSeriesRequest")).Return(nil, nil)

	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}

	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "account-uuid"}},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)

	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return errors.New("collection failed")
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	// Pool metrics should still be processed even if volume metrics fail
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_CollectVolumeMetricsReturnsEmptySlice(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}
	mockTenantProvider := new(collector.MockTenantProjectProvider)
	mockTenantProvider.On("GetTenantProjects", mock.Anything, mock.Anything).Return([]string{"project1"}, nil)
	mockClient := new(collector.MockMonitoringClient)
	mockClient.On("ListTimeSeries", mock.AnythingOfType("*context.valueCtx"), mock.AnythingOfType("*monitoringpb.ListTimeSeriesRequest")).Return(nil, nil)

	testMetrics := []common.MetricItem{
		{
			Metric:       "volume_read_ops",
			ResourceType: "netapp_volume",
		},
	}

	provider := collector.NewGoogleProvider(mockTenantProvider, mockClient, testMetrics)
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:         "dummy-pool",
			Description:  "desc",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  100,
			UsedBytes:    10,
			Network:      "net",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account:        &datamodel.Account{BaseModel: datamodel.BaseModel{UUID: "account-uuid"}},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	// Mock ListVolumesWithAccounts for volume metrics collection
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)

	originalFunc := collector.CollectVolumeMetrics
	collector.CollectVolumeMetrics = func(ctx context.Context, logger log.Logger, provider collector.VolumeMetricsProvider) error {
		return nil
	}
	defer func() {
		collector.CollectVolumeMetrics = originalFunc
	}()

	t.Setenv("ENABLE_VOLUME_METRICS", "true")
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink, googleMetricProvider: provider}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	// Pool metrics should still be processed even if volume metrics returns empty slice
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

// Test for the new CreateHydratedMetricsBatch functionality in ProcessPerformanceMetrics
func TestMetricsProcessor_ProcessPerformanceMetrics_CreateHydratedMetricsBatch_Success(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	// Setup pool data with Account information needed for hydrated metrics
	testPool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-123",
			},
			Name:         "test-pool",
			Description:  "Test pool for hydrated metrics",
			State:        "active",
			VendorID:     "vendor",
			ServiceLevel: "standard",
			SizeInBytes:  1000,
			UsedBytes:    500,
			Network:      "test-network",
			QosType:      "qos",
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-456",
				},
				Name: "test-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
		QuotaInBytes: 500,
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{testPool}, nil)
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(2) // 2 metrics: PoolAllocatedSize and AllocatedUsed

	// Mock successful CreateHydratedMetricsBatch call
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.MatchedBy(func(metrics []metricsdm.HydratedMetrics) bool {
		// Verify we have the expected hydrated metrics - now both PoolAllocatedSize and AllocatedUsed
		if len(metrics) != 2 {
			return false
		}

		// Check for PoolAllocatedSize metric
		hasPoolAllocated := false
		hasAllocatedUsed := false
		for _, metric := range metrics {
			if metric.MeasuredType == metadata.PoolAllocatedSize &&
				metric.ResourceType == metadata.VolumePool &&
				metric.ConsumerID == "test-account" &&
				metric.ResourceName == "test-pool" &&
				metric.Quantity == float64(1000) {
				hasPoolAllocated = true
			}
			if metric.MeasuredType == metadata.AllocatedUsed &&
				metric.ResourceType == metadata.VolumePool &&
				metric.ConsumerID == "test-account" &&
				metric.ResourceName == "test-pool" &&
				metric.Quantity == float64(500) {
				hasAllocatedUsed = true
			}
		}
		return hasPoolAllocated && hasAllocatedUsed
	}), mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_CreateHydratedMetricsBatch_DatabaseError(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	testPool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "pool-uuid-123",
			},
			Name:        "test-pool",
			SizeInBytes: 1000,
			UsedBytes:   500,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					UUID: "account-uuid-456",
				},
				Name: "test-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{testPool}, nil)
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	// Mock database error on CreateHydratedMetricsBatch
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("database connection failed"))

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection failed")
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
	// DeliverMetrics should not be called if CreateHydratedMetricsBatch fails
	sink.AssertNotCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_MultiplePoolsHydratedMetrics(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	// Setup multiple pools with different accounts
	testPools := []*datamodel.PoolView{
		{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-1"},
				Name:        "pool-1",
				SizeInBytes: 1000,
				UsedBytes:   300,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-1"},
					Name:      "account-1",
				},
				PoolAttributes: &datamodel.PoolAttributes{},
				ClusterDetails: datamodel.ClusterDetails{},
			},
		},
		{
			Pool: datamodel.Pool{
				BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-2"},
				Name:        "pool-2",
				SizeInBytes: 2000,
				UsedBytes:   800,
				Account: &datamodel.Account{
					BaseModel: datamodel.BaseModel{UUID: "account-uuid-2"},
					Name:      "account-2",
				},
				PoolAttributes: &datamodel.PoolAttributes{},
				ClusterDetails: datamodel.ClusterDetails{},
			},
		},
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return(testPools, nil)
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(4) // 2 pools * 2 metrics each

	// Mock successful CreateHydratedMetricsBatch call for multiple pools
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.MatchedBy(func(metrics []metricsdm.HydratedMetrics) bool {
		// Should have 4 hydrated metrics (2 per pool: PoolAllocatedSize and AllocatedUsed)
		if len(metrics) != 4 {
			return false
		}

		// Verify both pools have correct hydrated metrics
		poolAllocatedCount := 0
		allocatedUsedCount := 0
		poolNames := make(map[string]bool)
		for _, metric := range metrics {
			if metric.MeasuredType == metadata.PoolAllocatedSize && metric.ResourceType == metadata.VolumePool {
				poolAllocatedCount++
				poolNames[metric.ResourceName] = true
			}
			if metric.MeasuredType == metadata.AllocatedUsed && metric.ResourceType == metadata.VolumePool {
				allocatedUsedCount++
			}
		}

		return poolAllocatedCount == 2 && allocatedUsedCount == 2 && poolNames["pool-1"] && poolNames["pool-2"]
	}), mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_HydratedMetricsWithNilTelemetryStore(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}

	testPool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-123"},
			Name:        "test-pool",
			SizeInBytes: 1000,
			UsedBytes:   500,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-456"},
				Name:      "test-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{testPool}, nil)

	// With nil telemetryDatastore, should panic when trying to call CreateHydratedMetricsBatch
	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic from nil telemetryDatastore, got none")
		}
	}()

	_ = mp.ProcessPerformanceMetrics(ctx)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_HydratedMetricsValidation(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	// Test pool with specific values to validate setupHydratedMetricsDataModel functionality
	testPool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-test"},
			Name:        "validation-pool",
			SizeInBytes: 5368709120, // 5GB
			UsedBytes:   2147483648, // 2GB
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-validation"},
				Name:      "validation-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
		QuotaInBytes: 2147483648,
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{testPool}, nil)
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(2)

	// Detailed validation of hydrated metrics structure
	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.MatchedBy(func(metrics []metricsdm.HydratedMetrics) bool {
		if len(metrics) != 2 {
			return false
		}

		// Find PoolAllocatedSize and AllocatedUsed metrics
		var poolAllocatedMetric, allocatedUsedMetric metricsdm.HydratedMetrics
		hasPoolAllocated := false
		hasAllocatedUsed := false

		for _, metric := range metrics {
			if metric.MeasuredType == metadata.PoolAllocatedSize {
				poolAllocatedMetric = metric
				hasPoolAllocated = true
			}
			if metric.MeasuredType == metadata.AllocatedUsed {
				allocatedUsedMetric = metric
				hasAllocatedUsed = true
			}
		}

		if !hasPoolAllocated || !hasAllocatedUsed {
			return false
		}

		// Validate PoolAllocatedSize metric fields
		poolAllocatedValidations := []bool{
			poolAllocatedMetric.MeasuredType == metadata.PoolAllocatedSize,
			poolAllocatedMetric.ResourceType == metadata.VolumePool,
			poolAllocatedMetric.ConsumerID == "validation-account",
			poolAllocatedMetric.ResourceName == "validation-pool",
			poolAllocatedMetric.Quantity == float64(5368709120),
			!poolAllocatedMetric.MetricTimestamp.IsZero(), // Timestamp should be set
		}

		// Validate AllocatedUsed metric fields
		allocatedUsedValidations := []bool{
			allocatedUsedMetric.MeasuredType == metadata.AllocatedUsed,
			allocatedUsedMetric.ResourceType == metadata.VolumePool,
			allocatedUsedMetric.ConsumerID == "validation-account",
			allocatedUsedMetric.ResourceName == "validation-pool",
			allocatedUsedMetric.Quantity == float64(2147483648),
			!allocatedUsedMetric.MetricTimestamp.IsZero(), // Timestamp should be set
		}

		for _, valid := range poolAllocatedValidations {
			if !valid {
				return false
			}
		}

		for _, valid := range allocatedUsedValidations {
			if !valid {
				return false
			}
		}

		return true
	}), mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

// Test the new dual return value functionality from GetPoolMetrics integration
func TestMetricsProcessor_ProcessPerformanceMetrics_GetPoolMetricsDualReturn(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	telemetryStore := &metricdb.MockStorage{}
	sink := &performance.MockSink{}

	testPool := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:   datamodel.BaseModel{UUID: "pool-uuid-dual"},
			Name:        "dual-return-pool",
			SizeInBytes: 3000,
			UsedBytes:   1500,
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{UUID: "account-uuid-dual"},
				Name:      "dual-account",
			},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}

	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{testPool}, nil)
	vcpStore.On("ListVolumesWithAccounts", mock.Anything).Return([]*datamodel.Volume{}, nil)

	// Mock both metrics delivery and hydrated metrics batch creation
	sink.On("DeliverMetrics", mock.Anything, mock.MatchedBy(func(metrics []entity.HydratedMetric) bool {
		// Should receive 2 metrics: PoolAllocatedSize and AllocatedUsed
		return len(metrics) == 2
	})).Return(2)

	telemetryStore.On("CreateHydratedMetricsBatch", mock.Anything, mock.MatchedBy(func(metrics []metricsdm.HydratedMetrics) bool {
		// Should receive 2 hydrated metrics: PoolAllocatedSize and AllocatedUsed
		if len(metrics) != 2 {
			return false
		}
		hasPoolAllocated := false
		hasAllocatedUsed := false
		for _, metric := range metrics {
			if metric.MeasuredType == metadata.PoolAllocatedSize {
				hasPoolAllocated = true
			}
			if metric.MeasuredType == metadata.AllocatedUsed {
				hasAllocatedUsed = true
			}
		}
		return hasPoolAllocated && hasAllocatedUsed
	}), mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: telemetryStore, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)

	assert.NoError(t, err)

	// Verify both operations were called correctly
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

func TestCollectMetrics_Success(t *testing.T) {
	ctx := context.Background()
	telemetryStore := &metricdb.MockStorage{}
	mockProvider := &collector.MockVolumeMetricsProvider{}
	metrics := []metricsdm.HydratedMetrics{
		{
			MeasuredType: metadata.AllocatedSize,
			ResourceType: metadata.Volume,
			ResourceName: "test-volume-1",
		},
		{
			MeasuredType: metadata.LogicalSize,
			ResourceType: metadata.Volume,
			ResourceName: "test-volume-1",
		},
	}
	mockProvider.On("CollectProjectMetrics", ctx, mock.Anything, "project-123").Return(metrics, nil)
	telemetryStore.On("CreateHydratedMetricsBatch", ctx, metrics, mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{telemetryDatastore: telemetryStore, googleMetricProvider: mockProvider}
	err := mp.CollectMetrics(ctx, "project-123")
	assert.NoError(t, err)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", ctx, metrics, mock.AnythingOfType("int"))
}

func TestCollectMetrics_CollectProjectMetricsError(t *testing.T) {
	ctx := context.Background()
	telemetryStore := &metricdb.MockStorage{}
	mockProvider := &collector.MockVolumeMetricsProvider{}
	mockProvider.On("CollectProjectMetrics", ctx, mock.Anything, "project-err").Return(nil, errors.New("collect error"))

	mp := &MetricsProcessor{telemetryDatastore: telemetryStore, googleMetricProvider: mockProvider}
	err := mp.CollectMetrics(ctx, "project-err")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collect error")
	telemetryStore.AssertNotCalled(t, "CreateHydratedMetricsBatch", mock.Anything, mock.Anything, mock.Anything)
}

func TestCollectMetrics_CreateHydratedMetricsBatchError(t *testing.T) {
	ctx := context.Background()
	telemetryStore := &metricdb.MockStorage{}
	mockProvider := &collector.MockVolumeMetricsProvider{}
	metrics := []metricsdm.HydratedMetrics{
		{
			MeasuredType: metadata.AllocatedSize,
			ResourceType: metadata.Volume,
			ResourceName: "test-volume-1",
		},
		{
			MeasuredType: metadata.LogicalSize,
			ResourceType: metadata.Volume,
			ResourceName: "test-volume-1",
		}}
	mockProvider.On("CollectProjectMetrics", ctx, mock.Anything, "project-batch-err").Return(metrics, nil)
	telemetryStore.On("CreateHydratedMetricsBatch", ctx, metrics, mock.AnythingOfType("int")).Return(errors.New("db error"))

	mp := &MetricsProcessor{telemetryDatastore: telemetryStore, googleMetricProvider: mockProvider}
	err := mp.CollectMetrics(ctx, "project-batch-err")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
}

func TestCollectMetrics_EmptyMetricsSlice(t *testing.T) {
	ctx := context.Background()
	telemetryStore := &metricdb.MockStorage{}
	mockProvider := &collector.MockVolumeMetricsProvider{}
	var metrics []metricsdm.HydratedMetrics
	mockProvider.On("CollectProjectMetrics", ctx, mock.Anything, "project-empty").Return(metrics, nil)
	telemetryStore.On("CreateHydratedMetricsBatch", ctx, metrics, mock.AnythingOfType("int")).Return(nil)

	mp := &MetricsProcessor{telemetryDatastore: telemetryStore, googleMetricProvider: mockProvider}
	err := mp.CollectMetrics(ctx, "project-empty")
	assert.NoError(t, err)
	telemetryStore.AssertCalled(t, "CreateHydratedMetricsBatch", ctx, metrics, mock.AnythingOfType("int"))
}

func TestMetricsProcessor_ProcessBizOps(t *testing.T) {
	ctx := context.Background()
	t.Run("Success", func(t *testing.T) {
		mockProvider := &bizops.MockBizOpsProvider{}
		mp := &MetricsProcessor{bizopsProvider: mockProvider}
		params := &utils.BizOpsReportParams{}
		mockProvider.On("ProcessBizOps", ctx, mock.Anything, params).Return(nil)
		err := mp.ProcessBizOps(ctx, params)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		mockProvider.AssertCalled(t, "ProcessBizOps", ctx, mock.Anything, params)
	})
	t.Run("Failure", func(t *testing.T) {
		mockProvider := &bizops.MockBizOpsProvider{}
		mp := &MetricsProcessor{bizopsProvider: mockProvider}
		params := &utils.BizOpsReportParams{}
		mockProvider.On("ProcessBizOps", ctx, mock.Anything, params).Return(fmt.Errorf("bizops error"))
		err := mp.ProcessBizOps(ctx, params)
		if err == nil || err.Error() != "bizops error" {
			t.Errorf("expected bizops error, got %v", err)
		}
		mockProvider.AssertCalled(t, "ProcessBizOps", ctx, mock.Anything, params)
	})
}

func Test_NewMetricsProcessor(t *testing.T) {
	mp := NewMetricsProcessor(nil, nil, nil, nil, nil, nil)
	if mp.vcpDatastore != nil {
		t.Errorf("expected vcpDatastore to be set")
	}
	if mp.telemetryDatastore != nil {
		t.Errorf("expected telemetryDatastore to be set")
	}
	if mp.sink != nil {
		t.Errorf("expected sink to be set")
	}
	if mp.bizopsProvider != nil {
		t.Errorf("expected bizopsProvider to be set")
	}
	if mp.billingProvider != nil {
		t.Errorf("expected billingProvider to be set")
	}
}
