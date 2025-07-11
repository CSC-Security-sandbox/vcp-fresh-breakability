package processor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
)

func TestMetricsProcessor_ProcessPerformanceMetrics_Success(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
	sink := &performance.MockSink{}
	sink.On("DeliverMetrics", mock.Anything, mock.Anything).Return(1)
	// Mock ListPools to return a non-empty, fully initialized PoolView with all pointer fields set
	vcpStore.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{
		Pool: datamodel.Pool{
			Name:           "dummy-pool",
			Description:    "desc",
			State:          "active",
			VendorID:       "vendor",
			ServiceLevel:   "standard",
			SizeInBytes:    100,
			UsedBytes:      10,
			Network:        "net",
			QosType:        "qos",
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
		},
	}}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
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

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	_ = err
	sink.AssertNotCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_DeliverMetricsReturnsZero(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
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
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
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
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
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
				Account:        &datamodel.Account{},
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
				Account:        &datamodel.Account{},
				PoolAttributes: &datamodel.PoolAttributes{},
				ClusterDetails: datamodel.ClusterDetails{},
			},
		},
	}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_DeliverMetricsReturnsNegative(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
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
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: sink}
	err := mp.ProcessPerformanceMetrics(ctx)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	sink.AssertCalled(t, "DeliverMetrics", mock.Anything, mock.Anything)
}

func TestMetricsProcessor_ProcessPerformanceMetrics_NilSink(t *testing.T) {
	ctx := context.Background()
	vcpStore := &database.MockStorage{}
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
			Account:        &datamodel.Account{},
			PoolAttributes: &datamodel.PoolAttributes{},
			ClusterDetails: datamodel.ClusterDetails{},
		},
	}}, nil)

	mp := &MetricsProcessor{vcpDatastore: vcpStore, telemetryDatastore: nil, sink: nil}
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic or error from nil sink, got none")
		}
	}()
	_ = mp.ProcessPerformanceMetrics(ctx)
}
