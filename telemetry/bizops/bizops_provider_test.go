package bizops

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	telemetrydb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	vcpdb "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/bizops/sink"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_NewBizOpsProvider(t *testing.T) {
	mockBizOpsSink := &sink.MockBizOpsSink{}
	mockmetricDB := &telemetrydb.MockStorage{}
	mockVCPDB := &vcpdb.MockStorage{}

	bizOpsProvider := NewBizOpsProvider(mockmetricDB, mockVCPDB, mockBizOpsSink)
	if bizOpsProvider == nil {
		t.Error("Expected bizOpsProvider to be non-nil")
	}
}

func Test_ProcessBizOps(t *testing.T) {
	oldGoogleContinents := googleContinents
	oldRegion := region
	defer func() {
		googleContinents = oldGoogleContinents
		region = oldRegion
	}()
	googleContinents = "northamerica:northamerica,latinamerica:latinamerica,southamerica:latinamerica"
	region = "au-se1"

	testAccount := &vcpdb.AccountTelemetryData{
		ID:    1,
		Name:  "Test Account",
		State: accountEnabled,
	}
	testAccounts := []*vcpdb.AccountTelemetryData{testAccount}
	ctx := context.Background()
	aggrErr := errors.New("bizops aggr error")
	ingestErr := errors.New("ingest error")
	mockLogger := &log.MockLogger{}
	mockLogger.On("Info", mock.Anything).Return("Processing BizOps Report")

	t.Run("Success", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		mockMetricDB := &telemetrydb.MockStorage{}
		mockVCPDB := &vcpdb.MockStorage{}
		mockLogger.On("Debugf", mock.Anything, mock.Anything).Return("Processing BizOps Report")

		// mock DB calls - using optimized ListAccountsForTelemetry
		mockVCPDB.On("ListAccountsForTelemetry", ctx,
			&dbutils.Pagination{Limit: paginationLimit, Offset: 0}).Return(testAccounts, nil)
		mockVCPDB.On("ListAccountsForTelemetry", ctx,
			&dbutils.Pagination{Limit: paginationLimit, Offset: 1}).Return([]*vcpdb.AccountTelemetryData{}, nil)
		mockMetricDB.On("AggregateUsageForBizOps", ctx, mock.Anything).Return(nil)
		// mock Sink
		mockBizOpsSink.On("Ingest", ctx, mock.Anything).Return(nil)
		mockBizOpsSink.On("Type").Return("terminal")

		bizOpsProvider := NewBizOpsProvider(mockMetricDB, mockVCPDB, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "terminal",
		})
		assert.NoError(t, err)
		mockVCPDB.AssertExpectations(t)
		mockMetricDB.AssertExpectations(t)
		mockBizOpsSink.AssertExpectations(t)
	})
	t.Run("Sink Nil Error", func(t *testing.T) {
		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")

		bizOpsProvider := NewBizOpsProvider(nil, nil, nil)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "terminal",
		})
		assert.Error(t, err)
		assert.Equal(t, "biz ops sink is nil", err.Error())
	})
	t.Run("Invalid Sink", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		// mock Sink
		mockBizOpsSink.On("Type").Return("terminal")

		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")

		bizOpsProvider := NewBizOpsProvider(nil, nil, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "invalid-sink",
		})
		assert.Error(t, err)
		assert.Equal(t, "invalid biz ops sink type", err.Error())
		mockBizOpsSink.AssertExpectations(t)
	})
	t.Run("Invalid BizOps Type", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		// mock Sink
		mockBizOpsSink.On("Type").Return("terminal")

		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")

		bizOpsProvider := NewBizOpsProvider(nil, nil, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "invalid",
		})
		assert.Error(t, err)
		assert.Equal(t, "invalid biz ops type", err.Error())
		mockBizOpsSink.AssertExpectations(t)
	})
	t.Run("VCP DB Error", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		mockmetricDB := &telemetrydb.MockStorage{}
		mockVCPDB := &vcpdb.MockStorage{}

		// mock sink calls
		mockBizOpsSink.On("Type").Return("terminal")

		// mock DB calls - using optimized ListAccountsForTelemetry
		mockVCPDB.On("ListAccountsForTelemetry", ctx, mock.Anything).Return(nil, errors.New("db failure"))

		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")

		bizOpsProvider := NewBizOpsProvider(mockmetricDB, mockVCPDB, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "terminal",
		})
		assert.Error(t, err)
		assert.Equal(t, "db failure", err.Error())
		mockVCPDB.AssertExpectations(t)
	})
	t.Run("Metrics DB Error", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		mockMetricDB := &telemetrydb.MockStorage{}
		mockVCPDB := &vcpdb.MockStorage{}
		// mock DB calls - using optimized ListAccountsForTelemetry
		mockVCPDB.On("ListAccountsForTelemetry", ctx, mock.Anything).Return([]*vcpdb.AccountTelemetryData{}, nil)
		mockMetricDB.On("AggregateUsageForBizOps", ctx, mock.Anything).Return(aggrErr)

		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")

		// mock Sink
		mockBizOpsSink.On("Ingest", ctx, mock.Anything).Return(nil)
		mockBizOpsSink.On("Type").Return("terminal")

		bizOpsProvider := NewBizOpsProvider(mockMetricDB, mockVCPDB, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "terminal",
		})
		assert.Error(t, err)
		assert.Equal(t, aggrErr.Error(), err.Error())
		mockVCPDB.AssertExpectations(t)
		mockMetricDB.AssertExpectations(t)
		mockBizOpsSink.AssertExpectations(t)
	})
	t.Run("Sink Ingest Error", func(t *testing.T) {
		mockBizOpsSink := &sink.MockBizOpsSink{}
		mockMetricDB := &telemetrydb.MockStorage{}
		mockVCPDB := &vcpdb.MockStorage{}
		// mock DB calls - using optimized ListAccountsForTelemetry
		mockVCPDB.On("ListAccountsForTelemetry", ctx, mock.Anything).Return([]*vcpdb.AccountTelemetryData{}, nil)
		mockMetricDB.On("AggregateUsageForBizOps", ctx, mock.Anything).Return(nil)

		// mock logger call
		mockLogger.On("Errorf", mock.Anything, mock.Anything).Return("")
		// mock Sink
		mockBizOpsSink.On("Ingest", ctx, mock.Anything).Return(ingestErr)
		mockBizOpsSink.On("Type").Return("terminal")

		bizOpsProvider := NewBizOpsProvider(mockMetricDB, mockVCPDB, mockBizOpsSink)
		err := bizOpsProvider.ProcessBizOps(ctx, mockLogger, &utils.BizOpsReportParams{
			SinkType: "terminal",
		})
		assert.Error(t, err)
		assert.Equal(t, ingestErr.Error(), err.Error())
		mockVCPDB.AssertExpectations(t)
		mockMetricDB.AssertExpectations(t)
		mockBizOpsSink.AssertExpectations(t)
	})
}

func Test_getContinentMap_PanicsOnMalformedInput(t *testing.T) {
	// empty string or malformed entries shouldn't panic the routine
	assert.NotPanics(t, func() {
		_ = GetContinentMap("")
	})
}
