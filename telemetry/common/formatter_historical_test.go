package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func createHistoricalMetric(timestamp time.Time, deletedAt *time.Time, quantity float64, resourceType metadata.ResourceType, measuredType metadata.MeasuredType, autoTierEnabled *bool, serviceLevel string) entity.HydratedMetric {
	resourceMetadata := metadata.ResourceMetadata{}
	resourceMetadata.SetResourceName("resource-name")
	resourceMetadata.SetAccountName("account-name")
	resourceMetadata.SetResourceType(resourceType)
	if serviceLevel != "" {
		resourceMetadata.SetServiceLevel(serviceLevel)
	}
	if autoTierEnabled != nil {
		resourceMetadata.AutoTierEnabled = autoTierEnabled
	}
	if deletedAt != nil {
		resourceMetadata.SetDeletedAt(*deletedAt)
	}

	return entity.HydratedMetric{
		Metadata:     resourceMetadata,
		Timestamp:    entity.UnixNano(timestamp.UnixNano()),
		MeasuredType: measuredType,
		Quantity:     quantity,
	}
}

func TestHistoricalMetricsFormatter_Format_ActiveMetric(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), nil, 100, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, nil, "low"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 1)
	assert.Equal(t, start, result[0].DataPoints[0].Timestamp)
	assert.Equal(t, end, result[0].DataPoints[1].Timestamp)
}

func TestHistoricalMetricsFormatter_Format_DeletedBeforeEnd(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)
	deletedAt := start.Add(10 * time.Minute)

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), &deletedAt, 100, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, nil, "low"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 1)
	assert.Equal(t, deletedAt, result[0].AggregationEnd)
	assert.Equal(t, deletedAt, result[0].DataPoints[1].Timestamp)
}

func TestHistoricalMetricsFormatter_Format_SkipDeletedBeforeStart(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)
	deletedAt := start.Add(-5 * time.Minute)

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), &deletedAt, 100, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, nil, "low"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 0)
}

func TestHistoricalMetricsFormatter_Format_SkipNonBillable(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), nil, 100, metadata.VolumePool, metadata.AllocatedSize, nil, "low"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 1)
	assert.Equal(t, start, result[0].DataPoints[0].Timestamp)
	assert.Equal(t, end, result[0].DataPoints[1].Timestamp)
}

func TestHistoricalMetricsFormatter_Format_SkipAutoTierEnabled(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)
	enabled := true

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), nil, 100, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, &enabled, "low"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 1)
	assert.Equal(t, start, result[0].DataPoints[0].Timestamp)
	assert.Equal(t, end, result[0].DataPoints[1].Timestamp)
}

func TestHistoricalMetricsFormatter_Format_MetadataChange(t *testing.T) {
	formatter := HistoricalMetricsFormatter{}
	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)
	deletedAt := start.Add(20 * time.Minute)

	metrics := []entity.HydratedMetric{
		createHistoricalMetric(start.Add(-10*time.Minute), &deletedAt, 100, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, nil, "low"),
		createHistoricalMetric(start.Add(25*time.Minute), nil, 200, metadata.VolumePool, metadata.CoolTierDataReadSizeRaw, nil, "high"),
	}

	result := formatter.Format(context.Background(), nil, metrics, start, end)
	require.Len(t, result, 2)
	assert.Equal(t, "low", *result[0].Metadata.ServiceLevel)
	assert.Equal(t, deletedAt, result[0].AggregationEnd)
	assert.Equal(t, "high", *result[1].Metadata.ServiceLevel)
}
