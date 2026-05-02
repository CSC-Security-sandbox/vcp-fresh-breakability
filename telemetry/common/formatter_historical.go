package common

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var _ TimeSeriesFormatter = (*HistoricalMetricsFormatter)(nil)

// HistoricalMetricsFormatter creates time series for historical metrics. Historical metrics represent a
// precise interval in time when a resource has a specific state. This interval is defined by the created at
// (metric timestamp) and deleted at properties of the metrics. Historical metrics that have not yet been
// deleted represent the current state of the resource.
type HistoricalMetricsFormatter struct {
	BackfillLimit time.Duration
	Logger        log.Logger
}

// Format accepts a collection of historical hydrated metrics and returns a collection of time series that
// can be used to aggregate the metrics over the aggregation period specified by start and end.
func (f HistoricalMetricsFormatter) Format(ctx context.Context, logger log.Logger, metrics []entity.HydratedMetric, start, end time.Time) []TimeSeries {
	if len(metrics) < 1 {
		return nil
	}

	var timeSeries []TimeSeries
	var dataPoints []DataPoint
	var lastMetric *entity.HydratedMetric

	for _, metric := range metrics {
		// Shadow the loop variable since it is reused in each iteration
		hydratedMetric := metric
		metricTime := hydratedMetric.Timestamp.ToTime()
		deletedAt := hydratedMetric.Metadata.DeletedAt

		// Ignore metrics that were deleted before the start of the aggregation period
		if deletedAt != nil && deletedAt.Before(start) {
			continue
		}

		// Ignore all metrics that were created after the end of the aggregation period
		if metricTime.After(end) {
			break
		}

		// A change in metadata requires us to create a time series for the current collection of data points
		// that the old metadata applies to and start collecting data points for a new time series that the
		// new metadata applies to
		if lastMetric != nil && hasMetadataChanged(hydratedMetric, *lastMetric) {
			timeSeries = append(timeSeries, TimeSeries{
				AggregationStart: start,
				AggregationEnd:   *lastMetric.Metadata.DeletedAt,
				Metadata:         lastMetric.Metadata,
				MeasuredType:     lastMetric.MeasuredType,
				DataPoints:       dataPoints,
			})
			dataPoints = []DataPoint{
				{
					Timestamp:    *lastMetric.Metadata.DeletedAt,
					Quantity:     lastMetric.Quantity,
					TransferType: lastMetric.Metadata.TransferType,
				},
			}
		}

		// The first metric that represents the state of the resource during the aggregation period
		// determines where we start the aggregation
		if lastMetric == nil {
			if metricTime.Before(start) {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    start,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
			} else {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    metricTime,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
			}
		}

		// If the current metric has not been deleted then we know that it represents the state of the
		// resource at the end of the aggregation period. Otherwise, we must check when the metric was
		// deleted to determine where to add the next data point.
		if deletedAt != nil {
			if deletedAt.After(end) {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    end,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
			} else {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    *deletedAt,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
			}
		} else {
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    end,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
		}

		lastMetric = &hydratedMetric
	}

	if len(dataPoints) > 0 && lastMetric != nil {
		aggregationEnd := end
		if lastMetric.Metadata.DeletedAt != nil && lastMetric.Metadata.DeletedAt.Before(end) {
			aggregationEnd = *lastMetric.Metadata.DeletedAt
		}
		timeSeries = append(timeSeries, TimeSeries{
			AggregationStart: start,
			AggregationEnd:   aggregationEnd,
			Metadata:         lastMetric.Metadata,
			MeasuredType:     lastMetric.MeasuredType,
			DataPoints:       dataPoints,
		})
	}

	return timeSeries
}

// GetBackfillLimit returns the backfill limit of the formatter.
func (f HistoricalMetricsFormatter) GetBackfillLimit() time.Duration {
	return f.BackfillLimit
}

// SetBackfillLimit sets the backfill limit of the formatter.
func (f *HistoricalMetricsFormatter) SetBackfillLimit(limit time.Duration) {
	f.BackfillLimit = limit
}
