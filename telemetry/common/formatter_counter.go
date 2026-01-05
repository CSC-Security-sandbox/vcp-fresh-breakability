package common

import (
	"context"
	"time"

	database2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// CounterMetricsFormatter creates time series for sampled counter metrics. These metrics are used to compute deltas
// between consecutive datapoints. This formatter prepares a timeseries for back-aggregation, meaning that the aggregation
// for the current hour continue from the last datapoint of the previous aggregation period. This is opposite to the
// SampledMetricsFormatter, which handles a forward-aggregation, in which we include the first datapoint after the current aggregation period
// in the calculation.
//
// If this formatter is provided with datapoints before current aggregation period start, the closest datapoint to the start time
// will be kept in the time series with the timestamp adjusted to match the aggregation start.
// If this formatter is provided with datapoints after current aggregation period end, those datapoints will be dropped.
//
// BackfillLimit can be specified in order to set a maximum
// length of intervals allowed between data points. If this limit is reached, then the interval will not be
// preserved in the time series.
//
// SwitchoverNeeded is used to handle the upgrade from the previous system to the new.
// Before we used to include the next aggregation period's first datapoint in the current time series. since we are swapping
// the aggregation around to exclude the last datapoint and include the previous one,we need to make sure to exclude the
// previous hour data point on the switchover hour
type CounterMetricsFormatter struct {
	BackfillLimit time.Duration
	Logger        log.Logger
	MetricsDB     database2.Storage
}

// trimMetricsBeforeStart handles datapoints that happen before the start of the aggregation period. Of those, only the datapoint closest or
// equal to start time will be kept, and only if the time gap is within the BackfillLimit from the aggregation start.
// This function assumes that the metrics passed in are sorted ascending by timestamp. It works backwards through the datapoints until it finds the earliest
// datapoint that should be included in the time series.
// If no previous metrics are found and MetricsDB is available, it will fetch the latest metric before the aggregation start from the database.
func (f CounterMetricsFormatter) trimMetricsBeforeStart(ctx context.Context, metrics []entity.HydratedMetric, start time.Time) []entity.HydratedMetric {
	if len(metrics) == 0 {
		return metrics
	}

	for i := len(metrics) - 1; i >= 0; i-- {
		m := metrics[i]
		mTime := m.Timestamp.ToTime()
		// the first time we encounter a timestamp that is at or before the time series start, stop. We
		// will include this point if it is within the allowed backfill period.
		if mTime == start || mTime.Before(start) {
			// if the previous period datapoint is beyond the backfill limit, we will drop it. Backfill limit of 0 or below is ignored.
			if f.BackfillLimit > 0 && start.Sub(mTime) > f.BackfillLimit {
				if f.Logger != nil {
					f.Logger.Warn("Found a counter datapoint that is older than allowed backfill limit.")
				}
				return metrics[i+1:]
			} else {
				return metrics[i:]
			}
		}
	}

	return metrics
}

// trimMetricsAfterEnd handles datapoints that happen after the end of the aggregation period. All of those will be discarded. Datapoint happening
// exactly at the aggregation end will be kept.
func (f CounterMetricsFormatter) trimMetricsAfterEnd(metrics []entity.HydratedMetric, end time.Time) []entity.HydratedMetric {
	for i := 0; i < len(metrics); i++ {
		m := metrics[i]
		mTime := m.Timestamp.ToTime()
		// the first time we encounter a timestamp that is after the aggregation end we will return the time series.
		if mTime.After(end) {
			return metrics[:i]
		}
	}
	// If we have finished the loop without returning it means there were no timestamps after the aggregation end so we return the initial metrics
	return metrics
}

// Format accepts a collection of sampled hydrated metrics and returns a collection of time series that
// can be used to aggregate the metrics over the aggregation period specified by start and end.
func (f CounterMetricsFormatter) Format(ctx context.Context, logger log.Logger, metrics []entity.HydratedMetric, start, end time.Time) []TimeSeries {
	var timeSeries []TimeSeries
	var dataPoints []DataPoint

	if len(metrics) == 0 {
		return timeSeries
	}

	trimmedMetrics := f.trimMetricsBeforeStart(ctx, metrics, start)
	trimmedMetrics = f.trimMetricsAfterEnd(trimmedMetrics, end)
	if len(trimmedMetrics) < 2 {
		return timeSeries
	}
	lastMetric := trimmedMetrics[0]
	for _, metric := range trimmedMetrics {
		// A change in metadata requires us to create a time series for the current set of data points and
		// start collecting data points for a new time series that the new metadata applies to.
		if hasMetadataChanged(metric, lastMetric) {
			if len(dataPoints) > 1 {
				timeSeries = append(timeSeries, TimeSeries{
					AggregationStart: start,
					AggregationEnd:   end,
					Metadata:         lastMetric.Metadata,
					MeasuredType:     lastMetric.MeasuredType,
					DataPoints:       dataPoints,
				})
			}
			dataPoints = []DataPoint{}
			dataPoints = append(dataPoints, DataPoint{
				Timestamp: lastMetric.Timestamp.ToTime(),
				Quantity:  lastMetric.Quantity,
			})
		}

		dataPoints = append(dataPoints, DataPoint{
			Timestamp: metric.Timestamp.ToTime(),
			Quantity:  metric.Quantity,
		})
		lastMetric = metric
	}

	if len(dataPoints) > 1 {
		timeSeries = append(timeSeries, TimeSeries{
			AggregationStart: start,
			AggregationEnd:   end,
			Metadata:         lastMetric.Metadata,
			MeasuredType:     lastMetric.MeasuredType,
			DataPoints:       dataPoints,
		})
	}
	return timeSeries
}

// GetBackfillLimit returns the backfill limit of the formatter.
func (f CounterMetricsFormatter) GetBackfillLimit() time.Duration {
	return f.BackfillLimit
}

// SetBackfillLimit sets the backfill limit of the formatter.
func (f *CounterMetricsFormatter) SetBackfillLimit(limit time.Duration) {
	f.BackfillLimit = limit
}
