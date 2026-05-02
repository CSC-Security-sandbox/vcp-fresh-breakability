package common

import (
	"context"
	"sort"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var _ TimeSeriesFormatter = (*SampledMetricsFormatter)(nil)

// SampledMetricsFormatterMode is used to specify the operation mode of a SampledMetricsFormatter.
type SampledMetricsFormatterMode string

const (
	// Interval mode adds a data point for each billable metric that was measured during the aggregation
	// period and may additionally add data points at the start and end of the aggregation period in order
	// to preserve the intervals defined by the metrics. If we have a metric that was measured before the
	// start of the aggregation period and a metric that was measured during the period, then we add a
	// metric at the start of the period. In the same way a data point may be added at the end of the period.
	// This mode is required when creating time series that will be aggregated using a method that uses the
	// quantity of two data points at a time to calculate the aggregated quantity. An example of such a
	// method is integral aggregation, where the area defined by two consecutive data points is added to the
	// area defined by the previous data points.
	Interval = SampledMetricsFormatterMode("interval")
	// Point mode adds a single data point for each billable metric that was measured during the aggregation
	// period. This mode is required when creating time series that will be aggregated using a method that
	// uses the quantity of a single data point at a time to calculate the aggregate. An example of such a
	// method is sum aggregation where the quantity of a single data point is added to the sum of the
	// quantities of the previous data points.
	Point = SampledMetricsFormatterMode("point")
)

// SampledMetricsFormatter creates time series for sampled metrics. These metrics are usually sampled at
// fixed intervals and consequently do not represent the precise point in time when the state that they
// represent occurred. Instead, they are considered to represent the state of the resource being measured up
// to the point in time when the metric was measured.
//
// The Mode specifies the operation mode of the formatter and that determines what data points are added to
// time series. A BackfillLimit can be specified when operating in Interval mode in order to set a maximum
// length of intervals allowed between data points. If this limit is reached, then the interval will not be
// preserved in the time series.
type SampledMetricsFormatter struct {
	Mode          SampledMetricsFormatterMode
	BackfillLimit time.Duration
	Logger        log.Logger
}

// Format accepts a collection of sampled hydrated metrics and returns a collection of time series that
// can be used to aggregate the metrics over the aggregation period specified by start and end.
func (f SampledMetricsFormatter) Format(ctx context.Context, logger log.Logger, metrics []entity.HydratedMetric, start, end time.Time) []TimeSeries {
	if logger != nil {
		logger.Debug("Formatting sampled metrics")
	}

	if len(metrics) < 1 {
		return nil
	}

	// Sort the metrics in chronological order
	sort.Sort(entity.ByTimestamp(metrics))

	var timeSeries []TimeSeries
	var dataPoints []DataPoint
	var lastMetric *entity.HydratedMetric

	for _, metric := range metrics {
		// Shadow the loop variable since it is reused in each iteration
		hydratedMetric := metric
		hydratedMetricTime := hydratedMetric.Timestamp.ToTime()
		// Move past metrics that were measured before the start of the aggregation period
		if hydratedMetricTime.Before(start) {
			lastMetric = &hydratedMetric
			continue
		}

		// Here we handle the first billable hydratedMetric that was measured at or after the start of the aggregation
		// period, and we do not have a hydratedMetric that was measured before the period. If it was measured after
		// the end of the period, then we have nothing to aggregate and stop immediately.
		if lastMetric == nil {
			// -------|-----------|---x---
			if !hydratedMetricTime.Before(end) {
				break
			}
			// -------|-----x-----|-------
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    hydratedMetricTime,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
			lastMetric = &hydratedMetric
			continue
		}

		// Do not aggregate over intervals that exceed the backfill limit
		if f.Mode == Interval && hydratedMetricTime.Sub(lastMetric.Timestamp.ToTime()) > f.BackfillLimit {
			if len(dataPoints) > 0 {
				timeSeries = append(timeSeries, TimeSeries{
					AggregationStart: start,
					AggregationEnd:   lastMetric.Timestamp.ToTime(),
					Metadata:         lastMetric.Metadata,
					MeasuredType:     lastMetric.MeasuredType,
					DataPoints:       dataPoints,
				})
				dataPoints = []DataPoint{}
			}
			if !hydratedMetricTime.Before(end) {
				break
			}
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    hydratedMetricTime,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
			lastMetric = &hydratedMetric
			continue
		}

		// ---x---|-----------|---x---
		if lastMetric.Timestamp.ToTime().Before(start) && !hydratedMetricTime.Before(end) {
			if f.Mode == Interval {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    start,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    end,
					Quantity:     hydratedMetric.Quantity,
					TransferType: hydratedMetric.Metadata.TransferType,
				})
				lastMetric = &hydratedMetric
			}
			break
		}

		// ---x---|-----x-----|-------
		if lastMetric.Timestamp.ToTime().Before(start) {
			if f.Mode == Interval && !hydratedMetricTime.Equal(start) {
				dataPoints = append(dataPoints,
					DataPoint{
						Timestamp:    start,
						Quantity:     hydratedMetric.Quantity,
						TransferType: hydratedMetric.Metadata.TransferType,
					})
			}
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    hydratedMetricTime,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
			lastMetric = &hydratedMetric
			continue
		}

		// A change in metadata requires us to create a time series for the current set of data points and
		// start collecting data points for a new time series that the new metadata applies to. If we are
		// in interval mode, then we must preserve the interval between the metrics by adding a data point
		// for the last hydratedMetric to the new set of data points.
		if hasMetadataChanged(hydratedMetric, *lastMetric) && len(dataPoints) > 0 {
			timeSeries = append(timeSeries, TimeSeries{
				AggregationStart: start,
				AggregationEnd:   lastMetric.Timestamp.ToTime(),
				Metadata:         lastMetric.Metadata,
				MeasuredType:     lastMetric.MeasuredType,
				DataPoints:       dataPoints,
			})
			dataPoints = []DataPoint{}
			if f.Mode == Interval {
				dataPoints = append(dataPoints, DataPoint{
					Timestamp:    lastMetric.Timestamp.ToTime(),
					Quantity:     lastMetric.Quantity,
					TransferType: lastMetric.Metadata.TransferType,
				})
			}
		}

		// -------|--x-----x--|-------
		if hydratedMetricTime.Before(end) {
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    hydratedMetricTime,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
			lastMetric = &hydratedMetric
			continue
		}

		// -------|-----x-----|---x---
		if f.Mode == Interval {
			dataPoints = append(dataPoints, DataPoint{
				Timestamp:    end,
				Quantity:     hydratedMetric.Quantity,
				TransferType: hydratedMetric.Metadata.TransferType,
			})
			lastMetric = &hydratedMetric
		}
		break
	}

	if len(dataPoints) > 0 {
		var aggregationEnd time.Time
		if lastMetric.Timestamp.ToTime().Before(end) {
			aggregationEnd = lastMetric.Timestamp.ToTime()
		} else {
			aggregationEnd = end
		}

		timeSeries = append(timeSeries, TimeSeries{
			AggregationStart: start,
			AggregationEnd:   aggregationEnd,
			Metadata:         lastMetric.Metadata,
			MeasuredType:     lastMetric.MeasuredType,
			DataPoints:       dataPoints,
		})
	}

	if f.Logger != nil {
		f.Logger.Debugf("Time series created, %+v", timeSeries)
	}
	return timeSeries
}

// GetBackfillLimit returns the backfill limit of the formatter.
func (f SampledMetricsFormatter) GetBackfillLimit() time.Duration {
	return f.BackfillLimit
}

// SetBackfillLimit sets the backfill limit of the formatter.
func (f *SampledMetricsFormatter) SetBackfillLimit(limit time.Duration) {
	f.BackfillLimit = limit
}
