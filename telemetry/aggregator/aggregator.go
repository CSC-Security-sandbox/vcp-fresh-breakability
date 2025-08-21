package aggregator

import (
	datamodel2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"sort"
)

// JobType is used to select the appropriate pipeline to process the job.
type JobType string

const (
	// IntegralAggregation is the JobType for aggregating metrics saved in our database that require integral aggregation
	IntegralAggregation JobType = "IntegralAggregation"
	// CounterAggregation is the JobType for aggregating metrics saved in our database that require counter aggregation
	CounterAggregation JobType = "CounterAggregation"
	// SumAggregation is the JobType for aggregating metrics saved in our database that require sum aggregation
	SumAggregation JobType = "SumAggregation"
	// FirstAggregation is the JobType for aggregating metrics saved in our database that require 'first' aggregation
	FirstAggregation JobType = "FirstValueAggregation"
)

// Integral calculates the area under the curve defined by time series data points. The area between two
// data points is calculated by multiplying the value of the second data point by the difference in time
// between the two points measured in hours. It is assumed that the data points are sorted in chronological order.
func Integral(metrics []datamodel2.HydratedMetrics) float64 {
	if len(metrics) < 2 {
		return 0
	}

	// Sort metrics by timestamp
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].MetricTimestamp.Before(metrics[j].MetricTimestamp)
	})

	var integral float64
	for i := 1; i < len(metrics); i++ {
		duration := metrics[i].MetricTimestamp.Sub(metrics[i-1].MetricTimestamp).Hours()
		integral += metrics[i].Quantity * duration
	}

	return integral
}

// CounterDelta accepts a collection of data points and returns the sum of the difference between consecutive
// data point quantities. It is assumed that the data points represent the values of a monotonic counter,
// i.e., the value should always be increasing. Under some circumstances the counter can reset and this
// function handles that by using the value of the current data point as long as it is less that 25% of the
// previous data point. Otherwise, an anomalous dip has occurred and the data point is skipped. It is assumed
// that the data points have been sorted in chronological order.
func CounterDelta(metrics []datamodel2.HydratedMetrics) float64 {
	if len(metrics) < 2 {
		return 0
	}

	var aggregate float64
	var lastMetric *datamodel2.HydratedMetrics

	// Sort metrics by timestamp
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].MetricTimestamp.Before(metrics[j].MetricTimestamp)
	})

	// Iterate through the metrics
	for _, metric := range metrics {
		metric := metric

		// Initialize the lastMetric if it's nil
		if lastMetric == nil {
			lastMetric = &metric
			continue
		}

		// Calculate the difference in quantity
		quantity := metric.Quantity - lastMetric.Quantity

		// Check for counter reset
		if quantity < 0 {
			// If the current quantity is less than 25% of the previous quantity, assume a counter reset
			// Otherwise, skip the current metric
			if metric.Quantity < lastMetric.Quantity*0.25 {
				quantity = metric.Quantity
			} else {
				continue
			}
		}

		// Add the quantity to the aggregate
		aggregate += quantity
		lastMetric = &metric
	}

	return aggregate
}

// First accepts a collection of data points and returns the quantity of the first data point. It is assumed
// that the data points have been sorted in chronological order.
func First(metrics []datamodel2.HydratedMetrics) float64 {
	if len(metrics) < 1 {
		return 0
	}

	// Sort metrics by timestamp
	sort.Slice(metrics, func(i, j int) bool {
		return metrics[i].MetricTimestamp.Before(metrics[j].MetricTimestamp)
	})

	// Return the first value
	return metrics[0].Quantity
}

// Sum accepts a collection of data points and returns the sum of all the data point quantities.
func Sum(metrics []datamodel2.HydratedMetrics) float64 {
	sum := 0.0
	for _, m := range metrics {
		sum += m.Quantity
	}
	return sum
}
