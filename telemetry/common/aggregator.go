package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// JobType is used to select the appropriate pipeline to process the job.
type JobType string

// DataPoint represents a data point in a time series.
type DataPoint struct {
	Timestamp time.Time
	Quantity  float64
}

// TimeSeries is a collection of data points along with metadata that applies to these points.
type TimeSeries struct {
	AggregationStart time.Time
	AggregationEnd   time.Time
	Metadata         metadata.ResourceMetadata
	MeasuredType     metadata.MeasuredType
	DataPoints       []DataPoint
}

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
func Integral(points []DataPoint) float64 {
	if len(points) < 2 {
		return 0
	}

	var aggregate float64
	var lastPoint *DataPoint

	for _, point := range points {
		point := point

		if lastPoint == nil {
			lastPoint = &point
			continue
		}

		timeDelta := point.Timestamp.Sub(lastPoint.Timestamp).Hours()
		aggregate += point.Quantity * timeDelta
		lastPoint = &point
	}

	return aggregate
}

// CounterDelta accepts a collection of data points and returns the sum of the difference between consecutive
// data point quantities. It is assumed that the data points represent the values of a monotonic counter,
// i.e., the value should always be increasing. Under some circumstances the counter can reset and this
// function handles that by using the value of the current data point as long as it is less that 25% of the
// previous data point. Otherwise, an anomalous dip has occurred and the data point is skipped. It is assumed
// that the data points have been sorted in chronological order.
func CounterDelta(points []DataPoint, logger log.Logger) float64 {
	if len(points) < 2 {
		return 0
	}

	var aggregate float64
	var lastPoint *DataPoint

	for _, point := range points {
		point := point

		if lastPoint == nil {
			lastPoint = &point
			continue
		}

		quantity := point.Quantity - lastPoint.Quantity

		// Check for counter reset
		if quantity < 0 {
			// If the current quantity is less than 25% of the previous quantity, then we assume a counter
			// reset and use the quantity of the current data point. Otherwise, we assume an anomalous dip
			// and skip the current data point.
			if point.Quantity < lastPoint.Quantity*0.25 {
				logger.Warnf("Counter reset detected: previous value %.2f, current value %.2f at timestamp %v",
					lastPoint.Quantity, point.Quantity, point.Timestamp)
				quantity = point.Quantity
			} else {
				logger.Warnf("Anomalous counter dip detected and skipped: previous value %.2f, current value %.2f at timestamp %v",
					lastPoint.Quantity, point.Quantity, point.Timestamp)
				continue
			}
		}

		aggregate += quantity
		lastPoint = &point
	}

	return aggregate
}

// First accepts a collection of data points and returns the quantity of the first data point. It is assumed
// that the data points have been sorted in chronological order.
func First(points []DataPoint) float64 {
	if len(points) < 1 {
		return 0
	}

	return points[0].Quantity
}

// Sum accepts a collection of data points and returns the sum of all the data point quantities.
func Sum(points []DataPoint) float64 {
	sum := 0.0
	for _, m := range points {
		sum += m.Quantity
	}
	return sum
}
