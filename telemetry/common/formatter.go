package common

import (
	"context"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// A TimeSeriesFormatter accepts a collection of hydrated metrics and creates one or more time series in a
// standard format that can be used when aggregating the metrics. The start and end parameters represent the
// start and end of the aggregation period which is considered to be the half-open interval [start, end).
// This means that a metric measured at the end of the period is not a part of the period since it will be
// included at the start of the next aggregation period.
//
// The following applies to each time series:
//   - Each time series contains one or more data points that are sorted in chronological order.
//   - Each data point represents the state of the resource being measured up to that point in time. If we
//     have two metrics, m₁ and m₂, where m₁ was measured at 13:10 and has the quantity 100 and m₂ was
//     measured at 13:15 and has the quantity 150, then m₁ represents the state of the resource before
//     13:10 and m₂ represents the state of the resource between 13:10 and 13:15.
//   - The same metadata applies to each data point in a time series. This means that a new time series has
//     to be created each time there is a change in metadata.
//   - Each time series has the aggregation start set to the start of the aggregation period and the
//     aggregation end set to the timestamp of the last data point in the time series.
type TimeSeriesFormatter interface {
	Format(ctx context.Context, logger log.Logger, metrics []entity.HydratedMetric, start, end time.Time) []TimeSeries
	GetBackfillLimit() time.Duration
	SetBackfillLimit(limit time.Duration)
}

// hasMetadataChanged determines whether two hydrated metrics have equal values in metadata properties that are
// required for aggregated billing metrics. The metadata properties compared are resource name, service level,
// SDE account UUID, tags and vendor resource ID.
func hasMetadataChanged(metric1, metric2 entity.HydratedMetric) bool {
	return !(bothNilOrEqual(metric1.Metadata.ResourceName, metric2.Metadata.ResourceName) &&
		bothNilOrEqual(metric1.Metadata.AccountName, metric2.Metadata.AccountName) &&
		bothNilOrEqual(metric1.Metadata.ServiceLevel, metric2.Metadata.ServiceLevel))
}

// bothNilOrEqual determines whether two string pointers are both set to nil or have the same value
func bothNilOrEqual[T comparable](value1, value2 *T) bool {
	if value1 == nil {
		return value2 == nil
	}
	if value2 == nil {
		return false
	}
	return *value1 == *value2
}
