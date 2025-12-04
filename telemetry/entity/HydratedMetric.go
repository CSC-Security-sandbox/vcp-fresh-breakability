package entity

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

// HydratedMetric contains cooked and hydrated metrics to ship over to qstack usage
type HydratedMetric struct {
	Metadata      metadata.ResourceMetadata `json:"metadata,omitempty"`
	Timestamp     UnixNano                  `json:"timestamp,omitempty"`
	MeasuredType  metadata.MeasuredType     `json:"measuredType,omitempty"`
	Quantity      float64                   `json:"quantity"`
	CorrelationID string                    `json:"correlationId,omitempty"`
}

// ByTimestamp implements sort.Interface for []HydratedMetric based on the Timestamp field.
type ByTimestamp []HydratedMetric

// Len returns the number of hydrated metrics in the collection.
func (t ByTimestamp) Len() int {
	return len(t)
}

// Swap changes the position of two hydrated metrics.
func (t ByTimestamp) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less determines whether one hydrated metric was measured before another hydrated metric.
func (t ByTimestamp) Less(i, j int) bool {
	return t[i].Timestamp < t[j].Timestamp
}
