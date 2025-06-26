package entity

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"time"
)

// HydratedMetric represents a metric with associated metadata and a timestamp.
type HydratedMetric struct {
	Timestamp time.Time                 `json:"timestamp"`
	Metadata  metadata.ResourceMetadata `json:"metadata"`
	Type      metadata.MetricType       `json:"type"`
	Value     float64                   `json:"value"`
}
