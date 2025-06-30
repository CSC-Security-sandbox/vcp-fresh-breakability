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
