// Package model holds shared types and interfaces for the leaked-resources pipeline.
// Detectors and pipeline both use this package to avoid import cycles.
package model

// ResourceType identifies the kind of resource (pool, volume, snapshot, etc.).
type ResourceType string

const (
	ResourceTypePool     ResourceType = "pool"
	ResourceTypeVolume   ResourceType = "volume"
	ResourceTypeSnapshot ResourceType = "snapshot"
)

// LeakRecord is the unified record for a single leaked resource, produced by
// any detector and consumed by the reporting module.
type LeakRecord struct {
	ResourceType ResourceType      `json:"resourceType"`
	ResourceID   string            `json:"resourceId"`
	ResourceName string            `json:"resourceName,omitempty"`
	ProjectID    string            `json:"projectId,omitempty"`
	Region       string            `json:"region,omitempty"`
	Reason       string            `json:"reason"`
	Extra        map[string]string  `json:"extra,omitempty"`
}
