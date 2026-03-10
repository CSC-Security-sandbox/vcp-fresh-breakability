package model

import (
	"context"

	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

// Detector is the interface that each resource type (pool, volume, snapshot, etc.)
// implements. Detect runs the resource-specific scan and returns leak records.
type Detector interface {
	Name() string
	Detect(ctx context.Context, storage database.Storage) ([]LeakRecord, error)
}
