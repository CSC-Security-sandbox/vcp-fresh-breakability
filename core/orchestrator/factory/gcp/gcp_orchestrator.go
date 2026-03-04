package gcp

import (
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/client"
)

// GCPOrchestrator is the GCP implementation of OrchestratorFactory
type GCPOrchestrator struct {
	storage  database.Storage
	temporal client.Client
}

// NewGCPOrchestrator creates a new GCP orchestrator
func NewGCPOrchestrator(storage database.Storage, temporalClient client.Client) *GCPOrchestrator {
	return &GCPOrchestrator{
		storage:  storage,
		temporal: temporalClient,
	}
}
