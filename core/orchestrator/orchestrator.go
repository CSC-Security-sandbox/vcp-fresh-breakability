package orchestrator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"go.temporal.io/sdk/client"
)

type Orchestrator struct {
	storage  database.Storage
	temporal client.Client
}

func NewOrchestrator(storage database.Storage, temporalClient client.Client) *Orchestrator {
	return &Orchestrator{
		storage:  storage,
		temporal: temporalClient,
	}
}
