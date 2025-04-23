package orchestrator

import (
	"go.temporal.io/sdk/client"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
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
