package orchestrator

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/database"

type Orchestrator struct {
	storage database.Storage
}

func NewOrchestrator(storage database.Storage) *Orchestrator {
	return &Orchestrator{
		storage: storage,
	}
}
