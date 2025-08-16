package api

import (
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator orchestrator.OrchestratorFactory
}

func NewHandler(orch orchestrator.OrchestratorFactory) *Handler {
	return &Handler{
		Orchestrator: orch,
	}
}
