package api

import (
	"context"

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

func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	return &oasgenserver.Health{}, nil
}
