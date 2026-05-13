package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator factory.OrchestratorFactory
}

func NewHandler(orch factory.OrchestratorFactory) *Handler {
	return &Handler{
		Orchestrator: orch,
	}
}

func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	return &oasgenserver.Health{}, nil
}
