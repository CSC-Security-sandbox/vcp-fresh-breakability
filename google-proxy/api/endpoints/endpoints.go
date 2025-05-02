package api

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator *orchestrator.Orchestrator
}

func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	return &oasgenserver.GetHealthOK{}, nil
}
