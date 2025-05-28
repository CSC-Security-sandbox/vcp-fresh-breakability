package api

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

var (
	parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator orchestrator.OrchestratorFactory
}

func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	return &oasgenserver.Health{}, nil
}
