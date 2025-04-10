package api

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator *orchestrator.Orchestrator
}
