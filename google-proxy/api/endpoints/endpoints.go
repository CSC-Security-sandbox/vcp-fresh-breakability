package api

import (
	"context"
	"sync/atomic"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
)

var (
	parseAndValidateRegionAndZone = utils.ParseAndValidateRegionAndZone
)

// ServerState manages the server's readiness state for graceful shutdown.
// When shutting down, the health endpoint will return an error to signal
// to the load balancer that this pod should be removed from the backend pool.
type ServerState struct {
	shuttingDown atomic.Bool
}

// NewServerState creates a new ServerState instance.
func NewServerState() *ServerState {
	return &ServerState{}
}

// SetShuttingDown marks the server as shutting down.
// Once set, the health endpoint will return an error to fail readiness probes.
func (s *ServerState) SetShuttingDown() {
	s.shuttingDown.Store(true)
}

// IsShuttingDown returns true if the server is in the process of shutting down.
func (s *ServerState) IsShuttingDown() bool {
	return s.shuttingDown.Load()
}

type Handler struct {
	oasgenserver.UnimplementedHandler
	Orchestrator orchestrator.OrchestratorFactory
	ServerState  *ServerState
}

// GetHealth returns the server health status.
// During graceful shutdown, this returns an error to fail readiness probes,
// allowing the load balancer to stop routing traffic to this pod before it terminates.
func (h Handler) GetHealth(ctx context.Context) (oasgenserver.GetHealthRes, error) {
	if h.ServerState != nil && h.ServerState.IsShuttingDown() {
		return &oasgenserver.GetHealthInternalServerError{
			Code:    500,
			Message: "Server is shutting down",
		}, nil
	}
	return &oasgenserver.Health{
		Status: oasgenserver.NewOptString("healthy"),
	}, nil
}
