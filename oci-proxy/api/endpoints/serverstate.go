package api

import "sync/atomic"

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
