package temporalmanager

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"go.temporal.io/sdk/client"
)

type TemporalManager struct {
	Config workflowEngine.ClientConfig
	Client client.Client
	DBConn database.Storage
}

// returns the initialised temporal client
func (t *TemporalManager) GetClient() client.Client {
	return t.Client
}

// closed the temporal client
func (t *TemporalManager) CloseClient() {
	t.Client.Close()
}
