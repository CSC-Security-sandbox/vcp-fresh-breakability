package workflow_engine

import (
	"context"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
)

type ClientConfig interface {
	GetHostPort() string
	GetNamespace() string
	GetTLSCertPath() string
	GetTLSKeyPath() string
	GetEncryptionID() string
}

type WorkflowEngine interface {
	LoadConfig() ClientConfig
	InitializeClient(ctx context.Context, cfg ClientConfig, logger log.Logger) error
	RunWorker(ctx context.Context, client client.Client) error
	CloseClient(client client.Client)
}
