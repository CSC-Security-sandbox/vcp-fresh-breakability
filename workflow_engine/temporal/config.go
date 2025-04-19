package workflow_engine

import (
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/workflow"
)

type TemporalConfig struct {
	// Temporal settings
	TemporalHostPort      string // Host and port of the Temporal server (e.g., "localhost:7233").
	TemporalNamespaceName string // Namespace name for Temporal (e.g., "default").
	TemporalTLSCertPath   string // Path to the TLS certificate for Temporal.
	TemporalTLSKeyPath    string // Path to the TLS key for Temporal.
	TemporalEncryptionID  string // Encryption ID for Temporal (e.g., "").
}

func (c *TemporalConfig) GetHostPort() string {
	return c.TemporalHostPort
}

func (c *TemporalConfig) GetNamespace() string {
	return c.TemporalNamespaceName
}

func (c *TemporalConfig) GetTLSCertPath() string {
	return c.TemporalTLSCertPath
}

func (c *TemporalConfig) GetTLSKeyPath() string {
	return c.TemporalTLSKeyPath
}

func (c *TemporalConfig) GetEncryptionID() string {
	return c.TemporalEncryptionID
}

func LoadTemporalConfig() *TemporalConfig {
	temporalHostPort := env.GetString("TEMPORAL_ADDRESS", "localhost:7233")
	temporalNamespaceName := env.GetString("TEMPORAL_NAMESPACE", "default")
	temporalTlsCertPath := env.GetString("TEMPORAL_TLS_CERT", "")
	temporalTlsKeyPath := env.GetString("TEMPORAL_TLS_KEY", "")
	temporalEncryptionID := env.GetString("TEMPORAL_ENCRYPTION_ID", "")

	return &TemporalConfig{
		TemporalHostPort:      temporalHostPort,
		TemporalNamespaceName: temporalNamespaceName,
		TemporalTLSCertPath:   temporalTlsCertPath,
		TemporalTLSKeyPath:    temporalTlsKeyPath,
		TemporalEncryptionID:  temporalEncryptionID,
	}
}

func GeneratedUUID(ctx workflow.Context) string {
	encodedUUID := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return utils.RandomUUID()
	})

	var uuid string
	err := encodedUUID.Get(&uuid)
	if err != nil {
		// Replace print wiht logging once logger is available
		fmt.Println("Error generating UUID:", err)
		return uuid
	}
	return uuid
}
