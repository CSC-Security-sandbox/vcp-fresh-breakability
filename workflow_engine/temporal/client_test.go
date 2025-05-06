package workflow_engine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestCreateClientOptionsFromEnv(t *testing.T) {
	logger := log.NewLogger()

	t.Run("should return client options without TLS when cert and key paths are empty", func(t *testing.T) {
		mockCfg := new(workflow_engine.MockClientConfig)
		mockCfg.On("GetHostPort").Return("localhost:7233")
		mockCfg.On("GetNamespace").Return("default")
		mockCfg.On("GetTLSCertPath").Return("")

		clientOpts, err := createClientOptionsFromEnv(mockCfg, logger)

		assert.NoError(t, err)
		assert.Equal(t, "localhost:7233", clientOpts.HostPort)
		assert.Equal(t, "default", clientOpts.Namespace)
		assert.Nil(t, clientOpts.ConnectionOptions.TLS)
		mockCfg.AssertExpectations(t)
	})

	t.Run("should return error when TLS cert and key paths are invalid", func(t *testing.T) {
		mockCfg := new(workflow_engine.MockClientConfig)
		mockCfg.On("GetHostPort").Return("localhost:7233")
		mockCfg.On("GetNamespace").Return("default")
		mockCfg.On("GetTLSCertPath").Return("invalid-cert.pem")
		mockCfg.On("GetTLSKeyPath").Return("invalid-key.pem")

		clientOpts, err := createClientOptionsFromEnv(mockCfg, logger)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed loading tls key pair for temporal")
		assert.Equal(t, "localhost:7233", clientOpts.HostPort)
		assert.Equal(t, "default", clientOpts.Namespace)
		mockCfg.AssertExpectations(t)
	})
}
