package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
)

func Test_getProviderByNode(t *testing.T) {
	origAuthType := common.AuthType
	originalGetPasswordFromCacheOrSecretManager := activities.GetPasswordFromCacheOrSecretManager
	defer func() {
		common.AuthType = origAuthType
		activities.GetPasswordFromCacheOrSecretManager = originalGetPasswordFromCacheOrSecretManager
	}()

	ctx := context.Background()
	node := &models2.Node{
		Username:          "user",
		Password:          "pass",
		SecretID:          "secret-id",
		EndpointAddress:   "1.2.3.4",
		EndpointAddresses: []string{},
	}

	t.Run("Password from Secret Manager", func(t *testing.T) {
		common.AuthType = common.USERNAME_PWD_SEC_MGR
		// Mock GetPasswordFromCacheOrSecretManager
		activities.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) string {
			return "secret-pass"
		}
		node.EndpointAddresses = []string{}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("Password from Node", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("No Endpoint Address", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{}
		node.EndpointAddress = ""
		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("Already has EndpointAddresses", func(t *testing.T) {
		common.AuthType = common.USER_CERTIFICATE
		node.EndpointAddresses = []string{"5.6.7.8"}
		node.EndpointAddress = ""
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})
}
