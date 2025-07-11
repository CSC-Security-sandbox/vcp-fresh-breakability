package orchestrator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	models2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func Test_GetProviderByNode(t *testing.T) {
	ctx := context.Background()

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USER_CERTIFICATE

		node := &models2.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
		}

		origGetCert := activities.GetCertificateFromCacheOrSecretManager
		defer func() { activities.GetCertificateFromCacheOrSecretManager = origGetCert }()
		activities.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certID string) (*models2.Certificate, error) {
			return &models2.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
				RootCaCertificate:        "root-ca",
			}, nil
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USER_CERTIFICATE

		node := &models2.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
		}

		origGetCert := activities.GetCertificateFromCacheOrSecretManager
		defer func() { activities.GetCertificateFromCacheOrSecretManager = origGetCert }()
		activities.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certID string) (*models2.Certificate, error) {
			return nil, errors.New("error")
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error")
		assert.Nil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USERNAME_PWD_SEC_MGR

		node := &models2.Node{
			Name:                           "node2",
			SecretID:                       "secret-id",
			EndpointAddress:                "1.2.3.4",
			EndpointAddressesToHostNameMap: map[string]string{},
		}

		origGetPwd := activities.GetPasswordFromCacheOrSecretManager
		defer func() { activities.GetPasswordFromCacheOrSecretManager = origGetPwd }()
		activities.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "pwd", nil
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USERNAME_PWD_SEC_MGR

		node := &models2.Node{
			Name:                           "node2",
			SecretID:                       "secret-id",
			EndpointAddress:                "1.2.3.4",
			EndpointAddressesToHostNameMap: map[string]string{},
		}

		origGetPwd := activities.GetPasswordFromCacheOrSecretManager
		defer func() { activities.GetPasswordFromCacheOrSecretManager = origGetPwd }()
		activities.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("error")
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error")
		assert.Nil(t, provider)
	})

	t.Run("Password from node, missing endpoint address", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USERNAME_PWD

		node := &models2.Node{
			Name:                           "node3",
			Password:                       "pwd",
			EndpointAddressesToHostNameMap: map[string]string{},
			EndpointAddress:                "",
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("Password from node, endpoint address present", func(t *testing.T) {
		origAuthType := common.AuthType
		defer func() { common.AuthType = origAuthType }()
		common.AuthType = common.USERNAME_PWD

		node := &models2.Node{
			Name:                           "node3",
			Password:                       "pwd",
			EndpointAddressesToHostNameMap: map[string]string{},
			EndpointAddress:                "1.2.3.4",
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})
}
