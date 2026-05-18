package hyperscaler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	common2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscaler3 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	oci2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/oci"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func Test_GetProviderByNode(t *testing.T) {
	ctx := context.Background()

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USER_CERTIFICATE,
		}

		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USER_CERTIFICATE,
		}

		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return nil, errors.New("error")
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error")
		assert.Nil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node2",
			SecretID:                       "secret-id",
			EndpointAddress:                "1.2.3.4",
			EndpointAddressesToHostNameMap: map[string]string{},
			AuthType:                       env.USERNAME_PWD_SEC_MGR,
		}

		origGetPwd := GetPasswordFromCacheOrSecretManager
		defer func() { GetPasswordFromCacheOrSecretManager = origGetPwd }()
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "pwd", nil
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node2",
			SecretID:                       "secret-id",
			EndpointAddress:                "1.2.3.4",
			EndpointAddressesToHostNameMap: map[string]string{},
			AuthType:                       env.USERNAME_PWD_SEC_MGR,
		}

		origGetPwd := GetPasswordFromCacheOrSecretManager
		defer func() { GetPasswordFromCacheOrSecretManager = origGetPwd }()
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("error")
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error")
		assert.Nil(t, provider)
	})

	t.Run("Password from node, missing endpoint address", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node3",
			Password:                       "pwd",
			EndpointAddressesToHostNameMap: map[string]string{},
			EndpointAddress:                "",
			AuthType:                       env.USERNAME_PWD,
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("Password from node, endpoint address present", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node3",
			Password:                       "pwd",
			EndpointAddressesToHostNameMap: map[string]string{},
			EndpointAddress:                "1.2.3.4",
			AuthType:                       env.USERNAME_PWD,
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("GetProviderByNode_NilNode", func(t *testing.T) {
		// Test with nil node - covers lines 34-35
		provider, err := GetProviderByNode(ctx, nil)
		assert.Error(t, err)
		assert.Nil(t, provider)
		// The error is wrapped in VCPError, so check for the error type
		assert.Contains(t, err.Error(), "Resource not found")
	})

	t.Run("GetProviderByNode_EmptyEndpointAddressesToHostNameMap", func(t *testing.T) {
		// Test with empty EndpointAddressesToHostNameMap for certificate auth - covers lines 41-42
		node := &models.Node{
			Name:                           "node4",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{}, // Empty map
			AuthType:                       env.USER_CERTIFICATE,
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
		// The error is wrapped in VCPError, so check for the error type
		assert.Contains(t, err.Error(), "VSA cluster node IP address not found")
	})

	t.Run("GetProviderByNode_WithPassword", func(t *testing.T) {
		// Test with node.Password not empty - covers line 68
		node := &models.Node{
			Name:                           "node5",
			CertificateID:                  "cert-id",
			Password:                       "test-password",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USER_CERTIFICATE,
		}

		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}

		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	// Certificate auth with SecretID: GetPasswordFromCacheOrSecretManager returns error (lines 68-69).
	t.Run("GetProviderByNode_USER_CERTIFICATE_SecretID_GetPasswordError", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node-cert-secret",
			CertificateID:                  "cert-id",
			SecretID:                       "secret-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "host"},
			AuthType:                       env.USER_CERTIFICATE,
		}
		origGetCert := GetCertificateFromCacheOrSecretManager
		origGetPwd := GetPasswordFromCacheOrSecretManager
		defer func() {
			GetCertificateFromCacheOrSecretManager = origGetCert
			GetPasswordFromCacheOrSecretManager = origGetPwd
		}()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("secret not found")
		}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	// Certificate auth with SecretID: GetPassword returns empty string (lines 73-74).
	t.Run("GetProviderByNode_USER_CERTIFICATE_SecretID_EmptyPassword", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node-cert-secret-empty",
			CertificateID:                  "cert-id",
			SecretID:                       "secret-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "host"},
			AuthType:                       env.USER_CERTIFICATE,
		}
		origGetCert := GetCertificateFromCacheOrSecretManager
		origGetPwd := GetPasswordFromCacheOrSecretManager
		defer func() {
			GetCertificateFromCacheOrSecretManager = origGetCert
			GetPasswordFromCacheOrSecretManager = origGetPwd
		}()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", nil
		}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	// Certificate auth with SecretID: GetPassword returns non-empty (lines 70-72).
	t.Run("GetProviderByNode_USER_CERTIFICATE_SecretID_PasswordFromSecret", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node-cert-secret-ok",
			CertificateID:                  "cert-id",
			SecretID:                       "secret-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "host"},
			AuthType:                       env.USER_CERTIFICATE,
		}
		origGetCert := GetCertificateFromCacheOrSecretManager
		origGetPwd := GetPasswordFromCacheOrSecretManager
		defer func() {
			GetCertificateFromCacheOrSecretManager = origGetCert
			GetPasswordFromCacheOrSecretManager = origGetPwd
		}()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "secret-pwd", nil
		}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	// Certificate auth, no SecretID, no password (lines 76-78).
	t.Run("GetProviderByNode_USER_CERTIFICATE_NoPasswordNoSecretID", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node-cert-nopwd",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "host"},
			AuthType:                       env.USER_CERTIFICATE,
		}
		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}
		provider, err := GetProviderByNode(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})
}

// Test_GetProviderByNode_OCI_USERNAME_PWD_SEC_MGR_Success exercises the OCI
// branch added to _getProviderByNode: when env.GetHyperscaler() == "oci"
// and Node.AuthType == USERNAME_PWD_SEC_MGR, the provider builder must look
// the password up via GetPasswordFromCacheOrOCIVault using Node.ExternalSecret
// (NOT Node.SecretID, which is the GCP Secret Manager key). On the GCP path
// (env.GetHyperscaler() == "gcp", the default) the same branch falls through
// to GetPasswordFromCacheOrSecretManager — see the existing
// Test_GetProviderByNode subtests "USERNAME_PWD_SEC_MGR success" / "error"
// for that side.
//
// This test is the unified-path replacement for the deleted
// Test_GetOCIProviderByNode_SecMgrSuccess that previously guarded the OCI
// vault lookup against silent regressions to the GCP code path.
func Test_GetProviderByNode_OCI_USERNAME_PWD_SEC_MGR_Success(t *testing.T) {
	origHS := env.Hyperscaler
	defer func() { env.Hyperscaler = origHS }()
	env.Hyperscaler = common.ProviderOCI

	origGetVault := GetPasswordFromCacheOrOCIVault
	defer func() { GetPasswordFromCacheOrOCIVault = origGetVault }()

	expectedRef := &datamodel.ExternalCredRef{
		Name:               "ocnv-cafef00d-secret",
		ExternalIdentifier: "ocid1.vaultsecret.oc1..xyz",
		Version:            7,
	}
	var capturedRef *datamodel.ExternalCredRef
	GetPasswordFromCacheOrOCIVault = func(ctx context.Context, ref *datamodel.ExternalCredRef) (string, error) {
		capturedRef = ref
		return "vault-pw", nil
	}

	node := &models.Node{
		Name:                           "node-1",
		EndpointAddressesToHostNameMap: map[string]string{"10.0.0.1": "host-1"},
		AuthType:                       env.USERNAME_PWD_SEC_MGR,
		// SecretID intentionally populated. If anyone ever changes the OCI
		// branch to fall back to SecretID (the GCP key) instead of the
		// ExternalCredRef, this test catches it via the captured-ref assertion.
		SecretID:       "projects/foo/secrets/bar",
		ExternalSecret: expectedRef,
	}

	provider, err := _getProviderByNode(context.Background(), node)
	assert.NoError(t, err)
	assert.NotNil(t, provider)

	if assert.NotNil(t, capturedRef, "OCI Vault lookup must be invoked with a non-nil ref") {
		assert.Same(t, expectedRef, capturedRef,
			"the exact ExternalCredRef from Node must be passed through; never synthesised from SecretID")
	}
}

// Test_GetProviderByNode_OCI_USERNAME_PWD_SEC_MGR_VaultFetchFails verifies
// that vault-side failures on the OCI path are surfaced under
// coreerrors.ErrOCIResourceFetchError (not the generic ErrGCPResourceFetchError
// used by the default branch). This distinction lets steady-state callers
// retry / log OCI failures separately from GCP Secret Manager failures.
func Test_GetProviderByNode_OCI_USERNAME_PWD_SEC_MGR_VaultFetchFails(t *testing.T) {
	origHS := env.Hyperscaler
	defer func() { env.Hyperscaler = origHS }()
	env.Hyperscaler = common.ProviderOCI

	origGetVault := GetPasswordFromCacheOrOCIVault
	defer func() { GetPasswordFromCacheOrOCIVault = origGetVault }()
	GetPasswordFromCacheOrOCIVault = func(ctx context.Context, ref *datamodel.ExternalCredRef) (string, error) {
		return "", fmt.Errorf("vault: 403 forbidden")
	}

	node := &models.Node{
		Name:                           "node-1",
		EndpointAddressesToHostNameMap: map[string]string{"10.0.0.1": "host-1"},
		AuthType:                       env.USERNAME_PWD_SEC_MGR,
		ExternalSecret:                 &datamodel.ExternalCredRef{Name: "n", ExternalIdentifier: "o"},
	}

	provider, err := _getProviderByNode(context.Background(), node)
	assert.Nil(t, provider)
	require.Error(t, err)

	cerr, ok := err.(*coreerrors.CustomError)
	if assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
		assert.Equal(t, coreerrors.ErrOCIResourceFetchError, cerr.TrackingID,
			"vault failures must surface under ErrOCIResourceFetchError so callers can distinguish them from GCP Secret Manager failures")
	}
}

// Unit test for NewGcpServices in core/orchestrator/activities/pool_activities_test.go
func Test_newGcpServices_ReturnsInitializedGcpServices(t *testing.T) {
	ctx := context.TODO()
	services := NewGcpServices(ctx)

	assert.NotNil(t, services)
	assert.Equal(t, ctx, services.Ctx)
	assert.NotNil(t, services.Logger)
	assert.NotNil(t, services.Retry)
}

func Test_GetPasswordForVSACluster_Success(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		gcpService := NewMockGoogleServices(t)
		secretID := "test-secret-id"
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "super-secret"},
		}
		gcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := GetPasswordForVSACluster(gcpService, secretID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		gcpService.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		gcpService := NewMockGoogleServices(t)
		secretID := "test-secret-id"
		gcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

		secret, err := GetPasswordForVSACluster(gcpService, secretID)
		assert.Error(t, err)
		assert.Nil(t, secret)
		gcpService.AssertExpectations(t)
	})
}

func Test_GetCertificateFromCacheOrSecretManager(t *testing.T) {
	ctx := context.Background()
	certificateID := "test-cert-id"

	t.Run("Certificate found in cache", func(t *testing.T) {
		mockCert := &models.Certificate{
			SignedCertificate:        "signed-cert",
			PrivateKey:               "private-key",
			CommonName:               "common-name",
			InterMediateCertificates: []string{"intermediate"},
		}
		defer func() {
			common.RemoveFromCertAuthCache(certificateID)
		}()
		common.AddToCertAuthCache(certificateID, mockCert)
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)
		assert.NoError(t, err)
		assert.Equal(t, mockCert, cert)
	})
	t.Run("Certificate not in cache, found via GCP", func(t *testing.T) {
		origGetGCPService := GetGCPService
		origGetCertificateAndPrivateKeyByID := GetCertificateAndPrivateKeyByID
		defer func() {
			common.RemoveFromCertAuthCache(certificateID)
			GetGCPService = origGetGCPService
			GetCertificateAndPrivateKeyByID = origGetCertificateAndPrivateKeyByID
		}()
		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockCertResp := &hyperscaler3.CustomCertificateResponse{
			Certificate: &hyperscaler3.CustomCertificate{
				PemCertificate:      "signed-cert",
				SubjectCommonName:   "common-name",
				PemCertificateChain: []string{"intermediate"},
			},
			Secret: &hyperscaler3.CustomSecret{
				SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"},
			},
		}
		GetCertificateAndPrivateKeyByID = func(gcpService GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscaler3.CustomCertificateResponse, error) {
			return mockCertResp, nil
		}
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)
		assert.NoError(t, err)
		assert.Equal(t, "signed-cert", cert.SignedCertificate)
		assert.Equal(t, "private-key", cert.PrivateKey)
		assert.Equal(t, "common-name", cert.CommonName)
		assert.Equal(t, []string{"intermediate"}, cert.InterMediateCertificates)
	})
	t.Run("GCP service returns error", func(t *testing.T) {
		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)
		assert.Error(t, err)
		assert.Nil(t, cert)
	})
}

func Test_GeneratePasswordForVSACluster(t *testing.T) {
	userName := "test-user"

	t.Run("PasswordGenerationFails", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "", errors.New("password generation failed")
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)

		assert.Error(tt, err)
		assert.Nil(tt, secret)
		assert.Contains(tt, err.Error(), "password generation failed")
	})

	t.Run("SecretCreationFails", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "xyzpassword", nil
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()

		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret creation failed"))

		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)

		assert.Error(tt, err)
		assert.Nil(tt, secret)
		assert.Contains(tt, err.Error(), "secret creation failed")
		mockGCPService.AssertExpectations(tt)
	})

	t.Run("GetSecretWithLatestVersion success", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)
		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})

	t.Run("Success", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "secretID"}}, nil)
		defer func() {
			common.RemoveFromUserAuthCache("secretID")
		}()

		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)

		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})
}

func Test_getPasswordFromCacheOrSecretManager(t *testing.T) {
	ctx := context.Background()
	secretID := "test-secret"

	t.Run("PasswordExistsInCache", func(tt *testing.T) {
		common.AddToUserAuthCache(secretID, "cached-password")
		getPasswordForVSACluster := GetPasswordForVSACluster
		defer func() {
			GetPasswordForVSACluster = getPasswordForVSACluster
			common.RemoveFromUserAuthCache(secretID)
		}()
		GetPasswordForVSACluster = func(gcpService GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "cached-password"}}, nil
		}
		password, err := GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "cached-password", password)
		assert.NoError(tt, err)
	})

	t.Run("PasswordNotInCacheAndSecretManagerSucceeds", func(tt *testing.T) {
		getPasswordForVSACluster := GetPasswordForVSACluster
		originalGcpService := GetGCPService
		defer func() {
			common.RemoveFromUserAuthCache(secretID)
			GetPasswordForVSACluster = getPasswordForVSACluster
			common.RemoveFromUserAuthCache(secretID)
			GetGCPService = originalGcpService
		}()

		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		GetPasswordForVSACluster = func(gcpService GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "secret-manager-password"}}, nil
		}
		password, err := GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "secret-manager-password", password)
		assert.NoError(tt, err)
	})

	t.Run("PasswordNotInCacheAndSecretManagerFails", func(tt *testing.T) {
		originalGcpService := GetGCPService
		getPasswordForVSACluster := GetPasswordForVSACluster
		defer func() {
			GetPasswordForVSACluster = getPasswordForVSACluster
			common.RemoveFromUserAuthCache(secretID)
			GetGCPService = originalGcpService
			common.RemoveFromUserAuthCache(secretID)
		}()
		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		GetPasswordForVSACluster = func(gcpService GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return nil, assert.AnError
		}
		password, err := GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "", password)
		assert.Error(tt, err)
	})
}

func Test_deleteVSAClusterPassword(t *testing.T) {
	secretID := "test-secret"

	t.Run("DeleteSecret called when GetSecretWithLatestVersion passes", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("DeleteSecret returns error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete error"))

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("Delete Secret fails if GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("get secret error"))

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "get secret error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("RemoveFromUserAuthCache returns false", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		// Mock the cache removal to return false
		origRemove := common.RemoveFromUserAuthCache
		defer func() { common.RemoveFromUserAuthCache = origRemove }()
		common.RemoveFromUserAuthCache = func(secretID string) bool { return false }

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err) // Should still return nil even if cache removal fails
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - GetSecretWithLatestVersion returns nil secret (no error), cache removed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// GetSecretWithLatestVersion returns nil, nil (no error, but secret is nil)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		// DeleteSecret should NOT be called since secret is nil
		// We don't set up expectation for DeleteSecret, so if it's called, the test will fail

		// Mock cache removal to return true (cache was removed)
		origRemove := common.RemoveFromUserAuthCache
		defer func() { common.RemoveFromUserAuthCache = origRemove }()
		common.RemoveFromUserAuthCache = func(secretID string) bool { return true }

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
		// Verify DeleteSecret was NOT called
		mockGCP.AssertNotCalled(t, "DeleteSecret", mock.Anything, mock.Anything)
	})

	t.Run("certificate is revoked - GetSecretWithLatestVersion returns nil secret (no error), cache not in cache", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// GetSecretWithLatestVersion returns nil, nil (no error, but secret is nil)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		// DeleteSecret should NOT be called since secret is nil
		// We don't set up expectation for DeleteSecret, so if it's called, the test will fail

		// Mock cache removal to return false (certificate was not in cache)
		origRemove := common.RemoveFromUserAuthCache
		defer func() { common.RemoveFromUserAuthCache = origRemove }()
		common.RemoveFromUserAuthCache = func(secretID string) bool { return false }

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
		// Verify DeleteSecret was NOT called
		mockGCP.AssertNotCalled(t, "DeleteSecret", mock.Anything, mock.Anything)
	})
}

func Test_GetCertificateAndPrivateKeyByID(t *testing.T) {
	caProjectID := "ca-project"
	smProjectID := "sm-project"
	region := "us-central1"
	caPoolName := "test-pool"
	certificateID := "test-cert"

	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"},
		}

		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", smProjectID, certificateID).Return(expectedSecret, nil)

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, expectedCert, resp.Certificate)
		assert.Equal(t, expectedSecret, resp.Secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(nil, errors.New("cert error"))

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to get certficate")
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate nil", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(nil, nil)

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to get certficate")
		mockGCP.AssertExpectations(t)
	})

	t.Run("secret error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", smProjectID, certificateID).Return(nil, errors.New("secret error"))

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to get secret")
		mockGCP.AssertExpectations(t)
	})

	t.Run("secret nil", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", smProjectID, certificateID).Return(nil, nil)

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to get secret")
		mockGCP.AssertExpectations(t)
	})

	t.Run("secret version nil", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
		secretWithoutVersion := &hyperscaler3.CustomSecret{SecretVersion: nil}
		mockGCP.On("GetCertificate", caProjectID, region, caPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", smProjectID, certificateID).Return(secretWithoutVersion, nil)

		resp, err := GetCertificateAndPrivateKeyByID(mockGCP, caProjectID, smProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "failed to get secret")
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetOrCreateCloudDNSRecord(t *testing.T) {
	recordName := "test-record"
	ipAddress := "1.2.3.4"

	t.Run("record exists", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedRecord := &hyperscaler3.CustomCloudDNSRecord{RecordName: recordName}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(expectedRecord, nil)

		record, err := GetOrCreateCloudDNSRecord(mockGCP, recordName, ipAddress)
		assert.NoError(t, err)
		assert.Equal(t, expectedRecord, record)
		mockGCP.AssertExpectations(t)
	})

	t.Run("record does not exist, create succeeds", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedRecord := &hyperscaler3.CustomCloudDNSRecord{RecordName: recordName}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, nil)
		mockGCP.On("CreateResourceRecordSet", mock.Anything, mock.Anything, ipAddress, recordName).Return(expectedRecord, nil)

		record, err := GetOrCreateCloudDNSRecord(mockGCP, recordName, ipAddress)
		assert.NoError(t, err)
		assert.Equal(t, expectedRecord, record)
		mockGCP.AssertExpectations(t)
	})

	t.Run("record does not exist, create fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, nil)
		mockGCP.On("CreateResourceRecordSet", mock.Anything, mock.Anything, ipAddress, recordName).Return(nil, errors.New("create failed"))

		record, err := GetOrCreateCloudDNSRecord(mockGCP, recordName, ipAddress)
		assert.Error(t, err)
		assert.Nil(t, record)
		mockGCP.AssertExpectations(t)
	})
}

func Test_DeleteCloudDNSRecord(t *testing.T) {
	recordName := "test-record"

	t.Run("record exists, delete succeeds", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(&hyperscaler3.CustomCloudDNSRecord{}, nil)
		mockGCP.On("DeleteResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil)

		err := DeleteCloudDNSRecord(mockGCP, recordName)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("record exists, delete fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(&hyperscaler3.CustomCloudDNSRecord{}, nil)
		mockGCP.On("DeleteResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(errors.New("delete failed"))

		err := DeleteCloudDNSRecord(mockGCP, recordName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
		mockGCP.AssertExpectations(t)
	})

	t.Run("record does not exist", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, nil)

		err := DeleteCloudDNSRecord(mockGCP, recordName)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetGCPService(t *testing.T) {
	// We can't test the actual GetGCPService function easily since it creates real GCP services
	// Instead, we'll test the logic by creating a simple test that verifies the function signature
	t.Run("function exists and has correct signature", func(t *testing.T) {
		// This test verifies that GetGCPService function exists and can be called
		// We don't test the actual implementation since it requires real GCP credentials
		assert.NotNil(t, GetGCPService)
	})
}

func Test_CreateNodeForProvider(t *testing.T) {
	t.Run("USER_CERTIFICATE auth type", func(t *testing.T) {
		input := NodeProviderInput{
			Nodes: []*datamodel.Node{
				{EndpointAddress: "1.2.3.4", HostDNSName: "host1.example.com"},
				{EndpointAddress: "5.6.7.8", HostDNSName: "host2.example.com"},
				{EndpointAddress: "", HostDNSName: "host3.example.com"}, // empty endpoint should be ignored
			},
			DeploymentName: "test-deployment",
			OntapCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-123",
				SecretID:      "secret-123",
				AuthType:      env.USER_CERTIFICATE,
			},
		}

		node := CreateNodeForProvider(input)
		assert.NotNil(t, node)
		assert.Equal(t, "test-deployment", node.DeploymentName)
		assert.Equal(t, "cert-123", node.CertificateID)
		assert.Equal(t, "secret-123", node.SecretID)
		assert.Equal(t, env.USER_CERTIFICATE, node.AuthType)
		assert.Equal(t, "", node.Password) // Password should be empty when not provided in input

		expectedMap := map[string]string{
			"1.2.3.4": "host1.example.com",
			"5.6.7.8": "host2.example.com",
		}
		assert.Equal(t, expectedMap, node.EndpointAddressesToHostNameMap)
	})

	t.Run("non-certificate auth type", func(t *testing.T) {
		input := NodeProviderInput{
			Nodes: []*datamodel.Node{
				{EndpointAddress: "1.2.3.4", HostDNSName: "host1.example.com"},
				{EndpointAddress: "5.6.7.8", HostDNSName: "host2.example.com"},
				{EndpointAddress: "", HostDNSName: "host3.example.com"}, // empty endpoint should be ignored
			},
			DeploymentName: "test-deployment",
			OntapCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "secret-123",
				AuthType: env.USERNAME_PWD,
			},
		}

		node := CreateNodeForProvider(input)
		assert.NotNil(t, node)
		assert.Equal(t, "test-deployment", node.DeploymentName)
		assert.Equal(t, "", node.CertificateID)
		assert.Equal(t, "secret-123", node.SecretID)
		assert.Equal(t, env.USERNAME_PWD, node.AuthType)
		assert.Equal(t, "test-password", node.Password)

		expectedMap := map[string]string{
			"1.2.3.4": "1.2.3.4",
			"5.6.7.8": "5.6.7.8",
		}
		assert.Equal(t, expectedMap, node.EndpointAddressesToHostNameMap)
	})
}

func Test_PrepareOperationID(t *testing.T) {
	t.Run("valid inputs", func(t *testing.T) {
		projectNumber := "123456789"
		locationId := "us-central1"
		jobId := "job-123"

		expected := "/v1beta/projects/123456789/locations/us-central1/operations/job-123"
		result := PrepareOperationID(projectNumber, locationId, jobId)
		assert.Equal(t, expected, result)
	})

	t.Run("empty project number", func(t *testing.T) {
		result := PrepareOperationID("", "us-central1", "job-123")
		assert.Equal(t, "", result)
	})

	t.Run("empty location id", func(t *testing.T) {
		result := PrepareOperationID("123456789", "", "job-123")
		assert.Equal(t, "", result)
	})

	t.Run("empty job id", func(t *testing.T) {
		result := PrepareOperationID("123456789", "us-central1", "")
		assert.Equal(t, "", result)
	})

	t.Run("all empty", func(t *testing.T) {
		result := PrepareOperationID("", "", "")
		assert.Equal(t, "", result)
	})
}

func Test_GenerateCSR(t *testing.T) {
	commonName := "test.example.com"
	domains := []string{"*.test.example.com", "test.example.com"}

	t.Run("success", func(t *testing.T) {
		csrDER, key, err := GenerateCSR(commonName, domains, true)
		assert.NoError(t, err)
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
		assert.Greater(t, len(csrDER), 0)
		assert.Equal(t, 4096, key.Size()*8) // Key size should be 4096 bits
	})

	t.Run("empty common name", func(t *testing.T) {
		csrDER, key, err := GenerateCSR("", domains, true)
		assert.NoError(t, err) // Should still work with empty common name
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
	})

	t.Run("empty domains", func(t *testing.T) {
		csrDER, key, err := GenerateCSR(commonName, []string{}, true)
		assert.NoError(t, err) // Should still work with empty domains
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
	})

	t.Run("nil domains", func(t *testing.T) {
		csrDER, key, err := GenerateCSR(commonName, nil, true)
		assert.NoError(t, err) // Should still work with nil domains
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
	})
}

func Test_GetCertificateFromCacheOrSecretManager_GetGCPServiceError(t *testing.T) {
	ctx := context.Background()
	certificateID := "test-cert-id"

	t.Run("GetGCPService fails", func(t *testing.T) {
		origGetGCPService := GetGCPService
		defer func() { GetGCPService = origGetGCPService }()

		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Contains(t, err.Error(), "gcp service error")
	})

	t.Run("GetCertificateAndPrivateKeyByID fails", func(t *testing.T) {
		origGetGCPService := GetGCPService
		origGetCertificateAndPrivateKeyByID := GetCertificateAndPrivateKeyByID
		defer func() {
			GetGCPService = origGetGCPService
			GetCertificateAndPrivateKeyByID = origGetCertificateAndPrivateKeyByID
		}()

		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		GetCertificateAndPrivateKeyByID = func(gcpService GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscaler3.CustomCertificateResponse, error) {
			return nil, errors.New("get certificate by id error")
		}

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Contains(t, err.Error(), "get certificate by id error")
	})
}

func Test_GetPasswordFromCacheOrSecretManager_GetGCPServiceError(t *testing.T) {
	ctx := context.Background()
	secretID := "test-secret-id"

	t.Run("GetGCPService fails", func(t *testing.T) {
		origGetGCPService := GetGCPService
		defer func() {
			GetGCPService = origGetGCPService
			common.RemoveFromUserAuthCache(secretID)
		}()

		GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}

		password, err := GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Error(t, err)
		assert.Equal(t, "", password)
		assert.Contains(t, err.Error(), "gcp service error")
	})
}

func Test_AdditionalMissingLineCoverage(t *testing.T) {
	t.Run("Test constant values", func(t *testing.T) {
		// Test constants for coverage
		assert.Equal(t, "CERTIFICATE REQUEST", CsrType)
		assert.Equal(t, "RSA PRIVATE KEY", RsaKeyType)
		assert.Equal(t, 0x80, DigitalSignature)
		assert.Equal(t, 0x20, KeyEncipherment)
	})

	t.Run("Test PrepareOperationID edge cases", func(t *testing.T) {
		// Test with various combinations of empty strings
		assert.Equal(t, "", PrepareOperationID("", "loc", "job"))
		assert.Equal(t, "", PrepareOperationID("proj", "", "job"))
		assert.Equal(t, "", PrepareOperationID("proj", "loc", ""))
		assert.Equal(t, "", PrepareOperationID("", "", "job"))
		assert.Equal(t, "", PrepareOperationID("proj", "", ""))
		assert.Equal(t, "", PrepareOperationID("", "loc", ""))
	})

	t.Run("Test MaxRetries variable", func(t *testing.T) {
		// Test that MaxRetries is accessible and has a value
		assert.GreaterOrEqual(t, MaxRetries, 0)
	})
}

func Test_GeneratePasswordForVSACluster_AllScenarios(t *testing.T) {
	userName := "test-user"

	t.Run("GetSecretWithLatestVersion succeeds - use existing", func(t *testing.T) {
		mockGCPService := new(MockGoogleServices)
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "existing-password"},
		}
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("Secret generation and cache addition success path", func(t *testing.T) {
		mockGCPService := new(MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "generated-password", nil
		}
		defer func() {
			utils.GenerateStrongPassword = originalGeneratePassword
			common.RemoveFromUserAuthCache(userName)
		}()

		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "generated-password"},
		}
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, userName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCPService.AssertExpectations(t)
	})
}

func Test_NewGcpServices(t *testing.T) {
	t.Run("creates new gcp services with context", func(t *testing.T) {
		ctx := context.Background()
		services := NewGcpServices(ctx)

		assert.NotNil(t, services)
		assert.Equal(t, ctx, services.Ctx)
		assert.NotNil(t, services.Logger)
		assert.NotNil(t, services.Retry)
	})
}

func Test_DeleteCertificateAndSecret(t *testing.T) {
	certificateID := "test-cert-id"
	certificate := &hyperscaler3.CustomCertificate{}
	secret := &hyperscaler3.CustomSecret{}

	t.Run("both certificate and secret are not nil, all succeed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := DeleteCertificateAndSecret(mockGCP, certificate, secret, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate revoke fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", fmt.Errorf("revoke error"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := DeleteCertificateAndSecret(mockGCP, certificate, nil, poolCredentials)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "revoke error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("secret delete fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete error"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := DeleteCertificateAndSecret(mockGCP, nil, secret, poolCredentials)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("both certificate and secret are nil", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := DeleteCertificateAndSecret(mockGCP, nil, nil, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetCertificateAndSecret(t *testing.T) {
	certificateID := "test-cert-id"
	expectedCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
	expectedSecret := &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}

	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(expectedSecret, nil)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, secret, err := GetCertificateAndSecret(mockGCP, poolCredentials)
		assert.NoError(t, err)
		assert.Equal(t, expectedCert, cert)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("GetCertificate fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(nil, fmt.Errorf("cert error"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, secret, err := GetCertificateAndSecret(mockGCP, poolCredentials)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})

	t.Run("GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(nil, fmt.Errorf("secret error"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		cert, secret, err := GetCertificateAndSecret(mockGCP, poolCredentials)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})
}

func Test_CreateCertificateInCAS(t *testing.T) {
	certificate := &hyperscaler3.CustomCertificate{
		SubjectCommonName: "test-cn",
		CertificateID:     "cert-123",
	}
	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("CreateCertificate", certificate).Return(certificate, nil)

		cert, err := CreateCertificateInCAS(mockGCP, certificate)
		assert.NoError(t, err)
		assert.Equal(t, certificate, cert)
		mockGCP.AssertExpectations(t)
	})

	t.Run("CreateCertificate fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("CreateCertificate", certificate).Return(nil, fmt.Errorf("create error"))

		cert, err := CreateCertificateInCAS(mockGCP, certificate)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Contains(t, err.Error(), "create error")
		mockGCP.AssertExpectations(t)
	})
}

func Test_CreatePrivateKeyInSecretManager(t *testing.T) {
	certificate := &hyperscaler3.CustomCertificate{
		SubjectCommonName: "test-cn",
		CertificateID:     "cert-123",
		Region:            "us-central1",
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)

	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedSecret := &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("CreateSecret", env.SecretManagerProjectID, certificate.Region, certificate.CertificateID, mock.Anything).Return(expectedSecret, nil)

		secret, err := CreatePrivateKeyInSecretManager(mockGCP, certificate, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("CreateSecret", env.SecretManagerProjectID, certificate.Region, certificate.CertificateID, mock.Anything).Return(nil, fmt.Errorf("create failed"))
		secret, err := CreatePrivateKeyInSecretManager(mockGCP, certificate, key)
		assert.Nil(t, secret)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "create failed")
		mockGCP.AssertExpectations(t)
	})
}

func Test_CreateCertificateInCASAndPrivateKeyInSM(t *testing.T) {
	certificateID := "test-cert-id"
	clusterName := "test-cluster"
	username := "test-user"
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	expectedDomains := []string{fmt.Sprintf("*.%s.%s", clusterName, env.VsaDeployedDnsName)}

	// Patch GenerateCSR and ValidateAndConvertCertParam

	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origGenerateCSR := GenerateCSR
		origValidate := common2.ValidateAndConvertCertParams
		origCreateCertificateInCAS := CreateCertificateInCAS
		origCreatePrivateKeyInSecretManager := CreatePrivateKeyInSecretManager

		defer func() {
			GenerateCSR = origGenerateCSR
			common2.ValidateAndConvertCertParams = origValidate
			CreateCertificateInCAS = origCreateCertificateInCAS
			CreatePrivateKeyInSecretManager = origCreatePrivateKeyInSecretManager
		}()
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			assert.Equal(t, username, commonName)
			assert.Equal(t, expectedDomains, domains)
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			assert.Equal(t, certificateID, param.CertificateID)
			return &hyperscaler3.CustomCertificate{CertificateID: certificateID}, nil
		}
		CreateCertificateInCAS = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate) (*hyperscaler3.CustomCertificate, error) {
			return cert, nil
		}
		CreatePrivateKeyInSecretManager = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, k *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}, nil
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		cert, secret, err := CreateCertificateInCASAndPrivateKeyInSM(mockGCP, certificateID, clusterName, username, nil, true)
		assert.NoError(t, err)
		assert.NotNil(t, cert)
		assert.NotNil(t, secret)
	})

	t.Run("GenerateCSR fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origGenerateCSR := GenerateCSR

		defer func() {
			GenerateCSR = origGenerateCSR
		}()
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			return nil, nil, fmt.Errorf("csr error")
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		cert, secret, err := CreateCertificateInCASAndPrivateKeyInSM(mockGCP, certificateID, clusterName, username, nil, true)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})

	t.Run("ValidateAndConvertCertParams fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origGenerateCSR := GenerateCSR
		origValidate := common2.ValidateAndConvertCertParams

		defer func() {
			GenerateCSR = origGenerateCSR
			common2.ValidateAndConvertCertParams = origValidate
		}()
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return nil, fmt.Errorf("validate error")
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		cert, secret, err := CreateCertificateInCASAndPrivateKeyInSM(mockGCP, certificateID, clusterName, username, nil, true)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})

	t.Run("CreateCertificateInCAS fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origGenerateCSR := GenerateCSR
		origValidate := common2.ValidateAndConvertCertParams
		origCreateCertificateInCAS := CreateCertificateInCAS

		defer func() {
			GenerateCSR = origGenerateCSR
			common2.ValidateAndConvertCertParams = origValidate
			CreateCertificateInCAS = origCreateCertificateInCAS
		}()
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return &hyperscaler3.CustomCertificate{CertificateID: certificateID}, nil
		}
		CreatePrivateKeyInSecretManager = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, k *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}, nil
		}
		CreateCertificateInCAS = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate) (*hyperscaler3.CustomCertificate, error) {
			return nil, fmt.Errorf("cas error")
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		cert, secret, err := CreateCertificateInCASAndPrivateKeyInSM(mockGCP, certificateID, clusterName, username, nil, true)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})

	t.Run("CreatePrivateKeyInSecretManager fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origGenerateCSR := GenerateCSR
		origValidate := common2.ValidateAndConvertCertParams
		origCreateCertificateInCAS := CreateCertificateInCAS
		origCreatePrivateKeyInSecretManager := CreatePrivateKeyInSecretManager

		defer func() {
			GenerateCSR = origGenerateCSR
			common2.ValidateAndConvertCertParams = origValidate
			CreateCertificateInCAS = origCreateCertificateInCAS
			CreatePrivateKeyInSecretManager = origCreatePrivateKeyInSecretManager
		}()
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return &hyperscaler3.CustomCertificate{CertificateID: certificateID}, nil
		}
		CreatePrivateKeyInSecretManager = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, k *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return nil, fmt.Errorf("sm error")
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		cert, secret, err := CreateCertificateInCASAndPrivateKeyInSM(mockGCP, certificateID, clusterName, username, nil, true)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
	})
}

func Test_GenerateAndCreateCertificateForVSACluster(t *testing.T) {
	certificateID := "test-cert-id"
	clusterName := "test-cluster"
	username := "test-user"

	t.Run("returns cached certificate and secret if found", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{
			SubjectCommonName:   "test-cn",
			PemCertificate:      "pem-cert",
			PemCertificateChain: []string{"chain1", "chain2"},
		}
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"},
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// Mock GetCertificate with the parameters that will be used (defaultCredentials has env vars)
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(expectedCert, nil)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, certificateID).Return(&hyperscaler3.CustomSecret{SecretVersion: expectedSecret.SecretVersion}, nil)
		poolCreds := &datamodel.PoolCredentials{CertificateID: certificateID}
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGCP, clusterName, username, poolCreds, true)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, expectedCert, resp.Certificate)
		assert.Equal(t, expectedSecret, resp.Secret)
	})

	t.Run("GetCertificateAndSecret returns error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// Mock GetCertificate to return error
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(nil, fmt.Errorf("get cert error"))
		poolCreds := &datamodel.PoolCredentials{CertificateID: certificateID}
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGCP, clusterName, username, poolCreds, true)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})

	t.Run("DeleteCertificateAndSecret returns error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// Mock GetCertificate to return existing cert (so certificate is non-nil)
		existingCert := &hyperscaler3.CustomCertificate{CertificateID: certificateID}
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(existingCert, nil)
		// Mock GetSecretWithLatestVersion to return nil, nil (404 handled - googleResourceNotFoundCheck returns nil for 404)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, certificateID).Return(nil, nil)
		// Now when certificate is non-nil and secret is nil, delete path will be triggered
		// Mock RevokeCertificate to return error (this will cause _deleteCertificateAndSecret to fail)
		// RevokeCertificate is called with a CustomCertificate object and returns (string, error)
		mockGCP.On("RevokeCertificate", mock.MatchedBy(func(cert *hyperscaler3.CustomCertificate) bool {
			return cert != nil && cert.CertificateID == certificateID
		})).Return("", fmt.Errorf("delete error"))
		poolCreds := &datamodel.PoolCredentials{CertificateID: certificateID}
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGCP, clusterName, username, poolCreds, true)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("CreateCertificateInCASAndPrivateKeyInSM returns error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// Mock GetCertificate to return nil (404 handled - cert doesn't exist)
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(nil, nil)
		// Mock GetSecretWithLatestVersion to return nil (404 handled - secret doesn't exist)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, certificateID).Return(nil, nil)
		// Mock DeleteSecret (called during cleanup)
		mockGCP.On("DeleteSecret", mock.Anything, certificateID).Return(nil)
		// Mock certificate creation functions to fail
		originalGenerateCSR := GenerateCSR
		originalValidateAndConvertCertParams := common2.ValidateAndConvertCertParams
		originalCreateCertificateInCAS := CreateCertificateInCAS
		defer func() {
			GenerateCSR = originalGenerateCSR
			common2.ValidateAndConvertCertParams = originalValidateAndConvertCertParams
			CreateCertificateInCAS = originalCreateCertificateInCAS
		}()
		key, _ := rsa.GenerateKey(rand.Reader, 2048)
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return &hyperscaler3.CustomCertificate{CertificateID: certificateID}, nil
		}
		CreateCertificateInCAS = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate) (*hyperscaler3.CustomCertificate, error) {
			return nil, fmt.Errorf("create error")
		}
		poolCreds := &datamodel.PoolCredentials{CertificateID: certificateID}
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGCP, clusterName, username, poolCreds, true)
		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.Contains(t, err.Error(), "create error")
	})

	t.Run("successfully creates new certificate and secret", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		expectedCert := &hyperscaler3.CustomCertificate{
			SubjectCommonName:   "test-cn",
			PemCertificate:      "pem-cert",
			PemCertificateChain: []string{"chain1", "chain2"},
		}
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"},
		}
		mockGCP.On("GetLogger").Return(log.NewLogger())
		// Mock GetCertificate to return nil (404 handled - cert doesn't exist)
		mockGCP.On("GetCertificate", env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID).Return(nil, nil)
		// Mock GetSecretWithLatestVersion to return nil (404 handled - secret doesn't exist)
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, certificateID).Return(nil, nil)
		// Mock DeleteSecret (called during cleanup)
		mockGCP.On("DeleteSecret", mock.Anything, certificateID).Return(nil)
		// Mock certificate creation functions
		originalGenerateCSR := GenerateCSR
		originalValidateAndConvertCertParams := common2.ValidateAndConvertCertParams
		originalCreateCertificateInCAS := CreateCertificateInCAS
		originalCreatePrivateKeyInSecretManager := CreatePrivateKeyInSecretManager
		defer func() {
			GenerateCSR = originalGenerateCSR
			common2.ValidateAndConvertCertParams = originalValidateAndConvertCertParams
			CreateCertificateInCAS = originalCreateCertificateInCAS
			CreatePrivateKeyInSecretManager = originalCreatePrivateKeyInSecretManager
		}()
		key, _ := rsa.GenerateKey(rand.Reader, 2048)
		GenerateCSR = func(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
			assert.Equal(t, username, commonName)
			return []byte("csr"), key, nil
		}
		common2.ValidateAndConvertCertParams = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			assert.Equal(t, certificateID, param.CertificateID)
			return expectedCert, nil
		}
		CreateCertificateInCAS = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate) (*hyperscaler3.CustomCertificate, error) {
			return expectedCert, nil
		}
		CreatePrivateKeyInSecretManager = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, k *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return expectedSecret, nil
		}
		poolCreds := &datamodel.PoolCredentials{CertificateID: certificateID}
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGCP, clusterName, username, poolCreds, true)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, expectedCert, resp.Certificate)
		assert.Equal(t, expectedSecret, resp.Secret)
	})
}

func Test_RevokeCertificateAndDeleteFromCacheAndSecretManager(t *testing.T) {
	certificateID := "test-cert-id"
	mockLogger := log.NewLogger()
	cert := &hyperscaler3.CustomCertificate{}
	secret := &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}

	t.Run("success", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		origDeleteCertificateAndSecret := DeleteCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
			DeleteCertificateAndSecret = origDeleteCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return cert, secret, nil
		}
		DeleteCertificateAndSecret = func(gcpService GoogleServices, certificate *hyperscaler3.CustomCertificate, secret *hyperscaler3.CustomSecret, poolCredentials *datamodel.PoolCredentials) error {
			return nil
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
	})

	t.Run("GetCertificateAndSecret fails - best-effort cleanup so pool deletion can continue", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("get cert error")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(nil, fmt.Errorf("secret not found"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("DeleteCertificateAndSecret fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		origDeleteCertificateAndSecret := DeleteCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
			DeleteCertificateAndSecret = origDeleteCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return cert, secret, nil
		}
		DeleteCertificateAndSecret = func(gcpService GoogleServices, certificate *hyperscaler3.CustomCertificate, secret *hyperscaler3.CustomSecret, poolCredentials *datamodel.PoolCredentials) error {
			return fmt.Errorf("delete error")
		}
		mockGCP.On("GetLogger").Return(mockLogger)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("RemoveFromCertAuthCache returns false", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		origDeleteCertificateAndSecret := DeleteCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
			DeleteCertificateAndSecret = origDeleteCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return cert, secret, nil
		}
		DeleteCertificateAndSecret = func(gcpService GoogleServices, certificate *hyperscaler3.CustomCertificate, secret *hyperscaler3.CustomSecret, poolCredentials *datamodel.PoolCredentials) error {
			return nil
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return false }
		mockGCP.On("GetLogger").Return(mockLogger)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
	})

	t.Run("certificate is revoked - secret exists and deleted successfully, cache removed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", env.SecretManagerProjectID, certificateID).Return(nil)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - secret exists and deleted successfully, cache not in cache", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return false }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", env.SecretManagerProjectID, certificateID).Return(nil)

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - secret not found, cache removed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(nil, fmt.Errorf("secret not found"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err) // Should not return error even if secret doesn't exist
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - secret not found, cache not in cache", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return false }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(nil, fmt.Errorf("secret not found"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err) // Should not return error even if secret doesn't exist
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - secret exists but DeleteSecret fails, cache removed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", env.SecretManagerProjectID, certificateID).Return(fmt.Errorf("delete secret failed"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err) // Should not return error even if DeleteSecret fails
		mockGCP.AssertExpectations(t)
	})

	t.Run("certificate is revoked - secret exists but DeleteSecret fails, cache not in cache", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("certificate is revoked and cannot be used")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return false }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", env.SecretManagerProjectID, certificateID).Return(fmt.Errorf("delete secret failed"))

		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err) // Should not return error even if DeleteSecret fails
		mockGCP.AssertExpectations(t)
	})

	t.Run("permission denied (403) - log and continue so pool deletion can succeed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		permissionDeniedErr := fmt.Errorf("googleapi: Error 403: Permission 'privateca.certificates.get' denied on 'projects/266893635349/locations/us-east4/caPools/vsa-pool-ca/certificates/gcnv-9471c3a41714607-cert', forbidden")
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, permissionDeniedErr
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(nil, fmt.Errorf("secret not found"))
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("permission denied (403) - secret exists and deleted successfully, cache removed", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
		origGetCertificateAndSecret := GetCertificateAndSecret
		defer func() {
			common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache
			GetCertificateAndSecret = origGetCertificateAndSecret
		}()
		GetCertificateAndSecret = func(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, fmt.Errorf("googleapi: Error 403: Permission denied, forbidden")
		}
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }
		mockGCP.On("GetLogger").Return(mockLogger)
		mockGCP.On("GetSecretWithLatestVersion", env.SecretManagerProjectID, certificateID).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGCP.On("DeleteSecret", env.SecretManagerProjectID, certificateID).Return(nil)
		poolCredentials := &datamodel.PoolCredentials{CertificateID: certificateID}
		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGCP, poolCredentials)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetProviderByNodeWithFastConnection(t *testing.T) {
	ctx := context.Background()

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USER_CERTIFICATE,
		}

		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return &models.Certificate{
				SignedCertificate:        "signed",
				InterMediateCertificates: []string{"intermediate"},
				CommonName:               "common",
				PrivateKey:               "key",
			}, nil
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USER_CERTIFICATE get certificate error", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			CertificateID:                  "cert-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USER_CERTIFICATE,
		}

		origGetCert := GetCertificateFromCacheOrSecretManager
		defer func() { GetCertificateFromCacheOrSecretManager = origGetCert }()
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
			return nil, fmt.Errorf("certificate not found")
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			SecretID:                       "secret-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USERNAME_PWD_SEC_MGR,
		}

		origGetPassword := GetPasswordFromCacheOrSecretManager
		defer func() { GetPasswordFromCacheOrSecretManager = origGetPassword }()
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "password123", nil
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("USERNAME_PWD_SEC_MGR get password error", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			SecretID:                       "secret-id",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USERNAME_PWD_SEC_MGR,
		}

		origGetPassword := GetPasswordFromCacheOrSecretManager
		defer func() { GetPasswordFromCacheOrSecretManager = origGetPassword }()
		GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", fmt.Errorf("secret not found")
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})

	t.Run("USERNAME_PWD auth type with password", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			Password:                       "password123",
			EndpointAddressesToHostNameMap: map[string]string{"1.2.3.4": "1.2.3.4"},
			AuthType:                       env.USERNAME_PWD, // Different auth type that uses password directly
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
	})

	t.Run("empty endpoint addresses map with endpoint address", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			Password:                       "password123",
			EndpointAddress:                "192.168.1.1",
			EndpointAddressesToHostNameMap: map[string]string{}, // Empty map
			AuthType:                       env.USERNAME_PWD,
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.NoError(t, err)
		assert.NotNil(t, provider)
		// Check that the endpoint address was added to the map
		assert.Equal(t, "192.168.1.1", node.EndpointAddressesToHostNameMap["192.168.1.1"])
	})

	t.Run("empty endpoint addresses map and empty endpoint address", func(t *testing.T) {
		node := &models.Node{
			Name:                           "node1",
			Password:                       "password123",
			EndpointAddress:                "",                  // Empty endpoint address
			EndpointAddressesToHostNameMap: map[string]string{}, // Empty map
			AuthType:                       env.USERNAME_PWD,
		}

		provider, err := GetProviderByNodeWithFastConnection(ctx, node)
		assert.Error(t, err)
		assert.Nil(t, provider)
	})
}

// Test_validateOCIVaultConfig exercises the pre-flight validator that
// _generatePasswordForVSAClusterOCI and _deletePasswordForVSAClusterOCI rely on
// to surface missing OCI vault env vars as ErrOCIClientInitializationError
// instead of an opaque OCI HTTP 400 from the SDK.
func Test_validateOCIVaultConfig(t *testing.T) {
	// Snapshot and restore the env-package globals so the subtests don't leak
	// state into each other or into other tests in this package.
	origVault := env.OCIVaultOCID
	origCompartment := env.OCICompartmentOCID
	origMasterKey := env.OCIMasterKeyOCID
	defer func() {
		env.OCIVaultOCID = origVault
		env.OCICompartmentOCID = origCompartment
		env.OCIMasterKeyOCID = origMasterKey
	}()

	t.Run("write mode — all three vars set returns nil", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		assert.NoError(t, _validateOCIVaultConfig(true))
	})

	t.Run("write mode — only vault set reports compartment + master key missing", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = ""
		env.OCIMasterKeyOCID = ""

		err := _validateOCIVaultConfig(true)
		if !assert.Error(t, err) {
			return
		}

		cerr, ok := err.(*coreerrors.CustomError)
		if !assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
			return
		}
		assert.Equal(t, coreerrors.ErrOCIClientInitializationError, cerr.TrackingID)
		// CustomError.Error() returns the canned message from the error map; the
		// dynamic list of missing env var names lives on OriginalErr.
		assert.NotNil(t, cerr.OriginalErr)
		origMsg := cerr.OriginalErr.Error()
		assert.Contains(t, origMsg, "OCI_COMPARTMENT_OCID")
		assert.Contains(t, origMsg, "OCI_MASTER_KEY_OCID")
		assert.NotContains(t, origMsg, "OCI_VAULT_OCID")
	})

	t.Run("write mode — all empty reports all three", func(t *testing.T) {
		env.OCIVaultOCID = ""
		env.OCICompartmentOCID = ""
		env.OCIMasterKeyOCID = ""

		err := _validateOCIVaultConfig(true)
		if !assert.Error(t, err) {
			return
		}
		cerr, ok := err.(*coreerrors.CustomError)
		if !assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
			return
		}
		assert.NotNil(t, cerr.OriginalErr)
		origMsg := cerr.OriginalErr.Error()
		assert.Contains(t, origMsg, "OCI_VAULT_OCID")
		assert.Contains(t, origMsg, "OCI_COMPARTMENT_OCID")
		assert.Contains(t, origMsg, "OCI_MASTER_KEY_OCID")
	})

	t.Run("read mode — only vault required, all empty reports vault only", func(t *testing.T) {
		env.OCIVaultOCID = ""
		env.OCICompartmentOCID = ""
		env.OCIMasterKeyOCID = ""

		err := _validateOCIVaultConfig(false)
		if !assert.Error(t, err) {
			return
		}

		cerr, ok := err.(*coreerrors.CustomError)
		if !assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
			return
		}
		assert.Equal(t, coreerrors.ErrOCIClientInitializationError, cerr.TrackingID)
		assert.NotNil(t, cerr.OriginalErr)
		origMsg := cerr.OriginalErr.Error()
		assert.Contains(t, origMsg, "OCI_VAULT_OCID")
		assert.NotContains(t, origMsg, "OCI_COMPARTMENT_OCID")
		assert.NotContains(t, origMsg, "OCI_MASTER_KEY_OCID")
	})

	t.Run("read mode — vault set returns nil regardless of write-only vars", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = ""
		env.OCIMasterKeyOCID = ""

		assert.NoError(t, _validateOCIVaultConfig(false))
	})
}

// ---------------------------------------------------------------------------
// _getPasswordFromCacheOrOCIVault
// ---------------------------------------------------------------------------
//
// The cache-or-vault helper short-circuits in three observable ways: (1) bad
// input, (2) cache hit, and (3) vault fetch (which we cover only for the
// "OCI client could not be initialised" failure path because exercising the
// full vault round-trip would require a real OCI HTTP fixture).

func Test_GetPasswordFromCacheOrOCIVault_NilRef(t *testing.T) {
	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), nil)
	assert.Empty(t, pw)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCI vault reference is empty")
}

func Test_GetPasswordFromCacheOrOCIVault_EmptyName(t *testing.T) {
	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: ""})
	assert.Empty(t, pw)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "OCI vault reference is empty")
}

func Test_GetPasswordFromCacheOrOCIVault_CacheHit(t *testing.T) {
	// Use a unique name to avoid colliding with cache entries left behind by
	// other tests in this package (the cache is process-scoped).
	const cacheKey = "test-getpasswordfromcache-hit-secret"

	origGet := common.GetFromUserAuthCache
	defer func() { common.GetFromUserAuthCache = origGet }()
	common.GetFromUserAuthCache = func(key string) (*models.UserCache, bool) {
		assert.Equal(t, cacheKey, key)
		return &models.UserCache{Password: "cached-pw"}, true
	}

	// Force a panic if the function falls through to GetOCIService — that
	// would mean the cache hit was ignored.
	origGetOCI := GetOCIService
	defer func() { GetOCIService = origGetOCI }()
	GetOCIService = func(ctx context.Context) (*oci2.OciServices, error) {
		t.Fatalf("OCI service must not be initialised when cache contains a value")
		return nil, nil
	}

	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: cacheKey})
	assert.NoError(t, err)
	assert.Equal(t, "cached-pw", pw)
}

func Test_GetPasswordFromCacheOrOCIVault_GetOCIServiceFails(t *testing.T) {
	const cacheKey = "test-getpasswordfromcache-svcfail-secret"

	origGet := common.GetFromUserAuthCache
	defer func() { common.GetFromUserAuthCache = origGet }()
	// Cache miss so the function falls through to GetOCIService.
	common.GetFromUserAuthCache = func(key string) (*models.UserCache, bool) {
		return nil, false
	}

	origGetOCI := GetOCIService
	defer func() { GetOCIService = origGetOCI }()
	GetOCIService = func(ctx context.Context) (*oci2.OciServices, error) {
		return nil, fmt.Errorf("oci client init failed")
	}

	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: cacheKey})
	assert.Empty(t, pw)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "oci client init failed")
}

// ---------------------------------------------------------------------------
// OCI test infrastructure
//
// mockOCIHTTPDispatcher and mockOCIServiceError mirror the private helpers in
// hyperscaler/oci/provider_test.go so that tests in this package can build
// fully-initialised OciServices backed by HTTP-level mocks without needing a
// real OCI environment or credentials.
// ---------------------------------------------------------------------------

type mockOCIHTTPDispatcher struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockOCIHTTPDispatcher) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

// mockOCIServiceError implements oci-go-sdk/v65/common.ServiceError so that
// common.IsServiceError returns true and GetHTTPStatusCode() is honoured.
type mockOCIServiceError struct {
	statusCode int
	code       string
	message    string
}

func (e *mockOCIServiceError) GetHTTPStatusCode() int  { return e.statusCode }
func (e *mockOCIServiceError) GetMessage() string      { return e.message }
func (e *mockOCIServiceError) GetCode() string         { return e.code }
func (e *mockOCIServiceError) GetOpcRequestID() string { return "" }
func (e *mockOCIServiceError) Error() string {
	return fmt.Sprintf("Service error: %s (status %d)", e.message, e.statusCode)
}

// mockOCIJSONResponse builds a minimal *http.Response carrying a JSON body.
func mockOCIJSONResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type":   []string{"application/json"},
			"Opc-Request-Id": []string{"test-req-id"},
		},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/mock"}},
	}
}

// testOCIRSAPrivateKeyPEM generates a throwaway RSA private key in PEM format
// for use with ocicommon.NewRawConfigurationProvider in tests.
func testOCIRSAPrivateKeyPEM(t *testing.T) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := x509.MarshalPKCS1PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}))
}

// newTestOciServicesForHyperscaler creates an OciServices value backed by mock
// HTTP dispatchers. Pass nil for a dispatcher you don't need in a given test.
//
//   - vaultDispatcher   → intercepted by vault.VaultsClient (CreateSecret, GetSecret,
//     ScheduleSecretDeletion)
//   - secretsDispatcher → intercepted by secrets.SecretsClient (GetSecretBundleByName,
//     GetSecretBundle)
func newTestOciServicesForHyperscaler(t *testing.T, vaultDispatcher, secretsDispatcher *mockOCIHTTPDispatcher) oci2.OciServices {
	t.Helper()
	ctx := context.Background()
	pemKey := testOCIRSAPrivateKeyPEM(t)

	configProvider := ocicommon.NewRawConfigurationProvider(
		"ocid1.tenancy.oc1..test",
		"ocid1.user.oc1..test",
		"us-ashburn-1",
		"aa:bb:cc:dd:ee:ff:00:11",
		pemKey,
		nil,
	)

	vaultCl, err := vault.NewVaultsClientWithConfigurationProvider(configProvider)
	require.NoError(t, err)
	if vaultDispatcher != nil {
		vaultCl.HTTPClient = vaultDispatcher
	}

	secretsCl, err := secrets.NewSecretsClientWithConfigurationProvider(configProvider)
	require.NoError(t, err)
	if secretsDispatcher != nil {
		secretsCl.HTTPClient = secretsDispatcher
	}

	return oci2.OciServices{
		Ctx:             ctx,
		Logger:          log.NewLogger(),
		AdminOCIService: oci2.NewAdminOCIService(vaultCl, secretsCl),
	}
}

// ---------------------------------------------------------------------------
// _generatePasswordForVSAClusterOCI
// ---------------------------------------------------------------------------

func Test_GeneratePasswordForVSAClusterOCI(t *testing.T) {
	origVault := env.OCIVaultOCID
	origCompartment := env.OCICompartmentOCID
	origMasterKey := env.OCIMasterKeyOCID
	defer func() {
		env.OCIVaultOCID = origVault
		env.OCICompartmentOCID = origCompartment
		env.OCIMasterKeyOCID = origMasterKey
	}()

	const secretName = "test-generate-ontap-admin-secret"

	t.Run("config invalid — returns ErrOCIClientInitializationError before any OCI call", func(t *testing.T) {
		env.OCIVaultOCID = ""
		env.OCICompartmentOCID = ""
		env.OCIMasterKeyOCID = ""

		svc := newTestOciServicesForHyperscaler(t, nil, nil)
		result, err := _generatePasswordForVSAClusterOCI(svc, secretName)
		assert.Nil(t, result)
		require.Error(t, err)

		cerr, ok := err.(*coreerrors.CustomError)
		if assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
			assert.Equal(t, coreerrors.ErrOCIClientInitializationError, cerr.TrackingID)
		}
	})

	t.Run("GetSecretByName fails — error propagated", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusForbidden, code: "NotAuthorized", message: "access denied"}
			},
		}
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		result, err := _generatePasswordForVSAClusterOCI(svc, secretName)
		assert.Nil(t, result)
		assert.Error(t, err)
	})

	t.Run("secret already exists — existing secret returned without CreateSecret", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		encodedPW := base64.StdEncoding.EncodeToString([]byte("existing-password"))
		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return mockOCIJSONResponse(http.StatusOK, `{
					"secretId":      "ocid1.vaultsecret.oc1..existing",
					"versionNumber": 1,
					"secretBundleContent": {
						"contentType": "BASE64",
						"content":     "`+encodedPW+`"
					}
				}`), nil
			},
		}
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		result, err := _generatePasswordForVSAClusterOCI(svc, secretName)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, "existing-password", result.Value)
		assert.Equal(t, "ocid1.vaultsecret.oc1..existing", result.Ocid)
	})

	t.Run("secret not found — GenerateStrongPassword fails — error propagated", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusNotFound, code: "NotAuthorizedOrNotFound", message: "not found"}
			},
		}

		origGeneratePW := utils.GenerateStrongPassword
		defer func() { utils.GenerateStrongPassword = origGeneratePW }()
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "", fmt.Errorf("entropy source failed")
		}

		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		result, err := _generatePasswordForVSAClusterOCI(svc, secretName)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "entropy source failed")
	})

	t.Run("secret not found — CreateSecret fails — error propagated", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		origGeneratePW := utils.GenerateStrongPassword
		defer func() { utils.GenerateStrongPassword = origGeneratePW }()
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "StrongPassword1!", nil
		}

		// secrets client: 404 → GetSecretByName returns nil, nil
		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusNotFound, code: "NotAuthorizedOrNotFound", message: "not found"}
			},
		}
		// vault client: error on CreateSecret
		vaultDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusBadRequest, code: "InvalidParameter", message: "vault invalid"}
			},
		}

		svc := newTestOciServicesForHyperscaler(t, vaultDispatcher, secretsDispatcher)
		result, err := _generatePasswordForVSAClusterOCI(svc, secretName)
		assert.Nil(t, result)
		assert.Error(t, err)
	})

	t.Run("success — new secret created and password added to cache", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"
		env.OCICompartmentOCID = "ocid1.compartment.oc1..test"
		env.OCIMasterKeyOCID = "ocid1.key.oc1..test"

		const createdOCID = "ocid1.vaultsecret.oc1..created"
		const createdName = "test-generate-success-secret"

		origGeneratePW := utils.GenerateStrongPassword
		defer func() { utils.GenerateStrongPassword = origGeneratePW }()
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "StrongPassword1!", nil
		}
		defer common.RemoveFromUserAuthCache(createdName)

		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusNotFound, code: "NotAuthorizedOrNotFound", message: "not found"}
			},
		}
		vaultDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return mockOCIJSONResponse(http.StatusOK, `{
					"id":             "`+createdOCID+`",
					"secretName":     "`+createdName+`",
					"lifecycleState": "ACTIVE",
					"currentVersionNumber": 1
				}`), nil
			},
		}

		svc := newTestOciServicesForHyperscaler(t, vaultDispatcher, secretsDispatcher)
		result, err := _generatePasswordForVSAClusterOCI(svc, createdName)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, createdOCID, result.Ocid)
		assert.Equal(t, "StrongPassword1!", result.Value)
		assert.Equal(t, createdName, result.Name)

		// Cache must be populated after successful creation.
		cached, exists := common.GetFromUserAuthCache(createdName)
		assert.True(t, exists)
		if assert.NotNil(t, cached) {
			assert.Equal(t, "StrongPassword1!", cached.Password)
		}
	})
}

// ---------------------------------------------------------------------------
// _deletePasswordForVSAClusterOCI
// ---------------------------------------------------------------------------

func Test_DeletePasswordForVSAClusterOCI(t *testing.T) {
	origVault := env.OCIVaultOCID
	defer func() { env.OCIVaultOCID = origVault }()

	const secretOCID = "ocid1.vaultsecret.oc1..todelete"
	const scheduleDeletionPath = "scheduleDeletion"

	t.Run("config invalid — returns ErrOCIClientInitializationError before any OCI call", func(t *testing.T) {
		env.OCIVaultOCID = ""

		svc := newTestOciServicesForHyperscaler(t, nil, nil)
		err := _deletePasswordForVSAClusterOCI(svc, "any-secret")
		require.Error(t, err)

		cerr, ok := err.(*coreerrors.CustomError)
		if assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
			assert.Equal(t, coreerrors.ErrOCIClientInitializationError, cerr.TrackingID)
		}
	})

	t.Run("GetSecretByName fails — error propagated", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"

		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusForbidden, code: "NotAuthorized", message: "access denied"}
			},
		}
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		err := _deletePasswordForVSAClusterOCI(svc, "any-secret")
		assert.Error(t, err)
	})

	t.Run("secret not found — no-op, cache entry cleared", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"

		const name = "test-delete-notfound-secret"
		common.AddToUserAuthCache(name, "some-pw")

		// 404 → GetSecretByName returns nil, nil → DeleteSecret not called
		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return nil, &mockOCIServiceError{statusCode: http.StatusNotFound, code: "NotAuthorizedOrNotFound", message: "not found"}
			},
		}
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		err := _deletePasswordForVSAClusterOCI(svc, name)
		assert.NoError(t, err)

		_, exists := common.GetFromUserAuthCache(name)
		assert.False(t, exists, "cache entry should be removed even when secret is not in OCI Vault")
	})

	t.Run("DeleteSecret fails — error propagated", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"

		const name = "test-delete-delfail-secret"
		encodedPW := base64.StdEncoding.EncodeToString([]byte("pw"))

		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return mockOCIJSONResponse(http.StatusOK, `{
					"secretId":      "`+secretOCID+`",
					"versionNumber": 1,
					"secretBundleContent": {
						"contentType": "BASE64",
						"content":     "`+encodedPW+`"
					}
				}`), nil
			},
		}
		vaultDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, scheduleDeletionPath) {
					return nil, &mockOCIServiceError{statusCode: http.StatusConflict, code: "Conflict", message: "deletion conflict"}
				}
				return mockOCIJSONResponse(http.StatusOK, `{
					"id":             "`+secretOCID+`",
					"secretName":     "`+name+`",
					"lifecycleState": "ACTIVE"
				}`), nil
			},
		}
		svc := newTestOciServicesForHyperscaler(t, vaultDispatcher, secretsDispatcher)
		err := _deletePasswordForVSAClusterOCI(svc, name)
		assert.Error(t, err)
	})

	t.Run("success — secret deleted, cache cleared", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"

		const name = "test-delete-success-secret"
		common.AddToUserAuthCache(name, "cached-pw")

		encodedPW := base64.StdEncoding.EncodeToString([]byte("cached-pw"))
		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return mockOCIJSONResponse(http.StatusOK, `{
					"secretId":      "`+secretOCID+`",
					"versionNumber": 1,
					"secretBundleContent": {
						"contentType": "BASE64",
						"content":     "`+encodedPW+`"
					}
				}`), nil
			},
		}
		vaultDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, scheduleDeletionPath) {
					return mockOCIJSONResponse(http.StatusOK, `{}`), nil
				}
				return mockOCIJSONResponse(http.StatusOK, `{
					"id":             "`+secretOCID+`",
					"secretName":     "`+name+`",
					"lifecycleState": "ACTIVE"
				}`), nil
			},
		}
		svc := newTestOciServicesForHyperscaler(t, vaultDispatcher, secretsDispatcher)
		err := _deletePasswordForVSAClusterOCI(svc, name)
		assert.NoError(t, err)

		_, exists := common.GetFromUserAuthCache(name)
		assert.False(t, exists, "cache entry should be removed on successful deletion")
	})

	t.Run("success — cache miss is not an error", func(t *testing.T) {
		env.OCIVaultOCID = "ocid1.vault.oc1..test"

		const name = "test-delete-cachemiss-secret"
		// No cache entry pre-populated — function should still return nil.

		encodedPW := base64.StdEncoding.EncodeToString([]byte("pw"))
		secretsDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				return mockOCIJSONResponse(http.StatusOK, `{
					"secretId":      "`+secretOCID+`",
					"versionNumber": 1,
					"secretBundleContent": {
						"contentType": "BASE64",
						"content":     "`+encodedPW+`"
					}
				}`), nil
			},
		}
		vaultDispatcher := &mockOCIHTTPDispatcher{
			doFunc: func(req *http.Request) (*http.Response, error) {
				if strings.Contains(req.URL.Path, scheduleDeletionPath) {
					return mockOCIJSONResponse(http.StatusOK, `{}`), nil
				}
				return mockOCIJSONResponse(http.StatusOK, `{
					"id":             "`+secretOCID+`",
					"secretName":     "`+name+`",
					"lifecycleState": "ACTIVE"
				}`), nil
			},
		}
		svc := newTestOciServicesForHyperscaler(t, vaultDispatcher, secretsDispatcher)
		err := _deletePasswordForVSAClusterOCI(svc, name)
		assert.NoError(t, err)
	})
}

// ---------------------------------------------------------------------------
// _getPasswordFromCacheOrOCIVault — additional paths
//
// The tests below cover paths introduced in this branch that are not
// exercised by the earlier Test_GetPasswordFromCacheOrOCIVault_* tests:
//   - _validateOCIVaultConfig fails after GetOCIService succeeds
//   - Vault fetch succeeds (full happy path through GetSecretByName)
//   - Vault fetch returns nil (secret not found)
// ---------------------------------------------------------------------------

func Test_GetPasswordFromCacheOrOCIVault_ValidateConfigFails(t *testing.T) {
	const cacheKey = "test-cache-validate-fail-secret"

	origVault := env.OCIVaultOCID
	defer func() { env.OCIVaultOCID = origVault }()
	env.OCIVaultOCID = "" // triggers _validateOCIVaultConfig(false) failure

	origGet := common.GetFromUserAuthCache
	defer func() { common.GetFromUserAuthCache = origGet }()
	common.GetFromUserAuthCache = func(key string) (*models.UserCache, bool) {
		return nil, false // cache miss so we proceed past cache check
	}

	origGetOCI := GetOCIService
	defer func() { GetOCIService = origGetOCI }()
	GetOCIService = func(ctx context.Context) (*oci2.OciServices, error) {
		return &oci2.OciServices{Ctx: ctx, Logger: log.NewLogger()}, nil
	}

	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: cacheKey})
	assert.Empty(t, pw)
	require.Error(t, err)

	cerr, ok := err.(*coreerrors.CustomError)
	if assert.True(t, ok, "expected *coreerrors.CustomError, got %T", err) {
		assert.Equal(t, coreerrors.ErrOCIClientInitializationError, cerr.TrackingID)
	}
}

func Test_GetPasswordFromCacheOrOCIVault_VaultFetchSuccess(t *testing.T) {
	const cacheKey = "test-cache-vaultfetch-success-secret"

	origVault := env.OCIVaultOCID
	defer func() {
		env.OCIVaultOCID = origVault
		common.RemoveFromUserAuthCache(cacheKey)
	}()
	env.OCIVaultOCID = "ocid1.vault.oc1..test"

	origGet := common.GetFromUserAuthCache
	defer func() { common.GetFromUserAuthCache = origGet }()
	common.GetFromUserAuthCache = func(key string) (*models.UserCache, bool) {
		return nil, false // cache miss — fall through to OCI Vault
	}

	encodedPW := base64.StdEncoding.EncodeToString([]byte("vault-fetched-pw"))
	secretsDispatcher := &mockOCIHTTPDispatcher{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return mockOCIJSONResponse(http.StatusOK, `{
				"secretId":      "ocid1.vaultsecret.oc1..found",
				"versionNumber": 1,
				"secretBundleContent": {
					"contentType": "BASE64",
					"content":     "`+encodedPW+`"
				}
			}`), nil
		},
	}

	origGetOCI := GetOCIService
	defer func() { GetOCIService = origGetOCI }()
	GetOCIService = func(ctx context.Context) (*oci2.OciServices, error) {
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		return &svc, nil
	}

	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: cacheKey})
	assert.NoError(t, err)
	assert.Equal(t, "vault-fetched-pw", pw)
}

func Test_GetPasswordFromCacheOrOCIVault_SecretNotFound(t *testing.T) {
	const cacheKey = "test-cache-secretnotfound-secret"

	origVault := env.OCIVaultOCID
	defer func() { env.OCIVaultOCID = origVault }()
	env.OCIVaultOCID = "ocid1.vault.oc1..test"

	origGet := common.GetFromUserAuthCache
	defer func() { common.GetFromUserAuthCache = origGet }()
	common.GetFromUserAuthCache = func(key string) (*models.UserCache, bool) {
		return nil, false
	}

	// 404 → GetSecretByName returns nil, nil → "empty or not found" error
	secretsDispatcher := &mockOCIHTTPDispatcher{
		doFunc: func(req *http.Request) (*http.Response, error) {
			return nil, &mockOCIServiceError{statusCode: http.StatusNotFound, code: "NotAuthorizedOrNotFound", message: "not found"}
		},
	}

	origGetOCI := GetOCIService
	defer func() { GetOCIService = origGetOCI }()
	GetOCIService = func(ctx context.Context) (*oci2.OciServices, error) {
		svc := newTestOciServicesForHyperscaler(t, nil, secretsDispatcher)
		return &svc, nil
	}

	pw, err := _getPasswordFromCacheOrOCIVault(context.Background(), &datamodel.ExternalCredRef{Name: cacheKey})
	assert.Empty(t, pw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty or not found")
}

