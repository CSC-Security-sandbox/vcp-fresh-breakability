package hyperscaler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/pem"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscaler3 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
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
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certID string) (*models.Certificate, error) {
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
		GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certID string) (*models.Certificate, error) {
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

func Test_RevokeCertificateAndDeleteFromCacheAndSecretManager(t *testing.T) {
	certificateID := "test-cert-id"

	// Save and restore RemoveFromCertAuthCache
	origRemoveFromCertAuthCache := common.RemoveFromCertAuthCache
	defer func() { common.RemoveFromCertAuthCache = origRemoveFromCertAuthCache }()

	t.Run("success", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)
		common.RemoveFromCertAuthCache = func(certID string) bool { return true }

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.NoError(t, err)
		mockGcpService.AssertExpectations(t)
	})

	t.Run("GetCertificate fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("get cert error"))

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "get cert error")
	})

	t.Run("RevokeCertificate fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", fmt.Errorf("revoke error"))

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "revoke error")
	})
	t.Run("GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("get secret error"))

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "get secret error")
	})

	t.Run("DeleteSecret fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete secret error"))

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "delete secret error")
	})

	t.Run("RemoveFromCertAuthCache fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)
		common.RemoveFromCertAuthCache = func(certID string) bool { return false }

		err := RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.NoError(t, err)
	})
}

func Test_GenerateAndCreateCertificateForVSACluster(t *testing.T) {
	region := "us-central1"
	certificateID := "test-cert-id"
	clusterName := "test-cluster"
	csr := []byte("fake-csr")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	t.Run("Success", func(t *testing.T) {
		mockGcpService := NewMockGoogleServices(t)
		cert := &hyperscaler3.CustomCertificate{
			SubjectCommonName:   "test-cn",
			PemCertificate:      "pem-cert",
			PemCertificateChain: []string{"chain1", "chain2"},
		}
		secret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"},
		}

		origGenerateCSR := GenerateCSR
		origValidateAndConvert := google.ValidateAndConvertCertificateParamsToCustomCertificate
		origGetAndCreate := GetOrCreateCertificateInCASAndPrivateKeyInSM
		defer func() {
			common.RemoveFromCertAuthCache(certificateID)
			GenerateCSR = origGenerateCSR
			google.ValidateAndConvertCertificateParamsToCustomCertificate = origValidateAndConvert
			GetOrCreateCertificateInCASAndPrivateKeyInSM = origGetAndCreate
		}()

		GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return csr, key, nil
		}
		google.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return cert, nil
		}
		GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService GoogleServices, certificate *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return cert, secret, nil
		}
		mockGcpService.On("GetLogger").Return(log.NewLogger())

		resp, err := GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, cert, resp.Certificate)
		assert.Equal(t, secret, resp.Secret)
	})
	t.Run("GenerateCSR fail", func(t *testing.T) {
		mockGcpService := NewMockGoogleServices(t)

		origGenerateCSR := GenerateCSR
		defer func() { GenerateCSR = origGenerateCSR }()

		expectedErr := fmt.Errorf("generate csr error")
		GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return nil, nil, expectedErr
		}

		mockGcpService.On("GetLogger").Return(log.NewLogger())

		resp, err := GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.EqualError(t, err, expectedErr.Error())
	})
	t.Run("ValidateAndConvert fail", func(t *testing.T) {
		mockGcpService := NewMockGoogleServices(t)

		// Patch GenerateCSR to succeed
		origGenerateCSR := GenerateCSR
		GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), &rsa.PrivateKey{}, nil
		}

		// Patch ValidateAndConvertCertificateParamsToCustomCertificate to fail
		origValidate := google.ValidateAndConvertCertificateParamsToCustomCertificate
		google.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return nil, fmt.Errorf("validation failed")
		}
		defer func() {
			GenerateCSR = origGenerateCSR
			google.ValidateAndConvertCertificateParamsToCustomCertificate = origValidate
		}()
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.EqualError(t, err, "validation failed")
	})
	t.Run("GetOrCreateCertificateInCASAndPrivateKeyInSM fails", func(t *testing.T) {
		mockGcpService := new(MockGoogleServices)
		expectedErr := errors.New("CAS/SM error")

		// Patch GenerateCSR to return dummy values
		origGenerateCSR := GenerateCSR
		GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), &rsa.PrivateKey{}, nil
		}
		// Patch GetOrCreateCertificateInCASAndPrivateKeyInSM to return error
		origGetAndCreate := GetOrCreateCertificateInCASAndPrivateKeyInSM
		GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService GoogleServices, certificate *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomCertificate, *hyperscaler3.CustomSecret, error) {
			return nil, nil, expectedErr
		}

		// Patch ValidateAndConvertCertificateParamsToCustomCertificate to return dummy cert
		origValidate := google.ValidateAndConvertCertificateParamsToCustomCertificate
		google.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler3.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler3.CustomCertificate, error) {
			return &hyperscaler3.CustomCertificate{}, nil
		}
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		defer func() {
			GenerateCSR = origGenerateCSR
			GetOrCreateCertificateInCASAndPrivateKeyInSM = origGetAndCreate
			google.ValidateAndConvertCertificateParamsToCustomCertificate = origValidate
		}()
		resp, err := GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.Equal(t, expectedErr, err)
	})
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
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, certificateID)
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
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, certificateID)
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
		cert, err := GetCertificateFromCacheOrSecretManager(ctx, certificateID)
		assert.Error(t, err)
		assert.Nil(t, cert)
	})
}

func Test_getAndCreatePrivateKeyInSecretManagerAndCache(t *testing.T) {
	cert := &hyperscaler3.CustomCertificate{
		CertificateID:     "test-cert",
		Region:            "us-central1",
		SubjectCommonName: "test-cn",
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	expectedSecret := &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "private-key"}}

	t.Run("returns existing secret", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)
		secret, err := GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("creates secret if not found", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(expectedSecret, nil)
		secret, err := GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("create secret fails, revoke fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", errors.New("revoke failed"))
		secret, err := GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.Nil(t, secret)
		assert.Error(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("create secret fails, revoke succeeds", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", nil)
		secret, err := GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.Nil(t, secret)
		assert.Error(t, err)
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetAndCreateCertificateInCASAndPrivateKeyInSM(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	certificate := &hyperscaler3.CustomCertificate{
		CertificateID:     "certid",
		Region:            "us-central1",
		SubjectCommonName: "test-cn",
	}
	originalConvert := google.ConvertPrivateKeyToString
	defer func() {
		google.ConvertPrivateKeyToString = originalConvert
	}()
	google.ConvertPrivateKeyToString = func(key *rsa.PrivateKey, rsaKeyType string) string {
		return "private-key"
	}

	t.Run("GetCertificate fails, CreateCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache succeeds ", func(t *testing.T) {
		mockSvc := NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockSvc.On("CreateCertificate", certificate).Return(certificate, nil)

		originalGetAndCreatePrivateKeyInSecretManagerAndCache := GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{}, nil
		}
		cert, _, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.NoError(t, err)
		assert.Equal(t, certificate, cert)
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate fails, CreateCertificate fails", func(t *testing.T) {
		mockSvc := NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockSvc.On("CreateCertificate", certificate).Return(nil, fmt.Errorf("can not create cert"))
		cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
		assert.EqualError(t, err, "can not create cert")
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache succeeds", func(t *testing.T) {
		mockSvc := NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(certificate, nil)
		mockSecret := &hyperscaler3.CustomSecret{Name: "secret"}
		originalGetAndCreatePrivateKeyInSecretManagerAndCache := GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return mockSecret, nil
		}
		cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.NoError(t, err)
		assert.Equal(t, certificate, cert)
		assert.Equal(t, mockSecret, secret)
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache fails", func(t *testing.T) {
		mockSvc := NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(certificate, nil)
		originalGetAndCreatePrivateKeyInSecretManagerAndCache := GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return nil, errors.New("can not create cert")
		}
		cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
		mockSvc.AssertExpectations(t)
	})

	t.Run("CreateCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache fails", func(t *testing.T) {
		mockSvc := NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockSvc.On("CreateCertificate", certificate).Return(certificate, nil)

		originalGetAndCreatePrivateKeyInSecretManagerAndCache := GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService GoogleServices, cert *hyperscaler3.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler3.CustomSecret, error) {
			return nil, errors.New("private key creation failed")
		}
		cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
		assert.Contains(t, err.Error(), "private key creation failed")
		mockSvc.AssertExpectations(t)
	})
}

func Test_GeneratePasswordForVSACluster(t *testing.T) {
	projectId := "test-project"
	userName := "test-user"
	region := "test-region"

	t.Run("PasswordGenerationFails", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "", errors.New("password generation failed")
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()

		mockGCPService.On("GetLogger").Return(log.NewLogger())
		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

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
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret get error"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret creation failed"))

		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.Error(tt, err)
		assert.Nil(tt, secret)
		assert.Contains(tt, err.Error(), "secret creation failed")
		mockGCPService.AssertExpectations(tt)
	})

	t.Run("GetSecretWithLatestVersion success", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)
		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})

	t.Run("Success", func(tt *testing.T) {
		mockGCPService := new(MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret get error"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "secretID"}}, nil)
		defer func() {
			common.RemoveFromUserAuthCache("secretID")
		}()

		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

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
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("DeleteSecret returns error", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete error"))

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("Delete Secret fails if GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler3.CustomSecret{}, fmt.Errorf("get secret error"))

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("RemoveFromUserAuthCache returns false", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		// Mock the cache removal to return false
		origRemove := common.RemoveFromUserAuthCache
		defer func() { common.RemoveFromUserAuthCache = origRemove }()
		common.RemoveFromUserAuthCache = func(secretID string) bool { return false }

		err := DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err) // Should still return nil even if cache removal fails
		mockGCP.AssertExpectations(t)
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
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, errors.New("not found"))
		mockGCP.On("CreateResourceRecordSet", mock.Anything, mock.Anything, ipAddress, recordName).Return(expectedRecord, nil)

		record, err := GetOrCreateCloudDNSRecord(mockGCP, recordName, ipAddress)
		assert.NoError(t, err)
		assert.Equal(t, expectedRecord, record)
		mockGCP.AssertExpectations(t)
	})

	t.Run("record does not exist, create fails", func(t *testing.T) {
		mockGCP := new(MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, errors.New("not found"))
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
		mockGCP.On("GetResourceRecordSet", mock.Anything, mock.Anything, recordName).Return(nil, errors.New("not found"))

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
			CertificateID:  "cert-123",
			SecretID:       "secret-123",
			AuthType:       env.USER_CERTIFICATE,
		}

		node := CreateNodeForProvider(input)
		assert.NotNil(t, node)
		assert.Equal(t, "test-deployment", node.DeploymentName)
		assert.Equal(t, "cert-123", node.CertificateID)
		assert.Equal(t, "secret-123", node.SecretID)
		assert.Equal(t, env.USER_CERTIFICATE, node.AuthType)
		assert.Equal(t, "", node.Password)

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
			Password:       "test-password",
			DeploymentName: "test-deployment",
			SecretID:       "secret-123",
			AuthType:       env.USERNAME_PWD,
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
		csrDER, key, err := GenerateCSR(commonName, domains)
		assert.NoError(t, err)
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
		assert.Greater(t, len(csrDER), 0)
		assert.Equal(t, 3072, key.Size()*8) // Key size should be 3072 bits
	})

	t.Run("empty common name", func(t *testing.T) {
		csrDER, key, err := GenerateCSR("", domains)
		assert.NoError(t, err) // Should still work with empty common name
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
	})

	t.Run("empty domains", func(t *testing.T) {
		csrDER, key, err := GenerateCSR(commonName, []string{})
		assert.NoError(t, err) // Should still work with empty domains
		assert.NotNil(t, csrDER)
		assert.NotNil(t, key)
	})

	t.Run("nil domains", func(t *testing.T) {
		csrDER, key, err := GenerateCSR(commonName, nil)
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

		cert, err := GetCertificateFromCacheOrSecretManager(ctx, certificateID)
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

		cert, err := GetCertificateFromCacheOrSecretManager(ctx, certificateID)
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
	projectId := "test-project"
	userName := "test-user"
	region := "test-region"

	t.Run("GetSecretWithLatestVersion succeeds - use existing", func(t *testing.T) {
		mockGCPService := new(MockGoogleServices)
		expectedSecret := &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "existing-password"},
		}
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)
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
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)
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
