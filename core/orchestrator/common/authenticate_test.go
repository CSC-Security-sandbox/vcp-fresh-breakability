package common

import (
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateEnvironmentVariables(t *testing.T) {
	originalAuthtype := AuthType
	originalSecretmanagerProjectID := SecretManagerProjectID
	originalCaDeployedProjectID := CaPoolDeployedProjectID
	originalCaPoolName := CaPoolName
	originalCaName := CaName
	originalVsaDeployedDnsName := VsaDeployedDnsName
	originalVsaManagedZone := VsaManagedZone
	originalCertificateLifetime := CertificateLifetime
	originalRegion := Region
	originalCloudDNSCacheTTL := CloudDNSCacheTTL
	orignalNodePassword := NodePassword

	AuthType = USER_CERTIFICATE // Set AuthType to USER_CERTIFICATE for this test
	SecretManagerProjectID = ""
	CaPoolDeployedProjectID = "" // Reset CaPoolDeployedProjectID for this test
	CaName = ""                  // Reset CaName for this test
	CaPoolName = ""              // Reset CaPoolName for this test
	VsaDeployedDnsName = ""      // Reset VsaDeployedDnsName for this test
	VsaManagedZone = ""          // Reset VsaManagedZone for this test
	CertificateLifetime = ""     // Reset CertificateLifetime for this test
	Region = ""
	CloudDNSCacheTTL = 0
	NodePassword = ""

	defer func() {
		AuthType = originalAuthtype                             // Restore original AuthType after test
		CaPoolDeployedProjectID = originalCaDeployedProjectID   // Restore original CaPoolDeployedProjectID
		SecretManagerProjectID = originalSecretmanagerProjectID // Restore original SecretManagerProjectID
		CaPoolName = originalCaPoolName
		VsaDeployedDnsName = originalVsaDeployedDnsName
		CaName = originalCaName // Restore original CaName
		VsaManagedZone = originalVsaManagedZone
		CertificateLifetime = originalCertificateLifetime
		Region = originalRegion
		CloudDNSCacheTTL = originalCloudDNSCacheTTL // Restore original CloudDNSCacheTTL
		NodePassword = orignalNodePassword
	}()

	err := ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "LOCAL_REGION must be set for authentication")

	Region = "us-central1" // Reset Region for this test

	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_NAME must be set for authentication")

	CaName = "ca-name"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_NAME must be set for authentication")

	CaPoolName = "ca-pool-name"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_DEPLOYED_PROJECT_ID must be set for authentication")

	CaPoolDeployedProjectID = "ca-pool-deployed-project-id"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SECRET_MANAGER_PROJECT_ID must be set for authentication")

	SecretManagerProjectID = "secret-manager-project-id"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_DEPLOYED_DNS_NAME must be set for authentication")

	VsaDeployedDnsName = "vsa-deployed-dns-name"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_MANAGED_ZONE must be set for authentication")

	VsaManagedZone = "vsa-managed-zone"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CERTIFICATE_LIFETIME must be set for authentication")

	CertificateLifetime = "30000s"
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CLOUD_DNS_CACHE_TTL must be set for authentication")

	CloudDNSCacheTTL = 300
	err = ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_NODE_PASSWORD must be set for authentication")

	NodePassword = "node-password"
	err = ValidateEnvironmentVariables()
	assert.NoError(t, err)
}

func Test_convertPrivateCACertificateToCustomCertificate(t *testing.T) {
	t.Run("NilCertificate", func(tt *testing.T) {
		result, err := _validateAndConvertPrivateCACertificateToCustomCertificate("cert-id", nil)
		assert.Nil(tt, result)
		assert.EqualError(tt, fmt.Errorf("input certificate is nil"), err.Error())
	})

	t.Run("InvalidCreateTime", func(tt *testing.T) {
		input := &privateca.Certificate{
			CreateTime: "invalid",
			Lifetime:   time.Now().Format(time.RFC3339),
		}
		result, err := _validateAndConvertPrivateCACertificateToCustomCertificate("cert-id", input)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to parse time")
	})

	t.Run("ValidCertificate", func(tt *testing.T) {
		createTime := time.Now().Format(time.RFC3339)
		lifetime := "30000s"
		input := &privateca.Certificate{
			Name:                       "test-cert",
			PemCertificate:             "pem-data",
			CreateTime:                 createTime,
			Lifetime:                   lifetime,
			PemCertificateChain:        []string{"chain1", "chain2"},
			IssuerCertificateAuthority: "issuer",
			CertificateDescription: &privateca.CertificateDescription{
				SubjectDescription: &privateca.SubjectDescription{
					Subject: &privateca.Subject{
						CommonName:   "common",
						Organization: "org",
					},
					SubjectAltName: &privateca.SubjectAltNames{
						DnsNames: []string{"dns1", "dns2"},
					},
				},
			},
		}
		expectedCreateTime, _ := time.Parse(time.RFC3339, createTime)
		expected := &models.CustomCertificate{
			CertificateID:              "cert-id",
			Name:                       "test-cert",
			PemCertificate:             "pem-data",
			CreateTime:                 timestamppb.New(expectedCreateTime),
			LifeTime:                   lifetime,
			PemCertificateChain:        []string{"chain1", "chain2"},
			IssuerCertificateAuthority: "issuer",
			SubjectCommonName:          "common",
			SubjectOrganization:        "org",
			SubjectAltName:             []string{"dns1", "dns2"},
		}
		result, err := _validateAndConvertPrivateCACertificateToCustomCertificate("cert-id", input)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
}

func Test_convertSecretToCustomSecret(t *testing.T) {
	t.Run("WhenSecretIsNil", func(tt *testing.T) {
		result, err := _convertSecretToCustomSecret(nil, nil)
		assert.Nil(tt, result, "Expected result to be nil when input secret is nil")
		assert.EqualError(tt, err, "input secret is nil", "Expected error message to match")
	})

	t.Run("WhenCreateTimeIsInvalid", func(tt *testing.T) {
		input := &secretmanager.Secret{
			CreateTime: "invalid-time",
		}
		result, err := _convertSecretToCustomSecret(input, &models.CustomSecretVersion{
			Name:  "secret-name",
			Value: "secret-value",
		})
		assert.Nil(tt, result, "Expected result to be nil when CreateTime is invalid")
		assert.Contains(tt, err.Error(), "failed to parse time", "Expected error message to contain 'failed to parse CreateTime'")
	})

	t.Run("WhenExpireTimeIsInvalid", func(tt *testing.T) {
		input := &secretmanager.Secret{
			CreateTime: time.Now().Format(time.RFC3339),
			ExpireTime: "invalid-time",
		}
		result, err := _convertSecretToCustomSecret(input, &models.CustomSecretVersion{
			Name:  "secret-name",
			Value: "secret-value",
		})
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to parse time")
	})

	t.Run("WhenSecretIsValid", func(tt *testing.T) {
		createTime := time.Now().Format(time.RFC3339)
		expireTime := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
		input := &secretmanager.Secret{
			Name:       "test-secret",
			CreateTime: createTime,
			ExpireTime: expireTime,
		}
		secretVersion := &models.CustomSecretVersion{
			Name:  "test-version",
			Value: "test-value",
		}

		expectedCreateTime, _ := time.Parse(time.RFC3339, createTime)
		expectedExpireTime, _ := time.Parse(time.RFC3339, expireTime)

		expected := &models.CustomSecret{
			Name:          "test-secret",
			CreateTime:    timestamppb.New(expectedCreateTime),
			LifeTime:      timestamppb.New(expectedExpireTime),
			SecretVersion: secretVersion,
		}

		result, err := _convertSecretToCustomSecret(input, secretVersion)
		assert.NoError(tt, err, "Expected no error when secret is valid")
		assert.Equal(tt, expected, result, "Expected result to match the converted secret")
	})
}

func Test_convertSecretVersionToCustomSecretVersion(t *testing.T) {
	t.Run("WhenSecretVersionNameIsEmpty", func(tt *testing.T) {
		result, err := _convertSecretVersionToCustomSecretVersion("", "test-value")
		assert.Nil(tt, result, "Expected result to be nil when secret version name is empty")
		assert.EqualError(tt, err, "input secret is nil", "Expected error message to match")
	})

	t.Run("WhenSecretVersionNameIsValid", func(tt *testing.T) {
		secretVersionName := "projects/test-project/secrets/test-secret/versions/1"
		secretValue := "test-value"

		expected := &models.CustomSecretVersion{
			Name:  secretVersionName,
			Value: secretValue,
		}

		result, err := _convertSecretVersionToCustomSecretVersion(secretVersionName, secretValue)
		assert.NoError(tt, err, "Expected no error when secret version name is valid")
		assert.Equal(tt, expected, result, "Expected result to match the converted secret version")
	})
}

func Test__convertCertificateParamsToCustomCertificate(t *testing.T) {
	t.Run("PositiveCase", func(tt *testing.T) {
		param := &models.CustomCertificateParam{
			CertificateID:    "cert-id",
			CaName:           "ca-name",
			CertOwningEntity: "account-id",
			Region:           "region",
			CaPoolName:       "ca-pool-name",
		}
		pemBlock := pem.Block{
			Type:  "CERTIFICATE REQUEST",
			Bytes: []byte("test-bytes"),
		}
		result, err := _validateAndConvertCertificateParamsToCustomCertificate(param, pemBlock)
		if result == nil {
			tt.Fatal("Expected non-nil result")
		}
		if err != nil {
			tt.Fatal("Expected nil err")
		}
	})

	t.Run("NegativeCase", func(tt *testing.T) {
		pemBlock := pem.Block{
			Type:  "CERTIFICATE REQUEST",
			Bytes: []byte("test-bytes"),
		}
		_, err := _validateAndConvertCertificateParamsToCustomCertificate(nil, pemBlock)
		if err == nil {
			tt.Errorf("Expected err, got %+v", err)
		}
	})
}
