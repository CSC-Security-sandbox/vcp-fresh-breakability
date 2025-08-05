package google

import (
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateEnvironmentVariables(t *testing.T) {
	originalAuthtype := env.AuthType
	originalSecretmanagerProjectID := env.SecretManagerProjectID
	originalCaDeployedProjectID := env.CaPoolDeployedProjectID
	originalCaPoolName := env.CaPoolName
	originalCaName := env.CaName
	originalVsaDeployedDnsName := env.VsaDeployedDnsName
	originalVsaManagedZone := env.VsaManagedZone
	originalCertificateLifetime := env.CertificateLifetime
	originalRegion := env.Region
	originalCloudDNSCacheTTL := env.CloudDNSCacheTTL
	orignalNodePassword := env.NodePassword

	env.AuthType = env.USER_CERTIFICATE // Set AuthType to USER_CERTIFICATE for this test
	env.SecretManagerProjectID = ""
	env.CaPoolDeployedProjectID = "" // Reset CaPoolDeployedProjectID for this test
	env.CaName = ""                  // Reset CaName for this test
	env.CaPoolName = ""              // Reset CaPoolName for this test
	env.VsaDeployedDnsName = ""      // Reset VsaDeployedDnsName for this test
	env.VsaManagedZone = ""          // Reset VsaManagedZone for this test
	env.CertificateLifetime = ""     // Reset CertificateLifetime for this test
	env.Region = ""
	env.CloudDNSCacheTTL = 0
	env.NodePassword = ""

	defer func() {
		env.AuthType = originalAuthtype                             // Restore original AuthType after test
		env.CaPoolDeployedProjectID = originalCaDeployedProjectID   // Restore original CaPoolDeployedProjectID
		env.SecretManagerProjectID = originalSecretmanagerProjectID // Restore original SecretManagerProjectID
		env.CaPoolName = originalCaPoolName
		env.VsaDeployedDnsName = originalVsaDeployedDnsName
		env.CaName = originalCaName // Restore original CaName
		env.VsaManagedZone = originalVsaManagedZone
		env.CertificateLifetime = originalCertificateLifetime
		env.Region = originalRegion
		env.CloudDNSCacheTTL = originalCloudDNSCacheTTL // Restore original CloudDNSCacheTTL
		env.NodePassword = orignalNodePassword
	}()

	err := env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "LOCAL_REGION must be set for authentication")

	env.Region = "us-central1" // Reset Region for this test

	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_NAME must be set for authentication")

	env.CaName = "ca-name"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_NAME must be set for authentication")

	env.CaPoolName = "ca-pool-name"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CA_POOL_DEPLOYED_PROJECT_ID must be set for authentication")

	env.CaPoolDeployedProjectID = "ca-pool-deployed-project-id"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SECRET_MANAGER_PROJECT_ID must be set for authentication")

	env.SecretManagerProjectID = "secret-manager-project-id"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_DEPLOYED_DNS_NAME must be set for authentication")

	env.VsaDeployedDnsName = "vsa-deployed-dns-name"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_MANAGED_ZONE must be set for authentication")

	env.VsaManagedZone = "vsa-managed-zone"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CERTIFICATE_LIFETIME must be set for authentication")

	env.CertificateLifetime = "30000s"
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "CLOUD_DNS_CACHE_TTL must be set for authentication")

	env.CloudDNSCacheTTL = 300
	err = env.ValidateEnvironmentVariables()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "VSA_NODE_PASSWORD must be set for authentication")

	env.NodePassword = "node-password"
	err = env.ValidateEnvironmentVariables()
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
func Test_validateAndConvertToCustomCloudDNSRecord(t *testing.T) {
	t.Run("NilRecordSet", func(tt *testing.T) {
		result, err := _validateAndConvertToCustomCloudDNSRecord(nil, "zone")
		assert.Nil(tt, result)
		assert.Equal(tt, "resource record set is nil", err.Error())
	})

	t.Run("InvalidRecordSet_EmptyFields", func(tt *testing.T) {
		recordSet := &dns.ResourceRecordSet{}
		result, err := _validateAndConvertToCustomCloudDNSRecord(recordSet, "zone")
		assert.Nil(tt, result)
		assert.Equal(tt, "resource record set is invalid", err.Error())
	})

	t.Run("InvalidRecordSet_EmptyRrdatas", func(tt *testing.T) {
		recordSet := &dns.ResourceRecordSet{
			Name: "test.com.",
			Type: "A",
			Ttl:  300,
		}
		result, err := _validateAndConvertToCustomCloudDNSRecord(recordSet, "zone")
		assert.Nil(tt, result)
		assert.Equal(tt, "resource record set is invalid", err.Error())
	})

	t.Run("ValidRecordSet", func(tt *testing.T) {
		recordSet := &dns.ResourceRecordSet{
			Name:    "test.com.",
			Type:    "A",
			Ttl:     300,
			Rrdatas: []string{"1.2.3.4"},
		}
		managedZone := "zone"
		expected := &models.CustomCloudDNSRecord{
			RecordName:  "test.com.",
			Type:        "A",
			TTL:         300,
			Data:        "1.2.3.4",
			ManagedZone: managedZone,
		}
		result, err := _validateAndConvertToCustomCloudDNSRecord(recordSet, managedZone)
		assert.NoError(tt, err)
		assert.Equal(tt, expected, result)
	})
}
