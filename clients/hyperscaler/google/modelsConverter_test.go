package google

import (
	"encoding/pem"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/api/servicenetworking/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Test_convertComputeOpToComputeOp(t *testing.T) {
	t.Run("WhenOperationIsNil", func(tt *testing.T) {
		result := convertComputeOpToComputeOp(nil)
		assert.Nil(tt, result, "Expected result to be nil when input operation is nil")
	})

	t.Run("WhenOperationIsValid", func(tt *testing.T) {
		input := &compute.Operation{
			Name:     "test-operation",
			Status:   "DONE",
			Progress: 100,
		}

		expected := &models.ComputeOperation{
			Name:     "test-operation",
			Status:   "DONE",
			Progress: 100,
		}

		result := convertComputeOpToComputeOp(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted operation")
	})
}

func Test_convertServiceNetOpToComputeOp(t *testing.T) {
	t.Run("WhenOperationIsNil", func(tt *testing.T) {
		result := convertServiceNetOpToComputeOp(nil)
		assert.Nil(tt, result, "Expected result to be nil when input operation is nil")
	})

	t.Run("WhenErrorResponseShouldBeEmpty", func(tt *testing.T) {
		input := &servicenetworking.Operation{
			Name: "test-operation",
			Done: true,
		}

		expected := &models.ComputeOperation{
			Name:          "test-operation",
			Done:          true,
			ErrorResponse: "",
		}

		result := convertServiceNetOpToComputeOp(input)
		assert.Equal(tt, expected.ErrorResponse, result.ErrorResponse, "")
		assert.Equal(tt, expected, result, "Expected result to match the converted operation")
	})
	t.Run("WhenOperationIsValid", func(tt *testing.T) {
		input := &servicenetworking.Operation{
			Name:  "test-operation",
			Done:  true,
			Error: &servicenetworking.Status{Code: 400, Message: "test-error"},
		}

		expected := &models.ComputeOperation{
			Name:          "test-operation",
			Done:          true,
			ErrorResponse: "test-error",
		}

		result := convertServiceNetOpToComputeOp(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted operation")
	})
}

func Test_convertGoogleSubnetToSubnet(t *testing.T) {
	t.Run("WhenSubnetIsNil", func(tt *testing.T) {
		result := convertGoogleSubnetToSubnet(nil)
		assert.Nil(tt, result, "Expected result to be nil when input subnet is nil")
	})

	t.Run("WhenSubnetIsValid", func(tt *testing.T) {
		input := &compute.Subnetwork{
			Name:        "test-subnet",
			Network:     "test-network",
			IpCidrRange: "10.0.0.0/24",
		}

		expected := &models.Subnet{
			Name:        "test-subnet",
			Network:     "test-network",
			IpCidrRange: "10.0.0.0/24",
		}

		result := convertGoogleSubnetToSubnet(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted subnet")
	})
}

func Test_convertSubnetToGoogleSubnet(t *testing.T) {
	t.Run("WhenSubnetIsNil", func(tt *testing.T) {
		result := convertSubnetToGoogleSubnet(nil)
		assert.Nil(tt, result, "Expected result to be nil when input subnet is nil")
	})

	t.Run("WhenSubnetIsValid", func(tt *testing.T) {
		input := &models.Subnet{
			Name:        "test-subnet",
			Network:     "test-network",
			IpCidrRange: "10.0.0.0/24",
		}

		expected := &compute.Subnetwork{
			Name:                  "test-subnet",
			Network:               "test-network",
			IpCidrRange:           "10.0.0.0/24",
			PrivateIpGoogleAccess: true,
		}

		result := convertSubnetToGoogleSubnet(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted subnet")
	})
}

func Test_convertGoogleVPCToVPC(t *testing.T) {
	t.Run("WhenVPCIsNil", func(tt *testing.T) {
		result := convertGoogleVPCToVPC(nil)
		assert.Nil(tt, result, "Expected result to be nil when input VPC is nil")
	})

	t.Run("WhenVPCIsValid", func(tt *testing.T) {
		input := &compute.Network{
			Name:     "test-vpc",
			SelfLink: "projects/test-project/global/networks/test-vpc",
		}

		expected := &models.VPCNetwork{
			Name:        "test-vpc",
			ProjectName: "projects/test-project/global/networks/test-vpc",
		}

		result := convertGoogleVPCToVPC(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted VPC")
	})
}

func Test_convertVPCToGoogleVPC(t *testing.T) {
	t.Run("WhenVPCIsNil", func(tt *testing.T) {
		result := convertVPCToGoogleVPC(nil)
		assert.Nil(tt, result, "Expected result to be nil when input VPC is nil")
	})

	t.Run("WhenVPCIsValid", func(tt *testing.T) {
		input := &models.VPCNetwork{
			Name: "test-vpc",
		}

		expected := &compute.Network{
			Name:                  "test-vpc",
			AutoCreateSubnetworks: false,
			ForceSendFields:       []string{"AutoCreateSubnetworks"},
		}

		result := convertVPCToGoogleVPC(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted VPC")
	})
}

func Test_convertToGoogleFirewallRule(t *testing.T) {
	t.Run("WhenFirewallRequestIsNil", func(tt *testing.T) {
		result := convertToGoogleFirewallRule(nil)
		assert.Nil(tt, result, "Expected result to be nil when input firewall request is nil")
	})

	t.Run("WhenFirewallRequestIsValid", func(tt *testing.T) {
		input := &models.Firewall{
			Name:             "test-firewall",
			ProjectName:      "test-project",
			VPCNetworkName:   "test-vpc",
			AllowedPortRules: []string{"tcp", "udp"},
			SourceRanges:     []string{"0.0.0.0/0"},
			Direction:        "INGRESS",
			Priority:         1000,
		}

		expected := &compute.Firewall{
			Name:         "test-firewall",
			Description:  "Allow traffic on specific ports for test-firewall",
			Network:      "projects/test-project/global/networks/test-vpc",
			Allowed:      []*compute.FirewallAllowed{{IPProtocol: "tcp"}, {IPProtocol: "udp"}},
			SourceRanges: []string{"0.0.0.0/0"},
			Direction:    "INGRESS",
			Priority:     1000,
		}

		result := convertToGoogleFirewallRule(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted firewall rule")
	})
}

func Test_convertGCPFirewallToFirewall(t *testing.T) {
	t.Run("WhenFirewallIsNil", func(tt *testing.T) {
		result := convertGCPFirewallToFirewall(nil)
		assert.Nil(tt, result, "Expected result to be nil when input firewall is nil")
	})

	t.Run("WhenFirewallIsValid", func(tt *testing.T) {
		input := &compute.Firewall{
			Name:         "test-firewall",
			Description:  "Allow incoming traffic",
			Network:      "test-vpc",
			SourceRanges: []string{"0.0.0.0/0"},
		}

		expected := &models.Firewall{
			Name:           "test-firewall",
			Description:    "Allow incoming traffic",
			VPCNetworkName: "test-vpc",
			SourceRanges:   []string{"0.0.0.0/0"},
		}

		result := convertGCPFirewallToFirewall(input)
		assert.Equal(tt, expected, result, "Expected result to match the converted firewall")
	})
}

func Test_getFirewallAllowedRulesGCP(t *testing.T) {
	t.Run("WhenAllowedPortRulesIsEmpty", func(tt *testing.T) {
		allowedPortRules := []string{}
		result := getFirewallAllowedRulesGCP(allowedPortRules)
		assert.Empty(tt, result, "Expected result to be empty when allowedPortRules is empty")
	})

	t.Run("WhenAllowedPortRulesHasProtocols", func(tt *testing.T) {
		allowedPortRules := []string{"tcp", "udp", "icmp"}
		result := getFirewallAllowedRulesGCP(allowedPortRules)

		expected := []*compute.FirewallAllowed{
			{IPProtocol: "tcp"},
			{IPProtocol: "udp"},
			{IPProtocol: "icmp"},
		}

		assert.Equal(tt, expected, result, "Expected result to match the allowedPortRules")
	})
	t.Run("WhenAllowedPortRulesHasPorts", func(tt *testing.T) {
		allowedPortRules := []string{"tcp", "3290"}
		result := getFirewallAllowedRulesGCP(allowedPortRules)

		expected := []*compute.FirewallAllowed{
			{IPProtocol: "tcp", Ports: []string{"3290"}},
		}

		assert.Equal(tt, expected, result, "Expected result to match the allowedPortRules")
	})
	t.Run("WhenAllowedPortRulesHasPorts1", func(tt *testing.T) {
		allowedPortRules := []string{"tcp", "3290", "90"}
		result := getFirewallAllowedRulesGCP(allowedPortRules)

		expected := []*compute.FirewallAllowed{
			{IPProtocol: "tcp", Ports: []string{"3290", "90"}},
		}

		assert.Equal(tt, expected, result, "Expected result to match the allowedPortRules")
	})
	t.Run("WhenAllowedPortRulesHasPortsAndProtocols", func(tt *testing.T) {
		allowedPortRules := []string{"tcp", "3290", "90", "udp", "icmp"}
		result := getFirewallAllowedRulesGCP(allowedPortRules)

		expected := []*compute.FirewallAllowed{
			{IPProtocol: "tcp", Ports: []string{"3290", "90"}},
			{IPProtocol: "udp"},
			{IPProtocol: "icmp"},
		}

		assert.Equal(tt, expected, result, "Expected result to match the allowedPortRules")
	})
	t.Run("WhenMultiplePortsMultipleProtocols", func(tt *testing.T) {
		allowedPortRules := []string{"tcp", "3290", "90", "udp", "223", "icmp", "443"}
		result := getFirewallAllowedRulesGCP(allowedPortRules)

		expected := []*compute.FirewallAllowed{
			{IPProtocol: "tcp", Ports: []string{"3290", "90"}},
			{IPProtocol: "udp", Ports: []string{"223"}},
			{IPProtocol: "icmp", Ports: []string{"443"}},
		}

		assert.Equal(tt, expected, result, "Expected result to match the allowedPortRules")
	})
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
			Config:     &privateca.CertificateConfig{SubjectConfig: &privateca.SubjectConfig{Subject: &privateca.Subject{}, SubjectAltName: &privateca.SubjectAltNames{}}},
		}
		result, err := _validateAndConvertPrivateCACertificateToCustomCertificate("cert-id", input)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to parse time")
	})

	t.Run("InvalidLifeTime", func(tt *testing.T) {
		input := &privateca.Certificate{
			CreateTime: time.Now().Format(time.RFC3339),
			Lifetime:   "invalid",
			Config:     &privateca.CertificateConfig{SubjectConfig: &privateca.SubjectConfig{Subject: &privateca.Subject{}, SubjectAltName: &privateca.SubjectAltNames{}}},
		}
		result, err := _validateAndConvertPrivateCACertificateToCustomCertificate("cert-id", input)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "failed to parse time")
	})

	t.Run("ValidCertificate", func(tt *testing.T) {
		createTime := time.Now().Format(time.RFC3339)
		lifetime := time.Now().Add(24 * time.Hour).Format(time.RFC3339)
		input := &privateca.Certificate{
			Name:                       "test-cert",
			PemCertificate:             "pem-data",
			CreateTime:                 createTime,
			Lifetime:                   lifetime,
			PemCertificateChain:        []string{"chain1", "chain2"},
			IssuerCertificateAuthority: "issuer",
			Config: &privateca.CertificateConfig{
				SubjectConfig: &privateca.SubjectConfig{
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
		expectedLifetime, _ := time.Parse(time.RFC3339, lifetime)
		expected := &models.CustomCertificate{
			CertificateId:              "cert-id",
			Name:                       "test-cert",
			PemCertificate:             "pem-data",
			CreateTime:                 timestamppb.New(expectedCreateTime),
			LifeTime:                   timestamppb.New(expectedLifetime),
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
		result, err := _convertSecretToCustomSecret(input, nil)
		assert.Nil(tt, result, "Expected result to be nil when CreateTime is invalid")
		assert.Contains(tt, err.Error(), "failed to parse time", "Expected error message to contain 'failed to parse CreateTime'")
	})

	t.Run("WhenExpireTimeIsInvalid", func(tt *testing.T) {
		input := &secretmanager.Secret{
			CreateTime: time.Now().Format(time.RFC3339),
			ExpireTime: "invalid-time",
		}
		result, err := _convertSecretToCustomSecret(input, nil)
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
			CertificateId: "cert-id",
			CaName:        "ca-name",
			AccountId:     "account-id",
			Region:        "region",
			CaPoolName:    "ca-pool-name",
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
