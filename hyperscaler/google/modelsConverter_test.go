package google

import (
	"testing"

	"github.com/stretchr/testify/assert"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/servicenetworking/v1"
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
			Mtu:                   8896,
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
