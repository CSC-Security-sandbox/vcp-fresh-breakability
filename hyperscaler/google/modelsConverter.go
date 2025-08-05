package google

import (
	"fmt"
	"strconv"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/servicenetworking/v1"
)

func convertComputeOpToComputeOp(op *compute.Operation) *models.ComputeOperation {
	if op == nil {
		return nil
	}
	return &models.ComputeOperation{
		Name:     op.Name,
		Status:   op.Status,
		Progress: op.Progress,
	}
}

func convertServiceNetOpToComputeOp(op *servicenetworking.Operation) *models.ComputeOperation {
	if op == nil {
		return nil
	}
	operation := &models.ComputeOperation{
		Name:     op.Name,
		Done:     op.Done,
		Response: op.Response,
	}
	if op.Error != nil {
		operation.ErrorResponse = op.Error.Message
	}
	return operation
}

func convertGoogleSubnetToSubnet(subnet *compute.Subnetwork) *models.Subnet {
	if subnet == nil {
		return nil
	}
	return &models.Subnet{
		Name:           subnet.Name,
		Network:        subnet.Network,
		IpCidrRange:    subnet.IpCidrRange,
		GatewayAddress: subnet.GatewayAddress,
	}
}

func convertGoogleSubnetsToSubnets(subnets *compute.SubnetworkList) *[]models.Subnet {
	if subnets == nil {
		return nil
	}
	returnSubnets := make([]models.Subnet, len(subnets.Items))
	for i, subnet := range subnets.Items {
		returnSubnets[i] = *convertGoogleSubnetToSubnet(subnet)
	}
	return &returnSubnets
}

func convertSubnetToGoogleSubnet(subnet *models.Subnet) *compute.Subnetwork {
	if subnet == nil {
		return nil
	}
	return &compute.Subnetwork{
		Name:                  subnet.Name,
		Network:               subnet.Network,
		IpCidrRange:           subnet.IpCidrRange,
		PrivateIpGoogleAccess: true,
	}
}

func convertGoogleVPCToVPC(vpc *compute.Network) *models.VPCNetwork {
	if vpc == nil {
		return nil
	}
	return &models.VPCNetwork{
		Name:        vpc.Name,
		ProjectName: vpc.SelfLink,
	}
}

func convertVPCToGoogleVPC(vpc *models.VPCNetwork) *compute.Network {
	if vpc == nil {
		return nil
	}
	return &compute.Network{
		Name:                  vpc.Name,
		AutoCreateSubnetworks: false,
		// make sure AutoCreateSubnetworks field is included in request
		ForceSendFields: []string{"AutoCreateSubnetworks"},
	}
}

func convertToGoogleFirewallRule(firewallRequest *models.Firewall) *compute.Firewall {
	if firewallRequest == nil {
		return nil
	}
	return &compute.Firewall{
		Name:         firewallRequest.Name,
		Description:  "Allow traffic on specific ports for " + firewallRequest.Name,
		Network:      fmt.Sprintf("projects/%s/global/networks/%s", firewallRequest.ProjectName, firewallRequest.VPCNetworkName),
		Allowed:      getFirewallAllowedRulesGCP(firewallRequest.AllowedPortRules),
		SourceRanges: firewallRequest.SourceRanges,
		Direction:    firewallRequest.Direction,
		Priority:     firewallRequest.Priority,
	}
}

func convertGCPFirewallToFirewall(firewall *compute.Firewall) *models.Firewall {
	if firewall == nil {
		return nil
	}
	return &models.Firewall{
		Name:           firewall.Name,
		Description:    firewall.Description,
		VPCNetworkName: firewall.Network,
		SourceRanges:   firewall.SourceRanges,
	}
}

func getFirewallAllowedRulesGCP(allowedPortRules []string) []*compute.FirewallAllowed {
	firewallAllowedPortRules := []*compute.FirewallAllowed{}
	for _, rule := range allowedPortRules {
		if _, err := strconv.Atoi(rule); err == nil {
			if len(firewallAllowedPortRules) > 0 {
				lastRule := firewallAllowedPortRules[len(firewallAllowedPortRules)-1]
				lastRule.Ports = append(lastRule.Ports, rule)
			}
		} else {
			firewallAllowedPortRules = append(firewallAllowedPortRules, &compute.FirewallAllowed{IPProtocol: rule})
		}
	}
	return firewallAllowedPortRules
}
