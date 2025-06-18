package google

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strconv"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/api/servicenetworking/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ConvertPrivateKeyToString                              = _convertPrivateKeyToString
	ValidateAndConvertCertificateParamsToCustomCertificate = _validateAndConvertCertificateParamsToCustomCertificate
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

func _validateAndConvertPrivateCACertificateToCustomCertificate(certificateId string, cert *privateca.Certificate) (*models.CustomCertificate, error) {
	if cert == nil {
		return nil, fmt.Errorf("input certificate is nil")
	}
	customCert, err := convertPrivateCACertificateToCustomCertificate(certificateId, cert)
	if err != nil {
		return nil, err
	}
	if cert.Config != nil && cert.Config.SubjectConfig != nil {
		if cert.Config.SubjectConfig.Subject != nil {
			if cert.Config.SubjectConfig.Subject.CommonName != "" {
				customCert.SubjectCommonName = cert.Config.SubjectConfig.Subject.CommonName
			}
			if cert.Config.SubjectConfig.Subject.Organization != "" {
				customCert.SubjectOrganization = cert.Config.SubjectConfig.Subject.Organization
			}
			if cert.Config.SubjectConfig.SubjectAltName != nil {
				customCert.SubjectAltName = cert.Config.SubjectConfig.SubjectAltName.DnsNames
			}
		}
	}
	return customCert, nil
}

func _convertSecretToCustomSecret(secret *secretmanager.Secret, secretVersion *models.CustomSecretVersion) (*models.CustomSecret, error) {
	if secret == nil {
		return nil, fmt.Errorf("input secret is nil")
	}
	createTime, err := parseTimestamps(secret.CreateTime)
	if err != nil {
		return nil, err
	}
	lifeTime, err := parseTimestamps(secret.ExpireTime)
	if err != nil {
		return nil, err
	}
	customCert := &models.CustomSecret{
		Name:          secret.Name,
		CreateTime:    createTime,
		LifeTime:      lifeTime,
		SecretVersion: secretVersion,
	}
	return customCert, nil
}

func _convertSecretVersionToCustomSecretVersion(secretVersionName, secretValue string) (*models.CustomSecretVersion, error) {
	if secretVersionName == "" {
		return nil, fmt.Errorf("input secret is nil")
	}
	if secretValue == "" {
		return nil, fmt.Errorf("input secret value is nil")
	}
	customCert := &models.CustomSecretVersion{
		Name:  secretVersionName,
		Value: secretValue,
	}
	return customCert, nil
}

func _convertPrivateKeyToString(key *rsa.PrivateKey, rsaKeyType string) string {
	privateKeyPEM := &pem.Block{
		Type:  rsaKeyType,
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	privateKeyBytes := pem.EncodeToMemory(privateKeyPEM)
	return string(privateKeyBytes)
}

func convertPrivateCACertificateToCustomCertificate(certificateId string, cert *privateca.Certificate) (*models.CustomCertificate, error) {
	if cert == nil {
		return nil, fmt.Errorf("input certificate is nil")
	}
	createTime, err := parseTimestamps(cert.CreateTime)
	if err != nil {
		return nil, err
	}
	lifeTime, err := parseTimestamps(cert.Lifetime)
	if err != nil {
		return nil, err
	}
	customCert := &models.CustomCertificate{
		CertificateId:              certificateId,
		Name:                       cert.Name,
		PemCertificate:             cert.PemCertificate,
		CreateTime:                 createTime,
		LifeTime:                   lifeTime,
		PemCertificateChain:        cert.PemCertificateChain,
		IssuerCertificateAuthority: cert.IssuerCertificateAuthority,
	}
	return customCert, nil
}

func parseTimestamps(timeStr string) (*timestamppb.Timestamp, error) {
	var timeStamp *timestamppb.Timestamp

	if timeStr != "" {
		parsedTime, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %v", err)
		}
		timeStamp = timestamppb.New(parsedTime)
	}
	return timeStamp, nil
}

func _validateAndConvertCertificateParamsToCustomCertificate(param *models.CustomCertificateParam, pemBlock pem.Block) (*models.CustomCertificate, error) {
	if param == nil || param.CertificateId == "" && param.CaName == "" || param.AccountId == "" || param.Region == "" || param.CaPoolName == "" || pemBlock.Type == "" {
		return nil, fmt.Errorf("invalid certificate parameters")
	}
	return &models.CustomCertificate{
		CertificateId: param.CertificateId,
		CaName:        param.CaName,
		AccountId:     param.AccountId,
		Region:        param.Region,
		CaGroupName:   param.CaPoolName,
		PemCsr:        string(pem.EncodeToMemory(&pemBlock)),
	}, nil
}
