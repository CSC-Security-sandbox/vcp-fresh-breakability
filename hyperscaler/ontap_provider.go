package hyperscaler

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var GetProviderByNode = GetProviderByNodeOld

func GetProviderByNodeOld(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	if node.AuthType == env.USER_CERTIFICATE {
		certificate, err := GetCertificateFromCacheOrSecretManager(ctx, node.CertificateID)
		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get certificate for node %s: %v", node.Name, err)
			return nil, err
		}

		return vsa.NewProvider(ctx, vsa.ProviderDetails{
			Hosts: node.EndpointAddressesToHostNameMap,
			Certificate: &vsa.Certificate{
				SignedCertificate:        certificate.SignedCertificate,
				InterMediateCertificates: certificate.InterMediateCertificates,
				CommonName:               certificate.CommonName,
				PrivateKey:               certificate.PrivateKey,
			},
			InsecureSkipVerify: false,
		}), nil
	}

	var password string
	if node.AuthType == env.USERNAME_PWD_SEC_MGR {
		secret, err := GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get password for node %s: %v", node.Name, err)
			return nil, err
		}
		password = secret
	} else {
		password = node.Password
	}
	// if ipAddress in empty, populate it with the node's endpoint address
	if len(node.EndpointAddressesToHostNameMap) == 0 {
		if node.EndpointAddress == "" {
			return nil, errors.NewVCPError(errors.ErrVSAClusterNodeIPAddressNotFound, errors2.New("node endpoint address is empty"))
		}
		node.EndpointAddressesToHostNameMap[node.EndpointAddress] = node.EndpointAddress
	}

	return vsa.NewProvider(ctx, vsa.ProviderDetails{
		Hosts:              node.EndpointAddressesToHostNameMap,
		Password:           password,
		InsecureSkipVerify: true,
	}), nil
}

var GetCertificateFromCacheOrSecretManager = GetCertificateFromCacheOrSecretManager1

var RevokeCertificateAndDeleteFromCacheAndSecretManager = RevokeCertificateAndDeleteFromCacheAndSecretManager1

var GenerateAndCreateCertificateForVSACluster = GenerateAndCreateCertificateForVSACluster1

var GeneratePasswordForVSACluster = GeneratePasswordForVSACluster1

var GetPasswordForVSACluster = GetPasswordForVSACluster1

var GetPasswordFromCacheOrSecretManager = GetPasswordFromCacheOrSecretManager1

var GenerateCSR = GenerateCSR1

var DeletePasswordFromCacheAndSecretManager = DeletePasswordFromSecretManagerAndCache1

var DeleteCloudDNSRecord = DeleteCloudDNSRecord1

var GetOrCreateCloudDNSRecord = GetOrCreateCloudDNSRecord1

var GetCertificateAndPrivateKeyByID = GetCertificateAndPrivateKeyByID1

var GetOrCreatePrivateKeyInSecretManagerAndCache = GetOrCreatePrivateKeyInSecretManagerAndCache1

var GetOrCreateCertificateInCASAndPrivateKeyInSM = GetOrCreateCertificateInCASAndPrivateKeyInSM1

// GenerateAndCreateCertificateForVSACluster1 generates a CSR and creates a certificate in GCP Certificate Authority Service.
func GenerateAndCreateCertificateForVSACluster1(gcpService GoogleServices, Region, certificateID, clusterName string) (*hyperscalermodels.CustomCertificateResponse, error) {
	logger := gcpService.GetLogger()
	domains := fmt.Sprintf("*.%s.%s", clusterName, env.VsaDeployedDnsName)
	param := &hyperscalermodels.CustomCertificateParam{
		Region:           Region,
		CertificateID:    certificateID,
		CaPoolName:       env.CaPoolName,
		CaName:           env.CaName,
		CommonName:       env.VCP_ADMIN,
		Domains:          []string{domains},
		CertOwningEntity: env.CaPoolDeployedProjectID,
	}
	// Generate CSR
	csrDER, key, err := GenerateCSR(param.CommonName, param.Domains)
	if err != nil {
		logger.Errorf("failed to generate CSR for commonName: %s, certificateId : %s, err : %v", param.CommonName, param.CertificateID, err)
		return nil, err
	}

	pemBlock := pem.Block{
		Type:  CsrType,
		Bytes: csrDER,
	}
	logger.Debug("Generate CSR for commonName: %s, certificateId : %s", param.CommonName, param.CertificateID)

	customCertificate, err := google.ValidateAndConvertCertificateParamsToCustomCertificate(param, pemBlock)
	if err != nil {
		return nil, err
	}

	// Create the Certificate
	cert, secret, err := GetOrCreateCertificateInCASAndPrivateKeyInSM(gcpService, customCertificate, key)
	if err != nil {
		logger.Errorf("failed to create customCertificate in CAS and private key in SM for commonName: %s, certificateId : %s, err : %v", param.CommonName, param.CertificateID, err)
		return nil, err
	}

	// Add the certificate to the cache
	common.AddToCertAuthCache(certificateID, &models.Certificate{
		CommonName:               cert.SubjectCommonName,
		SignedCertificate:        cert.PemCertificate,
		PrivateKey:               secret.SecretVersion.Value,
		InterMediateCertificates: cert.PemCertificateChain,
	})
	return &hyperscalermodels.CustomCertificateResponse{
		Certificate: cert,
		Secret:      secret,
	}, nil
}

// GetOrCreateCertificateInCASAndPrivateKeyInSM1 creates a certificate in GCP Certificate Authority Service and stores the private key in Secret Manager.
func GetOrCreateCertificateInCASAndPrivateKeyInSM1(gcpService GoogleServices, certificate *hyperscalermodels.CustomCertificate, key *rsa.PrivateKey) (*hyperscalermodels.CustomCertificate, *hyperscalermodels.CustomSecret, error) {
	// Create the certificate if Get the certificate fails
	logger := gcpService.GetLogger()
	var secret *hyperscalermodels.CustomSecret
	var cert *hyperscalermodels.CustomCertificate
	cert, err := gcpService.GetCertificate(env.CaPoolDeployedProjectID, certificate.Region, env.CaPoolName, certificate.CertificateID)
	if err != nil {
		// Create the Certificate
		cert, err = gcpService.CreateCertificate(certificate)
		if err != nil {
			logger.Errorf("failed to create certificate in CAS for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
			return nil, nil, err
		}
		logger.Debugf("created certificate in CAS for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)

		secret, err = GetOrCreatePrivateKeyInSecretManagerAndCache(gcpService, certificate, key)
		if err != nil {
			return nil, nil, err
		}
		return cert, secret, nil
	}

	secret, err = GetOrCreatePrivateKeyInSecretManagerAndCache(gcpService, certificate, key)
	if err != nil {
		return nil, nil, err
	}
	return cert, secret, nil
}

func GetOrCreatePrivateKeyInSecretManagerAndCache1(gcpService GoogleServices, certificate *hyperscalermodels.CustomCertificate, key *rsa.PrivateKey) (*hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()
	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, certificate.CertificateID)
	if err != nil {
		// Store the private key in Secret Manager
		secretValue := google.ConvertPrivateKeyToString(key, RsaKeyType)
		secret, err = gcpService.CreateSecret(env.SecretManagerProjectID, certificate.Region, certificate.CertificateID, secretValue)
		if err != nil {
			logger.Errorf("failed to create secret in SM for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
			// Revoke the certificate if the secret creation fails
			_, revokeError := gcpService.RevokeCertificate(certificate)
			if revokeError != nil {
				logger.Errorf("failed to revoke certificate in CAS for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, revokeError)
				return nil, revokeError
			}
			return nil, err
		}
		logger.Debugf("created secret in SM for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	}
	return secret, nil
}

// GetCertificateAndPrivateKeyByID1 retrieves the certificate for a VSA cluster from GCP Certificate Authority Service and Private key from Secret Manager.
func GetCertificateAndPrivateKeyByID1(gcpService GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscalermodels.CustomCertificateResponse, error) {
	certificate, err := gcpService.GetCertificate(caDeployedProjectID, region, caPoolName, certificateID)
	if err != nil || certificate == nil {
		return nil, fmt.Errorf("failed to get certficate for project: %s, region: %s, caPoolName : %s, certificateID : %s, err: %s", caDeployedProjectID, region, caPoolName, certificateID, err)
	}
	secret, err := gcpService.GetSecretWithLatestVersion(secretManagerProjectID, certificateID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, certificateID: %s, err: %s", secretManagerProjectID, certificateID, err)
	}
	return &hyperscalermodels.CustomCertificateResponse{
		Certificate: certificate,
		Secret:      secret,
	}, nil
}

// GeneratePasswordForVSACluster1 generates a strong password and creates a secret in GCP Secret Manager.
func GeneratePasswordForVSACluster1(gcpService GoogleServices, projectID, region, secretID string) (*hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()
	password, err := utils.GenerateStrongPassword(12)
	if err != nil {
		logger.Errorf("failed to generate password for secretID: %s, err : %v", secretID, err)
		return nil, err
	}
	var secret *hyperscalermodels.CustomSecret
	secret, getSecretError := gcpService.GetSecretWithLatestVersion(projectID, secretID)
	if getSecretError != nil {
		secret, err = gcpService.CreateSecret(projectID, region, secretID, password)
		if err != nil {
			return nil, err
		}
		common.AddToUserAuthCache(secretID, secret.SecretVersion.Value)
	}
	return secret, nil
}

// GetPasswordForVSACluster1 retrieves the password for a VSA cluster from GCP Secret Manager.
func GetPasswordForVSACluster1(gcpService GoogleServices, secretID string) (*hyperscalermodels.CustomSecret, error) {
	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, userName: %s, err: %s", env.SecretManagerProjectID, secretID, err)
	}
	return secret, nil
}

// GetPasswordFromCacheOrSecretManager1 retrieves the password for a VSA cluster from cache or GCP Secret Manager if not found in cache.
func GetPasswordFromCacheOrSecretManager1(ctx context.Context, secretID string) (string, error) {
	password := ""
	userCache, exist := common.GetFromUserAuthCache(secretID)
	if !exist || userCache.Password == "" {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return "", err
		}
		secret, err := GetPasswordForVSACluster(gcpService, secretID)
		if err != nil {
			return "", err
		}
		password = secret.SecretVersion.Value
		common.AddToUserAuthCache(secretID, password)
		return password, nil
	}
	password = userCache.Password
	return password, nil
}

// DeletePasswordFromSecretManagerAndCache1 generates a strong password and creates a secret in GCP Secret Manager.
func DeletePasswordFromSecretManagerAndCache1(gcpService GoogleServices, secretID string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err == nil {
		err = gcpService.DeleteSecret(env.SecretManagerProjectID, secretID)
		if err != nil {
			logger.Errorf("failed to delete password for secretID: %s, err : %v", secretID, err)
			return err
		}

		done := common.RemoveFromUserAuthCache(secretID)
		if !done {
			logger.Errorf("failed to remove password from cache for secretID: %s", secretID)
			return nil
		}
	}
	return nil
}

// GetCertificateFromCacheOrSecretManager1 retrieves the certificate from cache or GCP Certificate and Secret Manager.
func GetCertificateFromCacheOrSecretManager1(ctx context.Context, certificateID string) (*models.Certificate, error) {
	certCache, exist := common.GetCertAuthCache(certificateID)
	// If not found in cache, fetch from GCP Certificate and Secret Manager
	if !exist || certCache.Certificate == nil {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return nil, err
		}
		certificateResponse, err := GetCertificateAndPrivateKeyByID(gcpService, env.CaPoolDeployedProjectID, env.SecretManagerProjectID, env.Region, env.CaPoolName, certificateID)
		if err != nil {
			return nil, err
		}
		cert := &models.Certificate{
			SignedCertificate:        certificateResponse.Certificate.PemCertificate,
			PrivateKey:               certificateResponse.Secret.SecretVersion.Value,
			CommonName:               certificateResponse.Certificate.SubjectCommonName,
			InterMediateCertificates: certificateResponse.Certificate.PemCertificateChain,
		}
		common.AddToCertAuthCache(certificateID, cert)
		return cert, nil
	}

	return certCache.Certificate, nil
}

// GetOrCreateCloudDNSRecord1 checks if a Cloud DNS record exists, and if not, creates it.
func GetOrCreateCloudDNSRecord1(gcpService GoogleServices, recordName, ipAddress string) (*hyperscalermodels.CustomCloudDNSRecord, error) {
	record, getErr := gcpService.GetResourceRecordSet(env.CaPoolDeployedProjectID, env.VsaManagedZone, recordName)
	if getErr != nil {
		gcpService.GetLogger().Debugf("Creating Cloud DNS record: %s.%s with type %s", recordName, env.VsaManagedZone, recordName)
		record, err := gcpService.CreateResourceRecordSet(env.CaPoolDeployedProjectID, env.VsaManagedZone, ipAddress, recordName)
		if err != nil {
			gcpService.GetLogger().Errorf("Failed to create Cloud DNS record: %v", err)
			return nil, errors.WrapAsTemporalApplicationError(err)
		}
		return record, nil
	}
	// If the record already exists, return it
	return record, nil
}

func DeleteCloudDNSRecord1(gcpService GoogleServices, recordName string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetResourceRecordSet(env.CaPoolDeployedProjectID, env.VsaManagedZone, recordName)
	if err == nil {
		logger.Debugf("Deleting Cloud DNS record: %s.%s", recordName, env.VsaManagedZone)
		err = gcpService.DeleteResourceRecordSet(env.CaPoolDeployedProjectID, env.VsaManagedZone, recordName)
		if err != nil {
			return err
		}
	}
	return nil
}

func RevokeCertificateAndDeleteFromCacheAndSecretManager1(gcpService GoogleServices, certificateID string) error {
	logger := gcpService.GetLogger()
	_, err := gcpService.GetCertificate(env.CaPoolDeployedProjectID, env.Region, env.CaPoolName, certificateID)
	if err != nil {
		logger.Errorf("Failed to get certificate from cache for project %s and region %s", env.CaPoolDeployedProjectID, env.Region)
		return err
	}
	certObject := &hyperscalermodels.CustomCertificate{
		CertOwningEntity: env.CaPoolDeployedProjectID,
		Region:           env.Region,
		CaGroupName:      env.CaPoolName,
		CertificateID:    certificateID,
	}

	// delete the certificate from CAS
	_, err = gcpService.RevokeCertificate(certObject)
	if err != nil {
		logger.Errorf("Failed to revoke certificate for project %s and region %s", env.CaPoolDeployedProjectID, env.Region)
		return err
	}

	_, err = gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, certificateID)
	if err != nil {
		logger.Errorf("Failed to get private key from secret manager for project %s and certificate %s", env.SecretManagerProjectID, certificateID)
		return err
	}

	// delete the private key from secret manager
	err = gcpService.DeleteSecret(env.SecretManagerProjectID, certificateID)
	if err != nil {
		logger.Errorf("Failed to delete private key from secret manager for project %s and certificate %s", env.SecretManagerProjectID, certificateID)
		return err
	}

	// delete from cache if not expired
	done := common.RemoveFromCertAuthCache(certificateID)
	if !done {
		logger.Errorf("Failed to remove certificate %s from cache", certificateID)
	}

	return nil
}

// GenerateCSR1 generates a Certificate Signing Request (CSR) with the specified common name and domains.
func GenerateCSR1(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
	// Generate an RSA private key.
	key, err := rsa.GenerateKey(rand.Reader, 3072)
	if err != nil {
		return nil, nil, err
	}

	// Build Key Usage extension. We want DigitalSignature and KeyEncipherment set.
	keyUsageVal := DigitalSignature | KeyEncipherment // Should be 0x80 | 0x20 = 0xA0 (10100000)

	// Create the ASN.1 BIT STRING for key usage.
	bitString := asn1.BitString{
		Bytes:     []byte{byte(keyUsageVal)},
		BitLength: 8, // We are encoding one full byte.
	}
	rawKeyUsage, err := asn1.Marshal(bitString)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal key usage: %s", err.Error())
	}

	// --- Build Extended Key Usage extension ---
	// We want both serverAuth and clientAuth.
	ekuOIDs := []asn1.ObjectIdentifier{
		{1, 3, 6, 1, 5, 5, 7, 3, 1},
		{1, 3, 6, 1, 5, 5, 7, 3, 2},
	}
	rawEKU, err := asn1.Marshal(ekuOIDs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal extended key usage: %v", err)
	}

	// Prepare the extensions.
	extensions := []pkix.Extension{
		{
			Id:       asn1.ObjectIdentifier{2, 5, 29, 15}, // Key Usage
			Critical: true,
			Value:    rawKeyUsage,
		},
		{
			Id:       asn1.ObjectIdentifier{2, 5, 29, 37}, // Extended Key Usage
			Critical: false,
			Value:    rawEKU,
		},
	}

	// Build the certificate request template.
	template := x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   commonName,
			Organization: []string{"Netapp"},
		},
		SignatureAlgorithm: x509.SHA256WithRSA,
		ExtraExtensions:    extensions,
		DNSNames:           domains,
	}

	// Create the CSR in DER format.
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &template, key)
	if err != nil {
		return nil, nil, err
	}

	return csrDER, key, nil
}

const CsrType = "CERTIFICATE REQUEST"

const RsaKeyType = "RSA PRIVATE KEY"

const DigitalSignature = 0x80 // 10000000 in binary (bit 0)

const KeyEncipherment = 0x20

var GetGCPService = GetGCPService1

// GetGCPService1 initializes and returns a GcpServices instance.
func GetGCPService1(ctx context.Context) (*google.GcpServices, error) {
	gcpService := NewGcpServices(ctx)

	gcpService.Logger.Debug("gcpService initialized")

	gcpService.Logger.Debug("Calling InitializeClients")
	err := gcpService.InitializeClients()
	if err != nil || !gcpService.IsAdminClientInitialized() {
		gcpService.Logger.Debug("Initialisation of service failed")
		return nil, errors.NewVCPError(errors.ErrGCPClientInitializationError, errors2.New("initialisation of Google GCP service failed"))
	}
	return gcpService, nil
}

// NewGcpServices creates a new instance of GcpServices with the provided context
func NewGcpServices(ctx context.Context) *google.GcpServices {
	return &google.GcpServices{
		Ctx:    ctx,
		Logger: util.GetLogger(ctx),
		Retry:  google.NewExponentialRetryStrategy(time.Second, uint(MaxRetries)),
	}
}

var MaxRetries = env.GetInt("GOOGLE_API_MAX_RETRIES", 6)

var CreateNodeForProvider = C1reateNodeForProvider

// CreateNodeForProvider creates a node for a given provider using the provided information.
func C1reateNodeForProvider(inp NodeProviderInput) *models.Node {
	endpointAddressToHostNameMap := make(map[string]string)
	if inp.AuthType == env.USER_CERTIFICATE {
		for _, node := range inp.Nodes {
			if node.EndpointAddress != "" {
				endpointAddressToHostNameMap[node.EndpointAddress] = node.HostDNSName
			}
		}
		return &models.Node{
			EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
			DeploymentName:                 inp.DeploymentName,
			CertificateID:                  inp.CertificateID,
			SecretID:                       inp.SecretID,
			AuthType:                       inp.AuthType,
		}
	}

	for _, node := range inp.Nodes {
		if node.EndpointAddress != "" {
			endpointAddressToHostNameMap[node.EndpointAddress] = node.EndpointAddress
		}
	}

	return &models.Node{
		EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
		Password:                       inp.Password,
		DeploymentName:                 inp.DeploymentName,
		SecretID:                       inp.SecretID,
		AuthType:                       inp.AuthType,
	}
}

type NodeProviderInput struct {
	Nodes          []*datamodel.Node
	Password       string
	SecretID       string
	CertificateID  string
	DeploymentName string
	AuthType       int
}

// PrepareOperationID constructs a GCP operation ID from the provided project number, location ID, and job ID.
func PrepareOperationID(projectNumber, locationId, jobId string) string {
	if projectNumber == "" || locationId == "" || jobId == "" {
		return ""
	}
	return "/v1beta/projects/" + projectNumber + "/locations/" + locationId + "/operations/" + jobId
}
