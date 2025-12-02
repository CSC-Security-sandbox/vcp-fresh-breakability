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
	common2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	errors2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// GetProviderByNode creates a VSA provider using CA fields from Node struct (with env var fallback)
var GetProviderByNode = _getProviderByNode

func _getProviderByNode(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	if node.AuthType == env.USER_CERTIFICATE {
		// Create PoolCredentials from Node's CA URI for certificate retrieval
		poolCredentials := &datamodel.PoolCredentials{
			CaURI:         node.GetCaURIWithFallback(),
			CertificateID: node.CertificateID,
		}

		certificate, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)

		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get certificate for node %s: %v", node.Name, err)
			return nil, errors.NewVCPError(errors.ErrGCPResourceFetchError, err)
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
			return nil, errors.NewVCPError(errors.ErrGCPResourceFetchError, err)
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

// GetProviderByNodeWithFastConnection creates a VSA provider with fast connection using CA fields from Node struct
var GetProviderByNodeWithFastConnection = _getProviderByNodeWithFastConnection

func _getProviderByNodeWithFastConnection(ctx context.Context, node *models.Node) (vsa.Provider, error) {
	if node.AuthType == env.USER_CERTIFICATE {
		// Create PoolCredentials from Node's CA URI for certificate retrieval
		poolCredentials := &datamodel.PoolCredentials{
			CaURI:         node.GetCaURIWithFallback(),
			CertificateID: node.CertificateID,
		}

		certificate, err := GetCertificateFromCacheOrSecretManager(ctx, poolCredentials)

		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get certificate for node %s: %v", node.Name, err)
			return nil, errors.NewVCPError(errors.ErrGCPResourceFetchError, err)
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
			FastConnection:     true, // Enable fast connection for quick health checks
		}), nil
	}

	var password string
	if node.AuthType == env.USERNAME_PWD_SEC_MGR {
		secret, err := GetPasswordFromCacheOrSecretManager(ctx, node.SecretID)
		if err != nil {
			util.GetLogger(ctx).Errorf("Failed to get password for node %s: %v", node.Name, err)
			return nil, errors.NewVCPError(errors.ErrGCPResourceFetchError, err)
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
		FastConnection:     true, // Enable fast connection for quick health checks
	}), nil
}

var GetCertificateFromCacheOrSecretManager = _getCertificateFromCacheOrSecretManager

var RevokeCertificateAndDeleteFromCacheAndSecretManager = _revokeCertificateAndDeleteFromCacheAndSecretManager

var GenerateAndCreateCertificateForVSACluster = _generateAndCreateCertificateForVSACluster

var GeneratePasswordForVSACluster = _generatePasswordForVSACluster

var GetPasswordForVSACluster = _getPasswordForVSACluster

var GetPasswordFromCacheOrSecretManager = _getPasswordFromCacheOrSecretManager

var GenerateCSR = _generateCSR

var DeletePasswordFromCacheAndSecretManager = _deletePasswordFromSecretManagerAndCache

var DeleteCloudDNSRecord = _deleteCloudDNSRecord

var GetOrCreateCloudDNSRecord = _getOrCreateCloudDNSRecord

var GetCertificateAndPrivateKeyByID = _getCertificateAndPrivateKeyByID

var CreatePrivateKeyInSecretManager = _createPrivateKeyInSecretManager

var CreateCertificateInCAS = _createCertificateInCAS

var CreateCertificateInCASAndPrivateKeyInSM = _createCertificateInCASAndPrivateKeyInSM

var GetCertificateAndSecret = _getCertificateAndSecret

var DeleteCertificateAndSecret = _deleteCertificateAndSecret

// _generateAndCreateCertificateForVSACluster generates a CSR and creates a certificate in GCP Certificate Authority Service.
func _generateAndCreateCertificateForVSACluster(gcpService GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscalermodels.CustomCertificateResponse, error) {
	logger := gcpService.GetLogger()
	certificateID := poolCredentials.CertificateID
	// Get Both Certificate and Secret
	certificate, secret, err := GetCertificateAndSecret(gcpService, poolCredentials)
	if err != nil {
		logger.Errorf("Failed to get Certificate and Secret for certificateID: %s, err: %v", certificateID, err)
		return nil, err
	}

	// NotFoundError already handled and hence certificate and secret will be nil if not found
	if certificate != nil && secret != nil {
		common.AddToCertAuthCache(certificateID, &models.Certificate{
			CommonName:               certificate.SubjectCommonName,
			SignedCertificate:        certificate.PemCertificate,
			PrivateKey:               secret.SecretVersion.Value,
			InterMediateCertificates: certificate.PemCertificateChain,
		})

		return &hyperscalermodels.CustomCertificateResponse{
			Certificate: certificate,
			Secret:      secret,
		}, nil
	}

	// Delete the certificate and Secret if any exist
	err = DeleteCertificateAndSecret(gcpService, certificate, secret, poolCredentials)
	if err != nil {
		logger.Errorf("Failed to delete certificate and private key for certificateID: %s, err: %v", certificateID, err)
		return nil, err
	}

	// If certificate and secret are nil so create a CSR and request new certificate in CAS and store the private key in secret manager
	logger.Debugf("Generating and creating certificate for cluster: %s with certificateID: %s", clusterName, certificateID)
	certificate, secret, err = CreateCertificateInCASAndPrivateKeyInSM(gcpService, certificateID, clusterName, username, poolCredentials, isServerAuthEnabled)
	if err != nil {
		logger.Errorf("Failed to create certificate and store private key for cluster: %s with certificateID: %s", clusterName, certificateID)
		return nil, err
	}

	logger.Debugf("certificate created successfully for certificateID: %s", certificateID)
	// Add the certificate to the cache
	common.AddToCertAuthCache(certificateID, &models.Certificate{
		CommonName:               certificate.SubjectCommonName,
		SignedCertificate:        certificate.PemCertificate,
		PrivateKey:               secret.SecretVersion.Value,
		InterMediateCertificates: certificate.PemCertificateChain,
	})

	return &hyperscalermodels.CustomCertificateResponse{
		Certificate: certificate,
		Secret:      secret,
	}, nil
}

func _deleteCertificateAndSecret(gcpService GoogleServices, certificate *hyperscalermodels.CustomCertificate, secret *hyperscalermodels.CustomSecret, poolCredentials *datamodel.PoolCredentials) error {
	logger := gcpService.GetLogger()
	certificateID := poolCredentials.CertificateID
	if certificate != nil {
		// Use environment variables for Region (always from env)
		region := env.Region

		// Parse CA URI from poolCredentials, fallback to environment variables
		caPoolDeployedProjectID, caPoolName, _ := poolCredentials.ParseCaURIWithFallback()

		certObject := &hyperscalermodels.CustomCertificate{
			CertOwningEntity: caPoolDeployedProjectID,
			Region:           region,
			CaGroupName:      caPoolName,
			CertificateID:    certificateID,
		}

		// delete the certificate from CAS
		_, err := gcpService.RevokeCertificate(certObject)
		if err != nil {
			logger.Errorf("Failed to revoke certificate for projectID: %s, region: %s and certificateID: %s", caPoolDeployedProjectID, region, certificateID)
			return err
		}
	}

	if secret != nil {
		// delete the private key from secret manager
		err := gcpService.DeleteSecret(env.SecretManagerProjectID, certificateID)
		if err != nil {
			logger.Errorf("Failed to delete private key from secret manager for projectID: %s and certificate: %s", env.SecretManagerProjectID, certificateID)
			return err
		}
	}
	return nil
}

func _getCertificateAndSecret(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) (*hyperscalermodels.CustomCertificate, *hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()
	certificateID := poolCredentials.CertificateID

	// Use environment variables for Region (always from env)
	region := env.Region

	// Parse CA URI from poolCredentials, fallback to environment variables
	caPoolDeployedProjectID, caPoolName, _ := poolCredentials.ParseCaURIWithFallback()

	cert, getErr := gcpService.GetCertificate(caPoolDeployedProjectID, region, caPoolName, certificateID)
	if getErr != nil {
		logger.Errorf("Failed to get certificate for certificateID: %s, err: %v", certificateID, getErr)
		return nil, nil, getErr
	}
	secret, getErr := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, certificateID)
	if getErr != nil {
		logger.Errorf("Failed to get secret for certificateID: %s, err: %v", certificateID, getErr)
		return nil, nil, getErr
	}
	return cert, secret, nil
}

func _createCertificateInCASAndPrivateKeyInSM(gcpService GoogleServices, certificateID string, clusterName string, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscalermodels.CustomCertificate, *hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()

	// Use environment variables for Region (always from env)
	region := env.Region

	// Parse CA URI from poolCredentials, fallback to environment variables
	caPoolDeployedProjectID, caPoolName, caName := poolCredentials.ParseCaURIWithFallback()

	domains := fmt.Sprintf("*.%s.%s", clusterName, env.VsaDeployedDnsName)
	certObj := &hyperscalermodels.CustomCertificateParam{
		Region:           region,
		CertificateID:    certificateID,
		CaPoolName:       caPoolName,
		CaName:           caName,
		CommonName:       username,
		Domains:          []string{domains},
		CertOwningEntity: caPoolDeployedProjectID,
	}
	// Generate CSR
	csrDER, key, err := GenerateCSR(certObj.CommonName, certObj.Domains, isServerAuthEnabled)
	if err != nil {
		logger.Errorf("failed to generate CSR for commonName: %s, certificateId : %s, err : %v", certObj.CommonName, certObj.CertificateID, err)
		return nil, nil, err
	}

	pemBlock := pem.Block{
		Type:  CsrType,
		Bytes: csrDER,
	}
	logger.Debugf("Generate CSR for commonName: %s, certificateId : %s", certObj.CommonName, certObj.CertificateID)

	customCertificate, err := common2.ValidateAndConvertCertParams(certObj, pemBlock)
	if err != nil {
		return nil, nil, err
	}

	// Create the private key in Secret Manager
	secret, err := CreatePrivateKeyInSecretManager(gcpService, customCertificate, key)
	if err != nil {
		logger.Errorf("failed to create private key in Secret Manager for projectID: %s, certificateId : %s, err : %v", env.SecretManagerProjectID, certObj.CertificateID, err)
		return nil, nil, err
	}

	// Create the Certificate in CAS
	cert, err := CreateCertificateInCAS(gcpService, customCertificate)
	if err != nil {
		logger.Errorf("failed to create Certificate in CAS for projectID: %s, certificateId : %s, err : %v", certObj.CertOwningEntity, certObj.CertificateID, err)
		return nil, nil, err
	}
	return cert, secret, nil
}

// _createCertificateInCAS creates a certificate in GCP Certificate Authority Service and stores the private key in Secret Manager.
func _createCertificateInCAS(gcpService GoogleServices, certificate *hyperscalermodels.CustomCertificate) (*hyperscalermodels.CustomCertificate, error) {
	logger := gcpService.GetLogger()
	logger.Debugf("Creating certificate in CAS for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	var cert *hyperscalermodels.CustomCertificate
	// Create the Certificate
	cert, err := gcpService.CreateCertificate(certificate)
	if err != nil {
		logger.Errorf("failed to create certificate in CAS for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
		return nil, err
	}
	logger.Debugf("created certificate in CAS for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	return cert, nil
}

func _createPrivateKeyInSecretManager(gcpService GoogleServices, certificate *hyperscalermodels.CustomCertificate, key *rsa.PrivateKey) (*hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()
	logger.Debugf("Creating private key in Secret Manager for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	// Store the private key in Secret Manager
	secretValue := common2.ConvertPrivateKeyToString(key, RsaKeyType)
	secret, err := gcpService.CreateSecret(env.SecretManagerProjectID, certificate.Region, certificate.CertificateID, secretValue)
	if err != nil {
		logger.Errorf("failed to create secret in SM for commonName: %s, certificateId : %s, err : %v", certificate.SubjectCommonName, certificate.CertificateID, err)
		return nil, err
	}
	logger.Debugf("created secret in SM for commonName: %s, certificateId : %s", certificate.SubjectCommonName, certificate.CertificateID)
	return secret, nil
}

// _getCertificateAndPrivateKeyByID retrieves the certificate for a VSA cluster from GCP Certificate Authority Service and Private key from Secret Manager.
func _getCertificateAndPrivateKeyByID(gcpService GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscalermodels.CustomCertificateResponse, error) {
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

// _generatePasswordForVSACluster generates a strong password and creates a secret in GCP Secret Manager.
func _generatePasswordForVSACluster(gcpService GoogleServices, secretID string) (*hyperscalermodels.CustomSecret, error) {
	logger := gcpService.GetLogger()
	var secret *hyperscalermodels.CustomSecret
	projectID := env.SecretManagerProjectID
	secret, getSecretError := gcpService.GetSecretWithLatestVersion(projectID, secretID)
	if getSecretError != nil {
		return nil, getSecretError
	}
	if secret == nil {
		password, err := utils.GenerateStrongPassword(12)
		if err != nil {
			logger.Errorf("failed to generate password for secretID: %s, err : %v", secretID, err)
			return nil, err
		}
		secret, err = gcpService.CreateSecret(projectID, env.Region, secretID, password)
		if err != nil {
			return nil, err
		}
		common.AddToUserAuthCache(secretID, secret.SecretVersion.Value)
	}
	return secret, nil
}

// _getPasswordForVSACluster retrieves the password for a VSA cluster from GCP Secret Manager.
func _getPasswordForVSACluster(gcpService GoogleServices, secretID string) (*hyperscalermodels.CustomSecret, error) {
	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil || secret == nil || secret.SecretVersion == nil {
		return nil, fmt.Errorf("failed to get secret for project: %s, userName: %s, err: %s", env.SecretManagerProjectID, secretID, err)
	}
	return secret, nil
}

// _getPasswordFromCacheOrSecretManager retrieves the password for a VSA cluster from cache or GCP Secret Manager if not found in cache.
func _getPasswordFromCacheOrSecretManager(ctx context.Context, secretID string) (string, error) {
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

// _deletePasswordFromSecretManagerAndCache generates a strong password and creates a secret in GCP Secret Manager.
func _deletePasswordFromSecretManagerAndCache(gcpService GoogleServices, secretID string) error {
	logger := gcpService.GetLogger()
	secret, err := gcpService.GetSecretWithLatestVersion(env.SecretManagerProjectID, secretID)
	if err != nil {
		return err
	}
	if secret != nil {
		err = gcpService.DeleteSecret(env.SecretManagerProjectID, secretID)
		if err != nil {
			logger.Errorf("failed to delete password for secretID: %s, err : %v", secretID, err)
			return err
		}
	}

	done := common.RemoveFromUserAuthCache(secretID)
	if !done {
		logger.Errorf("failed to remove password from cache for secretID: %s", secretID)
	}
	return nil
}

// _getCertificateFromCacheOrSecretManager retrieves the certificate from cache or GCP Certificate and Secret Manager.
func _getCertificateFromCacheOrSecretManager(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*models.Certificate, error) {
	certificateID := poolCredentials.CertificateID
	certCache, exist := common.GetCertAuthCache(certificateID)
	// If not found in cache, fetch from GCP Certificate and Secret Manager
	if !exist || certCache.Certificate == nil {
		gcpService, err := GetGCPService(ctx)
		if err != nil {
			return nil, err
		}

		// Use environment variables for Region (always from env)
		region := env.Region

		// Parse CA URI from poolCredentials, fallback to environment variables
		caPoolDeployedProjectID, caPoolName, _ := poolCredentials.ParseCaURIWithFallback()

		certificateResponse, err := GetCertificateAndPrivateKeyByID(gcpService, caPoolDeployedProjectID, env.SecretManagerProjectID, region, caPoolName, certificateID)
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

// _getOrCreateCloudDNSRecord checks if a Cloud DNS record exists, and if not, creates it.
func _getOrCreateCloudDNSRecord(gcpService GoogleServices, recordName, ipAddress string) (*hyperscalermodels.CustomCloudDNSRecord, error) {
	gcpService.GetLogger().Debugf("Get and Create Cloud DNS for projectID: %s, record: %s, managedZone: %s", env.SecretManagerProjectID, recordName, env.VsaManagedZone)
	record, err := gcpService.GetResourceRecordSet(env.SecretManagerProjectID, env.VsaManagedZone, recordName)
	if err != nil {
		gcpService.GetLogger().Errorf("Failed to get Cloud DNS record: %s, err: %v", recordName, err)
		return nil, err
	}
	if record == nil {
		gcpService.GetLogger().Debugf("Creating Cloud DNS record: %s, managedzone %s", recordName, env.VsaManagedZone)
		record, err = gcpService.CreateResourceRecordSet(env.SecretManagerProjectID, env.VsaManagedZone, ipAddress, recordName)
		if err != nil {
			gcpService.GetLogger().Errorf("Failed to create Cloud DNS record: %s, err: %v", recordName, err)
			return nil, err
		}
	}

	// If the record already exists, return it
	return record, nil
}

func _deleteCloudDNSRecord(gcpService GoogleServices, recordName string) error {
	logger := gcpService.GetLogger()
	record, err := gcpService.GetResourceRecordSet(env.SecretManagerProjectID, env.VsaManagedZone, recordName)
	if err != nil {
		logger.Errorf("Failed to get Cloud DNS record: %v", err)
		return err
	}
	if record != nil {
		logger.Debugf("Deleting Cloud DNS record: %s.%s", recordName, env.VsaManagedZone)
		err = gcpService.DeleteResourceRecordSet(env.SecretManagerProjectID, env.VsaManagedZone, recordName)
		if err != nil {
			return err
		}
	}
	return nil
}

func _revokeCertificateAndDeleteFromCacheAndSecretManager(gcpService GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
	logger := gcpService.GetLogger()
	certificateID := poolCredentials.CertificateID
	certificate, secret, err := GetCertificateAndSecret(gcpService, poolCredentials)
	if err != nil {
		logger.Errorf("Failed to get Certificate and Secret for certificateID: %s, err: %v", certificateID, err)
		return err
	}

	// Delete the certificate and Secret if any exist
	err = DeleteCertificateAndSecret(gcpService, certificate, secret, poolCredentials)
	if err != nil {
		logger.Errorf("Failed to delete certificate and private key for certificateID: %s, err: %v", certificateID, err)
		return err
	}

	// delete from cache if not expired
	done := common.RemoveFromCertAuthCache(certificateID)
	if !done {
		logger.Errorf("Failed to remove certificate %s from cache", certificateID)
	}
	return nil
}

// _generateCSR generates a Certificate Signing Request (CSR) with the specified common name and domains.
func _generateCSR(commonName string, domains []string, isServerAuthEnabled bool) ([]byte, *rsa.PrivateKey, error) {
	// Generate an RSA private key.
	key, err := rsa.GenerateKey(rand.Reader, env.PrivateKeyBits)
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
	// We want clientAuth for all certificates, and serverAuth for VCP_ADMIN_CERT_UN_SUFFIX certificates.
	ekuOIDs := []asn1.ObjectIdentifier{
		{1, 3, 6, 1, 5, 5, 7, 3, 2},
	}

	// If it is VCP_ADMIN_CERT_UN_SUFFIX, add serverAuth as well.
	if isServerAuthEnabled {
		ekuOIDs = append(ekuOIDs, asn1.ObjectIdentifier{1, 3, 6, 1, 5, 5, 7, 3, 1})
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

var GetGCPService = _getGCPService

// _getGCPService initializes and returns a GcpServices instance.
func _getGCPService(ctx context.Context) (*google.GcpServices, error) {
	gcpService := NewGcpServices(ctx)

	gcpService.Logger.Debug("gcpService initialized")
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

var CreateNodeForProvider = _createNodeForProvider

// _createNodeForProvider creates a node for a given provider using the provided information.
func _createNodeForProvider(inp NodeProviderInput) *models.Node {
	if inp.OntapCredentials == nil {
		// This should not happen in practice, but handle gracefully
		return &models.Node{
			DeploymentName: inp.DeploymentName,
		}
	}

	// Populate CA URI from OntapCredentials, fall back to environment variables if not set
	caURI := inp.OntapCredentials.GetCaURIWithFallback()

	endpointAddressToHostNameMap := make(map[string]string)
	if inp.OntapCredentials.AuthType == env.USER_CERTIFICATE {
		for _, node := range inp.Nodes {
			if node.EndpointAddress != "" {
				endpointAddressToHostNameMap[node.EndpointAddress] = node.HostDNSName
			}
		}
		return &models.Node{
			EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
			DeploymentName:                 inp.DeploymentName,
			CertificateID:                  inp.OntapCredentials.CertificateID,
			SecretID:                       inp.OntapCredentials.SecretID,
			AuthType:                       inp.OntapCredentials.AuthType,
			CaURI:                          caURI,
		}
	}

	for _, node := range inp.Nodes {
		if node.EndpointAddress != "" {
			endpointAddressToHostNameMap[node.EndpointAddress] = node.EndpointAddress
		}
	}

	return &models.Node{
		EndpointAddressesToHostNameMap: endpointAddressToHostNameMap,
		Password:                       inp.OntapCredentials.Password,
		DeploymentName:                 inp.DeploymentName,
		SecretID:                       inp.OntapCredentials.SecretID,
		AuthType:                       inp.OntapCredentials.AuthType,
		CaURI:                          caURI,
	}
}

type NodeProviderInput struct {
	Nodes          []*datamodel.Node
	DeploymentName string
	// Required: OntapCredentials from database (contains Password, SecretID, CertificateID, AuthType, and CA fields with env var fallback)
	OntapCredentials *datamodel.PoolCredentials
}

// PrepareOperationID constructs a GCP operation ID from the provided project number, location ID, and job ID.
func PrepareOperationID(projectNumber, locationId, jobId string) string {
	if projectNumber == "" || locationId == "" || jobId == "" {
		return ""
	}
	return "/v1beta/projects/" + projectNumber + "/locations/" + locationId + "/operations/" + jobId
}
