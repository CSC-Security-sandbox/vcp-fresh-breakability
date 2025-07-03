package common

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/go-openapi/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	USERNAME_PWD         = 0 // Username/Password authentication
	USERNAME_PWD_SEC_MGR = 1 // Username/Password authentication with secret manager
	USER_CERTIFICATE     = 2 // Certificate authentication
	VCP_ADMIN            = "vcp_admin"
)

var (
	AuthType                = env.GetInt("VSA_AUTH_TYPE", USERNAME_PWD) // 0 for username/password, 1 for username/password in secret manager and 2 for certificate authentication
	Region                  = env.GetString("LOCAL_REGION", "")
	CaName                  = env.GetString("CA_NAME", "")
	CaPoolName              = env.GetString("CA_POOL_NAME", "")
	CaPoolDeployedProjectID = env.GetString("CA_POOL_DEPLOYED_PROJECT_ID", "")
	SecretManagerProjectID  = env.GetString("SECRET_MANAGER_PROJECT_ID", "")
	VsaDeployedDnsName      = env.GetString("VSA_DEPLOYED_DNS_NAME", "")
	VsaManagedZone          = env.GetString("VSA_MANAGED_ZONE", "")
	CertificateLifetime     = env.GetString("CERTIFICATE_LIFETIME", "94608000s") // Default to 3 years
	NodePassword            = env.GetString("VSA_NODE_PASSWORD", "")
	CloudDNSCacheTTL        = env.GetInt64("CLOUD_DNS_CACHE_TTL", 300) // Default to 300 seconds
)

func ValidateEnvironmentVariables() error {
	switch AuthType {
	case USERNAME_PWD_SEC_MGR:
		if Region == "" {
			return errors.New(500, "LOCAL_REGION must be set when using username/password authentication with secret manager")
		}
		if SecretManagerProjectID == "" {
			return errors.New(500, "SECRET_MANAGER_PROJECT_ID must be set when using username/password authentication with secret manager")
		}
	case USER_CERTIFICATE:
		if Region == "" {
			return errors.New(500, "LOCAL_REGION must be set when using certificate authentication")
		}
		if CaName == "" {
			return errors.New(500, "CA_NAME must be set when using certificate authentication")
		}
		if CaPoolName == "" {
			return errors.New(500, "CA_POOL_NAME must be set when using certificate authentication")
		}
		if CaPoolDeployedProjectID == "" {
			return errors.New(500, "CA_POOL_DEPLOYED_PROJECT_ID must be set when using certificate authentication")
		}
		if SecretManagerProjectID == "" {
			return errors.New(500, "SECRET_MANAGER_PROJECT_ID must be set when using certificate authentication")
		}
		if VsaDeployedDnsName == "" {
			return errors.New(500, "VSA_DEPLOYED_DNS_NAME must be set when using certificate authentication")
		}
		if VsaManagedZone == "" {
			return errors.New(500, "VSA_MANAGED_ZONE must be set when using certificate authentication")
		}
		if CertificateLifetime == "" {
			return errors.New(500, "CERTIFICATE_LIFETIME must be set when using certificate authentication")
		}
		if CloudDNSCacheTTL == 0 {
			return errors.New(500, "CLOUD_DNS_CACHE_TTL must be set when using certificate authentication")
		}
	default:
		if NodePassword == "" {
			return errors.New(500, "VSA_NODE_PASSWORD must be set when using username/password authentication")
		}
	}
	return nil
}

var (
	ValidateAndConvertCertificateParamsToCustomCertificate    = _validateAndConvertCertificateParamsToCustomCertificate
	ConvertPrivateKeyToString                                 = _convertPrivateKeyToString
	ValidateAndConvertPrivateCACertificateToCustomCertificate = _validateAndConvertPrivateCACertificateToCustomCertificate
	ConvertSecretToCustomSecret                               = _convertSecretToCustomSecret
	ConvertSecretVersionToCustomSecretVersion                 = _convertSecretVersionToCustomSecretVersion
)

func _validateAndConvertPrivateCACertificateToCustomCertificate(certificateId string, cert *privateca.Certificate) (*models.CustomCertificate, error) {
	if cert == nil {
		return nil, fmt.Errorf("input certificate is nil")
	}
	customCert, err := convertPrivateCACertificateToCustomCertificate(certificateId, cert)
	if err != nil {
		return nil, err
	}
	if cert.CertificateDescription != nil && cert.CertificateDescription.SubjectDescription != nil {
		if cert.CertificateDescription.SubjectDescription.Subject != nil {
			if cert.CertificateDescription.SubjectDescription.Subject.CommonName != "" {
				customCert.SubjectCommonName = cert.CertificateDescription.SubjectDescription.Subject.CommonName
			}
			if cert.CertificateDescription.SubjectDescription.Subject.Organization != "" {
				customCert.SubjectOrganization = cert.CertificateDescription.SubjectDescription.Subject.Organization
			}
			if cert.CertificateDescription.SubjectDescription.SubjectAltName.DnsNames != nil {
				customCert.SubjectAltName = cert.CertificateDescription.SubjectDescription.SubjectAltName.DnsNames
			}
		}
	}
	return customCert, nil
}

func _convertSecretToCustomSecret(secret *secretmanager.Secret, secretVersion *models.CustomSecretVersion) (*models.CustomSecret, error) {
	if secret == nil {
		return nil, fmt.Errorf("input secret is nil")
	}
	if secretVersion == nil {
		return nil, fmt.Errorf("input secret version is nil")
	}
	createTime, err := parseTimestamps(secret.CreateTime)
	if err != nil {
		return nil, err
	}
	lifeTime, err := parseTimestamps(secret.ExpireTime)
	if err != nil {
		return nil, err
	}

	version, err := ConvertSecretVersionToCustomSecretVersion(secretVersion.Name, secretVersion.Value)
	if err != nil {
		return nil, err
	}
	customCert := &models.CustomSecret{
		Name:          secret.Name,
		CreateTime:    createTime,
		LifeTime:      lifeTime,
		SecretVersion: version,
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
	customCert := &models.CustomCertificate{
		CertificateID:              certificateId,
		Name:                       cert.Name,
		PemCertificate:             cert.PemCertificate,
		CreateTime:                 createTime,
		LifeTime:                   cert.Lifetime,
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
	if param == nil || param.CertificateID == "" || param.CaName == "" || param.CertOwningEntity == "" || param.Region == "" || param.CaPoolName == "" || pemBlock.Type == "" {
		return nil, fmt.Errorf("invalid certificate parameters")
	}
	return &models.CustomCertificate{
		CertificateID:    param.CertificateID,
		CaName:           param.CaName,
		CertOwningEntity: param.CertOwningEntity,
		Region:           param.Region,
		CaGroupName:      param.CaPoolName,
		PemCsr:           string(pem.EncodeToMemory(&pemBlock)),
	}, nil
}
