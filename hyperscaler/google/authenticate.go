package google

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"google.golang.org/api/dns/v1"
	"google.golang.org/api/privateca/v1"
	"google.golang.org/api/secretmanager/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ValidateAndConvertCertificateParamsToCustomCertificate    = _validateAndConvertCertificateParamsToCustomCertificate
	ConvertPrivateKeyToString                                 = _convertPrivateKeyToString
	ValidateAndConvertPrivateCACertificateToCustomCertificate = _validateAndConvertPrivateCACertificateToCustomCertificate
	ConvertSecretToCustomSecret                               = _convertSecretToCustomSecret
	ConvertSecretVersionToCustomSecretVersion                 = _convertSecretVersionToCustomSecretVersion
	ValidateAndConvertToCustomCloudDNSRecord                  = _validateAndConvertToCustomCloudDNSRecord
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
	if param == nil || param.CertificateID == "" || param.CaName == "" || param.CertOwningEntity == "" || param.Region == "" || param.CaPoolName == "" || pemBlock.Type == "" && param.CommonName == "" {
		return nil, fmt.Errorf("invalid certificate parameters")
	}
	return &models.CustomCertificate{
		CertificateID:     param.CertificateID,
		CaName:            param.CaName,
		CertOwningEntity:  param.CertOwningEntity,
		Region:            param.Region,
		CaGroupName:       param.CaPoolName,
		PemCsr:            string(pem.EncodeToMemory(&pemBlock)),
		SubjectCommonName: param.CommonName,
	}, nil
}

func _validateAndConvertToCustomCloudDNSRecord(recordSet *dns.ResourceRecordSet, managedZone string) (*models.CustomCloudDNSRecord, error) {
	if recordSet == nil {
		return nil, fmt.Errorf("resource record set is nil")
	}
	if recordSet.Name == "" || recordSet.Type == "" || recordSet.Ttl == 0 || recordSet.Rrdatas == nil || len(recordSet.Rrdatas) == 0 {
		return nil, fmt.Errorf("resource record set is invalid")
	}
	return &models.CustomCloudDNSRecord{
		RecordName:  recordSet.Name,
		Type:        recordSet.Type,
		TTL:         recordSet.Ttl,
		Data:        recordSet.Rrdatas[0],
		ManagedZone: managedZone,
	}, nil
}
