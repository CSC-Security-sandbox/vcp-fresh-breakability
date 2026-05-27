package common

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
)

var (
	ValidateAndConvertCertParams = _validateAndConvertCertParams
	ConvertPrivateKeyToString    = _convertPrivateKeyToString
	ConvertToCustomSecretVersion = _convertToCustomSecretVersion
)

// _validateAndConvertCertParams validates the custom certificate parameters and converts them to a CustomCertificate model.
func _validateAndConvertCertParams(param *models.CustomCertificateParam, pemBlock pem.Block) (*models.CustomCertificate, error) {
	if param == nil || param.CertificateID == "" || param.CaName == "" || param.CertOwningEntity == "" || param.Region == "" || param.CaPoolName == "" || pemBlock.Type == "" && param.CommonName == "" {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("invalid certificate parameters"))
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

// _convertPrivateKeyToString converts an RSA private key to a PEM encoded string.
func _convertPrivateKeyToString(key *rsa.PrivateKey, rsaKeyType string) string {
	privateKeyPEM := &pem.Block{
		Type:  rsaKeyType,
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	}
	privateKeyBytes := pem.EncodeToMemory(privateKeyPEM)
	return string(privateKeyBytes)
}

// _convertToCustomSecretVersion converts a secret version name and value to a CustomSecretVersion model.
func _convertToCustomSecretVersion(secretVersionName, secretValue string) (*models.CustomSecretVersion, error) {
	if secretVersionName == "" {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("input secret is nil"))
	}
	if secretValue == "" {
		return nil, vsaerrors.ExtractCustomError(fmt.Errorf("input secret value is nil"))
	}
	customCert := &models.CustomSecretVersion{
		Name:  secretVersionName,
		Value: secretValue,
	}
	return customCert, nil
}

// ParseTimestamps converts a timestamp string in RFC3339 format to a time.Time pointer.
func ParseTimestamps(timeStr string) (*time.Time, error) {
	var timeStamp *time.Time

	if timeStr != "" {
		parsedTime, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return nil, vsaerrors.ExtractCustomError(fmt.Errorf("failed to parse time: %v", err))
		}
		timeStamp = &parsedTime
	}
	return timeStamp, nil
}
