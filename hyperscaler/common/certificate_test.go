package common

import (
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
)

func Test_convertToCustomSecretVersion(t *testing.T) {
	t.Run("WhenSecretVersionNameIsEmpty", func(tt *testing.T) {
		result, err := _convertToCustomSecretVersion("", "test-value")
		assert.Nil(tt, result, "Expected result to be nil when secret version name is empty")
		assert.EqualError(tt, err.(*vsaerrors.CustomError).OriginalErr, "input secret is nil", "Expected error message to match")
	})

	t.Run("WhenSecretVersionNameIsValid", func(tt *testing.T) {
		secretVersionName := "projects/test-project/secrets/test-secret/versions/1"
		secretValue := "test-value"

		expected := &models.CustomSecretVersion{
			Name:  secretVersionName,
			Value: secretValue,
		}

		result, err := _convertToCustomSecretVersion(secretVersionName, secretValue)
		assert.NoError(tt, err, "Expected no error when secret version name is valid")
		assert.Equal(tt, expected, result, "Expected result to match the converted secret version")
	})
}

func Test_validateAndConvertCertParams(t *testing.T) {
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
		result, err := _validateAndConvertCertParams(param, pemBlock)
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
		_, err := _validateAndConvertCertParams(nil, pemBlock)
		if err == nil {
			tt.Errorf("Expected err, got %+v", err)
		}
	})
}

func Test_ValidateAndConvertCertParams(t *testing.T) {
	validParam := &models.CustomCertificateParam{
		CertificateID:    "cert-id",
		CaName:           "ca-name",
		CertOwningEntity: "entity",
		Region:           "region",
		CaPoolName:       "pool",
		CommonName:       "common",
	}
	pemBlock := pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: []byte("test-bytes"),
	}

	t.Run("ValidParams", func(tt *testing.T) {
		result, err := ValidateAndConvertCertParams(validParam, pemBlock)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, validParam.CertificateID, result.CertificateID)
		assert.Equal(tt, validParam.CaName, result.CaName)
		assert.Equal(tt, validParam.CertOwningEntity, result.CertOwningEntity)
		assert.Equal(tt, validParam.Region, result.Region)
		assert.Equal(tt, validParam.CaPoolName, result.CaGroupName)
		assert.Contains(tt, result.PemCsr, "CERTIFICATE REQUEST")
		assert.Equal(tt, validParam.CommonName, result.SubjectCommonName)
	})

	t.Run("NilParam", func(tt *testing.T) {
		result, err := ValidateAndConvertCertParams(nil, pemBlock)
		assert.Nil(tt, result)
		assert.Error(tt, err)
	})

	t.Run("MissingFields", func(tt *testing.T) {
		param := &models.CustomCertificateParam{}
		result, err := ValidateAndConvertCertParams(param, pemBlock)
		assert.Nil(tt, result)
		assert.Error(tt, err)
	})

	t.Run("EmptyPemBlockTypeAndCommonName", func(tt *testing.T) {
		param := &models.CustomCertificateParam{
			CertificateID:    "cert-id",
			CaName:           "ca-name",
			CertOwningEntity: "entity",
			Region:           "region",
			CaPoolName:       "pool",
			CommonName:       "",
		}
		emptyPem := pem.Block{
			Type:  "",
			Bytes: []byte("test-bytes"),
		}
		result, err := ValidateAndConvertCertParams(param, emptyPem)
		assert.Nil(tt, result)
		assert.Error(tt, err)
	})
}

// Unit test for ParseTimestamps
func TestParseTimestamps(t *testing.T) {
	t.Run("ValidRFC3339", func(tt *testing.T) {
		timeStr := "2024-06-01T12:34:56Z"
		result, err := ParseTimestamps(timeStr)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, "2024-06-01T12:34:56Z", result.UTC().Format(time.RFC3339))
	})

	t.Run("EmptyString", func(tt *testing.T) {
		result, err := ParseTimestamps("")
		assert.NoError(tt, err)
		assert.Nil(tt, result)
	})

	t.Run("InvalidFormat", func(tt *testing.T) {
		result, err := ParseTimestamps("not-a-time")
		assert.Error(tt, err)
		assert.Nil(tt, result)
	})
}
