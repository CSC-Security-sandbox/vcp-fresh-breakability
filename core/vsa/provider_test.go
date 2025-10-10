package vsa

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewProvider(t *testing.T) {
	t.Run("WhenProviderDetailsAreValid", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			Password:           "password",
			InsecureSkipVerify: true,
		}
		ctx := context.Background()
		result := NewProvider(ctx, providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, providerDetails.IPAddress, result.ClientParams.Host)
		assert.Equal(tt, log.Secret(providerDetails.Password), result.ClientParams.Password)
		assert.Equal(tt, providerDetails.InsecureSkipVerify, result.ClientParams.InsecureSkipVerify)
		assert.NotNil(tt, result.ClientParams.Trace)
		assert.NotNil(tt, result.Logger)
		assert.IsType(tt, &log.Slogger{}, result.Logger)
	})
}

func TestNewProviderWithCert(t *testing.T) {
	t.Run("WhenProviderDetailsAreValid", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			InsecureSkipVerify: true,
			Certificate: &Certificate{
				SignedCertificate:        "signedCert",
				PrivateKey:               "privateKey",
				InterMediateCertificates: []string{"intermediateCert1"},
				CommonName:               "cn",
				RootCaCertificate:        "rootCaCert",
			},
		}

		result := NewProvider(context.Background(), providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, providerDetails.IPAddress, result.ClientParams.Host)
		assert.Equal(tt, log.Secret(providerDetails.Password), result.ClientParams.Password)
		assert.Equal(tt, providerDetails.InsecureSkipVerify, result.ClientParams.InsecureSkipVerify)
		assert.NotNil(tt, result.ClientParams.Trace)
		assert.NotNil(tt, result.Logger)
		assert.IsType(tt, &log.Slogger{}, result.Logger)
		assert.NotNil(tt, result.ClientParams.Ctx)
	})
}

func TestNewProvider_WithFastConnection(t *testing.T) {
	t.Run("WhenFastConnectionIsTrue", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			Password:           "password",
			InsecureSkipVerify: true,
			FastConnection:     true, // Enable fast connection
		}
		ctx := context.Background()
		result := NewProvider(ctx, providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, providerDetails.IPAddress, result.ClientParams.Host)
		assert.Equal(tt, log.Secret(providerDetails.Password), result.ClientParams.Password)
		assert.Equal(tt, providerDetails.InsecureSkipVerify, result.ClientParams.InsecureSkipVerify)
		assert.Equal(tt, true, result.ClientParams.FastConnection) // Verify FastConnection is set
		assert.NotNil(tt, result.ClientParams.Trace)
		assert.NotNil(tt, result.Logger)
		assert.IsType(tt, &log.Slogger{}, result.Logger)
	})

	t.Run("WhenFastConnectionIsFalse", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			Password:           "password",
			InsecureSkipVerify: true,
			FastConnection:     false, // Disable fast connection explicitly
		}
		ctx := context.Background()
		result := NewProvider(ctx, providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, false, result.ClientParams.FastConnection) // Verify FastConnection is false
	})

	t.Run("WhenFastConnectionIsNotSet", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			Password:           "password",
			InsecureSkipVerify: true,
			// FastConnection not set, should default to false
		}
		ctx := context.Background()
		result := NewProvider(ctx, providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, false, result.ClientParams.FastConnection) // Verify FastConnection defaults to false
	})

	t.Run("WhenFastConnectionIsTrueWithCertificate", func(tt *testing.T) {
		hostMap := map[string]string{}
		hostMap["192.168.1.1"] = "test.vsa.com"
		providerDetails := ProviderDetails{
			Hosts:              hostMap,
			IPAddress:          "192.168.1.1",
			InsecureSkipVerify: true,
			FastConnection:     true, // Enable fast connection
			Certificate: &Certificate{
				SignedCertificate:        "signedCert",
				PrivateKey:               "privateKey",
				InterMediateCertificates: []string{"intermediateCert1"},
				CommonName:               "cn",
				RootCaCertificate:        "rootCaCert",
			},
		}
		ctx := context.Background()
		result := NewProvider(ctx, providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, true, result.ClientParams.FastConnection) // Verify FastConnection is set
		assert.Equal(tt, true, result.ClientParams.CertificateBasedAuthEnabled)
		assert.NotNil(tt, result.ClientParams.Certificate)
	})
}
