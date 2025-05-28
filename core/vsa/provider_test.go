package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestNewProvider(t *testing.T) {
	t.Run("WhenProviderDetailsAreValid", func(tt *testing.T) {
		providerDetails := ProviderDetails{
			IPAddress:          "192.168.1.1",
			UserName:           "admin",
			Password:           "password",
			InsecureSkipVerify: true,
		}

		result := NewProvider(providerDetails)

		assert.NotNil(tt, result)
		assert.Equal(tt, providerDetails, result.Provider)
		assert.Equal(tt, providerDetails.IPAddress, result.ClientParams.Host)
		assert.Equal(tt, providerDetails.UserName, result.ClientParams.Username)
		assert.Equal(tt, log.Secret(providerDetails.Password), result.ClientParams.Password)
		assert.Equal(tt, providerDetails.InsecureSkipVerify, result.ClientParams.InsecureSkipVerify)
		assert.NotNil(tt, result.ClientParams.Trace)
		assert.NotNil(tt, result.Logger)
		assert.IsType(tt, &log.Slogger{}, result.Logger)
	})
}
