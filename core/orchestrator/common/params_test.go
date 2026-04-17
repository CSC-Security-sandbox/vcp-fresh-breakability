package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVolumeFetchOptionsFromFields(t *testing.T) {
	t.Run("NilFieldSet", func(tt *testing.T) {
		opts := VolumeFetchOptionsFromFields(nil)
		assert.Equal(tt, VolumeFetchOptions{}, opts)
	})

	t.Run("DerivesRequestedOptions", func(tt *testing.T) {
		opts := VolumeFetchOptionsFromFields(map[string]bool{
			"activeDirectoryConfigId": true,
			"kmsConfigResourceId":     true,
			"throughputMibps":         true,
			"mountPoints":             true,
			"inReplication":           true,
		})
		assert.True(tt, opts.NeedActiveDirectory)
		assert.True(tt, opts.NeedKmsConfig)
		assert.True(tt, opts.NeedVolumePerformanceGroup)
		assert.True(tt, opts.NeedIPAddresses)
		assert.True(tt, opts.NeedInReplication)
	})
}
