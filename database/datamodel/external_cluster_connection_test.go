package datamodel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeExternalClusterProtocolAndPort_DefaultInsecureHTTPS443(t *testing.T) {
	p, port, err := NormalizeExternalClusterProtocolAndPort("", 0)
	require.NoError(t, err)
	assert.Equal(t, ExternalClusterProtocolInsecureHTTPS, p)
	assert.Equal(t, 443, port)
}

func TestNormalizeExternalClusterProtocolAndPort_HTTP80(t *testing.T) {
	p, port, err := NormalizeExternalClusterProtocolAndPort("HTTP", 0)
	require.NoError(t, err)
	assert.Equal(t, ExternalClusterProtocolHTTP, p)
	assert.Equal(t, 80, port)
}

func TestNormalizeExternalClusterProtocolAndPort_HTTPS443(t *testing.T) {
	p, port, err := NormalizeExternalClusterProtocolAndPort("HTTPS", 0)
	require.NoError(t, err)
	assert.Equal(t, ExternalClusterProtocolHTTPS, p)
	assert.Equal(t, 443, port)
}

func TestNormalizeExternalClusterProtocolAndPort_ExplicitPort(t *testing.T) {
	_, port, err := NormalizeExternalClusterProtocolAndPort("HTTPS", 8443)
	require.NoError(t, err)
	assert.Equal(t, 8443, port)
}

func TestNormalizeExternalClusterProtocolAndPort_InvalidProtocol(t *testing.T) {
	_, _, err := NormalizeExternalClusterProtocolAndPort("NFS", 0)
	require.Error(t, err)
}
