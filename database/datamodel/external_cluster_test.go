package datamodel

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterAttributes_Scan_Nil(t *testing.T) {
	var attrs ClusterAttributes
	require.NoError(t, attrs.Scan(nil))
	assert.Equal(t, ClusterAttributes{}, attrs)
}

func TestClusterAttributes_Scan_JSONBytes(t *testing.T) {
	payload, err := json.Marshal(ClusterAttributes{
		ManagementIP: "10.0.0.1",
		OntapVersion: "9.15.1",
	})
	require.NoError(t, err)

	var attrs ClusterAttributes
	require.NoError(t, attrs.Scan(payload))
	assert.Equal(t, "10.0.0.1", attrs.ManagementIP)
	assert.Equal(t, "9.15.1", attrs.OntapVersion)
}

func TestClusterAttributes_Scan_InvalidType(t *testing.T) {
	var attrs ClusterAttributes
	err := attrs.Scan("not-bytes")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type assertion to []byte failed")
}

func TestClusterAttributes_Value(t *testing.T) {
	attrs := ClusterAttributes{ManagementIP: "10.0.0.2"}
	val, err := attrs.Value()
	require.NoError(t, err)

	bytes, ok := val.([]byte)
	require.True(t, ok)

	var decoded ClusterAttributes
	require.NoError(t, json.Unmarshal(bytes, &decoded))
	assert.Equal(t, "10.0.0.2", decoded.ManagementIP)

	var _ driver.Valuer = ClusterAttributes{}
}
