package workflowquery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVMMetadata_InterclusterAndNodemgmtInternal(t *testing.T) {
	t.Parallel()
	const snippet = `{
  "cloud": {
    "ha_pair": [
      {
        "vm1": {
          "lifs": {
            "intercluster": { "ip": "10.38.25.146" },
            "nodemgmtinternal": { "ip": "10.38.18.182" }
          }
        },
        "vm2": {
          "lifs": {
            "intercluster": { "ip": "10.38.1.218" },
            "nodemgmtinternal": { "ip": "10.38.5.224" }
          }
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))
	require.Equal(t, []string{"10.38.25.146", "10.38.1.218"}, interclusterIPsFromEmbed(&cfg))
	require.Equal(t, []string{"10.38.18.182", "10.38.5.224"}, nodemgmtInternalIPsFromEmbed(&cfg))
}

func TestInterclusterIPsFromEmbed_NilAndDedup(t *testing.T) {
	t.Parallel()
	require.Nil(t, interclusterIPsFromEmbed(nil))
	require.Nil(t, nodemgmtInternalIPsFromEmbed(nil))
	require.Nil(t, poolVMMetadataFromEmbed(nil))

	const dupSnippet = `{
  "cloud": {
    "ha_pair": [
      {
        "vm1": {
          "lifs": {
            "intercluster": { "ip": "10.0.0.1" },
            "nodemgmtinternal": { "ip": "10.0.2.1" }
          }
        },
        "vm2": {
          "lifs": {
            "intercluster": { "ip": "10.0.0.1" },
            "nodemgmtinternal": { "ip": "10.0.2.1" }
          }
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(dupSnippet), &cfg))
	require.Equal(t, []string{"10.0.0.1"}, interclusterIPsFromEmbed(&cfg))
	require.Equal(t, []string{"10.0.2.1"}, nodemgmtInternalIPsFromEmbed(&cfg))
}

func TestPoolVMMetadataFromEmbed(t *testing.T) {
	t.Parallel()
	const snippet = `{
  "cloud": {
    "ha_pair": [
      {
        "vm1": {
          "name": "vm-01",
          "serial_number": "1234501",
          "vsa_management_ip": "150.136.212.147",
          "lifs": {
            "intercluster": { "ip": "10.38.25.146" },
            "nodemgmtinternal": { "ip": "10.38.18.182" }
          }
        },
        "vm2": {
          "name": "vm-02",
          "serial_number": "1234502",
          "vsa_management_ip": "158.101.109.167",
          "lifs": {
            "intercluster": { "ip": "10.38.1.218" },
            "nodemgmtinternal": { "ip": "10.38.5.224" }
          }
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))
	require.Equal(t, []OCICreatePoolVMMetadata{
		{
			Name:            "vm-01",
			SerialNumber:    "1234501",
			VSAManagementIP: "150.136.212.147",
			InterclusterIP:  "10.38.25.146",
			NodeIP:          "10.38.18.182",
		},
		{
			Name:            "vm-02",
			SerialNumber:    "1234502",
			VSAManagementIP: "158.101.109.167",
			InterclusterIP:  "10.38.1.218",
			NodeIP:          "10.38.5.224",
		},
	}, poolVMMetadataFromEmbed(&cfg))
}

func TestPoolVMMetadataFromEmbed_VM2Omitted(t *testing.T) {
	t.Parallel()
	const snippet = `{
  "cloud": {
    "ha_pair": [
      {
        "vm1": {
          "name": "vm-01",
          "serial_number": "1234501",
          "vsa_management_ip": "150.136.212.147",
          "lifs": {
            "intercluster": { "ip": "10.38.25.146" },
            "nodemgmtinternal": { "ip": "10.38.18.182" }
          }
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))
	require.Equal(t, []OCICreatePoolVMMetadata{
		{
			Name:            "vm-01",
			SerialNumber:    "1234501",
			VSAManagementIP: "150.136.212.147",
			InterclusterIP:  "10.38.25.146",
			NodeIP:          "10.38.18.182",
		},
	}, poolVMMetadataFromEmbed(&cfg))
}
