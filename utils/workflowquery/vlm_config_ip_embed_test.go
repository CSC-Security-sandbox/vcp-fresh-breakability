package workflowquery

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVlmConfigIPEmbed_InterclusterAndNodemgmtInternal(t *testing.T) {
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
