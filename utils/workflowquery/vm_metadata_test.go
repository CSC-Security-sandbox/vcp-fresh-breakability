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
          },
          "data_disks": [
            { "size": 50, "disk_iops": 1500, "disk_throughput": 954 },
            { "size": 50, "disk_iops": 1500, "disk_throughput": 954 }
          ]
        },
        "vm2": {
          "name": "vm-02",
          "serial_number": "1234502",
          "vsa_management_ip": "158.101.109.167",
          "lifs": {
            "intercluster": { "ip": "10.38.1.218" },
            "nodemgmtinternal": { "ip": "10.38.5.224" }
          },
          "data_disks": [
            { "size": 100, "disk_iops": 3000, "disk_throughput": 1908 }
          ]
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))
	expectedThroughputGBps := float64(1908) / MiBpsPerGBps
	require.Equal(t, []OCICreatePoolVMMetadata{
		{
			Name:            "vm-01",
			SerialNumber:    "1234501",
			VSAManagementIP: "150.136.212.147",
			InterclusterIP:  "10.38.25.146",
			HAPair:          "ha_pair-1",
			SizeInGiB:       100,
			IOPS:            3000,
			ThroughputGBps:  expectedThroughputGBps,
		},
		{
			Name:            "vm-02",
			SerialNumber:    "1234502",
			VSAManagementIP: "158.101.109.167",
			InterclusterIP:  "10.38.1.218",
			HAPair:          "ha_pair-1",
			SizeInGiB:       100,
			IOPS:            3000,
			ThroughputGBps:  expectedThroughputGBps,
		},
	}, poolVMMetadataFromEmbed(&cfg))
}

func TestPoolVMMetadataFromEmbed_NoDataDisks(t *testing.T) {
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
          },
          "data_disks": null
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
			HAPair:          "ha_pair-1",
		},
	}, poolVMMetadataFromEmbed(&cfg))
}

func TestPoolUUIDFromEmbed(t *testing.T) {
	t.Parallel()
	require.Empty(t, poolUUIDFromEmbed(nil), "nil cfg returns empty UUID")

	const withLabels = `{
  "cloud": { "ha_pair": [] },
  "deployment": {
    "labels": {
      "pool_ocid": "ocid1.pool.oc1.ashburn-1.testpool",
      "pool_uuid": "b5fb9baf-953b-9c65-19d5-31e3365cc2e3",
      "pool_name": "testpool"
    }
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(withLabels), &cfg))
	require.Equal(t, "b5fb9baf-953b-9c65-19d5-31e3365cc2e3", poolUUIDFromEmbed(&cfg))

	const withoutLabels = `{ "cloud": { "ha_pair": [] } }`
	var cfg2 vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(withoutLabels), &cfg2))
	require.Empty(t, poolUUIDFromEmbed(&cfg2), "missing deployment.labels returns empty UUID")
}

// TestPoolOCIDFromEmbed pins the helper that surfaces deployment.labels[pool_ocid]
// onto the OCICreatePoolMetadata.PoolOCID field. Empty/missing must return ""
// so callers can rely on `omitempty` to drop the wire field.
func TestPoolOCIDFromEmbed(t *testing.T) {
	t.Parallel()
	require.Empty(t, poolOCIDFromEmbed(nil), "nil cfg returns empty OCID")

	const withLabels = `{
  "cloud": { "ha_pair": [] },
  "deployment": {
    "labels": {
      "pool_ocid": "ocid1.pool.oc1.ashburn-1.testpool",
      "pool_uuid": "b5fb9baf-953b-9c65-19d5-31e3365cc2e3"
    }
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(withLabels), &cfg))
	require.Equal(t, "ocid1.pool.oc1.ashburn-1.testpool", poolOCIDFromEmbed(&cfg))

	const withoutLabels = `{ "cloud": { "ha_pair": [] } }`
	var cfg2 vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(withoutLabels), &cfg2))
	require.Empty(t, poolOCIDFromEmbed(&cfg2), "missing deployment.labels returns empty OCID")
}

// TestClusterIPFromEmbed verifies that the cluster-scoped RBAC LIF IP is
// surfaced once at the pool level. The RBAC LIF lives on exactly one VM at a
// time (VLM sets `lifs.rbac.ip` on the active VM and leaves it empty on the
// rest), so per-VM rbac IPs would mostly be empty strings — the API contract
// returns this single value in `OCICreatePoolMetadata.ClusterIP`.
func TestClusterIPFromEmbed(t *testing.T) {
	t.Parallel()

	require.Empty(t, clusterIPFromEmbed(nil), "nil cfg returns empty so callers omit clusterIP from the response")

	const noRbacAssigned = `{"cloud":{"ha_pair":[{
		"vm1":{"lifs":{"rbac":{"ip":""}}},
		"vm2":{"lifs":{"rbac":{"ip":""}}}
	}]}}`
	var cfgEmpty vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(noRbacAssigned), &cfgEmpty))
	require.Empty(t, clusterIPFromEmbed(&cfgEmpty),
		"every rbac.ip empty means the LIF has not been provisioned yet — omit the field rather than emit an empty string")

	// Single-pair case: vm1 hosts the RBAC LIF, vm2 is empty (real-world
	// shape from VLM config; rbac is cluster-scoped and lives on one VM).
	const vm1Active = `{"cloud":{"ha_pair":[{
		"vm1":{"lifs":{"rbac":{"ip":"10.38.23.99"}}},
		"vm2":{"lifs":{"rbac":{"ip":""}}}
	}]}}`
	var cfg1 vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(vm1Active), &cfg1))
	require.Equal(t, "10.38.23.99", clusterIPFromEmbed(&cfg1))

	// Multi-pair case: across pairs only one VM holds the LIF; the helper
	// must walk every pair until it finds it.
	const lateLIF = `{"cloud":{"ha_pair":[
		{"vm1":{"lifs":{"rbac":{"ip":""}}},"vm2":{"lifs":{"rbac":{"ip":""}}}},
		{"vm1":{"lifs":{"rbac":{"ip":""}}},"vm2":{"lifs":{"rbac":{"ip":"10.0.0.5"}}}}
	]}}`
	var cfg2 vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(lateLIF), &cfg2))
	require.Equal(t, "10.0.0.5", clusterIPFromEmbed(&cfg2))
}

// TestPoolVMMetadataFromEmbed_HAPairIndexing verifies that HAPair labels are
// 1-indexed on the API contract (ha_pair-1, ha_pair-2, ...) even though the
// underlying `cloud.ha_pair` slice is iterated with a 0-based index, and that
// both VMs in the same pair share the same label.
func TestPoolVMMetadataFromEmbed_HAPairIndexing(t *testing.T) {
	t.Parallel()
	const snippet = `{
  "cloud": {
    "ha_pair": [
      {
        "vm1": { "name": "vm-01", "lifs": { "intercluster": { "ip": "10.0.0.1" } } },
        "vm2": { "name": "vm-02", "lifs": { "intercluster": { "ip": "10.0.0.2" } } }
      },
      {
        "vm1": { "name": "vm-03", "lifs": { "intercluster": { "ip": "10.0.0.3" } } },
        "vm2": { "name": "vm-04", "lifs": { "intercluster": { "ip": "10.0.0.4" } } }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))
	got := poolVMMetadataFromEmbed(&cfg)
	require.Len(t, got, 4)
	require.Equal(t, "ha_pair-1", got[0].HAPair, "vm1 of slice index 0 must be ha_pair-1 (1-indexed wire format)")
	require.Equal(t, "ha_pair-1", got[1].HAPair, "vm2 of slice index 0 must be ha_pair-1")
	require.Equal(t, "ha_pair-2", got[2].HAPair, "vm1 of slice index 1 must be ha_pair-2")
	require.Equal(t, "ha_pair-2", got[3].HAPair, "vm2 of slice index 1 must be ha_pair-2")
}

func TestPoolVMMetadataFromEmbed_RBACOnlyVMOmitted(t *testing.T) {
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
            "nodemgmtinternal": { "ip": "10.38.18.182" },
            "rbac": { "ip": "10.38.23.99" }
          }
        },
        "vm2": {
          "lifs": {
            "rbac": { "ip": "" }
          }
        }
      }
    ]
  }
}`
	var cfg vlmConfigIPEmbed
	require.NoError(t, json.Unmarshal([]byte(snippet), &cfg))

	got := poolVMMetadataFromEmbed(&cfg)
	require.Len(t, got, 1,
		"VM2 carries no per-VM signal (name/serial/intercluster/nodemgmt/disks all empty) — even with RBAC tracked at pool level, it must not be promoted to a per-VM entry")
	require.Equal(t, "vm-01", got[0].Name,
		"VM1 must still be emitted because it carries real per-VM data; RBAC presence is irrelevant to per-VM emptiness")

	require.Equal(t, "10.38.23.99", clusterIPFromEmbed(&cfg),
		"RBAC LIF IP belongs at pool level; ClusterIP must be populated from whichever VM hosts the active RBAC LIF")
}

func TestVMMetadataIsEmpty_RBACDoesNotMakeVMNonEmpty(t *testing.T) {
	t.Parallel()
	vm := vmMetadata{}
	vm.Lifs.Rbac.IP = "10.38.23.99"
	require.True(t, vmMetadataIsEmpty(vm),
		"RBAC LIF IP alone must not flip a VM from empty to non-empty; RBAC is surfaced at pool level via clusterIPFromEmbed")
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
			HAPair:          "ha_pair-1",
		},
	}, poolVMMetadataFromEmbed(&cfg))
}
