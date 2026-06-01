package vlm

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSPConfig_DecodesAggrConfigs covers the schema VLM emits in
// vlm_config.deployment.spconfig.sp_ha_pair_config[].aggr_configs[]. After
// unifying the per-pair aggregate schema with the top-level data_aggr schema,
// the same DataAggrConfig type is used in both places: numeric `size` (uint64
// GB), `name` (NOT "aggr_name"), optional `uuid`, and `home_node`. The
// per-pair entries no longer carry standalone `iops` / `throughput` — those
// live at the pool-level SPConfig only.
func TestSPConfig_DecodesAggrConfigs(t *testing.T) {
	// Mirrors the post-unification VLM UpdateVSAClusterDeployment response.
	raw := []byte(`{
		"size": "3000Gi",
		"iops": 30512,
		"tput": 1907,
		"is_heterogeneous": false,
		"sp_ha_pair_config": [
			{
				"instance_type": "VM.DenseIO.E5.Flex"
			}
		]
	}`)

	var sp SPConfig
	require.NoError(t, json.Unmarshal(raw, &sp))
	assert.Equal(t, "3000Gi", sp.Size)
	assert.Equal(t, int64(30512), sp.IOps)
	assert.Equal(t, int64(1907), sp.Throughput)
	require.Len(t, sp.HAPairConfigs, 1)
	assert.Equal(t, "VM.DenseIO.E5.Flex", sp.HAPairConfigs[0].InstanceType)
}

// TestDataAggrConfig_DecodesNumericSize anchors the top-level data_aggr
// schema. Kept as a sibling to TestSPConfig_DecodesAggrConfigs even though
// the two now share DataAggrConfig — the value of the assertion is that the
// shared type successfully decodes both wire locations without divergence.
func TestDataAggrConfig_DecodesNumericSize(t *testing.T) {
	raw := []byte(`{
		"data_aggr": [
			{"name": "aggr_a", "uuid": "uuid-a", "size": 1024, "home_node": "node-1"}
		]
	}`)

	var cfg VLMConfig
	require.NoError(t, json.Unmarshal(raw, &cfg))
	require.Len(t, cfg.DataAggr, 1)
	assert.Equal(t, "aggr_a", cfg.DataAggr[0].Name)
	assert.Equal(t, "uuid-a", cfg.DataAggr[0].Aggruuid)
	assert.Equal(t, uint64(1024), cfg.DataAggr[0].Size)
	assert.Equal(t, "node-1", cfg.DataAggr[0].HomeNode)
}

// TestSPConfig_HomogeneousSendShape locks in what VCP emits on the wire today:
// the heterogeneous fields default to (false, nil) so the request is a clean
// homogeneous payload, and sp_ha_pair_config is omitted entirely via omitempty.
func TestSPConfig_HomogeneousSendShape(t *testing.T) {
	sp := SPConfig{Size: "3000GB", IOps: 1000, Throughput: 64}
	out, err := json.Marshal(sp)
	require.NoError(t, err)
	got := string(out)
	assert.Contains(t, got, `"is_heterogeneous":false`)
	assert.NotContains(t, got, `"sp_ha_pair_config"`)
}

// TestParseSizeStringGiB pins the canonical parsing rules for VLM size strings.
func TestParseSizeStringGiB(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want int64
	}{
		{"empty string returns 0", "", 0},
		{"non-numeric returns 0", "huge", 0},
		{"leading whitespace returns 0", " 100Gi", 0},

		// Accepted forms (digit prefix + recognised GiB suffix, case-insensitive).
		{"bare digits parse as GiB", "1024", 1024},
		{"Gi suffix is accepted", "400Gi", 400},
		{"GiB suffix is accepted", "400GiB", 400},
		{"GB suffix is accepted", "1500GB", 1500},
		{"G suffix is accepted", "200G", 200},
		{"lowercase gi is accepted", "777gi", 777},
		{"mixed-case Gib is accepted", "300Gib", 300},

		// Rejected suffixes — must return 0, NOT the digit prefix.
		{"Mi suffix is rejected (would be 1024x misread)", "200Mi", 0},
		{"Ti suffix is rejected (would be 1024x misread)", "200Ti", 0},
		{"MB suffix is rejected", "200MB", 0},
		{"Kib suffix is rejected", "200Kib", 0},
		{"trailing space after Gi is rejected", "400Gi ", 0},
		{"trailing punctuation after Gi is rejected", "400Gi.", 0},
		{"arbitrary garbage suffix is rejected", "400xyz", 0},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ParseSizeStringGiB(tc.in))
		})
	}
}

// TestSPConfig_SizeGiB anchors the convenience method on the pool-level
// SPConfig. DataAggrConfig has no equivalent helper (its Size is uint64 GB
// natively), so there is no parallel test for it — the type itself is the
// canonical representation.
func TestSPConfig_SizeGiB(t *testing.T) {
	assert.Equal(t, int64(3000), SPConfig{Size: "3000Gi"}.SizeGiB())
	assert.Equal(t, int64(0), SPConfig{Size: ""}.SizeGiB())
}
