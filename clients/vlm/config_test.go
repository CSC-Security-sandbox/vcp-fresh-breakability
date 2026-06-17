package vlm

import (
	"encoding/json"
	"sync"
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

// --- Provider factory tests ---

func resetFactory() {
	factoryOnce = sync.Once{}
	activeFactory = nil
}

func restoreOCIFactory() {
	factoryOnce = sync.Once{}
	activeFactory = nil
	SetActiveProvider(OCICloud)
}

func TestSetActiveProvider_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	SetActiveProvider(OCICloud)
	f, err := getFactory()
	require.NoError(t, err)
	assert.NotNil(t, f)
	assert.NotNil(t, f.NewProviderConfig)
	assert.NotNil(t, f.NewProviderDiskConfig)
	assert.NotNil(t, f.NewProviderNetConfig)
	assert.NotNil(t, f.NewProviderDevFlags)
}

func TestSetActiveProvider_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	SetActiveProvider(GCPCloud)
	f, err := getFactory()
	require.NoError(t, err)
	assert.NotNil(t, f)

	cfg := f.NewProviderConfig()
	_, ok := cfg.(*GCPConfig)
	assert.True(t, ok)

	dc := f.NewProviderDiskConfig()
	_, ok = dc.(*GCPDiskConfig)
	assert.True(t, ok)

	nc := f.NewProviderNetConfig()
	_, ok = nc.(*GCPNetworkConfig)
	assert.True(t, ok)

	df := f.NewProviderDevFlags()
	_, ok = df.(*GCPDevFlags)
	assert.True(t, ok)
}

func TestSetActiveProvider_Unknown(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	SetActiveProvider("unknown-cloud")
	_, err := getFactory()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

func TestSetActiveProvider_OnceSemantics(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	SetActiveProvider(OCICloud)
	SetActiveProvider(GCPCloud) // second call should be ignored

	f, err := getFactory()
	require.NoError(t, err)
	cfg := f.NewProviderConfig()
	_, ok := cfg.(*OCIConfig)
	assert.True(t, ok, "factory should remain OCI after second SetActiveProvider call")
}

func TestResetActiveProvider(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	SetActiveProvider(OCICloud)
	ResetActiveProvider()

	_, err := getFactory()
	assert.Error(t, err)
}

func TestGetFactory_NilReturnsError(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	_, err := getFactory()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

// --- ProviderConfigWrapper tests ---

func TestProviderConfigWrapper_UnmarshalJSON_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	data := []byte(`{"compartment_id":"ocid1.compartment.oc1..test","subnet_id":"ocid1.subnet.oc1..test"}`)
	var w ProviderConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	oci, err := w.AsOCI()
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..test", oci.CompartmentID)
	assert.Equal(t, "ocid1.subnet.oc1..test", oci.SubnetID)
}

func TestProviderConfigWrapper_UnmarshalJSON_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(GCPCloud)

	data := []byte(`{"project_id":"my-gcp-project","image_project_id":"img-project"}`)
	var w ProviderConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	gcp, err := w.AsGCP()
	require.NoError(t, err)
	assert.Equal(t, "my-gcp-project", gcp.ProjectID)
	assert.Equal(t, "img-project", gcp.ImageProjectID)
}

func TestProviderConfigWrapper_UnmarshalJSON_NoFactory(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	var w ProviderConfigWrapper
	err := w.UnmarshalJSON([]byte(`{}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

func TestProviderConfigWrapper_UnmarshalJSON_InvalidJSON(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	var w ProviderConfigWrapper
	err := w.UnmarshalJSON([]byte(`{invalid`))
	assert.Error(t, err)
}

func TestProviderConfigWrapper_MarshalJSON_Nil(t *testing.T) {
	w := ProviderConfigWrapper{}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestProviderConfigWrapper_MarshalJSON_OCI(t *testing.T) {
	w := ProviderConfigWrapper{ProviderConfig: &OCIConfig{CompartmentID: "test-compartment"}}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"compartment_id":"test-compartment"`)
}

func TestProviderConfigWrapper_AsOCI_WrongType(t *testing.T) {
	w := ProviderConfigWrapper{ProviderConfig: &GCPConfig{ProjectID: "proj"}}
	_, err := w.AsOCI()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot convert")
}

func TestProviderConfigWrapper_AsGCP_WrongType(t *testing.T) {
	w := ProviderConfigWrapper{ProviderConfig: &OCIConfig{CompartmentID: "comp"}}
	_, err := w.AsGCP()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot convert")
}

func TestProviderConfigWrapper_AsOCI_Nil(t *testing.T) {
	w := ProviderConfigWrapper{}
	_, err := w.AsOCI()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

// --- ProviderDiskConfigWrapper tests ---

func TestProviderDiskConfigWrapper_UnmarshalJSON_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	data := []byte(`{"device_name":"sda","availability_domain":"AD-1","vpus":20}`)
	var w ProviderDiskConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	oci, err := w.AsOCI()
	require.NoError(t, err)
	assert.Equal(t, "sda", oci.DeviceName)
	assert.Equal(t, "AD-1", oci.AvailabilityDomain)
	assert.Equal(t, int64(20), oci.Vpus)
}

func TestProviderDiskConfigWrapper_UnmarshalJSON_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(GCPCloud)

	data := []byte(`{"device_name":"disk-0"}`)
	var w ProviderDiskConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	gcp, err := w.AsGCP()
	require.NoError(t, err)
	assert.Equal(t, "disk-0", gcp.DeviceName)
}

func TestProviderDiskConfigWrapper_UnmarshalJSON_NoFactory(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	var w ProviderDiskConfigWrapper
	err := w.UnmarshalJSON([]byte(`{}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

func TestProviderDiskConfigWrapper_UnmarshalJSON_InvalidJSON(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	var w ProviderDiskConfigWrapper
	err := w.UnmarshalJSON([]byte(`not-json`))
	assert.Error(t, err)
}

func TestProviderDiskConfigWrapper_MarshalJSON_Nil(t *testing.T) {
	w := ProviderDiskConfigWrapper{}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestProviderDiskConfigWrapper_MarshalJSON_OCI(t *testing.T) {
	w := ProviderDiskConfigWrapper{ProviderDiskConfig: &OCIDiskConfig{DeviceName: "sda"}}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"device_name":"sda"`)
}

func TestProviderDiskConfigWrapper_AsOCI_WrongType(t *testing.T) {
	w := ProviderDiskConfigWrapper{ProviderDiskConfig: &GCPDiskConfig{DeviceName: "d"}}
	_, err := w.AsOCI()
	assert.Error(t, err)
}

func TestProviderDiskConfigWrapper_AsGCP_WrongType(t *testing.T) {
	w := ProviderDiskConfigWrapper{ProviderDiskConfig: &OCIDiskConfig{DeviceName: "d"}}
	_, err := w.AsGCP()
	assert.Error(t, err)
}

// --- ProviderNetworkConfigWrapper tests ---

func TestProviderNetworkConfigWrapper_UnmarshalJSON_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	data := []byte(`{"subnet_ocid":"ocid1.subnet.test"}`)
	var w ProviderNetworkConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	oci, err := w.AsOCI()
	require.NoError(t, err)
	assert.Equal(t, "ocid1.subnet.test", oci.SubnetOCID)
}

func TestProviderNetworkConfigWrapper_UnmarshalJSON_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(GCPCloud)

	data := []byte(`{"subnet_project_id":"gcp-proj-123"}`)
	var w ProviderNetworkConfigWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	gcp, err := w.AsGCP()
	require.NoError(t, err)
	assert.Equal(t, "gcp-proj-123", gcp.SubnetProjectID)
}

func TestProviderNetworkConfigWrapper_UnmarshalJSON_NoFactory(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	var w ProviderNetworkConfigWrapper
	err := w.UnmarshalJSON([]byte(`{}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

func TestProviderNetworkConfigWrapper_UnmarshalJSON_InvalidJSON(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	var w ProviderNetworkConfigWrapper
	err := w.UnmarshalJSON([]byte(`[broken`))
	assert.Error(t, err)
}

func TestProviderNetworkConfigWrapper_MarshalJSON_Nil(t *testing.T) {
	w := ProviderNetworkConfigWrapper{}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestProviderNetworkConfigWrapper_MarshalJSON_GCP(t *testing.T) {
	w := ProviderNetworkConfigWrapper{ProviderNetworkConfig: &GCPNetworkConfig{SubnetProjectID: "proj-456"}}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"subnet_project_id":"proj-456"`)
}

func TestProviderNetworkConfigWrapper_AsOCI_WrongType(t *testing.T) {
	w := ProviderNetworkConfigWrapper{ProviderNetworkConfig: &GCPNetworkConfig{}}
	_, err := w.AsOCI()
	assert.Error(t, err)
}

func TestProviderNetworkConfigWrapper_AsGCP_WrongType(t *testing.T) {
	w := ProviderNetworkConfigWrapper{ProviderNetworkConfig: &OCINetworkConfig{}}
	_, err := w.AsGCP()
	assert.Error(t, err)
}

// --- ProviderDevFlagsWrapper tests ---

func TestProviderDevFlagsWrapper_UnmarshalJSON_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	data := []byte(`{"allow_non_dense_shape_for_vsa":true,"use_secondary_ips_for_lifs":true}`)
	var w ProviderDevFlagsWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	oci, err := w.AsOCI()
	require.NoError(t, err)
	assert.True(t, oci.AllowNonDenseShapeForVsa)
	assert.True(t, oci.UseSecondaryIPsForLIFs)
}

func TestProviderDevFlagsWrapper_UnmarshalJSON_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(GCPCloud)

	data := []byte(`{}`)
	var w ProviderDevFlagsWrapper
	err := w.UnmarshalJSON(data)
	require.NoError(t, err)

	gcp, err := w.AsGCP()
	require.NoError(t, err)
	assert.Equal(t, GCPDevFlags{}, gcp)
}

func TestProviderDevFlagsWrapper_UnmarshalJSON_NoFactory(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()

	var w ProviderDevFlagsWrapper
	err := w.UnmarshalJSON([]byte(`{}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no active provider set")
}

func TestProviderDevFlagsWrapper_UnmarshalJSON_InvalidJSON(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	var w ProviderDevFlagsWrapper
	err := w.UnmarshalJSON([]byte(`!!!`))
	assert.Error(t, err)
}

func TestProviderDevFlagsWrapper_MarshalJSON_Nil(t *testing.T) {
	w := ProviderDevFlagsWrapper{}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Equal(t, "null", string(data))
}

func TestProviderDevFlagsWrapper_MarshalJSON_OCI(t *testing.T) {
	w := ProviderDevFlagsWrapper{ProviderDevFlags: &OCIDevFlags{AllowNonDenseShapeForVsa: true}}
	data, err := w.MarshalJSON()
	require.NoError(t, err)
	assert.Contains(t, string(data), `"allow_non_dense_shape_for_vsa":true`)
}

func TestProviderDevFlagsWrapper_AsOCI_WrongType(t *testing.T) {
	w := ProviderDevFlagsWrapper{ProviderDevFlags: &GCPDevFlags{}}
	_, err := w.AsOCI()
	assert.Error(t, err)
}

func TestProviderDevFlagsWrapper_AsGCP_WrongType(t *testing.T) {
	w := ProviderDevFlagsWrapper{ProviderDevFlags: &OCIDevFlags{}}
	_, err := w.AsGCP()
	assert.Error(t, err)
}

func TestProviderDevFlagsWrapper_AsOCI_Nil(t *testing.T) {
	w := ProviderDevFlagsWrapper{}
	_, err := w.AsOCI()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

// --- GetProvider / GetDiskConfigProvider / GetNetConfigProvider / GetDevFlagsProvider tests ---

func TestGetProvider(t *testing.T) {
	assert.Equal(t, GCPCloud, GCPConfig{}.GetProvider())
	assert.Equal(t, OCICloud, OCIConfig{}.GetProvider())
}

func TestGetDiskConfigProvider(t *testing.T) {
	assert.Equal(t, GCPCloud, GCPDiskConfig{}.GetDiskConfigProvider())
	assert.Equal(t, OCICloud, OCIDiskConfig{}.GetDiskConfigProvider())
}

func TestGetNetConfigProvider(t *testing.T) {
	assert.Equal(t, GCPCloud, GCPNetworkConfig{}.GetNetConfigProvider())
	assert.Equal(t, OCICloud, OCINetworkConfig{}.GetNetConfigProvider())
}

func TestGetDevFlagsProvider(t *testing.T) {
	assert.Equal(t, OCICloud, OCIDevFlags{}.GetDevFlagsProvider())
	assert.Equal(t, GCPCloud, GCPDevFlags{}.GetDevFlagsProvider())
}

// --- AsProviderType generic function tests ---

func TestAsProviderType_PointerToValue(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	cfg := &OCIConfig{CompartmentID: "ptr-test"}
	result, err := AsProviderType[OCIConfig](cfg)
	require.NoError(t, err)
	assert.Equal(t, "ptr-test", result.CompartmentID)
}

func TestAsProviderType_DirectValue(t *testing.T) {
	cfg := OCIConfig{CompartmentID: "val-test"}
	result, err := AsProviderType[OCIConfig](cfg)
	require.NoError(t, err)
	assert.Equal(t, "val-test", result.CompartmentID)
}

func TestAsProviderType_Nil(t *testing.T) {
	_, err := AsProviderType[OCIConfig](nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil")
}

func TestAsProviderType_WrongType(t *testing.T) {
	_, err := AsProviderType[OCIConfig](&GCPConfig{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot convert")
}

// --- unmarshalWithFactory tests ---

func TestUnmarshalWithFactory_NilFn(t *testing.T) {
	_, err := unmarshalWithFactory(nil, []byte(`{}`))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "factory function is nil")
}

// --- Round-trip marshal/unmarshal integration tests ---

func TestProviderConfigWrapper_RoundTrip_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	original := ProviderConfigWrapper{ProviderConfig: &OCIConfig{
		CompartmentID: "ocid1.compartment.oc1..roundtrip",
		SubnetID:      "ocid1.subnet.oc1..roundtrip",
	}}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ProviderConfigWrapper
	require.NoError(t, json.Unmarshal(data, &decoded))

	oci, err := decoded.AsOCI()
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..roundtrip", oci.CompartmentID)
	assert.Equal(t, "ocid1.subnet.oc1..roundtrip", oci.SubnetID)
}

func TestProviderDiskConfigWrapper_RoundTrip_GCP(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(GCPCloud)

	original := ProviderDiskConfigWrapper{ProviderDiskConfig: &GCPDiskConfig{DeviceName: "gcp-disk-1"}}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ProviderDiskConfigWrapper
	require.NoError(t, json.Unmarshal(data, &decoded))

	gcp, err := decoded.AsGCP()
	require.NoError(t, err)
	assert.Equal(t, "gcp-disk-1", gcp.DeviceName)
}

func TestProviderNetworkConfigWrapper_RoundTrip_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	original := ProviderNetworkConfigWrapper{ProviderNetworkConfig: &OCINetworkConfig{SubnetOCID: "ocid1.subnet"}}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ProviderNetworkConfigWrapper
	require.NoError(t, json.Unmarshal(data, &decoded))

	oci, err := decoded.AsOCI()
	require.NoError(t, err)
	assert.Equal(t, "ocid1.subnet", oci.SubnetOCID)
}

func TestProviderDevFlagsWrapper_RoundTrip_OCI(t *testing.T) {
	resetFactory()
	defer restoreOCIFactory()
	SetActiveProvider(OCICloud)

	original := ProviderDevFlagsWrapper{ProviderDevFlags: &OCIDevFlags{
		AllowNonDenseShapeForVsa: true,
		UseSecondaryIPsForLIFs:   true,
	}}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded ProviderDevFlagsWrapper
	require.NoError(t, json.Unmarshal(data, &decoded))

	oci, err := decoded.AsOCI()
	require.NoError(t, err)
	assert.True(t, oci.AllowNonDenseShapeForVsa)
	assert.True(t, oci.UseSecondaryIPsForLIFs)
}
