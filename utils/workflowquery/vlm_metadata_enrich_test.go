package workflowquery

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestVsaClusterChildMetadataFromPayloads(t *testing.T) {
	t.Parallel()

	rawObj := map[string]interface{}{
		"vlm_config": map[string]interface{}{
			"cloud": map[string]interface{}{
				"ha_pair": []interface{}{
					map[string]interface{}{
						"vm1": map[string]interface{}{
							"name":              "FsnIdocnv-vm-01",
							"serial_number":     "1234501",
							"vsa_management_ip": "150.136.212.147",
							"lifs": map[string]interface{}{
								"intercluster":     map[string]interface{}{"ip": "10.38.25.146"},
								"nodemgmtinternal": map[string]interface{}{"ip": "10.38.0.1"},
							},
						},
						"vm2": map[string]interface{}{
							"name":              "FsnIdocnv-vm-02",
							"serial_number":     "1234502",
							"vsa_management_ip": "158.101.109.167",
							"lifs": map[string]interface{}{
								"intercluster":     map[string]interface{}{"ip": "10.38.1.218"},
								"nodemgmtinternal": map[string]interface{}{"ip": "10.38.0.2"},
							},
						},
						"mediator": map[string]interface{}{
							"lifs": map[string]interface{}{
								"intercluster": map[string]interface{}{"ip": ""},
							},
						},
					},
				},
			},
		},
		"workflow_type": "OCICreatePoolWorkflow",
	}
	body, err := json.Marshal(rawObj)
	require.NoError(t, err)

	out := vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, out)

	require.Equal(t, []OCICreatePoolVMMetadata{
		{
			Name:            "FsnIdocnv-vm-01",
			SerialNumber:    "1234501",
			VSAManagementIP: "150.136.212.147",
			InterclusterIP:  "10.38.25.146",
			NodeIP:          "10.38.0.1",
		},
		{
			Name:            "FsnIdocnv-vm-02",
			SerialNumber:    "1234502",
			VSAManagementIP: "158.101.109.167",
			InterclusterIP:  "10.38.1.218",
			NodeIP:          "10.38.0.2",
		},
	}, out.Vms)
}

func TestVsaClusterChildMetadataFromPayloads_NoVlmConfigPassthrough(t *testing.T) {
	t.Parallel()
	in := map[string]interface{}{"cluster": "c1"}
	body, err := json.Marshal(in)
	require.NoError(t, err)
	out := vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: body}})
	require.Nil(t, out)
}

func TestVsaClusterChildMetadataFromPayloads_Base64WrappedPayload(t *testing.T) {
	t.Parallel()
	inner := map[string]interface{}{
		"vlm_config": map[string]interface{}{
			"cloud": map[string]interface{}{
				"ha_pair": []interface{}{
					map[string]interface{}{
						"vm1": map[string]interface{}{
							"name":              "single-vm",
							"serial_number":     "9001",
							"vsa_management_ip": "10.0.0.3",
							"lifs": map[string]interface{}{
								"intercluster":     map[string]interface{}{"ip": "10.0.0.1"},
								"nodemgmtinternal": map[string]interface{}{"ip": "10.0.0.2"},
							},
						},
					},
				},
			},
		},
	}
	innerBytes, err := json.Marshal(inner)
	require.NoError(t, err)
	wrapped := []byte(base64.StdEncoding.EncodeToString(innerBytes))

	out := vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: wrapped}})
	require.NotNil(t, out)
	require.Equal(t, []OCICreatePoolVMMetadata{
		{
			Name:            "single-vm",
			SerialNumber:    "9001",
			VSAManagementIP: "10.0.0.3",
			InterclusterIP:  "10.0.0.1",
			NodeIP:          "10.0.0.2",
		},
	}, out.Vms)
}

func TestVsaClusterChildMetadataFromPayloads_InvalidJSON(t *testing.T) {
	t.Parallel()
	require.Nil(t, vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: []byte(`not-json`)}}))
	require.Nil(t, vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: []byte(`{"vlm_config":null}`)}}))
	require.Nil(t, vsaClusterChildMetadataFromPayloads([]*commonpb.Payload{{Data: []byte(`{"vlm_config":"not-an-object"}`)}}))
}
