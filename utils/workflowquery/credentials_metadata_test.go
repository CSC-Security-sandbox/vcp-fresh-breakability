package workflowquery

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestOciCreatePoolCredentialsFromPayloads_SecretMappedFromName(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"ontap_credentials": map[string]interface{}{"admin_password": "ignored"},
		"secret": map[string]interface{}{
			"external_identifier": "ocid1.vaultsecret.oc1..xyz",
			"name":                "FsnIdocnv-2bbd1dd79fa45f97-ontap-admin",
			"version":             3,
		},
	})
	require.NoError(t, err)

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	// Per product requirement: surface the vault secret *name* in the `ocid`
	// field until a dedicated `name` field is added to the API contract.
	require.Equal(t, "FsnIdocnv-2bbd1dd79fa45f97-ontap-admin", got.Secret.Ocid)
	require.Equal(t, "3", got.Secret.Version)
	require.Nil(t, got.Certificate, "certificate is reserved for future PRs")
}

func TestOciCreatePoolCredentialsFromPayloads_Base64Wrapped(t *testing.T) {
	t.Parallel()
	inner, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"external_identifier": "ignored-external-id",
			"name":                "wrapped-secret",
			"version":             1,
		},
	})
	require.NoError(t, err)
	body := []byte(base64.StdEncoding.EncodeToString(inner))

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	require.Equal(t, "wrapped-secret", got.Secret.Ocid)
	require.Equal(t, "1", got.Secret.Version)
}

func TestOciCreatePoolCredentialsFromPayloads_NoSecretFieldReturnsNil(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"ontap_credentials": map[string]interface{}{"admin_password": "p"},
	})
	require.NoError(t, err)
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}}))
}

func TestOciCreatePoolCredentialsFromPayloads_EmptyAndInvalid(t *testing.T) {
	t.Parallel()
	require.Nil(t, ociCreatePoolCredentialsFromPayloads(nil))
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{}))
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: nil}}))
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: []byte("not-json")}}))
}

func TestOciCreatePoolCredentialsFromPayloads_ZeroVersionElidesVersionField(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"name":    "only-name",
			"version": 0,
		},
	})
	require.NoError(t, err)
	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	require.Equal(t, "only-name", got.Secret.Ocid)
	require.Equal(t, "", got.Secret.Version, "version should be empty when activity reports version 0")
}

func TestOciCreatePoolCredentialsFromPayloads_AllEmptySecretReturnsNil(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"name":    "",
			"version": 0,
		},
	})
	require.NoError(t, err)
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}}))
}
