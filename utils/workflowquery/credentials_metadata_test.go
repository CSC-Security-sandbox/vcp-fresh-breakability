package workflowquery

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestOciCreatePoolCredentialsFromPayloads_SecretMappedFromExternalIdentifier(t *testing.T) {
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
	// The API surfaces the OCI-assigned OCID (ExternalIdentifier), not the
	// human-readable secret Name.
	require.Equal(t, "ocid1.vaultsecret.oc1..xyz", got.Secret.Ocid)
	require.Equal(t, "3", got.Secret.Version)
	require.Nil(t, got.Certificate, "certificate is omitted when not present in the payload")
}

func TestOciCreatePoolCredentialsFromPayloads_CertificateMappedFromExternalIdentifier(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"certificate": map[string]interface{}{
			"external_identifier": "ocid1.certificate.oc1..abc",
			"name":                "FsnIdocnv-2bbd1dd79fa45f97-ontap-cert",
			"version":             7,
		},
	})
	require.NoError(t, err)

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Certificate)
	require.Equal(t, "ocid1.certificate.oc1..abc", got.Certificate.Ocid)
	require.Equal(t, "7", got.Certificate.Version)
	require.Nil(t, got.Secret, "secret is omitted when not present in the payload")
}

func TestOciCreatePoolCredentialsFromPayloads_SecretAndCertificateBothMapped(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"external_identifier": "ocid1.vaultsecret.oc1..xyz",
			"name":                "ontap-admin",
			"version":             3,
		},
		"certificate": map[string]interface{}{
			"external_identifier": "ocid1.certificate.oc1..abc",
			"name":                "ontap-cert",
			"version":             7,
		},
	})
	require.NoError(t, err)

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	require.Equal(t, "ocid1.vaultsecret.oc1..xyz", got.Secret.Ocid)
	require.Equal(t, "3", got.Secret.Version)
	require.NotNil(t, got.Certificate)
	require.Equal(t, "ocid1.certificate.oc1..abc", got.Certificate.Ocid)
	require.Equal(t, "7", got.Certificate.Version)
}

func TestOciCreatePoolCredentialsFromPayloads_Base64Wrapped(t *testing.T) {
	t.Parallel()
	inner, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"external_identifier": "ocid1.vaultsecret.oc1..wrapped",
			"name":                "wrapped-secret",
			"version":             1,
		},
	})
	require.NoError(t, err)
	body := []byte(base64.StdEncoding.EncodeToString(inner))

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	require.Equal(t, "ocid1.vaultsecret.oc1..wrapped", got.Secret.Ocid)
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
			"external_identifier": "ocid1.vaultsecret.oc1..onlyocid",
			"name":                "only-name",
			"version":             0,
		},
	})
	require.NoError(t, err)
	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.NotNil(t, got.Secret)
	require.Equal(t, "ocid1.vaultsecret.oc1..onlyocid", got.Secret.Ocid)
	require.Equal(t, "", got.Secret.Version, "version should be empty when activity reports version 0")
}

func TestOciCreatePoolCredentialsFromPayloads_AllEmptySecretReturnsNil(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"external_identifier": "",
			"name":                "",
			"version":             0,
		},
	})
	require.NoError(t, err)
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}}))
}

func TestOciCreatePoolCredentialsFromPayloads_AllEmptyCertificateReturnsNil(t *testing.T) {
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"certificate": map[string]interface{}{
			"external_identifier": "",
			"name":                "",
			"version":             0,
		},
	})
	require.NoError(t, err)
	require.Nil(t, ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}}))
}

func TestOciCreatePoolCredentialsFromPayloads_EmptySecretWithValidCertificateKeepsCertificate(t *testing.T) {
	// An empty secret block must not cause a valid certificate to be dropped.
	t.Parallel()
	body, err := json.Marshal(map[string]interface{}{
		"secret": map[string]interface{}{
			"external_identifier": "",
			"name":                "",
			"version":             0,
		},
		"certificate": map[string]interface{}{
			"external_identifier": "ocid1.certificate.oc1..abc",
			"name":                "ontap-cert",
			"version":             7,
		},
	})
	require.NoError(t, err)

	got := ociCreatePoolCredentialsFromPayloads([]*commonpb.Payload{{Data: body}})
	require.NotNil(t, got)
	require.Nil(t, got.Secret, "empty secret block must collapse to nil")
	require.NotNil(t, got.Certificate)
	require.Equal(t, "ocid1.certificate.oc1..abc", got.Certificate.Ocid)
	require.Equal(t, "7", got.Certificate.Version)
}
