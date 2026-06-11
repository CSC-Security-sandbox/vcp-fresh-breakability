package workflowquery

import (
	"encoding/json"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
)

// ociCreatePoolCredentialsResult mirrors the JSON shape of
// activities.OCICreatePoolCredentials. We redeclare the minimum set of fields
// needed for metadata extraction here so utils/workflowquery does not import
// the orchestrator package (which would create a layering cycle).
type ociCreatePoolCredentialsResult struct {
	Secret      *externalCredRefResult `json:"secret,omitempty"`
	Certificate *externalCredRefResult `json:"certificate,omitempty"`
}

type externalCredRefResult struct {
	ExternalIdentifier string `json:"external_identifier"`
	Name               string `json:"name"`
	Version            int64  `json:"version"`
}

// ociCreatePoolCredentialsFromPayloads decodes the activity result payload of
// CreateOnTapCredentialsForOCI and returns the API-shaped credentials metadata.
func ociCreatePoolCredentialsFromPayloads(payloads []*commonpb.Payload) *OCICreatePoolCredentialsMetadata {
	body := payloadDataJSONBytes(payloads)
	if len(body) == 0 {
		return nil
	}

	var result ociCreatePoolCredentialsResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil
	}

	out := &OCICreatePoolCredentialsMetadata{
		Secret:      externalCredRefToMetadata(result.Secret),
		Certificate: externalCredRefToMetadata(result.Certificate),
	}
	if out.Secret == nil && out.Certificate == nil {
		return nil
	}
	return out
}

// externalCredRefToMetadata converts the activity-side ExternalCredRef shape
// into the API-shaped OCICredentialRefMetadata. It returns nil for missing
// refs and for refs whose OCID and version are both empty so the caller can
// suppress empty placeholder objects from the response.
func externalCredRefToMetadata(ref *externalCredRefResult) *OCICredentialRefMetadata {
	if ref == nil {
		return nil
	}
	out := &OCICredentialRefMetadata{
		Ocid: ref.ExternalIdentifier,
	}
	// Skip emitting an empty version "0" when the activity has not set one.
	if ref.Version != 0 {
		out.Version = strconv.FormatInt(ref.Version, 10)
	}
	if out.Ocid == "" && out.Version == "" {
		return nil
	}
	return out
}
