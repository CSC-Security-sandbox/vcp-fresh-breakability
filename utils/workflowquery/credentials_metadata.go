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

	out := &OCICreatePoolCredentialsMetadata{}
	if result.Secret != nil {
		ref := &OCICredentialRefMetadata{
			Ocid:    result.Secret.Name,
			Version: strconv.FormatInt(result.Secret.Version, 10),
		}
		// Skip emitting an empty version "0" when the activity has not set one.
		if result.Secret.Version == 0 {
			ref.Version = ""
		}
		if ref.Ocid == "" && ref.Version == "" {
			return nil
		}
		out.Secret = ref
	}

	if out.Secret == nil && out.Certificate == nil {
		return nil
	}
	return out
}
