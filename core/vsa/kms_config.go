package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"strings"
)

func (rc *OntapRestProvider) CreateKmsConfig(params CreateKmsConfigParams) (*CreateKmsConfigResponse, error) {
	client := getOntapClientFunc(rc.ClientParams)
	gcpKmsCreateParams := &ontapRest.GcpKmsCreateParams{
		KeyName:                &params.KeyName,
		KeyRingLocation:        &params.KeyRingLocation,
		KeyRingName:            &params.KeyRingName,
		ProjectID:              &params.ProjectID,
		ApplicationCredentials: params.Credentials,
		SvmName:                &params.SvmName,
		PrivilegedAccount:      &params.PrivilegedAccount, // Cloud KMS account to impersonate.
	}
	gcpKmsResponse, err := client.Security().GcpKmsCreate(gcpKmsCreateParams)
	if err != nil {
		return nil, err
	}
	response := &CreateKmsConfigResponse{ProviderResponse: ProviderResponse{ExternalUUID: *gcpKmsResponse[0].UUID}}
	return response, nil
}

func (rc *OntapRestProvider) IsGcpKmsReachable(params GetKmsConfigParams) (bool, error) {
	client := getOntapClientFunc(rc.ClientParams)
	getKmsConfigParams := &ontapRest.GcpKmsGetParams{
		BaseParams: ontapRest.BaseParams{Fields: []string{"**"}},
		UUID:       params.ExternalKmsConfigID,
	}
	gcpKmsResponse, err := client.Security().GcpKmsGet(getKmsConfigParams)
	if err != nil {
		return false, err
	}

	if !nillable.GetBool(gcpKmsResponse.GoogleReachability.Reachable, false) {
		if strings.Contains(strings.ToLower(*gcpKmsResponse.GoogleReachability.Message), "permission_denied") {
			return false, errors.New("permission_denied")
		}
		return false, nil
	}
	for _, reachability := range gcpKmsResponse.GcpKmsInlineEkmipReachability {
		if !nillable.GetBool(reachability.Reachable, false) {
			return false, nil
		}
	}
	return true, nil
}
