package vsa

import (
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func (rc *OntapRestProvider) CreateSecurityLogForwarding(params CreateSecurityLogForwardingParams) (*CreateSecurityLogForwardingResponse, error) {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return nil, err
	}
	logForwarding, err := client.Security().SecurityLogForwardingCreate(&ontapRest.SecurityLogForwardingCreateParams{
		Address:      params.Address,
		Protocol:     params.Protocol,
		Port:         params.Port,
		Facility:     params.Facility,
		VerifyServer: params.VerifyServer,
	})

	if err != nil {
		return nil, err
	}

	if logForwarding != nil {
		response := &CreateSecurityLogForwardingResponse{ProviderResponse: ProviderResponse{Name: *logForwarding[0].Address}}
		return response, nil
	}
	return nil, err
}

func (rc *OntapRestProvider) GetSecurityLogForwarding(params GetSecurityLogForwardingParams) error {
	client, err := getOntapClientFunc(rc.ClientParams)
	if err != nil {
		return err
	}
	_, err = client.Security().SecurityLogForwardingGet(&ontapRest.SecurityLogForwardingGetParams{
		Address: params.Address,
		Port:    params.Port,
	})

	if err != nil {
		return err
	}

	return nil
}
