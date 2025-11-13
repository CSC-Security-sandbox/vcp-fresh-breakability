package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// NameServicesClient describes a name_services client
type NameServicesClient interface { // generate:mock
	DnsCreate(params *DNSCreateParams) (*models.DNSResponse, error)
	DNSGet(params *DNSGetParams) (*DNS, error)
	DNSModify(params *DNSModifyParams) error
}

type nameServicesClient struct {
	api *name_services.ClientService
}

// DnsCreate invokes pkg/ontap-rest/client/name_services/Client.DnsCreate
func (nsc *nameServicesClient) DnsCreate(params *DNSCreateParams) (*models.DNSResponse, error) {
	response, err := (*nsc.api).DNSCreate(dnsCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from DnsCreate")
	}

	return response.Payload, err
}

// DNSGet invokes pkg/ontap-rest/client/name_services/Client.DNSGet
func (nsc *nameServicesClient) DNSGet(params *DNSGetParams) (*DNS, error) {
	response, err := (*nsc.api).DNSGet(dnsGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	return &DNS{DNS: *response.Payload}, nil
}

// DNSModify modifies the DNS
func (nsc *nameServicesClient) DNSModify(params *DNSModifyParams) error {
	_, err := (*nsc.api).DNSModify(dnsModifyParamsToONTAP(params), nil)
	return err
}
