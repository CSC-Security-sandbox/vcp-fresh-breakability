package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

// NameServicesClient describes a name_services client
type NameServicesClient interface { // generate:mock
	DnsCreate(params *DNSCreateParams) (*models.DNSResponse, error)
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
