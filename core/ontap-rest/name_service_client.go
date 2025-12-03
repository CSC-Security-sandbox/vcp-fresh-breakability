package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/name_services"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	overide "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/overrideModels"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// NameServicesClient describes a name_services client
type NameServicesClient interface { // generate:mock
	DnsCreate(params *DNSCreateParams) (*models.DNSResponse, error)
	DNSGet(params *DNSGetParams) (*DNS, error)
	DNSModify(params *DNSModifyParams) error
	LdapCreate(params *LdapCreateParams) (*models.LdapServiceResponse, error)
	LdapDelete(params *LdapDeleteParams) error
	LdapGet(params *LdapGetParams) (*LdapService, error)
	LdapSchemaCreate(params *LdapSchemaCreateParams) error
	LdapSchemaModify(params *LdapSchemaModifyParams) error
	LdapModify(params *LdapModifyParams) error
	LdapModifyPreferredAdServers(params *LdapModifyParams) error
	NameMappingCollectionGet(params *NameMappingCollectionGetParams) ([]*NameMapping, error)
	NameMappingCreate(params *NameMappingCreateParams) error
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

// LdapCreate invokes pkg/ontap-rest/client/name_services/Client.LdapCreate
func (nsc *nameServicesClient) LdapCreate(params *LdapCreateParams) (*models.LdapServiceResponse, error) {
	response, err := (*nsc.api).LdapCreate(ldapCreateParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	if response == nil || response.Payload == nil {
		return nil, errors.New("unexpected response from LdapCreate")
	}

	return response.Payload, err
}

// LdapGet invokes pkg/ontap-rest/client/name_services/Client.LdapGet
func (nsc *nameServicesClient) LdapGet(params *LdapGetParams) (*LdapService, error) {
	response, err := (*nsc.api).LdapGet(ldapGetParamsToONTAP(params), nil)
	if err != nil {
		return nil, err
	}

	return &LdapService{LdapService: nillable.FromPointer(response.Payload)}, nil
}

// LdapDelete invokes pkg/ontap-rest/client/name_services/Client.LdapDelete
func (nsc *nameServicesClient) LdapDelete(params *LdapDeleteParams) error {
	_, err := (*nsc.api).LdapDelete(ldapDeleteParamsToONTAP(params), nil)
	return err
}

// LdapSchemaCreate invokes pkg/ontap-rest/client/name_services/Client.LdapSchemaCreate
func (nsc *nameServicesClient) LdapSchemaCreate(params *LdapSchemaCreateParams) error {
	_, err := (*nsc.api).LdapSchemaCreate(ldapSchemaCreateParamsToONTAP(params), nil)
	return err
}

// LdapSchemaModify invokes pkg/ontap-rest/client/name_services/Client.LdapSchemaModify
func (nsc *nameServicesClient) LdapSchemaModify(params *LdapSchemaModifyParams) error {
	_, err := (*nsc.api).LdapSchemaModify(ldapSchemaModifyParamsToONTAP(params), nil)
	return err
}

// LdapModify invokes pkg/ontap-rest/client/name_services/Client.LdapModify
func (nsc *nameServicesClient) LdapModify(params *LdapModifyParams) error {
	_, err := (*nsc.api).LdapModify(ldapModifyParamsToONTAP(params), nil)
	return err
}

// LdapModifyPreferredAdServers invokes pkg/ontap-rest/priv/client/operations/Client.LdapModify with a custom request writer to override 'LdapServiceInlinePreferredAdServers' field
func (nsc *nameServicesClient) LdapModifyPreferredAdServers(params *LdapModifyParams) error {
	lsm := overide.LdapServiceModified{}
	ldapModifyParams := ldapModifyParamsToONTAP(params)
	clientRequestWriter, err := lsm.SetClientRequestWriterForLdapPreferredAdServer(ldapModifyParams)
	if err != nil {
		return err
	}
	_, err = (*nsc.api).LdapModify(ldapModifyParams, nil, clientRequestWriter)
	return err
}

// NameMappingCollectionGet invokes pkg/ontap-rest/client/name_services/Client.NameMappingCollectionGet
func (nsc *nameServicesClient) NameMappingCollectionGet(params *NameMappingCollectionGetParams) ([]*NameMapping, error) {
	otParams := nameMappingCollectionGetParamsToONTAP(params)
	response, err := (*nsc.api).NameMappingCollectionGet(otParams, nil)
	if err != nil {
		return nil, err
	}

	if response.Payload == nil || response.Payload.NameMappingResponseInlineRecords == nil {
		return []*NameMapping{}, nil
	}

	result := make([]*NameMapping, len(response.Payload.NameMappingResponseInlineRecords))
	for i, record := range response.Payload.NameMappingResponseInlineRecords {
		result[i] = &NameMapping{NameMapping: *record}
	}
	return result, nil
}

// NameMappingCreate invokes pkg/ontap-rest/client/name_services/Client.NameMappingCreate
func (nsc *nameServicesClient) NameMappingCreate(params *NameMappingCreateParams) error {
	_, err := (*nsc.api).NameMappingCreate(nameMappingCreateParamsToONTAP(params), nil)
	return err
}
