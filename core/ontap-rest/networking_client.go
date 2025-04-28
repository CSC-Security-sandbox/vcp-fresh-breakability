package ontap_rest

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/networking"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	networkpriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

// NetworkingClient describes a networking client
type NetworkingClient interface { // generate:mock
	NetworkIPInterfacesGet(params *NetworkIPInterfacesGetParams, ucbf UserCallbackFunc[[]*IPInterface]) error
	NetworkIPRouteCreateDefault(params *NetworkIPDefaultRouteCreateParams) error
	NetworkIPInterfaceCreate(params *NetworkIPInterfacesCreateParams) (*IPInterface, error)

	NetworkPing(params *networkpriv.NetworkPingParams) (*networkpriv.NetworkPingOK, *networkpriv.NetworkPingAccepted, error)

	NetworkEthernetBroadcastDomainGet(params *NetworkEthernetBroadcastDomainsGetParams) (*BroadcastDomain, error)
	NetworkEthernetBroadcastDomainCreate(params *NetworkEthernetBroadcastDomainCreateParams) error
	NetworkEthernetBroadcastDomainDelete(params *NetworkEthernetBroadcastDomainDeleteParams) error

	IpspaceExists(name string) (bool, error)
	IpspaceCreate(name string) error
	IpspaceDelete(params *IpspaceDeleteParams) error
}

type networkingClient struct {
	api     networking.ClientService
	apiPriv *networkpriv.ClientService
	// poller  Poller
}

var (
	paginateNetworkIPInterfacesGet = _paginate[[]*IPInterface]
)

// NetworkIPInterfacesGet invokes pkg/ontap-rest/client/networking/Client.NetworkIPInterfacesGetUsingCallback
func (nc *networkingClient) NetworkIPInterfacesGet(params *NetworkIPInterfacesGetParams, ucbf UserCallbackFunc[[]*IPInterface]) error {
	otParams := networkIPInterfacesGetParamsToONTAP(params)
	return paginateNetworkIPInterfacesGet(func(next string) ([]*IPInterface, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := nc.api.NetworkIPInterfacesGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		interfaces := make([]*IPInterface, len(rsp.Payload.IPInterfaceResponseInlineRecords))
		for i, ip := range rsp.Payload.IPInterfaceResponseInlineRecords {
			interfaces[i] = &IPInterface{IPInterface: *ip}
		}
		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return interfaces, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return interfaces, "", nil
	}, ucbf)
}

// NetworkPing invokes pkg/ontap-rest/priv/client/operations/Client.NetworkPing
func (nc *networkingClient) NetworkPing(params *networkpriv.NetworkPingParams) (*networkpriv.NetworkPingOK, *networkpriv.NetworkPingAccepted, error) {
	responseOK, responseAccepted, err := (*nc.apiPriv).NetworkPing(params)
	return responseOK, responseAccepted, err
}

var paginateNetworkEthernetBroadcastDomainGet = _paginate[[]*BroadcastDomain]

// NetworkEthernetBroadcastDomainGet invokes pkg/ontap-rest/client/networking/Client.NetworkEthernetBroadcastDomainGetUsingCallback
func (nc *networkingClient) NetworkEthernetBroadcastDomainGet(params *NetworkEthernetBroadcastDomainsGetParams) (*BroadcastDomain, error) {
	var b *BroadcastDomain
	otParams := networkEthernetBroadcastDomainsGetParamsToONTAP(params)

	if err := paginateNetworkEthernetBroadcastDomainGet(func(next string) ([]*BroadcastDomain, string, error) {
		otParams.SetContext(setNext(otParams.Context, next))

		rsp, err := nc.api.NetworkEthernetBroadcastDomainsGet(otParams, nil)
		if err != nil {
			return nil, "", err
		}

		resp := make([]*BroadcastDomain, len(rsp.Payload.BroadcastDomainResponseInlineRecords))
		for i, s := range rsp.Payload.BroadcastDomainResponseInlineRecords {
			resp[i] = &BroadcastDomain{BroadcastDomain: *s}
		}
		if rsp.Payload.Links != nil && rsp.Payload.Links.Next != nil {
			return resp, nillable.FromPointer(rsp.Payload.Links.Next.Href), nil
		}

		return resp, "", nil
	}, func(bdomains []*BroadcastDomain) error {
		if b == nil {
			for _, bdomain := range bdomains {
				if *bdomain.Name == params.Name {
					b = bdomain
					break
				}
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if b == nil {
		return nil, errors.NewNotFoundErr("broadcast_domain", &params.Name)
	}

	return b, nil
}

var (
	mtu = nillable.ToPointer(int64(env.GetInt("BROADCAST_DOMAIN_MTU", 1500)))
)

// NetworkEthernetBroadcastDomainCreate invokes pkg/ontap-rest/client/networking/Client.NetworkEthernetBroadcastDomainCreate
func (nc *networkingClient) NetworkEthernetBroadcastDomainCreate(params *NetworkEthernetBroadcastDomainCreateParams) error {
	_, err := nc.api.NetworkEthernetBroadcastDomainsCreate(networking.NewNetworkEthernetBroadcastDomainsCreateParams().WithInfo(&models.BroadcastDomain{
		Name:    &params.Name,
		Mtu:     mtu,
		Ipspace: &models.BroadcastDomainInlineIpspace{Name: &params.IPSpace},
	}), nil)
	return err
}

// NetworkEthernetBroadcastDomainDelete invokes pkg/ontap-rest/client/networking/Client.NetworkEthernetBroadcastDomainDelete
func (nc *networkingClient) NetworkEthernetBroadcastDomainDelete(params *NetworkEthernetBroadcastDomainDeleteParams) error {
	_, err := nc.api.NetworkEthernetBroadcastDomainDeleteCollection(networkEthernetBroadcastDomainDeleteParamsToONTAP(params).WithReturnTimeout(returnTimeoutNoJob), nil)
	return err
}

// IpspaceExists checks if an ipsace exists
func (nc *networkingClient) IpspaceExists(name string) (bool, error) {
	ipSpaces, err := nc.api.IpspacesGet(networking.NewIpspacesGetParams().WithName(&name), nil)
	if err != nil {
		return false, err
	}

	if len(ipSpaces.GetPayload().IpspaceResponseInlineRecords) == 0 {
		return false, nil
	}

	if len(ipSpaces.GetPayload().IpspaceResponseInlineRecords) > 1 {
		return false, errors.New("Unexpected response from storage server when querying ip spaces")
	}

	return true, nil
}

// IpspaceCreate creates an ipsace
func (nc *networkingClient) IpspaceCreate(name string) error {
	_, err := nc.api.IpspacesCreate(networking.NewIpspacesCreateParams().WithInfo(&models.Ipspace{Name: &name}), nil)
	return err
}

// IpspaceDelete deletes an ipsace
func (nc *networkingClient) IpspaceDelete(params *IpspaceDeleteParams) error {
	_, err := nc.api.IpspaceDeleteCollection(ipspaceDeleteParamsToONTAP(params).WithReturnTimeout(returnTimeoutNoJob), nil)
	return err
}

// NetworkIPRouteCreateDefault creates the default network ip route
func (nc *networkingClient) NetworkIPRouteCreateDefault(params *NetworkIPDefaultRouteCreateParams) error {
	_, err := nc.api.NetworkIPRoutesCreate(networkIPRouteCreateParamsToONTAP(params), nil)
	return err
}

func (nc *networkingClient) NetworkIPInterfaceCreate(params *NetworkIPInterfacesCreateParams) (*IPInterface, error) {
	rsp, err := nc.api.NetworkIPInterfacesCreate(networking.NewNetworkIPInterfacesCreateParams().WithInfo(&models.IPInterface{
		Enabled:               nillable.ToPointer(true),
		FailIfSubnetConflicts: nillable.ToPointer(true),
		IP: &models.IPInfo{
			Address: nillable.ToPointer(models.IPAddress(params.IPAddress)),
			Netmask: nillable.ToPointer(models.IPNetmask(params.Netmask)),
		},
		Location: &models.IPInterfaceInlineLocation{
			HomeNode: &models.IPInterfaceInlineLocationInlineHomeNode{
				Name: &params.HomeNode,
			},
			HomePort: &models.IPInterfaceInlineLocationInlineHomePort{
				Name: &params.HomePort,
			},
		},
		Name:  &params.Name,
		Scope: nillable.ToPointer(models.IPInterfaceScopeSvm),
		ServicePolicy: &models.IPInterfaceInlineServicePolicy{
			Name: &params.ServicePolicy,
		},
		Svm: &models.IPInterfaceInlineSvm{
			Name: &params.SvmName,
		},
	}).WithReturnRecords(nillable.ToPointer("true")), nil)
	if err != nil {
		return nil, err
	}

	if len(rsp.Payload.IPInterfaceResponseInlineRecords) != 1 {
		return nil, errors.Errorf("Unexpected response from storage server when creating network interface: '%d'", len(rsp.Payload.IPInterfaceResponseInlineRecords))
	}

	return &IPInterface{IPInterface: *rsp.Payload.IPInterfaceResponseInlineRecords[0]}, nil
}
