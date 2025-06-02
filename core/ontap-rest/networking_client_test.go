package ontap_rest

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/networking"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	networkpriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestNetworkIPInterfacesGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkIPInterfacesGet(&NetworkIPInterfacesGetParams{}, nil)
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenReturnInterfaces", func(tt *testing.T) {
		ipInterface := &models.IPInterface{}
		transport := &mockTransport{response: &networking.NetworkIPInterfacesGetOK{
			Payload: &models.IPInterfaceResponse{
				IPInterfaceResponseInlineRecords: []*models.IPInterface{ipInterface},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		var interfaces []*IPInterface
		err := client.NetworkIPInterfacesGet(&NetworkIPInterfacesGetParams{}, func(data []*IPInterface) error {
			interfaces = append(interfaces, data...)
			return nil
		})
		assert.NoError(tt, err)
		assert.Len(tt, interfaces, 1)
		assert.Equal(tt, ipInterface, &interfaces[0].IPInterface)
	})
}

func TestNetworkEthernetBroadcastDomainCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkEthernetBroadcastDomainCreate(&NetworkEthernetBroadcastDomainCreateParams{Name: "domain1", IPSpace: "ipspace1"})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.NetworkEthernetBroadcastDomainsCreateCreated{}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkEthernetBroadcastDomainCreate(&NetworkEthernetBroadcastDomainCreateParams{Name: "domain1", IPSpace: "ipspace1"})
		assert.NoError(tt, err)
	})
}

func TestIpspaceExists(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		exists, err := client.IpspaceExists("ipspace1")
		assert.EqualError(tt, err, transport.err.Error())
		assert.False(tt, exists)
	})

	t.Run("WhenIpspaceDoesNotExist_ThenReturnFalse", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.IpspacesGetOK{
			Payload: &models.IpspaceResponse{
				IpspaceResponseInlineRecords: []*models.Ipspace{},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		exists, err := client.IpspaceExists("ipspace1")
		assert.NoError(tt, err)
		assert.False(tt, exists)
	})

	t.Run("WhenIpspaceExists_ThenReturnTrue", func(tt *testing.T) {
		ipspaceName := "ipspace1"
		transport := &mockTransport{response: &networking.IpspacesGetOK{
			Payload: &models.IpspaceResponse{
				IpspaceResponseInlineRecords: []*models.Ipspace{
					{Name: &ipspaceName},
				},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		exists, err := client.IpspaceExists(ipspaceName)
		assert.NoError(tt, err)
		assert.True(tt, exists)
	})

	t.Run("WhenIpspaceExists_ThenReturnTrue", func(tt *testing.T) {
		ipspaceName := "ipspace1"
		transport := &mockTransport{response: &networking.IpspacesGetOK{
			Payload: &models.IpspaceResponse{
				IpspaceResponseInlineRecords: []*models.Ipspace{
					{Name: &ipspaceName},
					{Name: &ipspaceName}, // duplicate entry
				},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		exists, err := client.IpspaceExists(ipspaceName)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Unexpected response from storage server when querying ip spaces")
		assert.False(tt, exists)
	})
}

func TestNetworkIPInterfaceCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		_, err := client.NetworkIPInterfaceCreate(&NetworkIPInterfacesCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		_, err := client.NetworkIPInterfaceCreate(&NetworkIPInterfacesCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenReturnIPInterface", func(tt *testing.T) {
		ipName := "ip1"
		transport := &mockTransport{response: &networking.NetworkIPInterfacesCreateCreated{
			Payload: &models.IPInterfaceResponse{
				IPInterfaceResponseInlineRecords: []*models.IPInterface{
					{Name: &ipName},
				},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		ipInterface, err := client.NetworkIPInterfaceCreate(&NetworkIPInterfacesCreateParams{Name: ipName})
		assert.NoError(tt, err)
		assert.NotNil(tt, ipInterface)
		assert.Equal(tt, ipName, *ipInterface.Name)
	})

	t.Run("WhenSuccessful_ThenReturnIPInterface", func(tt *testing.T) {
		ipName := "ip1"
		transport := &mockTransport{response: &networking.NetworkIPInterfacesCreateCreated{
			Payload: &models.IPInterfaceResponse{
				IPInterfaceResponseInlineRecords: []*models.IPInterface{
					{Name: &ipName},
					{Name: &ipName}, // duplicate entry
				},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		ipInterface, err := client.NetworkIPInterfaceCreate(&NetworkIPInterfacesCreateParams{Name: ipName})
		assert.Nil(tt, ipInterface)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Unexpected response from storage server when creating network interface: '2'")
	})
}

func TestNetworkPing(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		apiPriv := networkpriv.New(transport, nil)
		networkAPI := networking.New(transport, nil)

		client := &networkingClient{api: networkAPI, apiPriv: &apiPriv}

		_, _, err := client.NetworkPing(&networkpriv.NetworkPingParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenAcceptedResponse_ThenReturnAccepted", func(tt *testing.T) {
		transport := &mockTransport{response: &networkpriv.NetworkPingAccepted{}}
		apiPriv := networkpriv.New(transport, nil)
		client := &networkingClient{apiPriv: &apiPriv}
		_, responseAccepted, err := client.NetworkPing(&networkpriv.NetworkPingParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, responseAccepted)
	})

	t.Run("WhenOKResponse_ThenReturnOK", func(tt *testing.T) {
		transport := &mockTransport{response: &networkpriv.NetworkPingOK{}}
		apiPriv := networkpriv.New(transport, nil)
		client := &networkingClient{apiPriv: &apiPriv}
		responseOK, _, err := client.NetworkPing(&networkpriv.NetworkPingParams{})
		assert.NoError(tt, err)
		assert.NotNil(tt, responseOK)
	})
}

func TestNetworkEthernetBroadcastDomainGet(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		_, err := client.NetworkEthernetBroadcastDomainGet(&NetworkEthernetBroadcastDomainsGetParams{Name: "domain1"})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenNoMatchingBroadcastDomain_ThenReturnNotFoundError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.NetworkEthernetBroadcastDomainsGetOK{
			Payload: &models.BroadcastDomainResponse{
				BroadcastDomainResponseInlineRecords: []*models.BroadcastDomain{},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		_, err := client.NetworkEthernetBroadcastDomainGet(&NetworkEthernetBroadcastDomainsGetParams{Name: "domain1"})
		assert.EqualError(tt, err, "broadcast_domain 'domain1' not found")
	})

	t.Run("WhenMatchingBroadcastDomainFound_ThenReturnBroadcastDomain", func(tt *testing.T) {
		domainName := "domain1"
		transport := &mockTransport{response: &networking.NetworkEthernetBroadcastDomainsGetOK{
			Payload: &models.BroadcastDomainResponse{
				BroadcastDomainResponseInlineRecords: []*models.BroadcastDomain{
					{Name: &domainName},
				},
			},
		}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		broadcastDomain, err := client.NetworkEthernetBroadcastDomainGet(&NetworkEthernetBroadcastDomainsGetParams{Name: domainName})
		assert.NoError(tt, err)
		assert.NotNil(tt, broadcastDomain)
		assert.Equal(tt, domainName, *broadcastDomain.Name)
	})
}

func TestNetworkEthernetBroadcastDomainDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkEthernetBroadcastDomainDelete(&NetworkEthernetBroadcastDomainDeleteParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.NetworkEthernetBroadcastDomainDeleteCollectionOK{}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkEthernetBroadcastDomainDelete(&NetworkEthernetBroadcastDomainDeleteParams{})
		assert.NoError(tt, err)
	})
}

func TestIpspaceCreate(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.IpspaceCreate("ipspace1")
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.IpspacesCreateCreated{}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.IpspaceCreate("ipspace1")
		assert.NoError(tt, err)
	})
}

func TestIpspaceDelete(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.IpspaceDelete(&IpspaceDeleteParams{Name: "ipspace1"})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.IpspaceDeleteCollectionOK{}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.IpspaceDelete(&IpspaceDeleteParams{Name: "ipspace1"})
		assert.NoError(tt, err)
	})
}

func TestNetworkIPRouteCreateDefault(t *testing.T) {
	t.Run("WhenRESTCallFails_ThenReturnError", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkIPRouteCreateDefault(&NetworkIPDefaultRouteCreateParams{})
		assert.EqualError(tt, err, transport.err.Error())
	})

	t.Run("WhenSuccessful_ThenNoError", func(tt *testing.T) {
		transport := &mockTransport{response: &networking.NetworkIPRoutesCreateCreated{}}
		networkAPI := networking.New(transport, nil)
		client := &networkingClient{api: networkAPI}
		err := client.NetworkIPRouteCreateDefault(&NetworkIPDefaultRouteCreateParams{})
		assert.NoError(tt, err)
	})
}

func TestNetworkingClient_InterclusterLifGet(t *testing.T) {
	t.Run("WhenRESTCallFails", func(tt *testing.T) {
		transport := &mockTransport{err: errors.New("something went wrong")}
		n := networking.New(transport, nil)
		client := &networkingClient{api: n}
		servicePolicyName := "default-intercluster"
		networkIPInterfacesGetParams := &NetworkIPInterfacesGetParams{BaseParams: BaseParams{Fields: []string{"ip.address"}}, ServicePolicyName: &servicePolicyName}

		response, err := client.InterclusterLifsGet(networkIPInterfacesGetParams)
		if response != nil {
			tt.Errorf("Unexpected response returned: %v", response)
		}
		if err != transport.err {
			tt.Errorf("Unexpected error returned: %v", err)
		}
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mcs := networking.NewMockClientService(tt)
		client := &networkingClient{api: mcs}
		pp := &NetworkIPInterfacesGetParams{}
		firstBatchOntap := []*models.IPInterface{
			{Name: nillable.ToPointer("1")},
			{Name: nillable.ToPointer("2")},
			{Name: nillable.ToPointer("3")},
		}
		go func() {
			defer mcs.MockClientServiceDone()
			res, err := client.InterclusterLifsGet(pp)
			assert.Nil(tt, err)
			assert.Equal(tt, 3, len(res))
		}()
		otpp := networkIPInterfacesGetParamsToONTAP(pp)
		mcs.AssertNetworkIPInterfacesGet(otpp, nil, nil, &networking.NetworkIPInterfacesGetOK{
			Payload: &models.IPInterfaceResponse{
				IPInterfaceResponseInlineRecords: firstBatchOntap,
			}}, nil)
	})
}
