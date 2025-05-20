package vsa

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestGetInterclusterLIF(t *testing.T) {
	t.Run("WhenClientReturnsError", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockNetworkingClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		servicePolicyName := "default-intercluster"
		networkIPInterfacesGetParams := &ontaprest.NetworkIPInterfacesGetParams{BaseParams: ontaprest.BaseParams{Fields: []string{"ip.address"}}, ServicePolicyName: &servicePolicyName}

		ontapProvider := &OntapRestProvider{}
		mockClient.On("Networking").Return(mm)
		mm.On("InterclusterLifsGet", networkIPInterfacesGetParams).Return(nil, errors.New("Faceplanting")).Times(1)
		_, err := ontapProvider.GetInterclusterLIFs(servicePolicyName)
		if err == nil {
			tt.Error("No error returned")
		} else if err.Error() != "Faceplanting" {
			tt.Errorf("Wrong error returned: %v", err.Error())
		}
	})
	t.Run("WhenSuccessful", func(tt *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mm := new(ontaprest.MockNetworkingClient)
		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		ontapProvider := &OntapRestProvider{}
		servicePolicyName := "default-intercluster"
		networkIPInterfacesGetParams := &ontaprest.NetworkIPInterfacesGetParams{BaseParams: ontaprest.BaseParams{Fields: []string{"ip.address"}}, ServicePolicyName: &servicePolicyName}
		netmask := "24"
		name := "Name"
		IPAddress := "1.2.3.4"
		IPSpace := "Ipspace"
		returnedIPInterfaceModel := []*ontaprest.IPInterface{{
			IPInterface: models.IPInterface{
				Name: &name,
				IP: &models.IPInfo{
					Address: nillable.ToPointer(models.IPAddress(IPAddress)),
					Netmask: nillable.ToPointer(models.IPNetmask(netmask)),
				},
				Location: &models.IPInterfaceInlineLocation{
					Node: &models.IPInterfaceInlineLocationInlineNode{
						UUID: nillable.ToPointer("node-uuid-1"),
					},
				},
				Ipspace: &models.IPInterfaceInlineIpspace{
					Name: &IPSpace,
				},
				ServicePolicy: &models.IPInterfaceInlineServicePolicy{
					Name: &servicePolicyName,
				},
			},
		},
		}
		mockClient.On("Networking").Return(mm)
		mm.On("InterclusterLifsGet", networkIPInterfacesGetParams).Return(returnedIPInterfaceModel, nil).Times(1)
		_, err := ontapProvider.GetInterclusterLIFs(servicePolicyName)
		require.NoError(tt, err)
	})
}
