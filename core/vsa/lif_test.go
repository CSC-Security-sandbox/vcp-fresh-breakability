package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateDataLIF(t *testing.T) {
	params := CreateLifParams{
		Name:      "testLIF",
		IpAddress: "192.168.1.1",
		HomePort:  "e0c",
		NodeName:  "node1",
		SvmName:   "testSVM",
	}

	t.Run("WhenLIFCreationSucceeds_ThenReturnLIFDetails", func(tt *testing.T) {
		mockNetworking := new(ontaprest.MockNetworkingClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Networking").Return(mockNetworking)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockLif := &ontaprest.IPInterface{
			IPInterface: models.IPInterface{
				Name: nillable.ToPointer("testLIF"),
				UUID: nillable.ToPointer("testUUID"),
				IP: &models.IPInfo{
					Address: nillable.ToPointer(models.IPAddress("192.168.1.1")),
					Netmask: nillable.ToPointer(models.IPNetmask("255.255.255.255")),
				},
			},
		}

		mockNetworking.On("NetworkIPInterfaceCreate", mock.Anything).Return(mockLif, nil)

		resp, err := rc.CreateDataLIF(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "testLIF", resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)
		assert.Equal(tt, "192.168.1.1", resp.IPAddress)
		assert.Equal(tt, DefaultNetmask, resp.SubnetMask)

		mockNetworking.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenLIFCreationFails_ThenReturnError", func(tt *testing.T) {
		mockNetworking := new(ontaprest.MockNetworkingClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Networking").Return(mockNetworking)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockNetworking.On("NetworkIPInterfaceCreate", mock.Anything).Return(nil, errors.New("creation error"))

		resp, err := rc.CreateDataLIF(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)

		mockNetworking.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestCreateDataLIF_InvalidLIFResponse(t *testing.T) {
	params := CreateLifParams{
		Name:      "testLIF",
		IpAddress: "192.168.1.1",
		HomePort:  "e0c",
		NodeName:  "node1",
		SvmName:   "testSVM",
	}

	mockNetworking := new(ontaprest.MockNetworkingClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Networking").Return(mockNetworking)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	tests := []struct {
		name      string
		mockLif   *ontaprest.IPInterface
		expectErr string
	}{
		{
			name:      "WhenLIFResponseIsNil_ThenReturnError",
			mockLif:   nil,
			expectErr: "invalid LIF response from API",
		},
		{
			name: "WhenLIFNameIsNil_ThenReturnError",
			mockLif: &ontaprest.IPInterface{
				IPInterface: models.IPInterface{
					Name: nil,
					UUID: nillable.ToPointer("testUUID"),
					IP: &models.IPInfo{
						Address: nillable.ToPointer(models.IPAddress("192.168.1.1")),
						Netmask: nillable.ToPointer(models.IPNetmask("255.255.255.255")),
					},
				},
			},
			expectErr: "invalid LIF response from API",
		},
		{
			name: "WhenLIFUUIDIsNil_ThenReturnError",
			mockLif: &ontaprest.IPInterface{
				IPInterface: models.IPInterface{
					Name: nillable.ToPointer("testLIF"),
					UUID: nil,
					IP: &models.IPInfo{
						Address: nillable.ToPointer(models.IPAddress("192.168.1.1")),
						Netmask: nillable.ToPointer(models.IPNetmask("255.255.255.255")),
					},
				},
			},
			expectErr: "invalid LIF response from API",
		},
		{
			name: "WhenLIFIPIsNil_ThenReturnError",
			mockLif: &ontaprest.IPInterface{
				IPInterface: models.IPInterface{
					Name: nillable.ToPointer("testLIF"),
					UUID: nillable.ToPointer("testUUID"),
					IP:   nil,
				},
			},
			expectErr: "invalid LIF response from API",
		},
		{
			name: "WhenLIFIPAddressIsNil_ThenReturnError",
			mockLif: &ontaprest.IPInterface{
				IPInterface: models.IPInterface{
					Name: nillable.ToPointer("testLIF"),
					UUID: nillable.ToPointer("testUUID"),
					IP: &models.IPInfo{
						Address: nil,
						Netmask: nillable.ToPointer(models.IPNetmask("255.255.255.255")),
					},
				},
			},
			expectErr: "invalid LIF response from API",
		},
		{
			name: "WhenLIFNetmaskIsNil_ThenReturnError",
			mockLif: &ontaprest.IPInterface{
				IPInterface: models.IPInterface{
					Name: nillable.ToPointer("testLIF"),
					UUID: nillable.ToPointer("testUUID"),
					IP: &models.IPInfo{
						Address: nillable.ToPointer(models.IPAddress("192.168.1.1")),
						Netmask: nil,
					},
				},
			},
			expectErr: "invalid LIF response from API",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			mockNetworking.On("NetworkIPInterfaceCreate", mock.Anything).Return(test.mockLif, nil)

			resp, err := rc.CreateDataLIF(params)

			assert.Error(tt, err)
			assert.Nil(tt, resp)
			assert.Equal(tt, test.expectErr, err.Error())

			mockNetworking.AssertExpectations(tt)
			mockClient.AssertExpectations(tt)
		})
	}
}
