package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestCreateNetworkIpRoute(t *testing.T) {
	params := CreateNetworkIPRouteParams{
		SvmName: "testSVM",
		Gateway: "192.168.1.1",
	}

	t.Run("WhenRouteCreationSucceeds_ThenNoErrorIsReturned", func(tt *testing.T) {
		mockNetworking := new(ontaprest.MockNetworkingClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Networking").Return(mockNetworking)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockNetworking.On("NetworkIPRouteCreateDefault", mock.Anything).Return(nil)

		err := rc.CreateNetworkIpRoute(params)

		assert.NoError(tt, err)

		mockNetworking.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenRouteCreationFails_ThenReturnError", func(tt *testing.T) {
		mockNetworking := new(ontaprest.MockNetworkingClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Networking").Return(mockNetworking)

		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockNetworking.On("NetworkIPRouteCreateDefault", mock.Anything).Return(errors.New("route creation error"))

		err := rc.CreateNetworkIpRoute(params)

		assert.Error(tt, err)
		assert.Equal(tt, "route creation error", err.Error())

		mockNetworking.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenRouteCreationFails_getOntapClientFuncError", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClient error")
		}
		rc := &OntapRestProvider{}

		err := rc.CreateNetworkIpRoute(params)

		assert.Error(tt, err)
		assert.Equal(tt, "getOntapClient error", err.Error())
	})
}
