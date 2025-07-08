package vsa

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"testing"
)

func TestOntapRestProvider_CreateDns_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	provider := &OntapRestProvider{}

	params := CreateDnsParams{
		Domains: []string{"example.com"},
		Servers: []string{"8.8.8.8"},
	}
	mockClient.On("NameServices").Return(mockNameServices)
	mockNameServices.On("DnsCreate", &ontapRest.DNSCreateParams{
		Domains:    params.Domains,
		DNSServers: params.Servers,
	}).Return(nil, nil)

	err := provider.CreateDns(params)
	assert.NoError(t, err)
}

func TestOntapRestProvider_CreateDns_Failure(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockNameServices := new(ontapRest.MockNameServicesClient)

	getOntapClientFunc = func(clientParams ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	provider := &OntapRestProvider{}

	params := CreateDnsParams{
		Domains: []string{"example.com"},
		Servers: []string{"8.8.8.8"},
	}
	expectedErr := fmt.Errorf("API error")
	mockClient.On("NameServices").Return(mockNameServices)
	mockNameServices.On("DnsCreate", &ontapRest.DNSCreateParams{
		Domains:    params.Domains,
		DNSServers: params.Servers,
	}).Return(nil, expectedErr)

	err := provider.CreateDns(params)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}
