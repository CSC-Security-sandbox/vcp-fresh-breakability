// Ensure correct package declaration
package vsa

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"testing"

	"github.com/stretchr/testify/assert"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateSecurityLogForwarding_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}
	response := []*ontapRest.SecurityAuditLogForward{{}}
	response[0].Address = nillable.GetStringPtr("test-address")
	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingCreate", &ontapRest.SecurityLogForwardingCreateParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}).Return(response, nil)

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "test-address", resp.Name)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestCreateSecurityLogForwarding_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingCreate", &ontapRest.SecurityLogForwardingCreateParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}).Return(nil, fmt.Errorf("Rest Error"))

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestCreateSecurityLogForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := CreateSecurityLogForwardingParams{
		Address:  nillable.GetStringPtr("test-address"),
		Protocol: nillable.GetStringPtr("test-protocol"),
		Port:     nillable.GetInt64Ptr(1009),
	}

	resp, err := ontapProvider.CreateSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestGetSecurityLogForwarding_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}
	response := ontapRest.SecurityAuditLogForward{
		SecurityAuditLogForward: models.SecurityAuditLogForward{
			Address: nillable.GetStringPtr("test-address"),
		},
	}
	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingGet", &ontapRest.SecurityLogForwardingGetParams{
		Address: "test-address",
		Port:    1009,
	}).Return(&response, nil)

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityLogForwarding_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityLogForwardingGet", &ontapRest.SecurityLogForwardingGetParams{
		Address: "test-address",
		Port:    1009,
	}).Return(nil, fmt.Errorf("Rest Error"))

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityLogForwarding_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := GetSecurityLogForwardingParams{
		Address: "test-address",
		Port:    1009,
	}

	err := ontapProvider.GetSecurityLogForwarding(params)
	assert.Error(t, err)
	assert.Equal(t, "getOntapClient error", err.Error())
}
