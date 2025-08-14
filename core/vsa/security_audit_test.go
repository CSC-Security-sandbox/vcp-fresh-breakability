// Ensure correct package declaration
package vsa

import (
	"fmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"

	"github.com/stretchr/testify/assert"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func TestCreateSecurityAudit_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateSecurityAuditParams{
		Cli:    true,
		HTTP:   true,
		Ontapi: true,
	}

	response := ontapRest.SecurityAudit{
		SecurityAudit: models.SecurityAudit{
			Cli:    nillable.GetBoolPtr(true),
			HTTP:   nillable.GetBoolPtr(true),
			Ontapi: nillable.GetBoolPtr(true),
		},
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityAuditUpdate", &ontapRest.SecurityAuditUpdateParams{
		Cli:    true,
		HTTP:   true,
		Ontapi: true,
	}).Return(&response, nil)

	resp, err := ontapProvider.UpdateSecurityAudit(params)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.HTTP)
	assert.True(t, resp.Cli)
	assert.True(t, resp.Ontapi)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestCreateSecurityAudit_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateSecurityAuditParams{
		Cli:    true,
		HTTP:   true,
		Ontapi: true,
	}

	resp, err := ontapProvider.UpdateSecurityAudit(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestCreateSecurityAudit_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	params := UpdateSecurityAuditParams{
		Cli:    true,
		HTTP:   true,
		Ontapi: true,
	}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityAuditUpdate", &ontapRest.SecurityAuditUpdateParams{
		Cli:    true,
		HTTP:   true,
		Ontapi: true,
	}).Return(nil, fmt.Errorf("Rest Error"))

	resp, err := ontapProvider.UpdateSecurityAudit(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityAudit_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}

	response := ontapRest.SecurityAudit{
		SecurityAudit: models.SecurityAudit{
			Cli:    nillable.GetBoolPtr(true),
			HTTP:   nillable.GetBoolPtr(true),
			Ontapi: nillable.GetBoolPtr(true),
		},
	}
	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityAuditGet").Return(&response, nil)

	resp, err := ontapProvider.GetSecurityAudit()
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.HTTP)
	assert.True(t, resp.Cli)
	assert.True(t, resp.Ontapi)
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}

func TestGetSecurityAudit_GetOntapClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, fmt.Errorf("getOntapClient error")
	}

	ontapProvider := &OntapRestProvider{}

	resp, err := ontapProvider.GetSecurityAudit()
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestGetSecurityAudit_ResponseError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSecurityClient := new(ontapRest.MockSecurityClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}

	mockClient.On("Security").Return(mockSecurityClient)
	mockSecurityClient.On("SecurityAuditGet").Return(nil, fmt.Errorf("Rest Error"))

	resp, err := ontapProvider.GetSecurityAudit()
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "Rest Error", err.Error())
	mockClient.AssertExpectations(t)
	mockSecurityClient.AssertExpectations(t)
}
