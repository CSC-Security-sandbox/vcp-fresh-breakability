package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
)

func strPtr(s string) *string {
	return &s
}

func TestCloudTargetCreateSucceeds(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockCloudClient := new(ontapRest.MockCloudClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.CloudTargetCreateParams{
		Name:      strPtr("targetName"),
		Container: strPtr("containerName"),
	}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}
	ct := models.CloudTarget{
		Name:      strPtr("targetName"),
		Container: strPtr("container"),
	}
	expectedCloudTarget := &ontapRest.CloudTarget{CloudTarget: ct}

	// Mocking the behavior of the Cloud and Poll methods
	mockClient.On("Cloud").Return(mockCloudClient)
	mockCloudClient.On("CloudTargetCreate", expectedParams).Return(nil, expectedJob, nil)
	mockCloudClient.On("CloudTargetGet", strPtr("targetName")).Return(expectedCloudTarget, nil)
	mockClient.On("Poll", "jobUUID").Return(nil) // Mocking Poll to return no error

	result, err := ontapProvider.CloudTargetCreate("targetName", "containerName")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "targetName", *result.Name)
	assert.Equal(t, "container", *result.Container)
}

func TestCloudTargetCreateFailsOnAPIError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockCloudClient := new(ontapRest.MockCloudClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}

	expectedParams := &ontapRest.CloudTargetCreateParams{
		Name:      strPtr("targetName"),
		Container: strPtr("containerName"),
	}
	expectedError := fmt.Errorf("API error")

	mockClient.On("Cloud").Return(mockCloudClient)
	mockCloudClient.On("CloudTargetCreate", expectedParams).Return(nil, nil, expectedError)

	result, err := ontapProvider.CloudTargetCreate("targetName", "containerName")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
}

func TestCloudTargetGetReturnsValidTarget(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockCloudClient := new(ontapRest.MockCloudClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}

	expectedCloudTarget := &ontapRest.CloudTarget{
		CloudTarget: models.CloudTarget{
			Name:      strPtr("targetName"),
			Container: strPtr("container"),
		},
	}

	mockClient.On("Cloud").Return(mockCloudClient)
	mockCloudClient.On("CloudTargetGet", strPtr("targetName")).Return(expectedCloudTarget, nil)

	result, err := ontapProvider.CloudTargetGet(strPtr("targetName"))
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "targetName", *result.Name)
	assert.Equal(t, "container", *result.Container)
}

func TestCloudTargetGetFailsOnInvalidName(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockCloudClient := new(ontapRest.MockCloudClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}

	expectedError := fmt.Errorf("invalid CloudTarget response from API")

	mockClient.On("Cloud").Return(mockCloudClient)
	mockCloudClient.On("CloudTargetGet", strPtr("invalidName")).Return(nil, expectedError)

	result, err := ontapProvider.CloudTargetGet(strPtr("invalidName"))
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
}
