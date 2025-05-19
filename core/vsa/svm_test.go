package vsa

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateSVM(t *testing.T) {
	params := CreateSvmParams{
		Name: "testSVM",
		Protocols: Protocols{
			EnableIscsi: true,
		},
	}

	t.Run("WhenSVMCreationSucceeds_ThenReturnSVMDetails", func(tt *testing.T) {
		mockSVM := new(ontaprest.MockSVMClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SVM").Return(mockSVM)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockSvm := &ontaprest.Svm{
			Svm: models.Svm{
				Name: nillable.ToPointer("testSVM"),
				UUID: nillable.ToPointer("testUUID"),
			},
		}

		mockSVM.On("SvmCreate", mock.Anything).Return(mockSvm, nil, nil)

		resp, err := rc.CreateSVM(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "testSVM", resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockSVM.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSVMCreationFails_ThenReturnError", func(tt *testing.T) {
		mockSVM := new(ontaprest.MockSVMClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SVM").Return(mockSVM)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockSVM.On("SvmCreate", mock.Anything).Return(nil, nil, errors.New("creation error"))

		resp, err := rc.CreateSVM(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)

		mockSVM.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenSVMResponseIsInvalid_ThenReturnError", func(tt *testing.T) {
		mockSVM := new(ontaprest.MockSVMClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SVM").Return(mockSVM)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockSVM.On("SvmCreate", mock.Anything).Return(nil, nil, nil)

		resp, err := rc.CreateSVM(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, "invalid SVM response from API", err.Error())

		mockSVM.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestCreateSVM_PollJob(t *testing.T) {
	params := CreateSvmParams{
		Name: "testSVM",
		Protocols: Protocols{
			EnableIscsi: true,
		},
	}

	t.Run("WhenJobPollingSucceeds_ThenReturnSVMDetails", func(tt *testing.T) {
		mockSVM := new(ontaprest.MockSVMClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SVM").Return(mockSVM)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockAcceptedJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}

		mockSvm := &ontaprest.Svm{
			Svm: models.Svm{
				Name: nillable.ToPointer("testSVM"),
				UUID: nillable.ToPointer("testUUID"),
			},
		}

		mockSVM.On("SvmCreate", mock.Anything).Return(mockSvm, mockAcceptedJob, nil)
		mockClient.On("Poll", "testJobUUID").Return(nil)

		resp, err := rc.CreateSVM(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "testSVM", resp.Name)
		assert.Equal(tt, "testUUID", resp.ExternalUUID)

		mockSVM.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenJobPollingFails_ThenReturnError", func(tt *testing.T) {
		mockSVM := new(ontaprest.MockSVMClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("SVM").Return(mockSVM)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		mockAcceptedJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}

		mockSVM.On("SvmCreate", mock.Anything).Return(nil, mockAcceptedJob, nil)
		mockClient.On("Poll", "testJobUUID").Return(fmt.Errorf("polling error"))

		resp, err := rc.CreateSVM(params)

		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, "polling error", err.Error())

		mockSVM.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}
