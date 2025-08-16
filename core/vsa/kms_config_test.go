package vsa

import (
	"errors"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateKmsConfig(t *testing.T) {
	t.Run("CreateKmsConfigReturnsResponseOnSuccess", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		expectedUUID := "external-uuid"
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		response := []*ontaprest.GcpKms{{}}
		response[0].UUID = nillable.ToPointer(expectedUUID)
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsCreate", mock.Anything).Return(response, nil)
		provider := &OntapRestProvider{}
		params := CreateKmsConfigParams{
			KeyName:           "key",
			KeyRingLocation:   "us",
			KeyRingName:       "ring",
			ProjectID:         "project",
			Credentials:       nillable.ToPointer(strfmt.Password("credentials")),
			SvmName:           "svm",
			PrivilegedAccount: "svc@project.iam.gserviceaccount.com",
		}
		resp, err := provider.CreateKmsConfig(params)
		assert.NoError(t, err)
		assert.Equal(t, expectedUUID, resp.ExternalUUID)
	})
	t.Run("CreateKmsConfigReturnsResponseOnFailure", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsCreate", mock.Anything).Return(nil, errors.New("failed to create KMS config"))
		provider := &OntapRestProvider{}
		params := CreateKmsConfigParams{
			KeyName:           "key",
			KeyRingLocation:   "us",
			KeyRingName:       "ring",
			ProjectID:         "project",
			Credentials:       nillable.ToPointer(strfmt.Password("credentials")),
			SvmName:           "svm",
			PrivilegedAccount: "svc@project.iam.gserviceaccount.com",
		}
		resp, err := provider.CreateKmsConfig(params)
		assert.Error(t, err)
		assert.Nil(t, resp)
	})
}

func TestIsGcpKmsReachable(t *testing.T) {
	t.Run("IsGcpKmsReachableReturnsTrueWhenReachable", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		reachability := &models.GcpKmsInlineGoogleReachability{
			Reachable: nillable.ToPointer(true),
			Message:   nillable.ToPointer("ok"),
		}
		gcpKmsResponse := &ontaprest.GcpKms{}
		gcpKmsResponse.GoogleReachability = reachability
		mockSecurity.On("GcpKmsGet", mock.Anything).Return(gcpKmsResponse, nil)
		provider := &OntapRestProvider{}
		params := GetKmsConfigParams{ExternalKmsConfigID: "uuid"}
		result, err := provider.IsGcpKmsReachable(params)
		assert.NoError(t, err)
		assert.True(t, result)
	})
	t.Run("IsGcpKmsReachableReturnsFalseWhenNotReachable", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		reachability := &models.GcpKmsInlineGoogleReachability{
			Reachable: nillable.ToPointer(false),
			Message:   nillable.ToPointer("unreachable"),
		}
		gcpKmsResponse := &ontaprest.GcpKms{}
		gcpKmsResponse.GoogleReachability = reachability
		mockSecurity.On("GcpKmsGet", mock.Anything).Return(gcpKmsResponse, nil)
		provider := &OntapRestProvider{}
		params := GetKmsConfigParams{ExternalKmsConfigID: "uuid"}
		result, err := provider.IsGcpKmsReachable(params)
		assert.NoError(t, err)
		assert.False(t, result)
	})
	t.Run("IsGcpKmsReachableReturnsPermissionDeniedError", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		reachability := &models.GcpKmsInlineGoogleReachability{
			Reachable: nillable.ToPointer(false),
			Message:   nillable.ToPointer("PERMISSION_DENIED: access denied"),
		}
		inlineEkmip := &models.GcpKmsInlineEkmipReachabilityInlineArrayItem{
			Reachable: nillable.ToPointer(false),
		}
		inline := []*models.GcpKmsInlineEkmipReachabilityInlineArrayItem{inlineEkmip}
		gcpKmsResponse := &ontaprest.GcpKms{}
		gcpKmsResponse.GoogleReachability = reachability
		gcpKmsResponse.GcpKmsInlineEkmipReachability = inline

		mockSecurity.On("GcpKmsGet", mock.Anything).Return(gcpKmsResponse, nil)
		provider := &OntapRestProvider{}
		params := GetKmsConfigParams{ExternalKmsConfigID: "uuid"}
		result, err := provider.IsGcpKmsReachable(params)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "permission_denied")
		assert.False(t, result)
	})
	t.Run("IsGcpKmsReachableReturnsFalseWhenInlineEkmipNotReachable", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)

		reachability := &models.GcpKmsInlineGoogleReachability{
			Reachable: nillable.ToPointer(true),
			Message:   nillable.ToPointer("ok"),
		}
		inlineEkmip := &models.GcpKmsInlineEkmipReachabilityInlineArrayItem{
			Reachable: nillable.ToPointer(false),
		}
		inline := []*models.GcpKmsInlineEkmipReachabilityInlineArrayItem{inlineEkmip}
		gcpKmsResponse := &ontaprest.GcpKms{}
		gcpKmsResponse.GoogleReachability = reachability
		gcpKmsResponse.GcpKmsInlineEkmipReachability = inline
		mockSecurity.On("GcpKmsGet", mock.Anything).Return(gcpKmsResponse, nil)
		provider := &OntapRestProvider{}
		params := GetKmsConfigParams{ExternalKmsConfigID: "uuid"}
		result, err := provider.IsGcpKmsReachable(params)
		assert.NoError(t, err)
		assert.False(t, result)
	})
	t.Run("IsGcpKmsReachableReturnsErrorOnClientFailure", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsGet", mock.Anything).Return(nil, errors.New("client failure"))
		provider := &OntapRestProvider{}
		params := GetKmsConfigParams{ExternalKmsConfigID: "uuid"}
		result, err := provider.IsGcpKmsReachable(params)
		assert.Error(t, err)
		assert.False(t, result)
	})
}

func TestDeleteEkmConfig(t *testing.T) {
	t.Run("WhenGetOntapClientFuncReturnsError", func(t *testing.T) {
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("get ontap client error")
		}
		defer func() { getOntapClientFunc = origGetClient }()
		provider := &OntapRestProvider{}
		params := DeleteKmsConfigParams{}
		err := provider.DeleteEkmConfig(params)
		assert.Error(t, err)
		assert.Errorf(t, err, "get ontap client error")
	})
	t.Run("WhenSecurityGcpKmsDeleteReturnsError", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("get ontap client error")
		}
		defer func() { getOntapClientFunc = origGetClient }()
		provider := &OntapRestProvider{}
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsDelete", mock.Anything).Return(errors.New("ekm delete failed"))
		params := DeleteKmsConfigParams{ExternalKmsConfigID: "uuid1"}

		err := provider.DeleteEkmConfig(params)
		assert.Error(t, err)
		assert.Errorf(t, err, "ekm delete failed")
	})
	t.Run("WhenDeleteEkmConfigIsSuccessful", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()
		provider := &OntapRestProvider{}
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsDelete", mock.Anything).Return(nil)
		params := DeleteKmsConfigParams{ExternalKmsConfigID: "uuid1"}

		err := provider.DeleteEkmConfig(params)
		assert.NoError(t, err)
		assert.Nil(t, err)
	})
}

func TestModifyGcpKms(t *testing.T) {
	t.Run("ModifyGcpKmsReturnsGcpKmsOnSuccess", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		expectedUUID := "modified-uuid"
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()

		gcpKmsResponse := &ontaprest.GcpKms{
			GcpKms: models.GcpKms{
				UUID: nillable.ToPointer(expectedUUID),
			},
		}
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsModify", mock.Anything).Return(gcpKmsResponse, nil, nil)

		provider := &OntapRestProvider{}
		credentials := (*log.Secret)(nillable.ToPointer(strfmt.Password("new-credentials")))
		result, jobUUID, err := provider.ModifyGcpKms("external-uuid", credentials)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedUUID, *result.UUID)
		assert.Nil(t, jobUUID)
	})

	t.Run("ModifyGcpKmsReturnsJobUUIDWhenAsync", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		expectedJobUUID := "job-123"
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()

		job := &ontaprest.JobAccepted{
			JobUUID: expectedJobUUID,
		}
		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsModify", mock.Anything).Return(nil, job, nil)

		provider := &OntapRestProvider{}
		credentials := (*log.Secret)(nillable.ToPointer(strfmt.Password("new-credentials")))
		result, jobUUID, err := provider.ModifyGcpKms("external-uuid", credentials)

		assert.NoError(t, err)
		assert.Nil(t, result)
		assert.NotNil(t, jobUUID)
		assert.Equal(t, expectedJobUUID, *jobUUID)
	})

	t.Run("ModifyGcpKmsReturnsErrorOnFailure", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockSecurity := new(ontaprest.MockSecurityClient)
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		defer func() { getOntapClientFunc = origGetClient }()

		mockClient.On("Security").Return(mockSecurity)
		mockSecurity.On("GcpKmsModify", mock.Anything).Return(nil, nil, errors.New("modify failed"))

		provider := &OntapRestProvider{}
		credentials := (*log.Secret)(nillable.ToPointer(strfmt.Password("new-credentials")))
		result, jobUUID, err := provider.ModifyGcpKms("external-uuid", credentials)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Failed to establish connectivity with the cloud key management service while updating GCP KMS")
		assert.Nil(t, result)
		assert.Nil(t, jobUUID)
	})

	t.Run("ModifyGcpKmsReturnsErrorOnClientCreationFailure", func(t *testing.T) {
		origGetClient := getOntapClientFunc
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}
		defer func() { getOntapClientFunc = origGetClient }()

		provider := &OntapRestProvider{}
		credentials := (*log.Secret)(nillable.ToPointer(strfmt.Password("new-credentials")))
		result, jobUUID, err := provider.ModifyGcpKms("external-uuid", credentials)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "client creation failed")
		assert.Nil(t, result)
		assert.Nil(t, jobUUID)
	})
}
