package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/active_directories"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	customerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	vcpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestV1betaCreateActiveDirectory_Success(t *testing.T) {
	// Set CVP_HOST to localhost:8009 to use CVS path
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "localhost:8009"
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	mockAD := &vcpModels.ActiveDirectory{
		BaseModel: vcpModels.BaseModel{
			UUID:      "ad-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		AdName:       "ad-name",
		Username:     "user",
		Domain:       "domain",
		DNS:          "dns",
		NetBIOS:      "netbios",
		StateDetails: "details",

		ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
			SecurityOperators:          []string{"secop"},
			BackupOperators:            []string{"backupop"},
			Administrators:             []string{"admin"},
			AesEncryption:              true,
			AllowLocalNFSUsersWithLdap: true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			OrganizationalUnit:         "ou",
			Site:                       "site",
			KdcIP:                      "kdcip",
			KdcHostname:                "kdchost",
		},
	}
	mockJobID := "job-uuid"
	mockOrch := mockOrchestrator
	mockOrch.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(mockAD, mockJobID, nil)
	handler.Orchestrator = mockOrch

	req := &gcpgenserver.ActiveDirectoryV1beta{
		Username:                    "user",
		ResourceId:                  "ad-name",
		Description:                 gcpgenserver.NewOptString("desc"),
		Password:                    "pass",
		Domain:                      "domain",
		DNS:                         "dns",
		NetBIOS:                     "netbios",
		OrganizationalUnit:          gcpgenserver.NewOptString("ou"),
		Site:                        gcpgenserver.NewOptString("site"),
		KdcIP:                       gcpgenserver.NewOptString("kdcip"),
		KdcHostname:                 gcpgenserver.NewOptString("kdchost"),
		ActiveDirectoryStateDetails: gcpgenserver.NewOptString("details"),
		CreatedAt:                   gcpgenserver.NewOptDateTime(time.Now()),
		UpdatedAt:                   gcpgenserver.NewOptDateTime(time.Now()),
		DeletedAt:                   gcpgenserver.NewOptDateTime(time.Now()),
		LdapSigning:                 gcpgenserver.NewOptBool(true),
		AllowLocalNFSUsersWithLdap:  gcpgenserver.NewOptBool(true),
		EncryptDCConnections:        gcpgenserver.NewOptBool(true),
		SecurityOperators:           []string{"secop"},
		BackupOperators:             []string{"backupop"},
		Administrators:              []string{"admin"},
		AesEncryption:               gcpgenserver.NewOptBool(true),
	}
	params := gcpgenserver.V1betaCreateActiveDirectoryParams{
		ProjectNumber: "pn",
		LocationId:    "loc",
	}
	res, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)
	assert.NoError(t, err)
	op, ok := res.(*gcpgenserver.OperationV1beta)
	assert.True(t, ok)
	assert.Contains(t, op.Name.Value, "job-uuid")
	assert.False(t, op.Done.Value)
	assert.NotNil(t, op.Response)
}

func TestMergeActiveDirectoryResponses(t *testing.T) {
	t.Run("ReturnsEmptySliceWhenNoCVPAds", func(t *testing.T) {
		result := mergeActiveDirectoryResponses(nil, nil)
		assert.Empty(t, result)
	})
	t.Run("PreservesCVPDataWhenNoVCPData", func(t *testing.T) {
		cvpAds := []*models.ActiveDirectoryV1beta{
			{
				ActiveDirectoryID:    "ad-1",
				ResourceID:           nillable.GetStringPtr("resource-1"),
				Username:             nillable.GetStringPtr("user"),
				Password:             nillable.GetStringPtr("pass"),
				Domain:               nillable.GetStringPtr("example.com"),
				DNS:                  nillable.GetStringPtr("10.0.0.1"),
				NetBIOS:              nillable.GetStringPtr("EXAMPLE"),
				ActiveDirectoryState: string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
			},
		}
		merged := mergeActiveDirectoryResponses(cvpAds, nil)
		assert.Len(t, merged, 1)
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY, merged[0].ActiveDirectoryState.Value)
		assert.Equal(t, "resource-1", merged[0].ResourceId)
	})
	t.Run("PrefersVCPStateHierarchy", func(t *testing.T) {
		cvpAds := []*models.ActiveDirectoryV1beta{
			{
				ActiveDirectoryID:    "ad-1",
				ResourceID:           nillable.GetStringPtr("resource-1"),
				Username:             nillable.GetStringPtr("user"),
				Password:             nillable.GetStringPtr("pass"),
				Domain:               nillable.GetStringPtr("example.com"),
				DNS:                  nillable.GetStringPtr("10.0.0.1"),
				NetBIOS:              nillable.GetStringPtr("EXAMPLE"),
				ActiveDirectoryState: string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
			},
		}
		vcpADMap := map[string]*vcpModels.ActiveDirectory{
			"ad-1": {
				BaseModel: vcpModels.BaseModel{
					UUID:      "ad-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:                    "resource-1",
				Domain:                    "example.com",
				DNS:                       "10.0.0.1",
				NetBIOS:                   "EXAMPLE",
				State:                     "UPDATING",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{},
			},
		}

		merged := mergeActiveDirectoryResponses(cvpAds, vcpADMap)
		assert.Len(t, merged, 1)
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING, merged[0].ActiveDirectoryState.Value)
		assert.Equal(t, "resource-1", merged[0].ResourceId)
	})
}

func TestV1betaCreateActiveDirectory_OnlyRequiredFields_Success(t *testing.T) {
	// Set CVP_HOST to localhost:8009 to use CVS path
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "localhost:8009"
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	mockAD := &vcpModels.ActiveDirectory{
		BaseModel: vcpModels.BaseModel{
			UUID:      "ad-uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		AdName:       "ad-name",
		Username:     "user",
		Domain:       "domain",
		DNS:          "dns",
		NetBIOS:      "netbios",
		StateDetails: "details",

		ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{},
	}
	mockJobID := "job-uuid"
	mockOrch := mockOrchestrator
	mockOrch.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(mockAD, mockJobID, nil)
	handler.Orchestrator = mockOrch

	req := &gcpgenserver.ActiveDirectoryV1beta{
		Username:   "user",
		ResourceId: "ad-name",
		Password:   "pass",
		Domain:     "domain",
		DNS:        "dns",
		NetBIOS:    "netbios",
	}
	params := gcpgenserver.V1betaCreateActiveDirectoryParams{
		ProjectNumber: "pn",
		LocationId:    "loc",
	}
	res, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)
	assert.NoError(t, err)
	op, ok := res.(*gcpgenserver.OperationV1beta)
	assert.True(t, ok)
	assert.Contains(t, op.Name.Value, "job-uuid")
	assert.False(t, op.Done.Value)
	assert.NotNil(t, op.Response)
}

func TestV1betaCreateActiveDirectory_BadRequest(t *testing.T) {
	// Set CVP_HOST to localhost:8009 to use CVS path
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "localhost:8009"
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	mockOrchestrator.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(nil, "", customerrors.NewUserInputValidationErr("bad request"))
	handler.Orchestrator = mockOrchestrator

	req := &gcpgenserver.ActiveDirectoryV1beta{}
	params := gcpgenserver.V1betaCreateActiveDirectoryParams{}
	res, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)
	assert.NoError(t, err)
	_, ok := res.(*gcpgenserver.V1betaCreateActiveDirectoryBadRequest)
	assert.True(t, ok)
}

func TestV1betaCreateActiveDirectory_InternalServerError(t *testing.T) {
	// Set CVP_HOST to localhost:8009 to use CVS path
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "localhost:8009"
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	handler := Handler{Orchestrator: mockOrchestrator}
	mockOrchestrator.On("CreateActiveDirectory", mock.Anything, mock.Anything).Return(nil, "", errors.New("internal error"))
	handler.Orchestrator = mockOrchestrator

	req := &gcpgenserver.ActiveDirectoryV1beta{}
	params := gcpgenserver.V1betaCreateActiveDirectoryParams{}
	res, err := handler.V1betaCreateActiveDirectory(context.Background(), req, params)
	assert.NoError(t, err)
	_, ok := res.(*gcpgenserver.V1betaCreateActiveDirectoryInternalServerError)
	assert.True(t, ok)
}

func TestConvertToActiveDirectoryV1Beta(t *testing.T) {
	ad := &vcpModels.ActiveDirectory{
		BaseModel: vcpModels.BaseModel{
			UUID:      "uuid",
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		AdName:       "ad-name",
		Username:     "user",
		Domain:       "domain",
		DNS:          "dns",
		NetBIOS:      "netbios",
		StateDetails: "details",
		ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
			SecurityOperators:          []string{"secop"},
			BackupOperators:            []string{"backupop"},
			Administrators:             []string{"admin"},
			AesEncryption:              true,
			AllowLocalNFSUsersWithLdap: true,
			EncryptDCConnections:       true,
			LdapSigning:                true,
			OrganizationalUnit:         "ou",
			Site:                       "site",
			KdcIP:                      "kdcip",
			KdcHostname:                "kdchost",
		},
	}
	res := convertToActiveDirectoryV1Beta(ad)
	assert.Equal(t, "uuid", res.ActiveDirectoryId.Value)
	assert.Equal(t, "ad-name", res.ResourceId)
	assert.Equal(t, "domain", res.Domain)
	assert.Equal(t, "dns", res.DNS)
	assert.Equal(t, "netbios", res.NetBIOS)
	assert.Equal(t, "details", res.ActiveDirectoryStateDetails.Value)
	assert.Equal(t, []string{"secop"}, res.SecurityOperators)
	assert.Equal(t, []string{"backupop"}, res.BackupOperators)
	assert.Equal(t, []string{"admin"}, res.Administrators)
	assert.True(t, res.AesEncryption.Value)
	assert.True(t, res.AllowLocalNFSUsersWithLdap.Value)
	assert.True(t, res.EncryptDCConnections.Value)
	assert.True(t, res.LdapSigning.Value)
	assert.Equal(t, "ou", res.OrganizationalUnit.Value)
	assert.Equal(t, "site", res.Site.Value)
	assert.Equal(t, "kdcip", res.KdcIP.Value)
	assert.Equal(t, "kdchost", res.KdcHostname.Value)
}

func TestEncodeActiveDirectoryV1(t *testing.T) {
	ad := &gcpgenserver.ActiveDirectoryV1beta{
		ActiveDirectoryId: gcpgenserver.NewOptString("id"),
		ResourceId:        "rid",
	}
	raw, err := encodeActiveDirectoryV1(ad)
	assert.NoError(t, err)
	assert.NotNil(t, raw)
	// Should be valid JSON
	assert.True(t, json.Valid(raw))
}

func TestV1betaListActiveDirectories(t *testing.T) {
	// Set CVP_HOST to localhost:8009 to use CVS path
	originalCVPHost := cvp.CVP_HOST
	cvp.CVP_HOST = "localhost:8009"
	defer func() { cvp.CVP_HOST = originalCVPHost }()

	// Create a mock client
	mockClient := active_directories.NewMockClientService(t)

	// Define input parameters
	params := gcpgenserver.V1betaListActiveDirectoriesParams{
		LocationId:     "test-location",
		ProjectNumber:  "12345",
		XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
	}

	// Define mock response
	mockResponse := &active_directories.V1betaListActiveDirectoriesOK{
		Payload: &active_directories.V1betaListActiveDirectoriesOKBody{
			ActiveDirectories: []*models.ActiveDirectoryV1beta{
				{
					ActiveDirectoryID:           "ad-1",
					ResourceID:                  nillable.GetStringPtr("resource-1"),
					Username:                    nillable.GetStringPtr("user1"),
					Password:                    nillable.GetStringPtr("pass1"),
					Domain:                      nillable.GetStringPtr("domain1"),
					DNS:                         nillable.GetStringPtr("dns1"),
					NetBIOS:                     nillable.GetStringPtr("netbios1"),
					OrganizationalUnit:          new(string),
					Site:                        new(string),
					ActiveDirectoryState:        "ACTIVE",
					ActiveDirectoryStateDetails: "Details",
					LdapSigning:                 new(bool),
					AllowLocalNFSUsersWithLdap:  new(bool),
					EncryptDCConnections:        new(bool),
					SecurityOperators:           []string{"operator1"},
					BackupOperators:             []string{"backup1"},
					Administrators:              []string{"admin1"},
					AesEncryption:               new(bool),
				},
			},
		},
	}

	// Set up the mock client behavior
	mockClient.EXPECT().
		V1betaListActiveDirectories(mock.Anything).
		Return(mockResponse, nil)
	cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
	originalCreateClient := createClient
	defer func() { createClient = originalCreateClient }()
	createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
	mockOrchestrator.On("ListActiveDirectories", mock.Anything, params.ProjectNumber).Return([]*vcpModels.ActiveDirectory{}, nil)
	handler := Handler{Orchestrator: mockOrchestrator}
	// Call the method under test
	result, err := handler.V1betaListActiveDirectories(context.Background(), params)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result.(*gcpgenserver.V1betaListActiveDirectoriesOK).ActiveDirectories))
	assert.Equal(t, "ad-1", result.(*gcpgenserver.V1betaListActiveDirectoriesOK).ActiveDirectories[0].ActiveDirectoryId.Value)
	mockOrchestrator.AssertExpectations(t)
}

// V1betaDeleteActiveDirectory unittests
func TestV1betaDeleteActiveDirectory(t *testing.T) {
	t.Run("WhenDeleteActiveDirectorySuccess", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Define request
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock response
		mockResponse := &active_directories.V1betaDeleteActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaDeleteActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryBadRequest).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithConflict", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &active_directories.V1betaDeleteActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryConflict).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaDeleteActiveDirectoryUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnprocessableEntity).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDeleteActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaDeleteActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryForbidden).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDeleteActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDeleteActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenDeleteActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDeleteActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaDeleteActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDeleteActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDeleteActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDeleteActiveDirectoryInternalServerError).Code)
	})
}

// V1betaDescribeActiveDirectory unittests
func TestV1betaDescribeActiveDirectory(t *testing.T) {
	defaultGetActiveDirectory := func(ctx context.Context, h Handler, activeDirectoryId string) (*gcpgenserver.ActiveDirectoryV1beta, error) {
		return &gcpgenserver.ActiveDirectoryV1beta{
			ActiveDirectoryId:    gcpgenserver.NewOptString(activeDirectoryId),
			ActiveDirectoryState: gcpgenserver.NewOptActiveDirectoryV1betaActiveDirectoryState(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
		}, nil
	}
	originalGetActiveDirectory := getActiveDirectoryFromVCP
	getActiveDirectoryFromVCP = defaultGetActiveDirectory
	defer func() { getActiveDirectoryFromVCP = originalGetActiveDirectory }()

	t.Run("WhenDescribeActiveDirectorySuccess", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Define request
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock response
		dns := "10.20.2.2"
		domainName := "test-domain.com"
		netBios := "test-domain"
		userName := "test-user"
		password := "test-password"
		description := "test description"

		mockResponse := &active_directories.V1betaDescribeActiveDirectoryOK{
			Payload: &models.ActiveDirectoryV1beta{
				ActiveDirectoryID:          "ad-1",
				ResourceID:                 nillable.GetStringPtr("resource-id"),
				DNS:                        &dns,
				Domain:                     &domainName,
				NetBIOS:                    &netBios,
				Username:                   &userName,
				Password:                   &password,
				Description:                &description,
				AesEncryption:              nillable.GetBoolPtr(false),
				EncryptDCConnections:       nillable.GetBoolPtr(false),
				LdapSigning:                nillable.GetBoolPtr(false),
				AllowLocalNFSUsersWithLdap: nillable.GetBoolPtr(false),
				KdcIP:                      dns,
				KdcHostname:                "test-hostname",
				Site:                       nillable.GetStringPtr("test-site"),
				OrganizationalUnit:         nillable.GetStringPtr("test-ou"),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the resource name is as expected
		assert.Equal(t, "ad-1", result.(*gcpgenserver.ActiveDirectoryV1beta).ActiveDirectoryId.Value)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaDescribeActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryBadRequest).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaDescribeActiveDirectoryUnprocessableEntity{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnprocessableEntity).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnprocessableEntity).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDescribeActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaDescribeActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryForbidden).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaDescribeActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaDescribeActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenDescribeActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaDescribeActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError).Code)
	})

	t.Run("WhenDescribeActiveDirectoryReturnsNilPayload", func(t *testing.T) {
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockClient := active_directories.NewMockClientService(t)

		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}

		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(&active_directories.V1betaDescribeActiveDirectoryOK{}, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		handler := Handler{}
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)
		assert.NoError(t, err)
		assert.IsType(t, &gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError{}, result)
		internalErr := result.(*gcpgenserver.V1betaDescribeActiveDirectoryInternalServerError)
		assert.Equal(t, float64(500), internalErr.Code)
		assert.Equal(t, "unknown error during the describe active directory", internalErr.Message)
	})

	t.Run("WhenDescribeActiveDirectoryReturnsCVPDataWhenVCPNotFound", func(t *testing.T) {
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockClient := active_directories.NewMockClientService(t)

		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}

		dns := "10.10.0.1"
		domain := "example.com"
		netbios := "example"
		username := "user"
		password := "pass"

		mockResponse := &active_directories.V1betaDescribeActiveDirectoryOK{
			Payload: &models.ActiveDirectoryV1beta{
				ActiveDirectoryID:    "ad-1",
				ResourceID:           nillable.GetStringPtr("resource-id"),
				DNS:                  &dns,
				Domain:               &domain,
				NetBIOS:              &netbios,
				Username:             &username,
				Password:             &password,
				ActiveDirectoryState: string(gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY),
			},
		}

		mockClient.EXPECT().
			V1betaDescribeActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		getActiveDirectoryFromVCP = func(ctx context.Context, h Handler, activeDirectoryId string) (*gcpgenserver.ActiveDirectoryV1beta, error) {
			return nil, customerrors.NewNotFoundErr("ActiveDirectory", nil)
		}
		defer func() { getActiveDirectoryFromVCP = defaultGetActiveDirectory }()

		handler := Handler{}
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)
		assert.NoError(t, err)
		adResult := result.(*gcpgenserver.ActiveDirectoryV1beta)
		assert.Equal(t, "ad-1", adResult.ActiveDirectoryId.Value)
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY, adResult.ActiveDirectoryState.Value)
	})
}

// V1betaUpdateActiveDirectory unittests
func TestV1betaUpdateActiveDirectory(t *testing.T) {
	t.Run("WhenUpdateActiveDirectorySuccess", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{
			Username:                   gcpgenserver.NewOptString("user1"),
			Password:                   gcpgenserver.NewOptString("pass1"),
			Domain:                     gcpgenserver.NewOptString("domain1.com"),
			DNS:                        gcpgenserver.NewOptString("10.20.0.20"),
			NetBIOS:                    gcpgenserver.NewOptString("domain1"),
			OrganizationalUnit:         gcpgenserver.NewOptString("OU=Test,DC=domain1,DC=com"),
			Site:                       gcpgenserver.NewOptString("site.com"),
			LdapSigning:                gcpgenserver.NewOptBool(true),
			AllowLocalNFSUsersWithLdap: gcpgenserver.NewOptBool(true),
			EncryptDCConnections:       gcpgenserver.NewOptBool(true),
			BackupOperators:            []string{"backup1"},
			Administrators:             []string{"admin1"},
			SecurityOperators:          []string{"operator1"},
			AesEncryption:              gcpgenserver.NewOptBool(true),
			Description:                gcpgenserver.NewOptString("Test AD"),
			KdcIP:                      gcpgenserver.NewOptString("10.20.0.20"),
			KdcHostname:                gcpgenserver.NewOptString("KdcHostname"),
		}

		// Define mock response
		mockResponse := &active_directories.V1betaUpdateActiveDirectoryAccepted{
			Payload: &models.OperationV1beta{
				Name: "operation-id",
				Done: nillable.GetBoolPtr(true),
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the operation name is as expected
		assert.Equal(t, "operation-id", result.(*gcpgenserver.OperationV1beta).Name.Value)
		// Check if the operation done value is as expected
		assert.Equal(t, true, result.(*gcpgenserver.OperationV1beta).Done.Value)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithBadRequest", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaUpdateActiveDirectoryBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryBadRequest).Message)
	})
	t.Run("WhenUpdateActiveDirectoryFailsWithNotFound", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorCode := float64(404)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaUpdateActiveDirectoryNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryNotFound).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithConflict", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Conflict error"
		errorCode := float64(409)
		mockError := &active_directories.V1betaUpdateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnprocessableEntry", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Unprocessable error"
		errorCode := float64(422)
		mockError := &active_directories.V1betaUpdateActiveDirectoryConflict{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryConflict).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnauthorized", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaUpdateActiveDirectoryUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryUnauthorized).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithForbidden", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaUpdateActiveDirectoryForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryForbidden).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithTooManyRequests", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaUpdateActiveDirectoryTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryTooManyRequests).Message)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithDefault", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaUpdateActiveDirectoryDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Code)
	})

	t.Run("WhenUpdateActiveDirectoryFailsWithUnknownError", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaUpdateActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-1",
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryUpdateV1beta{}
		// Define mock error
		errorMessage := "unknown error during the update active directory"
		errorCode := float64(500)
		mockError := &active_directories.V1betaUpdateActiveDirectoryInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaUpdateActiveDirectory(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaUpdateActiveDirectory(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaUpdateActiveDirectoryInternalServerError).Message)
	})
}

// V1betaGetMultipleActiveDirectories unittests
func TestV1betaGetMultipleActiveDirectories(t *testing.T) {
	t.Run("WhenGetMultipleActiveDirectoriesSuccess", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		ads := make([]*models.ActiveDirectoryV1beta, 0)
		resourceID := "AD0"
		dns := "10.20.2.3"
		domainName := "domain1.com"
		netBios := "domain1"
		userName := "user1"
		password := "pass1"
		description := "Test AD"

		ads = append(ads, &models.ActiveDirectoryV1beta{
			ActiveDirectoryID: "AD0",
			ResourceID:        &resourceID,
			DNS:               &dns,
			Domain:            &domainName,
			NetBIOS:           &netBios,
			Username:          &userName,
			Password:          &password,
			Description:       &description,
		})

		// Define mock response
		mockResponse := &active_directories.V1betaGetMultipleActiveDirectoriesOK{
			Payload: &active_directories.V1betaGetMultipleActiveDirectoriesOKBody{
				ActiveDirectories: ads,
			},
		}

		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(mockResponse, nil)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		mockOrchestrator.On("GetMultipleActiveDirectories", mock.Anything, req.ActiveDirectoryUuids).Return([]*vcpModels.ActiveDirectory{
			{
				BaseModel: vcpModels.BaseModel{
					UUID:      "AD0",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:                    resourceID,
				Domain:                    domainName,
				DNS:                       dns,
				NetBIOS:                   netBios,
				State:                     "UPDATING",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{},
			},
		}, nil)
		handler := Handler{Orchestrator: mockOrchestrator}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		resp := result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesOK)
		assert.Equal(t, "AD0", resp.ActiveDirectories[0].ActiveDirectoryId.Value)
		assert.Equal(t, 1, len(resp.ActiveDirectories))
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING, resp.ActiveDirectories[0].ActiveDirectoryState.Value)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithBadRequest", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorCode := float64(400)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesBadRequest{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesBadRequest).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesBadRequest).Message)
	})
	t.Run("WhenGetMultipleActiveDirectoriesFailsWithNotFound", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorCode := float64(404)
		errorMessage := "Bad Request"
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesNotFound{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)
		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesNotFound).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesNotFound).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithUnauthorized", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Unauthorized error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesUnauthorized{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesUnauthorized).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesUnauthorized).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithForbidden", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Forbidden error"
		errorCode := float64(403)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesForbidden{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesForbidden).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesForbidden).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithTooManyRequests", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "Too many requests error"
		errorCode := float64(401)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesTooManyRequests{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesTooManyRequests).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesTooManyRequests).Message)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithDefault", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}

		// Define mock error
		errorMessage := "default error"
		errorCode := float64(500)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesDefault{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Code)
	})

	t.Run("WhenGetMultipleActiveDirectoriesFailsWithUnknownError", func(t *testing.T) {
		// Set CVP_HOST to localhost:8009 to use CVS path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = "localhost:8009"
		defer func() { cvp.CVP_HOST = originalCVPHost }()
		// Create a mock client
		mockClient := active_directories.NewMockClientService(t)

		// Define input parameters
		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}
		// Define request
		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"AD0"},
		}
		// Define mock error
		errorMessage := "unknown error during the get multiple active directories"
		errorCode := float64(500)
		mockError := &active_directories.V1betaGetMultipleActiveDirectoriesInternalServerError{
			Payload: &models.Error{
				Code:    errorCode,
				Message: errorMessage,
			},
		}
		// Set up the mock client behavior
		mockClient.EXPECT().
			V1betaGetMultipleActiveDirectories(mock.Anything).
			Return(nil, mockError)
		cvpClient := &cvpapi.Cvp{ActiveDirectories: mockClient}
		originalCreateClient := createClient
		defer func() { createClient = originalCreateClient }()
		createClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}
		handler := Handler{}
		// Call the method under test
		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		// Assertions
		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Check if the code is as expected
		assert.Equal(t, errorCode, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Code)
		assert.Equal(t, errorMessage, result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError).Message)
	})

	t.Run("WhenGetMultipleActiveDirectories_VCPPath_Success", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"ad-uuid-1", "ad-uuid-2"},
		}

		// Mock orchestrator response
		mockADs := []*vcpModels.ActiveDirectory{
			{
				BaseModel: vcpModels.BaseModel{
					UUID:      "ad-uuid-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:   "ad-name-1",
				Username: "user1",
				Password: "pass1",
				Domain:   "domain1.com",
				DNS:      "8.8.8.8",
				NetBIOS:  "NETBIOS1",
				State:    "READY",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
					SecurityOperators: []string{"sec1"},
					BackupOperators:   []string{"backup1"},
					Administrators:    []string{"admin1"},
				},
			},
			{
				BaseModel: vcpModels.BaseModel{
					UUID:      "ad-uuid-2",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:   "ad-name-2",
				Username: "user2",
				Password: "pass2",
				Domain:   "domain2.com",
				DNS:      "8.8.4.4",
				NetBIOS:  "NETBIOS2",
				State:    "CREATING",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
					SecurityOperators: []string{"sec2"},
					BackupOperators:   []string{"backup2"},
					Administrators:    []string{"admin2"},
				},
			},
		}

		mockOrchestrator.On("GetMultipleActiveDirectories", mock.Anything, req.ActiveDirectoryUuids).Return(mockADs, nil)

		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		okResult, ok := result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesOK)
		assert.True(t, ok)
		assert.Len(t, okResult.ActiveDirectories, 2)
		assert.Equal(t, "ad-uuid-1", okResult.ActiveDirectories[0].ActiveDirectoryId.Value)
		assert.Equal(t, "ad-name-1", okResult.ActiveDirectories[0].ResourceId)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("WhenGetMultipleActiveDirectories_VCPPath_OrchestratorError", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaGetMultipleActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		req := &gcpgenserver.ActiveDirectoryIdListV1beta{
			ActiveDirectoryUuids: []string{"ad-uuid-1"},
		}

		mockOrchestrator.On("GetMultipleActiveDirectories", mock.Anything, req.ActiveDirectoryUuids).Return(nil, errors.New("orchestrator error"))

		result, err := handler.V1betaGetMultipleActiveDirectories(context.Background(), req, params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		errResult, ok := result.(*gcpgenserver.V1betaGetMultipleActiveDirectoriesInternalServerError)
		assert.True(t, ok)
		assert.Equal(t, float64(500), errResult.Code)
		assert.Contains(t, errResult.Message, "orchestrator error")
		mockOrchestrator.AssertExpectations(t)
	})
}

func TestV1betaDescribeActiveDirectory_VCPPath(t *testing.T) {
	t.Run("WhenDescribeActiveDirectory_VCPPath_Success", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "ad-uuid-1",
		}

		mockAD := &vcpModels.ActiveDirectory{
			BaseModel: vcpModels.BaseModel{
				UUID:      "ad-uuid-1",
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			AdName:       "test-ad",
			Username:     "testuser",
			Password:     "testpass",
			Domain:       "example.com",
			DNS:          "8.8.8.8",
			NetBIOS:      "EXAMPLE",
			State:        "READY",
			StateDetails: "Active Directory is ready",
			ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
				OrganizationalUnit:   "OU=Test",
				Site:                 "Default-Site",
				SecurityOperators:    []string{"sec-op"},
				BackupOperators:      []string{"backup-op"},
				Administrators:       []string{"admin"},
				KdcIP:                "1.2.3.4",
				AesEncryption:        true,
				EncryptDCConnections: true,
				LdapSigning:          false,
			},
		}

		mockOrchestrator.On("GetActiveDirectory", mock.Anything, "ad-uuid-1").Return(mockAD, nil)

		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		adResult, ok := result.(*gcpgenserver.ActiveDirectoryV1beta)
		assert.True(t, ok)
		assert.Equal(t, "ad-uuid-1", adResult.ActiveDirectoryId.Value)
		assert.Equal(t, "test-ad", adResult.ResourceId)
		assert.Equal(t, "example.com", adResult.Domain)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("WhenDescribeActiveDirectory_VCPPath_NotFound", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaDescribeActiveDirectoryParams{
			LocationId:        "test-location",
			ProjectNumber:     "12345",
			XCorrelationID:    gcpgenserver.NewOptString("test-correlation-id"),
			ActiveDirectoryId: "non-existent",
		}

		originalGetActiveDirectoryFromVCP := getActiveDirectoryFromVCP
		getActiveDirectoryFromVCP = func(ctx context.Context, h Handler, activeDirectoryId string) (*gcpgenserver.ActiveDirectoryV1beta, error) {
			return nil, customerrors.NewNotFoundErr("AD", nil)
		}
		defer func() { getActiveDirectoryFromVCP = originalGetActiveDirectoryFromVCP }()
		result, err := handler.V1betaDescribeActiveDirectory(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, float64(404), result.(*gcpgenserver.V1betaDescribeActiveDirectoryNotFound).Code)
		assert.Equal(t, "AD not found", result.(*gcpgenserver.V1betaDescribeActiveDirectoryNotFound).Message)
		mockOrchestrator.AssertExpectations(t)
	})
}

func TestV1betaListActiveDirectories_VCPPath(t *testing.T) {
	t.Run("WhenListActiveDirectories_VCPPath_Success", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaListActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		mockADs := []*vcpModels.ActiveDirectory{
			{
				BaseModel: vcpModels.BaseModel{
					UUID:      "ad-1",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:   "ad-name-1",
				Username: "user1",
				Password: "pass1",
				Domain:   "domain1.com",
				DNS:      "8.8.8.8",
				NetBIOS:  "NET1",
				State:    "READY",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
					SecurityOperators: []string{"sec1"},
					BackupOperators:   []string{"backup1"},
					Administrators:    []string{"admin1"},
				},
			},
			{
				BaseModel: vcpModels.BaseModel{
					UUID:      "ad-2",
					CreatedAt: time.Now(),
					UpdatedAt: time.Now(),
				},
				AdName:   "ad-name-2",
				Username: "user2",
				Password: "pass2",
				Domain:   "domain2.com",
				DNS:      "8.8.4.4",
				NetBIOS:  "NET2",
				State:    "CREATING",
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
					SecurityOperators: []string{"sec2"},
					BackupOperators:   []string{"backup2"},
					Administrators:    []string{"admin2"},
				},
			},
		}

		mockOrchestrator.On("ListActiveDirectories", mock.Anything, "12345").Return(mockADs, nil)

		result, err := handler.V1betaListActiveDirectories(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		okResult, ok := result.(*gcpgenserver.V1betaListActiveDirectoriesOK)
		assert.True(t, ok)
		assert.Len(t, okResult.ActiveDirectories, 2)
		assert.Equal(t, "ad-1", okResult.ActiveDirectories[0].ActiveDirectoryId.Value)
		assert.Equal(t, "ad-name-1", okResult.ActiveDirectories[0].ResourceId)
		assert.Equal(t, "ad-2", okResult.ActiveDirectories[1].ActiveDirectoryId.Value)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("WhenListActiveDirectories_VCPPath_EmptyList", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaListActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		mockOrchestrator.On("ListActiveDirectories", mock.Anything, "12345").Return([]*vcpModels.ActiveDirectory{}, nil)

		result, err := handler.V1betaListActiveDirectories(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		okResult, ok := result.(*gcpgenserver.V1betaListActiveDirectoriesOK)
		assert.True(t, ok)
		assert.Len(t, okResult.ActiveDirectories, 0)
		mockOrchestrator.AssertExpectations(t)
	})

	t.Run("WhenListActiveDirectories_VCPPath_OrchestratorError", func(t *testing.T) {
		// Set CVP_HOST to empty to use VCP path
		originalCVPHost := cvp.CVP_HOST
		cvp.CVP_HOST = ""
		defer func() { cvp.CVP_HOST = originalCVPHost }()

		mockOrchestrator := orchestrator.NewMockOrchestratorFactory(t)
		handler := Handler{Orchestrator: mockOrchestrator}

		params := gcpgenserver.V1betaListActiveDirectoriesParams{
			LocationId:     "test-location",
			ProjectNumber:  "12345",
			XCorrelationID: gcpgenserver.NewOptString("test-correlation-id"),
		}

		mockOrchestrator.On("ListActiveDirectories", mock.Anything, "12345").Return(nil, errors.New("orchestrator error"))

		result, err := handler.V1betaListActiveDirectories(context.Background(), params)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		errResult, ok := result.(*gcpgenserver.V1betaListActiveDirectoriesInternalServerError)
		assert.True(t, ok)
		assert.Equal(t, float64(500), errResult.Code)
		assert.Contains(t, errResult.Message, "orchestrator error")
		mockOrchestrator.AssertExpectations(t)
	})
}

func TestConvertOrchestratorActiveDirectoryToV1Beta(t *testing.T) {
	t.Run("WhenConvertingWithAllFields", func(t *testing.T) {
		now := time.Now()
		deletedAt := time.Now().Add(time.Hour)
		ad := &vcpModels.ActiveDirectory{
			BaseModel: vcpModels.BaseModel{
				UUID:      "test-uuid",
				CreatedAt: now,
				UpdatedAt: now,
				DeletedAt: &deletedAt,
			},
			AdName:       "test-ad",
			Username:     "testuser",
			Password:     "testpass",
			Domain:       "example.com",
			DNS:          "8.8.8.8",
			NetBIOS:      "EXAMPLE",
			State:        "READY",
			StateDetails: "Ready",
			ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{
				SecurityOperators: []string{"sec1", "sec2"},
				BackupOperators:   []string{"backup1"},
				Administrators:    []string{"admin1", "admin2"},
			},
		}

		result := convertOrchestratorActiveDirectoryToV1Beta(ad)

		assert.Equal(t, "test-uuid", result.ActiveDirectoryId.Value)
		assert.Equal(t, "test-ad", result.ResourceId)
		assert.Equal(t, "example.com", result.Domain)
		assert.Equal(t, "8.8.8.8", result.DNS)
		assert.Equal(t, "EXAMPLE", result.NetBIOS)
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY, result.ActiveDirectoryState.Value)
		assert.Equal(t, "Ready", result.ActiveDirectoryStateDetails.Value)
		assert.Equal(t, []string{"sec1", "sec2"}, result.SecurityOperators)
		assert.Equal(t, []string{"backup1"}, result.BackupOperators)
		assert.Equal(t, []string{"admin1", "admin2"}, result.Administrators)
		assert.True(t, result.DeletedAt.IsSet())
	})

	t.Run("WhenConvertingWithNilAttributes", func(t *testing.T) {
		ad := &vcpModels.ActiveDirectory{
			BaseModel: vcpModels.BaseModel{
				UUID: "test-uuid",
			},
			AdName:                    "test-ad",
			State:                     "CREATING",
			ActiveDirectoryAttributes: nil,
		}

		result := convertOrchestratorActiveDirectoryToV1Beta(ad)

		assert.Equal(t, "test-uuid", result.ActiveDirectoryId.Value)
		assert.Equal(t, "test-ad", result.ResourceId)
		assert.Equal(t, gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateCREATING, result.ActiveDirectoryState.Value)
		assert.Empty(t, result.SecurityOperators)
		assert.Empty(t, result.BackupOperators)
		assert.Empty(t, result.Administrators)
	})

	t.Run("WhenConvertingDifferentStates", func(t *testing.T) {
		stateTests := []struct {
			inputState    string
			expectedState gcpgenserver.ActiveDirectoryV1betaActiveDirectoryState
		}{
			{"CREATING", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateCREATING},
			{"READY", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY},
			{"UPDATING", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateUPDATING},
			{"IN_USE", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateINUSE},
			{"DELETING", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateDELETING},
			{"ERROR", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateERROR},
			{"UNKNOWN", gcpgenserver.ActiveDirectoryV1betaActiveDirectoryStateREADY}, // Default
		}

		for _, tt := range stateTests {
			ad := &vcpModels.ActiveDirectory{
				BaseModel:                 vcpModels.BaseModel{UUID: "test"},
				AdName:                    "test",
				State:                     tt.inputState,
				ActiveDirectoryAttributes: &vcpModels.ActiveDirectoryAttributes{},
			}

			result := convertOrchestratorActiveDirectoryToV1Beta(ad)
			assert.Equal(t, tt.expectedState, result.ActiveDirectoryState.Value, "Failed for state: %s", tt.inputState)
		}
	})
}
