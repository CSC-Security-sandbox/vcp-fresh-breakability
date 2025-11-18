package vsa

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	utilsErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestSnapmirrorRelationshipTransferCreateWithFiles_Success(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "snapmirror-uuid"
	snapshotName := "snapshot-name"
	smcToken := nillable.ToPointer("smc-token")
	files := []*commonparams.SnapmirrorTransferFile{
		{SourcePath: "/source/file1.txt", DestinationPath: "/dest/file1.txt"},
		{SourcePath: "/source/file2.txt", DestinationPath: "/dest/file2.txt"},
	}

	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:             snapmirrorUUID,
		SnapshotName:     snapshotName,
		AccessToken:      smcToken,
		Files:            files,
		CleanUpOnFailure: true,
	}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(nil)

	err := ontapProvider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, smcToken, files)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSnapmirrorClient.AssertExpectations(t)
}

func TestSnapmirrorRelationshipTransferCreateWithFiles_GetClientError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, errors.New("failed to get ontap client")
	}

	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "snapmirror-uuid"
	snapshotName := "snapshot-name"
	smcToken := nillable.ToPointer("smc-token")
	files := []*commonparams.SnapmirrorTransferFile{
		{SourcePath: "/source/file1.txt", DestinationPath: "/dest/file1.txt"},
	}

	err := ontapProvider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, smcToken, files)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ontap client")
}

func TestSnapmirrorRelationshipTransferCreateWithFiles_TransferCreateError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "snapmirror-uuid"
	snapshotName := "snapshot-name"
	smcToken := nillable.ToPointer("smc-token")
	files := []*commonparams.SnapmirrorTransferFile{
		{SourcePath: "/source/file1.txt", DestinationPath: "/dest/file1.txt"},
	}

	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:             snapmirrorUUID,
		SnapshotName:     snapshotName,
		AccessToken:      smcToken,
		Files:            files,
		CleanUpOnFailure: true,
	}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(errors.New("transfer create failed"))

	err := ontapProvider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, smcToken, files)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transfer create failed")
	mockClient.AssertExpectations(t)
	mockSnapmirrorClient.AssertExpectations(t)
}

func TestSnapmirrorRelationshipTransferCreateWithFiles_WithNilToken(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "snapmirror-uuid"
	snapshotName := "snapshot-name"
	var smcToken *string = nil
	files := []*commonparams.SnapmirrorTransferFile{
		{SourcePath: "/source/file1.txt", DestinationPath: "/dest/file1.txt"},
	}

	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:             snapmirrorUUID,
		SnapshotName:     snapshotName,
		AccessToken:      nil,
		Files:            files,
		CleanUpOnFailure: true,
	}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(nil)

	err := ontapProvider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, smcToken, files)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSnapmirrorClient.AssertExpectations(t)
}

func TestSnapmirrorRelationshipTransferCreateWithFiles_WithEmptyFiles(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}

	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "snapmirror-uuid"
	snapshotName := "snapshot-name"
	smcToken := nillable.ToPointer("smc-token")
	files := []*commonparams.SnapmirrorTransferFile{}

	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:             snapmirrorUUID,
		SnapshotName:     snapshotName,
		AccessToken:      smcToken,
		Files:            files,
		CleanUpOnFailure: true,
	}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(nil)

	err := ontapProvider.SnapmirrorRelationshipTransferCreateWithFiles(snapmirrorUUID, snapshotName, smcToken, files)
	assert.NoError(t, err)
	mockClient.AssertExpectations(t)
	mockSnapmirrorClient.AssertExpectations(t)
}

func TestSnapmirrorRelationshipCreateSucceeds(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		AccessToken:     nil,
	}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}
	expectedSnapmirror := []*ontapRest.SnapmirrorRelationship{{SnapmirrorRelationship: models.SnapmirrorRelationship{}}}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, expectedJob, nil)
	mockClient.On("Poll", "jobUUID").Return(nil)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontapRest.SnapmirrorRelationshipListParams{DestinationPath: "destinationPath", SourcePath: "sourcePath"}).Return(expectedSnapmirror, nil)

	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SourceUUID:      nil,
		IsRestore:       false,
	}
	result, err := ontapProvider.SnapmirrorRelationshipCreate(SnapmirrorRelationshipParams, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSnapmirrorRelationshipCreateNotFound(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		AccessToken:     nil,
	}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}
	var expectedSnapmirror []*ontapRest.SnapmirrorRelationship

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, expectedJob, nil)
	mockClient.On("Poll", "jobUUID").Return(nil)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontapRest.SnapmirrorRelationshipListParams{DestinationPath: "destinationPath", SourcePath: "sourcePath"}).Return(expectedSnapmirror, nil)

	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SourceUUID:      nil,
		IsRestore:       false,
	}
	result, err := ontapProvider.SnapmirrorRelationshipCreate(SnapmirrorRelationshipParams, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSnapmirrorRelationshipCreateFailsOngetOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, errors.New("getOntapClient error")
	}
	ontapProvider := &OntapRestProvider{}
	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SourceUUID:      nil,
		IsRestore:       false,
	}
	result, err := ontapProvider.SnapmirrorRelationshipCreate(SnapmirrorRelationshipParams, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestSnapmirrorRelationshipCreateFailsOnAPIError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		AccessToken:     nil,
	}
	expectedError := fmt.Errorf("API error")

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, nil, expectedError)
	SnapmirrorRelationshipParams := &commonparams.SnapmirrorRelationshipParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SourceUUID:      nil,
		IsRestore:       false,
	}
	result, err := ontapProvider.SnapmirrorRelationshipCreate(SnapmirrorRelationshipParams, nil)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
}

func TestSnapmirrorRelationshipDeleteSucceeds(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipDeleteParams{UUID: "snapmirrorUUID"}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", expectedParams).Return(true, expectedJob, nil)
	mockClient.On("Poll", "jobUUID").Return(nil)

	_, err := ontapProvider.SnapmirrorRelationshipDelete("snapmirrorUUID")
	assert.NoError(t, err)
}

func TestSnapmirrorRelationshipDeleteFailsOnJobError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipDeleteParams{UUID: "snapmirrorUUID"}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", expectedParams).Return(false, nil, errors.New("failed"))

	_, err := ontapProvider.SnapmirrorRelationshipDelete("snapmirrorUUID")
	assert.Error(t, err)
}

func TestSnapmirrorRelationshipDeleteFailsOnGetOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, errors.New("getOntapClient error")
	}
	ontapProvider := &OntapRestProvider{}
	result, err := ontapProvider.SnapmirrorRelationshipDelete("snapmirrorUUID")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestSnapmirrorRelationshipGetSuccess(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}

	expectedSnapmirror := []*ontapRest.SnapmirrorRelationship{
		{SnapmirrorRelationship: models.SnapmirrorRelationship{UUID: nillable.ToPointer(strfmt.UUID("4ea7a442-86d1-11e0-ae1c-123478563412"))}},
	}
	destinationPath := "dest"
	sourcePath := "src"
	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList",
		&ontapRest.SnapmirrorRelationshipListParams{DestinationPath: destinationPath, SourcePath: sourcePath},
	).Return(expectedSnapmirror, nil)

	result, err := ontapProvider.SnapmirrorRelationshipGet(destinationPath, sourcePath)
	assert.NoError(t, err)
	assert.Equal(t, expectedSnapmirror[0], result)
}

func TestSnapmirrorRelationshipGetNotFound(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	destinationPath := "dest"
	sourcePath := "src"
	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList",
		&ontapRest.SnapmirrorRelationshipListParams{DestinationPath: destinationPath, SourcePath: sourcePath},
	).Return([]*ontapRest.SnapmirrorRelationship{}, nil)

	result, err := ontapProvider.SnapmirrorRelationshipGet(destinationPath, sourcePath)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.True(t, utilsErrors.IsNotFoundErr(err))
	assert.Contains(t, err.Error(), "snapmirror relationship not found")
}

func TestSnapmirrorRelationshipGetAPIError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}

	destinationPath := "dest"
	sourcePath := "src"
	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList",
		&ontapRest.SnapmirrorRelationshipListParams{DestinationPath: destinationPath, SourcePath: sourcePath},
	).Return(nil, errors.New("api error"))

	result, err := ontapProvider.SnapmirrorRelationshipGet(destinationPath, sourcePath)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.EqualError(t, err, "api error")
}

func TestSnapmirrorRelationshipGetFailsOnGetOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return nil, errors.New("getOntapClient error")
	}
	ontapProvider := &OntapRestProvider{}
	result, err := ontapProvider.SnapmirrorRelationshipGet("dest", "src")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, "getOntapClient error", err.Error())
}

func TestSnapmirrorRelationshipTransferCreate(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "test-uuid"
	snapshotName := "test-snapshot"
	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:         snapmirrorUUID,
		SnapshotName: snapshotName,
		AccessToken:  nil,
	}

	t.Run("success", func(t *testing.T) {
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(nil)

		err := ontapProvider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName, nil)
		assert.NoError(t, err)
	})

	t.Run("api error", func(t *testing.T) {
		mockClient = new(ontapRest.MockRESTClient)
		mockSnapmirrorClient = new(ontapRest.MockSnapmirrorClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(fmt.Errorf("api error"))

		err := ontapProvider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName, nil)
		assert.Error(t, err)
		assert.EqualError(t, err, "api error")
	})

	t.Run("getOntapClientFunc error", func(t *testing.T) {
		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("getOntapClient error")
		}
		err := ontapProvider.SnapmirrorRelationshipTransferCreate("snapmirrorUUID", "snapshotName", nil)
		assert.Error(t, err)
		assert.Equal(t, "getOntapClient error", err.Error())
	})
}
func TestSnapmirrorRelationshipTransferGet(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "test-uuid"
	snapshotName := "test-snapshot"
	expectedParams := &ontapRest.SnapmirrorRelationshipTransferGetParams{
		SnapmirrorUUID: snapmirrorUUID,
		SnapshotName:   snapshotName,
	}
	expectedTransfer := &ontapRest.SnapmirrorTransfer{}

	t.Run("success", func(t *testing.T) {
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferGet", expectedParams).Return(expectedTransfer, nil)

		result, err := ontapProvider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
		assert.NoError(t, err)
		assert.Equal(t, expectedTransfer, result)
	})

	t.Run("api error", func(t *testing.T) {
		mockClient = new(ontapRest.MockRESTClient)
		mockSnapmirrorClient = new(ontapRest.MockSnapmirrorClient)
		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferGet", expectedParams).Return(nil, fmt.Errorf("api error"))

		result, err := ontapProvider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, "api error")
	})

	t.Run("getOntapClientFunc error", func(t *testing.T) {
		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("getOntapClient error")
		}
		result, err := ontapProvider.SnapmirrorRelationshipTransferGet("uuid", "host")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, "getOntapClient error", err.Error())
	})
}

func TestSnapmirrorObjectStoreEndpointDelete(t *testing.T) {
	t.Run("OnSuccessWithJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		expectedParams := &ontapRest.SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
		}

		jobResponse := &ontapRest.JobAccepted{JobUUID: "jobUUID"}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreEndpointDelete", expectedParams).Return(jobResponse, nil)

		job, err := ontapProvider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, endpointUUID)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, "jobUUID", job.JobUUID)
	})
	t.Run("OnNotError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		expectedParams := &ontapRest.SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreEndpointDelete", expectedParams).Return(nil, utilsErrors.NewNotFoundErr("Snapmirror endpoint not found", nil))

		job, err := ontapProvider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, endpointUUID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
	t.Run("OnSuccessWithoutJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		expectedParams := &ontapRest.SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreEndpointDelete", expectedParams).Return(nil, nil)

		job, err := ontapProvider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, endpointUUID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
	t.Run("OnError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		expectedParams := &ontapRest.SnapmirrorCloudEndpointDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreEndpointDelete", expectedParams).Return(nil, fmt.Errorf("api error"))

		job, err := ontapProvider.SnapmirrorObjectStoreEndpointDelete(objectStoreUUID, endpointUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "api error")
	})
}

func TestSnapmirrorObjectStoreSnapshotDelete(t *testing.T) {
	t.Run("OnSuccessWithJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		snapshotUUID := "snapshotUUID"
		expectedParams := &ontapRest.SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
			SnapshotUUID:    snapshotUUID,
		}

		jobResponse := &ontapRest.JobAccepted{JobUUID: "jobUUID"}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotDelete", expectedParams).Return(jobResponse, nil)

		job, err := ontapProvider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, endpointUUID, snapshotUUID)
		assert.NoError(t, err)
		assert.NotNil(t, job)
		assert.Equal(t, "jobUUID", job.JobUUID)
	})
	t.Run("OnNotFoundError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		snapshotUUID := "snapshotUUID"
		expectedParams := &ontapRest.SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
			SnapshotUUID:    snapshotUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotDelete", expectedParams).Return(nil, utilsErrors.NewNotFoundErr("Snapshot not found", nil))

		job, err := ontapProvider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, endpointUUID, snapshotUUID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
	t.Run("OnSuccessWithoutJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		snapshotUUID := "snapshotUUID"
		expectedParams := &ontapRest.SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
			SnapshotUUID:    snapshotUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotDelete", expectedParams).Return(nil, nil)

		job, err := ontapProvider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, endpointUUID, snapshotUUID)
		assert.NoError(t, err)
		assert.Nil(t, job)
	})
	t.Run("OnError", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}
		ontapProvider := &OntapRestProvider{}
		objectStoreUUID := "objectStoreUUID"
		endpointUUID := "endpointUUID"
		snapshotUUID := "snapshotUUID"
		expectedParams := &ontapRest.SnapmirrorCloudSnapshotDeleteParams{
			ObjectStoreUUID: objectStoreUUID,
			EndpointUUID:    endpointUUID,
			SnapshotUUID:    snapshotUUID,
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotDelete", expectedParams).Return(nil, fmt.Errorf("api error"))

		job, err := ontapProvider.SnapmirrorObjectStoreSnapshotDelete(objectStoreUUID, endpointUUID, snapshotUUID)
		assert.Error(t, err)
		assert.Nil(t, job)
		assert.EqualError(t, err, "api error")
	})
}

func TestSnapmirrorObjectStoreSnapshotGet(t *testing.T) {
	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		ontapProvider := &OntapRestProvider{}
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("client creation failed")
		}

		result, err := ontapProvider.SnapmirrorObjectStoreSnapshotGet("obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "client creation failed")
	})

	t.Run("WhenSnapshotNotFoundError", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotGet", mock.AnythingOfType("*ontap_rest.SnapmirrorCloudSnapshotGetParams")).Return(nil, errors.New("snapshot not found"))

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.SnapmirrorObjectStoreSnapshotGet("obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "snapshot snapshot-uuid not found in object store")
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})

	t.Run("WhenOtherErrorOccurs", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotGet", mock.AnythingOfType("*ontap_rest.SnapmirrorCloudSnapshotGetParams")).Return(nil, errors.New("other error"))

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.SnapmirrorObjectStoreSnapshotGet("obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "other error")
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})

	t.Run("WhenResponseIsNil", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotGet", mock.AnythingOfType("*ontap_rest.SnapmirrorCloudSnapshotGetParams")).Return(nil, nil)

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.SnapmirrorObjectStoreSnapshotGet("obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "snapshot snapshot-uuid not found in object store")
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})

	t.Run("WhenResponseIsValid", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		expectedResponse := &ontapRest.SnapmirrorEndpointSnapshot{
			SnapmirrorObjectStoreEndpointSnapshot: models.SnapmirrorObjectStoreEndpointSnapshot{
				UUID:              nillable.ToPointer(strfmt.UUID("snapshot-uuid")),
				Name:              nillable.ToPointer("snapshot-name"),
				ArchivedObjects:   nillable.ToPointer(true),
				GroupMemberCount:  nillable.ToPointer(int64(5)),
				LogicalSize:       nillable.ToPointer(int64(1024)),
				SnapshotLockState: nillable.ToPointer("not_locked"),
				CreateTime:        nillable.ToPointer(strfmt.DateTime(time.Now())),
				SnapshotState:     nillable.ToPointer("transferred"),
				SnapmirrorLabel:   nillable.ToPointer("label"),
			},
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorObjectStoreSnapshotGet", mock.AnythingOfType("*ontap_rest.SnapmirrorCloudSnapshotGetParams")).Return(expectedResponse, nil)

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.SnapmirrorObjectStoreSnapshotGet("obj-uuid", "endpoint-uuid", "snapshot-uuid")

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedResponse.UUID, result.UUID)
		assert.Equal(tt, expectedResponse.Name, result.Name)
		assert.Equal(tt, expectedResponse.ArchivedObjects, result.ArchivedObjects)
		assert.Equal(tt, expectedResponse.GroupMemberCount, result.GroupMemberCount)
		assert.Equal(tt, expectedResponse.LogicalSize, result.LogicalSize)
		assert.Equal(tt, expectedResponse.SnapshotLockState, result.SnapshotLockState)
		assert.Equal(tt, expectedResponse.CreateTime, result.CreateTime)
		assert.Equal(tt, expectedResponse.SnapshotState, result.SnapshotState)
		assert.Equal(tt, expectedResponse.SnapmirrorLabel, result.SnapmirrorLabel)
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})
}

func TestSnapmirrorRelationshipDeleteFailsOnJobNotFoundError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

	getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
		return mockClient, nil
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipDeleteParams{UUID: "snapmirrorUUID"}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", expectedParams).Return(false, nil, utilsErrors.NewNotFoundErr("Snapmirror relationship not found", nil))

	_, err := ontapProvider.SnapmirrorRelationshipDelete("snapmirrorUUID")
	assert.NoError(t, err)
}

func TestObjectStoreEndpointInfoGet(t *testing.T) {
	t.Run("WhenGetOntapClientFails", func(tt *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return nil, errors.New("getOntapClient error")
		}

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ObjectStoreEndpointInfoGet("obj-uuid", "endpoint-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, "getOntapClient error")
	})

	t.Run("WhenAPICallFails", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("ObjectStoreEndpointInfoGet", mock.AnythingOfType("*ontap_rest.ObjectStoreEndpointInfoGetParams")).Return(nil, errors.New("api call failed"))

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ObjectStoreEndpointInfoGet("obj-uuid", "endpoint-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, "api call failed")
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})

	t.Run("WhenResponseIsNil", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("ObjectStoreEndpointInfoGet", mock.AnythingOfType("*ontap_rest.ObjectStoreEndpointInfoGetParams")).Return(nil, nil)

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ObjectStoreEndpointInfoGet("obj-uuid", "endpoint-uuid")

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.Contains(tt, err.Error(), "object store endpoint endpoint-uuid not found in object store obj-uuid")
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})

	t.Run("WhenResponseIsValid", func(tt *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() { getOntapClientFunc = originalgetOntapClientFunc }()

		getOntapClientFunc = func(params ontapRest.RESTClientParams) (ontapRest.RESTClient, error) {
			return mockClient, nil
		}

		expectedResponse := &ontapRest.ObjectStoreEndpointInfo{
			ObjectStoreEndpointInfo: models.ObjectStoreEndpointInfo{
				UUID: nillable.ToPointer(strfmt.UUID("endpoint-uuid")),
				Destination: &models.ObjectStoreEndpointInfoInlineDestination{
					LogicalSize: nillable.ToPointer(int64(1024)),
				},
			},
		}

		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("ObjectStoreEndpointInfoGet", mock.AnythingOfType("*ontap_rest.ObjectStoreEndpointInfoGetParams")).Return(expectedResponse, nil)

		ontapProvider := &OntapRestProvider{}

		result, err := ontapProvider.ObjectStoreEndpointInfoGet("obj-uuid", "endpoint-uuid")

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, expectedResponse.UUID, result.UUID)
		assert.Equal(tt, expectedResponse.Destination.LogicalSize, result.LogicalSize)
		mockClient.AssertExpectations(tt)
		mockSnapmirrorClient.AssertExpectations(tt)
	})
}
