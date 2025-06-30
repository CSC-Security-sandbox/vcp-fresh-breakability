package vsa

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestSnapmirrorRelationshipCreateSucceeds(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SetAccessToken:  true,
	}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}
	expectedSnapmirror := []*ontapRest.SnapmirrorRelationship{{SnapmirrorRelationship: models.SnapmirrorRelationship{}}}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, expectedJob, nil)
	mockClient.On("Poll", "jobUUID").Return(nil)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontapRest.SnapmirrorRelationshipListParams{DestinationPath: "destinationPath", SourcePath: "sourcePath"}).Return(expectedSnapmirror, nil)

	result, err := ontapProvider.SnapmirrorRelationshipCreate("destinationPath", "sourcePath")
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestSnapmirrorRelationshipCreateNotFound(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SetAccessToken:  true,
	}
	expectedJob := &ontapRest.JobAccepted{JobUUID: "jobUUID"}
	var expectedSnapmirror []*ontapRest.SnapmirrorRelationship

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, expectedJob, nil)
	mockClient.On("Poll", "jobUUID").Return(nil)
	mockSnapmirrorClient.On("SnapmirrorRelationshipList", &ontapRest.SnapmirrorRelationshipListParams{DestinationPath: "destinationPath", SourcePath: "sourcePath"}).Return(expectedSnapmirror, nil)

	result, err := ontapProvider.SnapmirrorRelationshipCreate("destinationPath", "sourcePath")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSnapmirrorRelationshipCreateFailsOnAPIError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipCreateParams{
		SourcePath:      "sourcePath",
		DestinationPath: "destinationPath",
		SetAccessToken:  true,
	}
	expectedError := fmt.Errorf("API error")

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipCreate", expectedParams).Return(nil, nil, expectedError)

	result, err := ontapProvider.SnapmirrorRelationshipCreate("destinationPath", "sourcePath")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
}

func TestSnapmirrorRelationshipDeleteSucceeds(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
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

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	expectedParams := &ontapRest.SnapmirrorRelationshipDeleteParams{UUID: "snapmirrorUUID"}

	mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
	mockSnapmirrorClient.On("SnapmirrorRelationshipDelete", expectedParams).Return(false, nil, errors.New("failed"))

	_, err := ontapProvider.SnapmirrorRelationshipDelete("snapmirrorUUID")
	assert.Error(t, err)
}

func TestSnapmirrorRelationshipGetSuccess(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
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

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
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
	assert.EqualError(t, err, fmt.Sprintf("snapmirror relationship not found for destination: %s and source: %s", destinationPath, sourcePath))
}

func TestSnapmirrorRelationshipGetAPIError(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
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

func TestSnapmirrorRelationshipTransferCreate(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
	}
	ontapProvider := &OntapRestProvider{}
	snapmirrorUUID := "test-uuid"
	snapshotName := "test-snapshot"
	expectedParams := &ontapRest.SnapmirrorRelationshipTransferCreateParams{
		UUID:           snapmirrorUUID,
		SnapshotName:   snapshotName,
		SetAccessToken: true,
	}

	t.Run("success", func(t *testing.T) {
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(nil)

		err := ontapProvider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName)
		assert.NoError(t, err)
	})

	t.Run("api error", func(t *testing.T) {
		mockClient = new(ontapRest.MockRESTClient)
		mockSnapmirrorClient = new(ontapRest.MockSnapmirrorClient)
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferCreate", expectedParams).Return(fmt.Errorf("api error"))

		err := ontapProvider.SnapmirrorRelationshipTransferCreate(snapmirrorUUID, snapshotName)
		assert.Error(t, err)
		assert.EqualError(t, err, "api error")
	})
}
func TestSnapmirrorRelationshipTransferGet(t *testing.T) {
	mockClient := new(ontapRest.MockRESTClient)
	mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

	getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
		return mockClient
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
		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
		}
		mockClient.On("Snapmirror").Return(mockSnapmirrorClient)
		mockSnapmirrorClient.On("SnapmirrorRelationshipTransferGet", expectedParams).Return(nil, fmt.Errorf("api error"))

		result, err := ontapProvider.SnapmirrorRelationshipTransferGet(snapmirrorUUID, snapshotName)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, "api error")
	})
}

func TestSnapmirrorObjectStoreEndpointDelete(t *testing.T) {
	t.Run("OnSuccessWithJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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
	t.Run("OnSuccessWithoutJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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
	t.Run("OnSuccessWithoutJob", func(t *testing.T) {
		mockClient := new(ontapRest.MockRESTClient)
		mockSnapmirrorClient := new(ontapRest.MockSnapmirrorClient)

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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

		getOntapClientFunc = func(params ontapRest.RESTClientParams) ontapRest.RESTClient {
			return mockClient
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
