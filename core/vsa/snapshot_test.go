package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	ontaprest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
)

func TestCreateSnapshot(t *testing.T) {
	t.Run("CreateSnapshotSuccess", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				Comment:     nillable.ToPointer("testComment"),
				Name:        nillable.ToPointer("testSnapshot"),
				Size:        nillable.ToPointer(int64(1024)),
				UUID:        nillable.ToPointer("testUUID"),
				LogicalSize: nillable.ToPointer(int64(1024)),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotGet", mock.Anything).Return(mockSnapshot, nil)
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(mockJob)

		resp, err := rc.CreateSnapshot(params)

		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, "testSnapshot", resp.Name)
		assert.Equal(t, "testUUID", resp.ExternalUUID)
		assert.Equal(t, int64(1024), resp.SizeInBytes)
	})

	t.Run("CreateSnapshotErrorOnCreate", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockStorage.On("SnapshotCreate", mock.Anything).Return(nil, nil, errors.New("creation error"))

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("CreateSnapshotErrorOnPoll", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{
			Snapshot: models.Snapshot{
				Comment: nillable.ToPointer("testComment"),
				Name:    nillable.ToPointer("testSnapshot"),
				Size:    nillable.ToPointer(int64(1024)),
			},
		}
		mockJob := &ontaprest.JobAccepted{
			JobUUID: "testJobUUID",
		}

		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, mockJob, nil)
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("polling error"))

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("CreateSnapshotInvalidResponse", func(t *testing.T) {
		mockClient := new(ontaprest.MockRESTClient)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		params := CreateSnapshotParams{
			VolumeUUID: "testVolumeUUID",
			Name:       "testSnapshot",
			Comment:    "testComment",
		}

		mockSnapshot := &ontaprest.Snapshot{}

		mockStorage.On("SnapshotGet", mock.Anything).Return(nil, nil)
		mockStorage.On("SnapshotCreate", mock.Anything).Return(mockSnapshot, nil, nil)

		resp, err := rc.CreateSnapshot(params)

		assert.Error(t, err)
		assert.Nil(t, resp)
		assert.ErrorContains(t, err, "invalid Snapshot create response from API")
	})
}

func TestDeleteSnapshot(t *testing.T) {
	t.Run("DeleteSnapshotSuccess", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		snapshotUUID := "testUUID"
		snapshotName := "testSnapshot"

		mockStorage.On("SnapshotDelete", mock.Anything).Return(nil)

		err := rc.DeleteSnapshot(snapshotUUID, snapshotName)

		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("DeleteSnapshotError", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
			return mockClient
		}
		rc := &OntapRestProvider{}

		snapshotUUID := "testUUID"
		snapshotName := "testSnapshot"

		mockStorage.On("SnapshotDelete", mock.Anything).Return(errors.New("deletion error"))

		err := rc.DeleteSnapshot(snapshotUUID, snapshotName)

		assert.Error(t, err)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
}
