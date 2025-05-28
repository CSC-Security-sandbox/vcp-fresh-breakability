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

func TestCreateVolume_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volSpace := int64(1024)
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          volSpace,
		VolumeType:    "rw",
	}

	mockJob := &ontaprest.JobAccepted{
		JobUUID:      "testJobUUID",
		ResourceUUID: "testResourceUUID",
	}
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: nillable.ToPointer("testUUID"),
			Name: &volumeName,
			Space: &models.VolumeInlineSpace{
				Available: &volSpace,
			},
			State: nillable.ToPointer(models.VolumeStateOnline),
		},
	}

	mockStorage.On("VolumeCreate", mock.Anything).Return(mockVolume, mockJob, nil)
	mockClient.On("Poll", mockJob.JobUUID).Return(nil)

	resp, err := rc.CreateVolume(params)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, volumeName, resp.Name)
	assert.Equal(t, "testUUID", resp.ExternalUUID)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateVolume_ErrorOnCreate(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          int64(1024),
		VolumeType:    "rw",
	}

	mockStorage.On("VolumeCreate", mock.Anything).Return(nil, nil, errors.New("creation error"))

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateVolume_DuplicateErrorOnCreate(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          int64(1024),
		VolumeType:    "rw",
	}

	mockStorage.On("VolumeCreate", mock.Anything).Return(nil, nil, errors.New("Duplicate volume name"))

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateVolume_ErrorOnNilResponse(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          int64(1024),
		VolumeType:    "rw",
	}

	mockStorage.On("VolumeCreate", mock.Anything).Return(nil, nil, nil)

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, err.Error(), "invalid Volume response from API")

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestCreateVolume_ErrorOnPoll(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          int64(1024),
		VolumeType:    "rw",
	}

	mockJob := &ontaprest.JobAccepted{
		JobUUID:      "testJobUUID",
		ResourceUUID: "testResourceUUID",
	}
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: nillable.ToPointer("testUUID"),
			Name: &volumeName,
		},
	}

	mockStorage.On("VolumeCreate", mock.Anything).Return(mockVolume, mockJob, nil)
	mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("polling error"))

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDeleteVolume_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeUUID := "testUUID"
	volumeName := "testVolume"

	mockStorage.On("VolumeDelete", mock.Anything).Return(nil)

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.NoError(t, err)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestDeleteVolume_Error(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeUUID := "testUUID"
	volumeName := "testVolume"

	mockStorage.On("VolumeDelete", mock.Anything).Return(errors.New("deletion error"))

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.Error(t, err)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetVolume_WhenVolumeIsFound_ThenReturnVolumeResponse(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: nillable.ToPointer(volumeUUID),
			Name: &volumeName,
			Space: &models.VolumeInlineSpace{
				Available: nillable.GetInt64Ptr(100000), // Example available space
			},
			State: nillable.ToPointer(models.VolumeStateOnline),
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	resp, err := rc.GetVolume(params)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, volumeName, resp.Name)
	assert.Equal(t, volumeUUID, resp.ExternalUUID)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetVolume_WhenVolumeGetReturnsError_ThenReturnError(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"
	mockStorage.On("VolumeGet", mock.Anything).Return(nil, errors.New("get error"))

	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	resp, err := rc.GetVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "get error", err.Error())

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetVolume_WhenVolumeGetReturnsNilVolume_ThenReturnNotFoundError(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"
	mockStorage.On("VolumeGet", mock.Anything).Return(nil, nil)

	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	resp, err := rc.GetVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "not found")

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetVolume_WhenVolumeGetReturnsVolumeWithNilNameOrUUID_ThenReturnNotFoundError(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)

	getOntapClientFunc = func(params ontaprest.RESTClientParams) ontaprest.RESTClient {
		return mockClient
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"
	// Case 1: Name is nil
	mockVolume1 := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: nillable.ToPointer(volumeUUID),
			Name: nil,
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume1, nil)

	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	resp, err := rc.GetVolume(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "not found")

	// Case 2: UUID is nil
	mockVolume2 := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: nil,
			Name: &volumeName,
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume2, nil)

	resp, err = rc.GetVolume(params)
	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "not found")

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}
