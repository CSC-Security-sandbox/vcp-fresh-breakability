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
	t.Run("TestCreateVolumesSuccess_WithoutAutoTieringConfig", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
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
	})

	t.Run("TestCreateVolumesSuccess_WithAutoTieringConfig", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		volumeName := "testVolume"
		volSpace := int64(1024)
		tieringPolicy := TieringPolicy{
			CoolnessPeriod:            30,
			CoolAccessRetrievalPolicy: "Default",
			CoolAccessTieringPolicy:   "auto",
		}
		params := CreateVolumeParams{
			VolumeName:    volumeName,
			SvmName:       "testSVM",
			AggregateName: "testAggregate",
			Size:          volSpace,
			VolumeType:    "rw",
			TieringPolicy: &tieringPolicy,
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
	})
}

func TestCreateVolume_ErrorOnCreate(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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

func TestCreateVolumesFailure_getOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("getOntapClientFunc error")
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volSpace := int64(1024)
	tieringPolicy := TieringPolicy{
		CoolnessPeriod:            30,
		CoolAccessRetrievalPolicy: "Default",
		CoolAccessTieringPolicy:   "auto",
	}
	params := CreateVolumeParams{
		VolumeName:    volumeName,
		SvmName:       "testSVM",
		AggregateName: "testAggregate",
		Size:          volSpace,
		VolumeType:    "rw",
		TieringPolicy: &tieringPolicy,
	}

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClientFunc error", err.Error())
}

func TestDeleteVolume_Success(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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

func TestDeleteVolume_Error_getOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("getOntapClientFunc error")
	}
	rc := &OntapRestProvider{}

	volumeUUID := "testUUID"
	volumeName := "testVolume"

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.Error(t, err)
	assert.Equal(t, "getOntapClientFunc error", err.Error())
}

func TestGetVolume_WhenVolumeIsFound_ThenReturnVolumeResponse(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
				Size:      nillable.GetInt64Ptr(200000), // Example total size
				LogicalSpace: &models.VolumeInlineSpaceInlineLogicalSpace{
					Used: nillable.GetInt64Ptr(100000),
				},
			},
			State: nillable.ToPointer(models.VolumeStateOnline),
			SnapshotPolicy: &models.VolumeInlineSnapshotPolicy{
				Name: nillable.GetStringPtr("none"), // Example snapshot policy
			},
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
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

func TestGetVolume_WhenVolumeGetReturnsError_getOntapClientFuncError(t *testing.T) {
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("getOntapClientFunc error")
	}
	rc := &OntapRestProvider{}

	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"

	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	resp, err := rc.GetVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClientFunc error", err.Error())
}

func TestUpdateVolume(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := UpdateVolumeParams{
		UUID:               "testUUID",
		Size:               2048,
		SnapshotPolicyName: "testSnapshot",
		TieringPolicy: &TieringPolicy{
			CoolnessPeriod:            7,
			CoolAccessRetrievalPolicy: "default",
			CoolAccessTieringPolicy:   "auto",
		},
	}

	// Case 1: VolumeModify returns error
	mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("modify error")).Once()
	err := rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "modify error", err.Error())

	// Case 2: VolumeModify returns success == true
	mockStorage.On("VolumeModify", mock.Anything).Return(true, nil, nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

	// Case 3: VolumeModify returns success == false, Poll returns nil
	mockJob := &ontaprest.JobAccepted{JobUUID: "job-uuid"}
	mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()
	mockClient.On("Poll", mockJob.JobUUID).Return(nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

	// Case 4: VolumeModify returns success == false, Poll returns error
	mockJob2 := &ontaprest.JobAccepted{JobUUID: "job-uuid-2"}
	mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob2, nil).Once()
	mockClient.On("Poll", mockJob2.JobUUID).Return(errors.New("poll error")).Once()
	err = rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "poll error", err.Error())

	// Case 5: getOntapClientFunc returns error
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("getOntapClientFunc error")
	}
	err = rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "getOntapClientFunc error", err.Error())

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestGetVolumes(t *testing.T) {
	t.Run("GetVolumesSuccess", func(t *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockStorage.On("VolumeCollectionGet", mock.Anything, mock.Anything).Return(nil)

		_, err := rc.GetVolumes()
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
	t.Run("GetVolumesFailure", func(t *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		rc := &OntapRestProvider{}
		_, err := rc.GetVolumes()
		assert.Error(t, err)
		assert.Equal(t, "getOntapClientFunc error", err.Error())
	})
}

func TestUpdateVolume_ForSplit(t *testing.T) {
	mockStorage := new(ontaprest.MockStorageClient)
	mockClient := new(ontaprest.MockRESTClient)
	mockClient.On("Storage").Return(mockStorage)
	originalgetOntapClientFunc := getOntapClientFunc
	defer func() {
		getOntapClientFunc = originalgetOntapClientFunc
	}()
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return mockClient, nil
	}
	rc := &OntapRestProvider{}

	params := UpdateVolumeParams{
		UUID:          "testUUID",
		InitiateSplit: true,
	}

	// Case 1: VolumeModify returns error
	mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("modify error")).Once()
	err := rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "modify error", err.Error())

	// Case 2: VolumeModify returns success == true
	mockStorage.On("VolumeModify", mock.Anything).Return(true, nil, nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

	// Case 3: VolumeModify returns success == false, Poll returns nil
	mockJob := &ontaprest.JobAccepted{JobUUID: "job-uuid"}
	mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()
	mockClient.On("Poll", mockJob.JobUUID).Return(nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

	// Case 4: VolumeModify returns success == false, Poll returns error
	mockJob2 := &ontaprest.JobAccepted{JobUUID: "job-uuid-2"}
	mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob2, nil).Once()
	mockClient.On("Poll", mockJob2.JobUUID).Return(errors.New("poll error")).Once()
	err = rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "poll error", err.Error())

	// Case 5: getOntapClientFunc returns error
	getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
		return nil, errors.New("getOntapClientFunc error")
	}
	err = rc.UpdateVolume(params)
	assert.Error(t, err)
	assert.Equal(t, "getOntapClientFunc error", err.Error())

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestVolumeEncryptionStatus(t *testing.T) {
	volumeName := "testVolume"
	volumeUUID := "testUUID"
	svmName := "testSVM"
	params := GetVolumeParams{
		UUID:       volumeUUID,
		VolumeName: volumeName,
		SvmName:    svmName,
	}
	t.Run("WhenGetVolumeEncryptionStatusReturnsError", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockStorage.On("VolumeGet", mock.Anything).Return(nil, errors.New("Volume not found"))
		result, err := rc.GetVolumeEncryptionStatus(params)
		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Volume not found")
	})
	t.Run("WhenGetVolumeEncryptionStatusCallReturnsWithoutVolume", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockStorage.On("VolumeGet", mock.Anything).Return(nil, nil)
		result, err := rc.GetVolumeEncryptionStatus(params)
		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "volume not found")
	})
	t.Run("WhenGetVolumeEncryptionStatusDoesNotHaveEncryptionField", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockVolume := &ontaprest.Volume{
			Volume: models.Volume{
				UUID: nillable.ToPointer(volumeUUID),
				Name: &volumeName,
				Space: &models.VolumeInlineSpace{
					Available: nillable.GetInt64Ptr(100000), // Example available space
					Size:      nillable.GetInt64Ptr(200000), // Example total size
				},
				State: nillable.ToPointer(models.VolumeStateOnline),
			},
		}
		mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)
		result, err := rc.GetVolumeEncryptionStatus(params)
		assert.Nil(tt, result)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "Encryption field is not populated in Get Volume from VSA")
	})
	t.Run("WhenGetVolumeEncryptionStatusSucceeds", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}
		enabled := true
		state := "encrypted"
		typeEncryption := "volume"
		mockVolume := &ontaprest.Volume{
			Volume: models.Volume{
				UUID: nillable.ToPointer(volumeUUID),
				Name: &volumeName,
				Space: &models.VolumeInlineSpace{
					Available: nillable.GetInt64Ptr(100000), // Example available space
					Size:      nillable.GetInt64Ptr(200000), // Example total size
				},
				State: nillable.ToPointer(models.VolumeStateOnline),
				Encryption: &models.VolumeInlineEncryption{
					Enabled: &enabled,
					State:   &state,
					Type:    &typeEncryption,
				},
			},
		}
		mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

		result, err := rc.GetVolumeEncryptionStatus(params)
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.NotNil(tt, result.Encryption)
		assert.True(tt, *result.Enabled)
		assert.Equal(tt, "volume", *result.Type)
		assert.Equal(tt, "encrypted", *result.Encryption.State)
	})
}

func TestUpdateVolumeEnableEncryption(t *testing.T) {
	params := UpdateVolumeParams{
		UUID:             "testUUID",
		EncryptionEnable: true,
	}
	t.Run("WhenUpdateVolumeEnableEncryptionReturnsError", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("volume modify error"))

		err := rc.UpdateVolumeEnableEncryption(params)
		assert.Error(tt, err)
		assert.EqualError(tt, err, "volume modify error")
	})
	t.Run("WhenUpdateVolumeEnableEncryptionReturnsSuccess", func(tt *testing.T) {
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		mockClient.On("Storage").Return(mockStorage)

		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return mockClient, nil
		}
		rc := &OntapRestProvider{}

		mockJob := &ontaprest.JobAccepted{JobUUID: "job-uuid"}
		mockStorage.On("VolumeModify", mock.Anything).Return(true, mockJob, nil)

		err := rc.UpdateVolumeEnableEncryption(params)
		assert.NoError(tt, err)
		assert.Nil(tt, err)
	})
}
