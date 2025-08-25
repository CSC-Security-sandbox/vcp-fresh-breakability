package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
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

	// Mock VolumeGet to return a volume with no clones or split state
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: &volumeUUID,
			Name: &volumeName,
		},
	}
	// The Clone and Snapmirror fields are nil by default, which means no clones or snapmirror protection
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

	// Mock VolumeDelete to succeed
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

	// Mock VolumeGet to return a volume with no clones or split state
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: &volumeUUID,
			Name: &volumeName,
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

	// Mock VolumeDelete to fail
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

func TestDeleteVolume_ErrorWhenVolumeHasClones(t *testing.T) {
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

	// Mock VolumeGet to return a volume with clones
	hasFlexclone := true
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: &volumeUUID,
			Name: &volumeName,
			Clone: &models.VolumeInlineClone{
				HasFlexclone: &hasFlexclone,
			},
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.Error(t, err)
	assertErrContains(t, err, "Cannot delete a volume that is being actively used in a Volume Replication relationship or a file clone split triggered by Snapshot RestoreFiles operation or used as a reference snapshot for a backup")
}

func TestDeleteVolume_ErrorWhenVolumeHasSplitInitiated(t *testing.T) {
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

	// Mock VolumeGet to return a volume with split initiated
	splitInitiated := true
	mockVolume := &ontaprest.Volume{
		Volume: models.Volume{
			UUID: &volumeUUID,
			Name: &volumeName,
			Clone: &models.VolumeInlineClone{
				SplitInitiated: &splitInitiated,
			},
		},
	}
	mockStorage.On("VolumeGet", mock.Anything).Return(mockVolume, nil)

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.Error(t, err)
	assertErrContains(t, err, "Cannot delete a volume that is being actively used in a Volume Replication relationship or a file clone split triggered by Snapshot RestoreFiles operation or used as a reference snapshot for a backup")
}

func TestDeleteVolume_ErrorWhenVolumeGetFails(t *testing.T) {
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

	// Mock VolumeGet to fail
	mockStorage.On("VolumeGet", mock.Anything).Return(nil, errors.New("volume get error"))

	err := rc.DeleteVolume(volumeUUID, volumeName)

	assert.Error(t, err)
	assert.Equal(t, "volume get error", err.Error())
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
		IsRestore:  false,
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
		IsRestore:  false,
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
		IsRestore:  false,
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
		IsRestore:  false,
	}
	resp, err := rc.GetVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assertErrContains(t, err, "getOntapClientFunc error")
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
	assertErrContains(t, err, "modify error")

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
	assertErrContains(t, err, "poll error")

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
	assertErrContains(t, err, "modify error")

	// Case 2: VolumeModify returns success == true
	mockStorage.On("VolumeModify", mock.Anything).Return(true, nil, nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

	// Case 3: VolumeModify returns success == false, Poll returns nil
	mockJob := &ontaprest.JobAccepted{JobUUID: "job-uuid"}
	mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()
	err = rc.UpdateVolume(params)
	assert.NoError(t, err)

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
		IsRestore:  false,
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

func TestRevertVolume(t *testing.T) {
	t.Run("TestRevertVolume_Success", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
			PreRevertVolume: &datamodel.Volume{
				SizeInBytes: 1000000,
			},
		}

		// Mock VolumeModify returns done = true
		mockStorage.On("VolumeModify", mock.Anything).Return(true, nil, nil).Once()

		err := rc.RevertVolume(params)
		assert.NoError(t, err)

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_GetOntapClientError", func(t *testing.T) {
		originalgetOntapClientFunc := getOntapClientFunc
		defer func() {
			getOntapClientFunc = originalgetOntapClientFunc
		}()
		getOntapClientFunc = func(params ontaprest.RESTClientParams) (ontaprest.RESTClient, error) {
			return nil, errors.New("getOntapClientFunc error")
		}
		rc := &OntapRestProvider{}

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "getOntapClientFunc error")
	})

	t.Run("TestRevertVolume_VolumeModifyError", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("volume modify error"))

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "volume modify error")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_VolumeModifyErrorWithReplicationDestination", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("Only a Snapshot copy of a read/write volume can be promoted"))

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Cannot revert a Volume Replication Destination Volume")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_PollError", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		// Mock VolumeModify returns done = false with job
		mockJob := &ontaprest.JobAccepted{JobUUID: "test-job-uuid"}
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()

		// Mock Poll returns error
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("poll error")).Once()

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "poll error")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_PollErrorWithSnapshotNotFound", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		// Mock VolumeModify returns done = false with job
		mockJob := &ontaprest.JobAccepted{JobUUID: "test-job-uuid"}
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()

		// Mock Poll returns not found error
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.NewNotFoundErr("snapshot", nil)).Once()

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "not found")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_PollErrorWithEntryNotFound", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		// Mock VolumeModify returns done = false with job
		mockJob := &ontaprest.JobAccepted{JobUUID: "test-job-uuid"}
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()

		// Mock Poll returns error with "entry doesn't exist"
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("entry doesn't exist")).Once()

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "entry doesn't exist")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestRevertVolume_PollErrorWithReplicationDestination", func(t *testing.T) {
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

		params := RevertVolumeParams{
			VolumeID:   "testVolumeUUID",
			SnapshotID: "testSnapshotUUID",
		}

		// Mock VolumeModify returns done = false with job
		mockJob := &ontaprest.JobAccepted{JobUUID: "test-job-uuid"}
		mockStorage.On("VolumeModify", mock.Anything).Return(false, mockJob, nil).Once()

		// Mock Poll returns error with replication destination message
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("Only a Snapshot copy of a read/write volume can be promoted")).Once()

		err := rc.RevertVolume(params)

		assert.Error(t, err)
		assertErrContains(t, err, "Only a Snapshot copy of a read/write volume can be promoted")

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
}
