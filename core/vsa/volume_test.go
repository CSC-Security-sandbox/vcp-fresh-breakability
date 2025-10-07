package vsa

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
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
			VolumeName:        volumeName,
			SvmName:           "testSVM",
			Aggregates:        []string{"testAggregate"},
			Size:              volSpace,
			VolumeType:        "rw",
			SnapshotDirectory: true,
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
					Available:                 &volSpace,
					SizeAvailableForSnapshots: nillable.GetInt64Ptr(1029202020),
				},
				Size:  nillable.GetInt64Ptr(1029202020),
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
		assert.Equal(t, volSpace, resp.AvailableSpace)

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
			Aggregates:    []string{"testAggregate"},
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
					Available:                 &volSpace,
					SizeAvailableForSnapshots: nillable.GetInt64Ptr(1029202020),
				},
				Size:  nillable.GetInt64Ptr(1029202020),
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
		assert.Equal(t, volSpace, resp.AvailableSpace)

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
		VolumeName: volumeName,
		SvmName:    "testSVM",
		Aggregates: []string{"testAggregate"},
		Size:       int64(1024),
		VolumeType: "rw",
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
		VolumeName: volumeName,
		SvmName:    "testSVM",
		Aggregates: []string{"testAggregate"},
		Size:       int64(1024),
		VolumeType: "rw",
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
		VolumeName: volumeName,
		SvmName:    "testSVM",
		Aggregates: []string{"testAggregate"},
		Size:       int64(1024),
		VolumeType: "rw",
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
		VolumeName: volumeName,
		SvmName:    "testSVM",
		Aggregates: []string{"testAggregate"},
		Size:       int64(1024),
		VolumeType: "rw",
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
		Aggregates:    []string{"testAggregate"},
		Size:          volSpace,
		VolumeType:    "rw",
		TieringPolicy: &tieringPolicy,
	}

	resp, err := rc.CreateVolume(params)

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, "getOntapClientFunc error", err.Error())
}

func TestCreateVolume_NilSpaceHandling(t *testing.T) {
	t.Run("TestCreateVolume_WithNilSpace", func(t *testing.T) {
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
			VolumeName: volumeName,
			SvmName:    "testSVM",
			Aggregates: []string{"testAggregate"},
			Size:       int64(1024),
			VolumeType: "rw",
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		// Mock volume with nil Space (like FlexGroup volumes with large number of constituents)
		mockVolume := &ontaprest.Volume{
			Volume: models.Volume{
				UUID:  nillable.ToPointer("testUUID"),
				Name:  &volumeName,
				Space: nil, // Nil space to test the nil pointer check
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
		assert.Equal(t, int64(0), resp.AvailableSpace) // Should be 0 when Space is nil

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})

	t.Run("TestCreateVolume_WithNilAvailableSpace", func(t *testing.T) {
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
			VolumeName: volumeName,
			SvmName:    "testSVM",
			Aggregates: []string{"testAggregate"},
			Size:       int64(1024),
			VolumeType: "rw",
		}

		mockJob := &ontaprest.JobAccepted{
			JobUUID:      "testJobUUID",
			ResourceUUID: "testResourceUUID",
		}
		// Mock volume with Space but nil Available
		mockVolume := &ontaprest.Volume{
			Volume: models.Volume{
				UUID: nillable.ToPointer("testUUID"),
				Name: &volumeName,
				Space: &models.VolumeInlineSpace{
					Available: nil, // Nil Available to test the nil pointer check
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
		assert.Equal(t, int64(0), resp.AvailableSpace) // Should be 0 when Available is nil

		mockStorage.AssertExpectations(t)
		mockClient.AssertExpectations(t)
	})
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
			SnapshotDirectoryAccessEnabled: nillable.GetBoolPtr(false), // Add this field
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

func TestUpdateVolume_WithMaxSizeError(t *testing.T) {
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
		Size:               999999999999999, // Set to a very large size that exceeds the maximum
		SnapshotPolicyName: "testSnapshot",
	}

	// Test with a typical ONTAP error message for exceeding maximum volume size
	ontapErrorMsg := "Request to grow volume \"volug13\" in SVM \"gcnv-3eb33bf58bfdd5c-svm-01\" failed because the resulting volume size is greater than the maximum size. The maximum possible size is 900TB (989560464998400B)"
	mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New(ontapErrorMsg)).Once()

	err := rc.UpdateVolume(params)

	// Check that it's the right error type and contains the right message
	assert.Error(t, err)

	// Check if it's a CustomError with the expected code
	customErr, ok := err.(*vsaerrors.CustomError)
	assert.True(t, ok, "Error should be of type *vsaerrors.CustomError")
	assert.Equal(t, vsaerrors.ErrVolumeExceedsMaximumSize, customErr.TrackingID)

	// Check that the error message is sanitized (doesn't contain SVM name)
	assert.NotContains(t, err.Error(), "gcnv-3eb33bf58bfdd5c-svm-01")
	assert.Contains(t, err.Error(), "The volume size exceeds the maximum allowed size for this storage pool.")

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestUpdateVolume_WithMaxSizeErrorButNoMaxSizeInfo(t *testing.T) {
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
		Size:               999999999999999, // Set to a very large size that exceeds the maximum
		SnapshotPolicyName: "testSnapshot",
	}

	// Test with an ONTAP error message that mentions maximum size but doesn't include the specific size info
	// This tests the case where maxSizeStart > 0 check fails
	mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New(ErrMsgVolumeMaxSizeExceeded)).Once()

	err := rc.UpdateVolume(params)

	// Should fall back to the default error handling
	assert.Error(t, err)
	customErr, ok := err.(*vsaerrors.CustomError)
	assert.True(t, ok, "Error should be of type *vsaerrors.CustomError")
	assert.Equal(t, vsaerrors.ErrOntapRestAPIError, customErr.TrackingID)
	// For the general API error, we should get the generic error message
	assert.Contains(t, err.Error(), "An internal error occurred.")

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

func TestUpdateVolume_WithExportPolicyAndJunctionPath(t *testing.T) {
	t.Run("WhenExportPolicyIsSet_ShouldSetExportPolicyInVolumeModifyParams", func(tt *testing.T) {
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

		exportPolicy := "test-export-policy"
		params := UpdateVolumeParams{
			UUID:         "testUUID",
			ExportPolicy: &exportPolicy,
		}

		// Mock VolumeModify to capture the parameters and verify ExportPolicy is set
		mockStorage.On("VolumeModify", mock.MatchedBy(func(volumeModifyParams *ontaprest.VolumeModifyParams) bool {
			return volumeModifyParams.ExportPolicy != nil && *volumeModifyParams.ExportPolicy == exportPolicy
		})).Return(true, nil, nil).Once()

		err := rc.UpdateVolume(params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenJunctionPathIsSet_ShouldSetPathInVolumeModifyParams", func(tt *testing.T) {
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

		junctionPath := "/test/junction/path"
		params := UpdateVolumeParams{
			UUID:         "testUUID",
			JunctionPath: &junctionPath,
		}

		// Mock VolumeModify to capture the parameters and verify Path is set
		mockStorage.On("VolumeModify", mock.MatchedBy(func(volumeModifyParams *ontaprest.VolumeModifyParams) bool {
			return volumeModifyParams.Path != nil && *volumeModifyParams.Path == junctionPath
		})).Return(true, nil, nil).Once()

		err := rc.UpdateVolume(params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenBothExportPolicyAndJunctionPathAreSet_ShouldSetBothInVolumeModifyParams", func(tt *testing.T) {
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

		exportPolicy := "test-export-policy"
		junctionPath := "/test/junction/path"
		params := UpdateVolumeParams{
			UUID:         "testUUID",
			ExportPolicy: &exportPolicy,
			JunctionPath: &junctionPath,
		}

		// Mock VolumeModify to capture the parameters and verify both are set
		mockStorage.On("VolumeModify", mock.MatchedBy(func(volumeModifyParams *ontaprest.VolumeModifyParams) bool {
			return volumeModifyParams.ExportPolicy != nil && *volumeModifyParams.ExportPolicy == exportPolicy &&
				volumeModifyParams.Path != nil && *volumeModifyParams.Path == junctionPath
		})).Return(true, nil, nil).Once()

		err := rc.UpdateVolume(params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenExportPolicyAndJunctionPathAreNil_ShouldNotSetInVolumeModifyParams", func(tt *testing.T) {
		// This test ensures the conditional logic works correctly when fields are nil
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
			UUID:         "testUUID",
			ExportPolicy: nil, // nil - should not trigger line 254
			JunctionPath: nil, // nil - should not trigger line 257
		}

		// Mock VolumeModify to capture the parameters and verify neither is set
		mockStorage.On("VolumeModify", mock.MatchedBy(func(volumeModifyParams *ontaprest.VolumeModifyParams) bool {
			return volumeModifyParams.ExportPolicy == nil && volumeModifyParams.Path == nil
		})).Return(true, nil, nil).Once()

		err := rc.UpdateVolume(params)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
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

	t.Run("TestRevertVolume_VolumeModifyErrorWithSnapshotInUse", func(t *testing.T) {
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

		// Test for "Failed to restore snapshot"
		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("Failed to restore snapshot due to clone")).Once()
		err := rc.RevertVolume(params)
		assert.Error(t, err)
		customErr, ok := err.(*vsaerrors.CustomError)
		assert.True(t, ok)
		assert.Equal(t, vsaerrors.ErrRevertVolumeWhenSnapshotInUse, customErr.TrackingID)
		assert.Contains(t, err.Error(), "Cannot revert the volume as snapshot is in use by the cloned volume")

		// Test for "Volume snap restore error"
		mockStorage.On("VolumeModify", mock.Anything).Return(false, nil, errors.New("Volume snap restore error: snapshot in use")).Once()
		err = rc.RevertVolume(params)
		assert.Error(t, err)
		customErr, ok = err.(*vsaerrors.CustomError)
		assert.True(t, ok)
		assert.Equal(t, vsaerrors.ErrRevertVolumeWhenSnapshotInUse, customErr.TrackingID)
		assert.Contains(t, err.Error(), "Cannot revert the volume as snapshot is in use by the cloned volume")

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
		mockClient.On("Poll", mockJob.JobUUID).Return(errors.New("snapshot not found")).Once()

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

func TestDeleteVolume_WhenVolumeDoesNotExist_ThenReturnNil(t *testing.T) {
	// Test to cover line 96: return nil when volume doesn't exist
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

	// Mock VolumeGet to return error indicating volume doesn't exist
	mockStorage.On("VolumeGet", mock.Anything).Return(nil, errors.New("entry doesn't exist"))

	err := rc.DeleteVolume(volumeUUID, volumeName)

	// Should return nil when volume doesn't exist (line 96)
	assert.NoError(t, err)
	assert.Nil(t, err)

	mockStorage.AssertExpectations(t)
	mockClient.AssertExpectations(t)
}

func TestUnmountVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		accepted := &ontaprest.JobAccepted{JobUUID: ""}

		volumeUUID := "testUUID"

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeUnmount(mock.Anything).Return(accepted, nil)
		mockClient.EXPECT().Poll("").Return(nil)

		resp, err := rc.UnmountVolume(volumeUUID)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFuncError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		rc := &OntapRestProvider{}
		errMsg := "client error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.UnmountVolume("testUUID")
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenVolumeUnmountError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		errMsg := "unmount error"
		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeUnmount(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.UnmountVolume("testUUID")
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenPollingError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		accepted := &ontaprest.JobAccepted{JobUUID: "job-uuid-123"}
		pollErr := errors.New("polling failed")

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeUnmount(mock.Anything).Return(accepted, nil)
		mockClient.EXPECT().Poll("job-uuid-123").Return(pollErr)

		resp, err := rc.UnmountVolume("testUUID")
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, pollErr, err)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}

func TestMountVolume(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		accepted := &ontaprest.JobAccepted{JobUUID: ""}
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "/test/path",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeMount(mock.MatchedBy(func(p *ontaprest.VolumeMountParams) bool {
			return p.UUID == params.UUID && p.JunctionPath == params.JunctionPath
		})).Return(accepted, nil)
		mockClient.EXPECT().Poll("").Return(nil)

		resp, err := rc.MountVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, "", resp.JobUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("SuccessWithJob", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		jobUUID := "job-uuid-123"
		accepted := &ontaprest.JobAccepted{JobUUID: jobUUID}
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "/test/path",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeMount(mock.MatchedBy(func(p *ontaprest.VolumeMountParams) bool {
			return p.UUID == params.UUID && p.JunctionPath == params.JunctionPath
		})).Return(accepted, nil)
		mockClient.EXPECT().Poll(jobUUID).Return(nil)

		resp, err := rc.MountVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)
		assert.Equal(tt, jobUUID, resp.JobUUID)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenGetOntapClientFuncError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		rc := &OntapRestProvider{}
		errMsg := "client error"
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "/test/path",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.MountVolume(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)
	})

	t.Run("WhenVolumeMountError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}
		errMsg := "mount error"
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "/test/path",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeMount(mock.Anything).Return(nil, errors.New(errMsg))

		resp, err := rc.MountVolume(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Contains(tt, err.Error(), errMsg)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("WhenPollingError", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		jobUUID := "job-uuid-123"
		accepted := &ontaprest.JobAccepted{JobUUID: jobUUID}
		pollErr := errors.New("polling failed")
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "/test/path",
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeMount(mock.Anything).Return(accepted, nil)
		mockClient.EXPECT().Poll(jobUUID).Return(pollErr)

		resp, err := rc.MountVolume(params)
		assert.Error(tt, err)
		assert.Nil(tt, resp)
		assert.Equal(tt, pollErr, err)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})

	t.Run("EmptyJunctionPath", func(tt *testing.T) {
		mm := newMonkeyMockAndPatch(tt)
		mockStorage := new(ontaprest.MockStorageClient)
		mockClient := new(ontaprest.MockRESTClient)
		rc := &OntapRestProvider{}

		accepted := &ontaprest.JobAccepted{JobUUID: ""}
		params := MountVolumeParams{
			UUID:         "test-uuid",
			JunctionPath: "", // Empty junction path
		}

		mm.EXPECT().getOntapClientFunc(mock.Anything).Return(mockClient, nil)
		mockClient.EXPECT().Storage().Return(mockStorage)
		mockStorage.EXPECT().VolumeMount(mock.MatchedBy(func(p *ontaprest.VolumeMountParams) bool {
			return p.UUID == params.UUID && p.JunctionPath == ""
		})).Return(accepted, nil)
		mockClient.EXPECT().Poll("").Return(nil)

		resp, err := rc.MountVolume(params)

		assert.NoError(tt, err)
		assert.NotNil(tt, resp)

		mockStorage.AssertExpectations(tt)
		mockClient.AssertExpectations(tt)
	})
}
