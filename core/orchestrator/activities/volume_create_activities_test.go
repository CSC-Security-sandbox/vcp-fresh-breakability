package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestCreateVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume"}

	mockStorage.On("CreateVolume", ctx, volume).Return(volume, nil)

	// Act
	result, err := activity.CreateVolume(ctx, volume)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, volume, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateVolume_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume"}
	expectedError := errors.New("failed to create volume")

	mockStorage.On("CreateVolume", ctx, volume).Return(nil, expectedError)

	// Act
	result, err := activity.CreateVolume(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestCreateVolumeInONTAP_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

	// Mock the CreateVolume method
	mockProvider.On("CreateVolume", mock.Anything).Return(expectedResponse, nil)

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)
	mockProvider.AssertExpectations(t)
}

func TestCreateVolumeInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	expectedError := errors.New("failed to create volume in ONTAP")

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, expectedError)

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	hostParams := []*common.HostParams{
		{HostName: "host1", OsType: "linux", HostIQNs: []string{"iqn.1993-08.org.debian:01:123456789"}},
	}

	// Mock IgroupExists and IgroupCreate methods
	mockProvider.On("IgroupExists", "host1", "test-svm").Return(false, nil)
	mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
		IgroupName: "host1",
		SvmName:    "test-svm",
		OsType:     "linux",
		Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
	}).Return("host1", nil)

	// Act
	err := activity.CreateIgroup(ctx, volume, hostParams, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Exists(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	hostParams := []*common.HostParams{
		{HostName: "host1", OsType: "linux", HostIQNs: []string{"iqn.1993-08.org.debian:01:123456789"}},
	}

	// Mock IgroupExists method
	mockProvider.On("IgroupExists", "host1", "test-svm").Return(true, nil)

	// Act
	err := activity.CreateIgroup(ctx, volume, hostParams, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Failure_IgroupExists(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	hostParams := []*common.HostParams{
		{HostName: "host1", OsType: "linux", HostIQNs: []string{"iqn.1993-08.org.debian:01:123456789"}},
	}

	// Mock IgroupExists method to return an error
	mockProvider.On("IgroupExists", "host1", "test-svm").Return(false, errors.New("failed to check igroup existence"))

	// Act
	err := activity.CreateIgroup(ctx, volume, hostParams, node)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "failed to check igroup existence", err.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Failure_IgroupCreate(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Svm: &datamodel.Svm{Name: "test-svm"}}
	node := &models.Node{}
	hostParams := []*common.HostParams{
		{HostName: "host1", OsType: "linux", HostIQNs: []string{"iqn.1993-08.org.debian:01:123456789"}},
	}

	// Mock IgroupExists and IgroupCreate methods
	mockProvider.On("IgroupExists", "host1", "test-svm").Return(false, nil)
	mockProvider.On("IgroupCreate", vsa.IgroupCreateParams{
		IgroupName: "host1",
		SvmName:    "test-svm",
		OsType:     "linux",
		Initiator:  []string{"iqn.1993-08.org.debian:01:123456789"},
	}).Return("", errors.New("failed to create igroup"))

	// Act
	err := activity.CreateIgroup(ctx, volume, hostParams, node)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "failed to create igroup", err.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateLun_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400, // minimum value 100 GiB
	}
	node := &models.Node{}
	expectedLun := &vsa.ProviderResponse{Name: "lun_test-volume"}

	// Mock LunCreate method
	mockProvider.On("LunCreate", vsa.LunCreateParams{
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		OsType:     "linux",
		Size:       106836996096, // 99.4997 GiB, we subtract 0.5 GiB from the available space for LUN creation
	}).Return(expectedLun, nil)

	// Act
	availableSpace := int64(107373867008) // 99.9997 GiB, this is the available space after creating the volume
	lunName, err := activity.CreateLun(ctx, volume, node, availableSpace)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "lun_test-volume", lunName)
	mockProvider.AssertExpectations(t)
}

func TestCreateLun_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400,
	}
	node := &models.Node{}
	expectedError := errors.New("failed to create LUN")

	// Mock LunCreate method to return an error
	mockProvider.On("LunCreate", vsa.LunCreateParams{
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		OsType:     "linux",
		Size:       106836996096,
	}).Return(nil, expectedError)

	// Act
	lunName, err := activity.CreateLun(ctx, volume, node, 107373867008)

	// Assert
	assert.Error(t, err)
	assert.Empty(t, lunName)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateLunMap_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := &common.CreateLunMapParams{
		LunName:   "lun_test-volume",
		SvmName:   "test-svm",
		HostNames: []string{"host1"},
	}
	node := &models.Node{}

	// Mock LunMapCreate method
	mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
		LunName:    "lun_test-volume",
		SvmName:    "test-svm",
		IGroupName: []string{"host1"},
	}).Return(nil)

	// Act
	err := activity.CreateLunMap(ctx, params, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestCreateLunMap_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := &common.CreateLunMapParams{
		LunName:   "lun_test-volume",
		SvmName:   "test-svm",
		HostNames: []string{"host1"},
	}
	node := &models.Node{}
	expectedError := errors.New("failed to create lun map")

	// Mock LunMapCreate method to return an error
	mockProvider.On("LunMapCreate", vsa.LunMapCreateParams{
		LunName:    "lun_test-volume",
		SvmName:    "test-svm",
		IGroupName: []string{"host1"},
	}).Return(expectedError)

	// Act
	err := activity.CreateLunMap(ctx, params, node)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestUpdateVolumeDetails_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{VolumeAttributes: &datamodel.VolumeAttributes{}}
	volCreateResponse := &vsa.ProviderResponse{ExternalUUID: "uuid-123"}

	mockStorage.On("UpdateVolume", ctx, volume).Return(nil)

	// Act
	err := activity.UpdateVolumeDetails(ctx, volume, volCreateResponse)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "uuid-123", volume.VolumeAttributes.ExternalUUID)
	assert.Equal(t, models.LifeCycleStateREADY, volume.State)
	assert.Equal(t, models.LifeCycleStateAvailableDetails, volume.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeDetails_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{VolumeAttributes: &datamodel.VolumeAttributes{}}
	volCreateResponse := &vsa.ProviderResponse{ExternalUUID: "uuid-123"}
	expectedError := errors.New("failed to update volume")

	mockStorage.On("UpdateVolume", ctx, volume).Return(expectedError)

	// Act
	err := activity.UpdateVolumeDetails(ctx, volume, volCreateResponse)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestGetHosts_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupUUIDs: []string{"uuid1", "uuid2"},
			},
		},
		AccountID: 123,
	}
	expectedHostGroups := []*datamodel.HostGroup{
		{Name: "host-group-1"},
		{Name: "host-group-2"},
	}

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"uuid1", "uuid2"}, int64(123)).Return(expectedHostGroups, nil)

	// Act
	hostGroups, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedHostGroups, hostGroups)
	mockStorage.AssertExpectations(t)
}

func TestGetHosts_Failure_BlockPropertiesNotFound(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: nil,
		},
	}

	// Act
	hostGroups, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, hostGroups)
	assert.Equal(t, "block properties not found", err.Error())
}

func TestGetHosts_Failure_HostGroupsNotFound(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupUUIDs: []string{"uuid1", "uuid2"},
			},
		},
		AccountID: 123,
	}
	expectedError := errors.New("all host groups could not be found")

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"uuid1", "uuid2"}, int64(123)).Return([]*datamodel.HostGroup{}, nil)

	// Act
	hostGroups, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, hostGroups)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestGetHosts_Failure_GetMultipleHostGroupsError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{
				HostGroupUUIDs: []string{"uuid1", "uuid2"},
			},
		},
		AccountID: 123,
	}
	expectedError := errors.New("failed to fetch host groups")

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"uuid1", "uuid2"}, int64(123)).Return(nil, expectedError)

	// Act
	hostGroups, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, hostGroups)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}
