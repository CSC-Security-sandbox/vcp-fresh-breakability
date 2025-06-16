package activities_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"google.golang.org/api/iam/v1"
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
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
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

func TestCreateVolumeInONTAP_Success_AlreadyCreated(t *testing.T) {
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
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
	node := &models.Node{}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024, State: "online"}

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, utilErrors.NewConflictErr("volume already exists"))
	mockProvider.On("GetVolume", vsa.GetVolumeParams{UUID: "", VolumeName: "test-volume", SvmName: "test-svm"}).Return(expectedResponse, nil)

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
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
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
	expectedLun := &vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test-volume",
			ExternalUUID: "lun-uuid-123",
		},
		SerialNumber: "6c5738423724595454686164",
	}

	// Mock LunCreate method
	mockProvider.On("LunCreate", vsa.LunCreateParams{
		LunName:    "lun_test-volume",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		OsType:     "linux",
		Size:       107373867008,
	}).Return(expectedLun, nil)

	// Act
	availableSpace := int64(107373867008) // 99.9997 GiB, this is the available space after creating the volume
	lunResponse, err := activity.CreateLun(ctx, volume, node, availableSpace)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedLun, lunResponse)
	mockProvider.AssertExpectations(t)
}

func TestCreateLun_Success_AlreadyExists(t *testing.T) {
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

	mockProvider.On("LunCreate", mock.Anything).Return(nil, utilErrors.NewConflictErr("LUN already exists in SVM"))

	lunResponse := &vsa.LunResponse{
		ProviderResponse: vsa.ProviderResponse{
			Name:         "lun_test-volume",
			ExternalUUID: "lun-uuid-123",
		},
		SerialNumber: "6c5738423724595454686164",
	}
	mockProvider.On("LunGet", mock.Anything, mock.Anything, mock.Anything).Return(lunResponse, nil)

	// Act
	availableSpace := int64(107373867008) // 99.9997 GiB, this is the available space after creating the volume
	lun, err := activity.CreateLun(ctx, volume, node, availableSpace)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, lunResponse, lun)
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
		Size:       107373867008,
	}).Return(nil, expectedError)

	// Act
	lunResponse, err := activity.CreateLun(ctx, volume, node, 107373867008)

	// Assert
	assert.Error(t, err)
	assert.Empty(t, lunResponse)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateLun_SkipForDataProtectionVolume(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "dp-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
			BlockProperties:  &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400,
	}
	node := &models.Node{}

	lun, err := activity.CreateLun(ctx, volume, node, 107374182400)
	assert.NoError(t, err)
	assert.NotNil(t, lun)
}

func TestCreateLun_LunGetError(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

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
	mockProvider.On("LunCreate", mock.Anything).Return(nil, utilErrors.NewConflictErr("LUN already exists in SVM"))
	mockProvider.On("LunGet", mock.Anything).Return(nil, errors.New("lun get error"))

	// Act
	lunName, err := activity.CreateLun(ctx, volume, node, 107374182400)

	// Assert
	assert.Error(t, err)
	assert.Empty(t, lunName)
	mockProvider.AssertExpectations(t)
}

func TestCreateLunMap_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400, // minimum value 100 GiB
	}
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
	err := activity.CreateLunMap(ctx, volume, params, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestCreateLunMap_Success_AlreadyExists(t *testing.T) {
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
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
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
	}).Return(utilErrors.NewConflictErr("lun map already exists"))

	// Act
	err := activity.CreateLunMap(ctx, volume, params, node)

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
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400, // minimum value 100 GiB
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
	err := activity.CreateLunMap(ctx, volume, params, node)

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

func TestCreateVolumeInONTAP_CheckVolumeExistsError(t *testing.T) {
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
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
	node := &models.Node{}

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, utilErrors.NewConflictErr("volume already exists"))
	mockProvider.On("GetVolume", vsa.GetVolumeParams{UUID: "", VolumeName: "test-volume", SvmName: "test-svm"}).Return(nil, errors.New("volume not found"))

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockProvider.AssertExpectations(t)
}

func TestHandleVolumeCreateConflict_SuccessOnline(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
	}
	expectedRes := &vsa.VolumeResponse{
		State: ontapModels.VolumeStateOnline,
	}
	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}).Return(expectedRes, nil)

	res, err := activities.HandleVolumeCreateConflict(volume, mockProvider)
	assert.NoError(t, err)
	assert.Equal(t, expectedRes, res)
	mockProvider.AssertExpectations(t)
}

func TestHandleVolumeCreateConflict_NotOnline_DeleteSuccess(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	volRes := &vsa.VolumeResponse{
		State: ontapModels.VolumeStateOffline,
	}
	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}).Return(volRes, nil)
	mockProvider.On("DeleteVolume", "uuid-123", "test-volume").Return(nil)

	res, err := activities.HandleVolumeCreateConflict(volume, mockProvider)
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Contains(t, err.Error(), "is not in online state")
	mockProvider.AssertExpectations(t)
}

func TestHandleVolumeCreateConflict_GetVolumeError(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
	}
	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}).Return(nil, errors.New("get volume error"))

	res, err := activities.HandleVolumeCreateConflict(volume, mockProvider)
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Equal(t, "get volume error", err.Error())
	mockProvider.AssertExpectations(t)
}

func TestHandleVolumeCreateConflict_DeleteVolumeError(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "uuid-123",
		},
	}
	volRes := &vsa.VolumeResponse{
		State: ontapModels.VolumeStateOffline,
	}
	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		VolumeName: volume.Name,
		SvmName:    volume.Svm.Name,
	}).Return(volRes, nil)
	mockProvider.On("DeleteVolume", "uuid-123", "test-volume").Return(errors.New("delete error"))

	res, err := activities.HandleVolumeCreateConflict(volume, mockProvider)
	assert.Error(t, err)
	assert.Nil(t, res)
	assert.Equal(t, "delete error", err.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateLunMap_SkipForDataProtectionVolume(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	params := &common.CreateLunMapParams{
		LunName:   "lun_test-volume",
		SvmName:   "test-svm",
		HostNames: []string{"host1"},
	}
	node := &models.Node{}

	err := activity.CreateLunMap(ctx, volume, params, node)
	assert.NoError(t, err)
}

func TestCreateVolumeInONTAP_DataProtectionVolume(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "dp-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	node := &models.Node{}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 2048}

	mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
		return params.VolumeType == "dp"
	})).Return(expectedResponse, nil)

	result, err := activity.CreateVolumeInONTAP(ctx, volume, node)

	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)
	mockProvider.AssertExpectations(t)
}

func TestCheckForBucketResourceName_ReturnsBucketDetails(t *testing.T) {
	t.Run("CheckForBucketResourceName_ReturnsBucketDetails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				VendorSubnetID: "subnet-id",
			},
		}
		bucketDetails := &datamodel.BucketDetails{
			BucketName:          "bucket-vault-id",
			ServiceAccountName:  "service-account",
			VendorSubnetID:      "subnet-id",
			TenantProjectNumber: "project-number",
		}
		backupVault := &datamodel.BackupVault{
			BucketDetails: datamodel.BucketDetailsArray{bucketDetails},
		}

		mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(backupVault, nil)

		result, err := activity.CheckForBucketResourceName(ctx, volume)

		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Equal(tt, bucketDetails.BucketName, result.BucketName)
		assert.Equal(tt, bucketDetails.ServiceAccountName, result.ServiceAccountName)
		assert.Equal(tt, bucketDetails.VendorSubnetID, result.VendorSubnetID)
		assert.Equal(tt, bucketDetails.TenantProjectNumber, result.TenantProjectNumber)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("CheckForBucketResourceName_ReturnsNilWhenNoMatchingBucket", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				VendorSubnetID: "subnet-id",
			},
		}
		bucketDetails := &datamodel.BucketDetails{
			BucketName:          "bucket-other-id",
			ServiceAccountName:  "service-account",
			VendorSubnetID:      "other-subnet-id",
			TenantProjectNumber: "project-number",
		}
		backupVault := &datamodel.BackupVault{
			BucketDetails: datamodel.BucketDetailsArray{bucketDetails},
		}

		mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(backupVault, nil)

		result, err := activity.CheckForBucketResourceName(ctx, volume)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("CheckForBucketResourceName_ReturnsErrorWhenBackupVaultFetchFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		}

		expectedError := errors.New("failed to fetch backup vault")
		mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(nil, expectedError)

		result, err := activity.CheckForBucketResourceName(ctx, volume)

		assert.Error(tt, err)
		assert.Nil(tt, result)
		assert.EqualError(tt, err, expectedError.Error())
		mockStorage.AssertExpectations(tt)
	})
	t.Run("CheckForBucketResourceName_ReturnsNilWhenBackupVaultNotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		}

		mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(nil, errors.New("backup vault not found"))

		result, err := activity.CheckForBucketResourceName(ctx, volume)

		assert.NoError(tt, err)
		assert.Nil(tt, result)
		mockStorage.AssertExpectations(tt)
	})
}

func TestUpdateBackupVaultWithBucketDetails_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-id",
		},
	}
	bucketDetails := &common.BucketDetails{
		BucketName:          "bucket-name",
		ServiceAccountName:  "service-account",
		TenantProjectNumber: "project-number",
		VendorSubnetID:      "subnet-id",
	}
	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-id"},
		BucketDetails: datamodel.BucketDetailsArray{
			{
				BucketName:          "bucket-name",
				ServiceAccountName:  "service-account@project-number.iam.gserviceaccount.com",
				TenantProjectNumber: "project-number",
				VendorSubnetID:      "subnet-id",
			},
		},
	}

	mockStorage.On("UpdateBackupVault", ctx, backupVault).Return(nil)

	err := activity.UpdateBackupVaultWithBucketDetails(ctx, volume, bucketDetails)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateBackupVaultWithBucketDetails_Failure_UpdateError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			VendorSubnetID: "subnet-id",
		},
	}
	bucketDetails := &common.BucketDetails{
		BucketName:          "bucket-name",
		ServiceAccountName:  "service-account",
		TenantProjectNumber: "project-number",
		VendorSubnetID:      "subnet-id",
	}
	backupVault := &datamodel.BackupVault{
		BaseModel: datamodel.BaseModel{UUID: "vault-id"},
		BucketDetails: datamodel.BucketDetailsArray{
			{
				BucketName:          "bucket-name",
				ServiceAccountName:  "service-account@project-number.iam.gserviceaccount.com",
				TenantProjectNumber: "project-number",
				VendorSubnetID:      "subnet-id",
			},
		},
	}

	expectedError := errors.New("failed to update backup vault")
	mockStorage.On("UpdateBackupVault", ctx, backupVault).Return(expectedError)

	err := activity.UpdateBackupVaultWithBucketDetails(ctx, volume, bucketDetails)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExists_ReturnsNil(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
	}
	backupVault := &datamodel.BackupVault{}

	mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(backupVault, nil)

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExists_ReturnsNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
	}

	mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(nil, errors.New("backup vault not found"))

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultVCPError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
	}

	mockStorage.On("GetBackupVaultByUUID", ctx, "vault-id").Return(nil, errors.New("some error"))

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_FindTenancy(t *testing.T) {
	ctx := context.TODO()
	consumerVPC := "test-vpc"
	customerProjectNumber := "123456"
	tenantProjectRegion := "us-central1"
	logger := util.GetLogger(ctx)
	t.Run("WhenGetTenantProjectFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("", errors.New("Error finding tenancy unit"))

		tenancyInfo, err := activities.FindTenancy(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetTenantProjectSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("tp-projct", nil)

		tenancyInfo, err := activities.FindTenancy(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.NoError(tt, err)
		assert.NotNil(tt, tenancyInfo)
	})
	t.Run("WhenGetTenantProjectSucceedsWithEmptyTPRegion", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, "").Return("tp-projct", nil)

		tenancyInfo, err := activities.FindTenancy(ctx, mgs, consumerVPC, customerProjectNumber, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, tenancyInfo)
	})
	t.Run("WhenGetGCPServiceFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}

		tpName := "tp-projct"
		// Act
		result, err := activity.FindTenancy(ctx, "test-vpc", "123456", &tpName)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
}

func TestGenerateResourceNames_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	gcpRegion := "us-central1"
	originalGetResourceNamesForBackup := activities.GetResourceNamesForBackup
	defer func() { activities.GetResourceNamesForBackup = originalGetResourceNamesForBackup }()

	activities.GetResourceNamesForBackup = func(region, location, project, vaultID string) (string, string, string, error) {
		return "test-email", "test-bucket", "test-service-account", nil
	}

	resourceNames, err := activity.GenerateResourceNames(ctx, volume, tenancyDetails, gcpRegion)

	assert.NoError(t, err)
	assert.NotNil(t, resourceNames)
	assert.Equal(t, "test-email", resourceNames.Email)
	assert.Equal(t, "test-bucket", resourceNames.BucketName)
	assert.Equal(t, "test-service-account", resourceNames.ServiceAccountId)
}

func TestGenerateResourceNames_ErrorFetchingResourceNames(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	gcpRegion := "us-central1"

	expectedError := errors.New("failed to fetch resource names")
	originalGetResourceNamesForBackup := activities.GetResourceNamesForBackup
	defer func() { activities.GetResourceNamesForBackup = originalGetResourceNamesForBackup }()
	activities.GetResourceNamesForBackup = func(region, location, project, vaultID string) (string, string, string, error) {
		return "", "", "", expectedError
	}

	resourceNames, err := activity.GenerateResourceNames(ctx, volume, tenancyDetails, gcpRegion)

	assert.Error(t, err)
	assert.Nil(t, resourceNames)
	assert.EqualError(t, err, expectedError.Error())
}

func TestServiceAccountAlreadyExists(t *testing.T) {
	mockGcpService := hyperscaler.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(&iam.ServiceAccount{}, nil)
	mockGcpService.On("AttachOrUpdateRolesForServiceAccounts", mock.Anything, email, projectNumber).Return(nil)
	mockGcpService.On("CreateBucketIfNotExists", mock.Anything, projectNumber, bucketName, tenantProjectRegion).Return(nil)

	account, bucketDetails, err := activities.GetOrCreateAndGCSResources(mockGcpService, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType)

	assert.NoError(t, err)
	assert.NotNil(t, account)
	assert.NotNil(t, bucketDetails)
	assert.Equal(t, bucketName, bucketDetails[0].BucketName)
}

func TestServiceAccountCreationFails(t *testing.T) {
	mockGcpService := hyperscaler.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(nil, errors.New("service account not found"))
	mockGcpService.On("CreateServiceAccount", &iam.CreateServiceAccountRequest{
		AccountId: serviceAccountId,
		ServiceAccount: &iam.ServiceAccount{
			DisplayName: bucketName,
		},
	}, projectNumber, email).Return(nil, errors.New("failed to create service account"))

	account, bucketDetails, err := activities.GetOrCreateAndGCSResources(mockGcpService, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType)

	assert.Error(t, err)
	assert.Nil(t, account)
	assert.Nil(t, bucketDetails)
	assert.EqualError(t, err, "failed to create service account")
}

func TestBucketCreationFails(t *testing.T) {
	mockGcpService := hyperscaler.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(&iam.ServiceAccount{}, nil)
	mockGcpService.On("AttachOrUpdateRolesForServiceAccounts", mock.Anything, email, projectNumber).Return(nil)
	mockGcpService.On("CreateBucketIfNotExists", mock.Anything, projectNumber, bucketName, tenantProjectRegion).Return(errors.New("failed to create bucket"))

	account, bucketDetails, err := activities.GetOrCreateAndGCSResources(mockGcpService, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType)

	assert.Error(t, err)
	assert.Nil(t, account)
	assert.Nil(t, bucketDetails)
	assert.EqualError(t, err, "failed to create bucket")
}

func TestCreateBucketSuccess(t *testing.T) {
	mockGcpService := hyperscaler.NewMockGoogleServices(t)
	resourceName := &common.ResourceNames{
		ServiceAccountId: "test-service-account",
		Email:            "test-email",
		BucketName:       "test-bucket",
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	region := "us-central1"

	originalGetOrCreateAndGCSResources := activities.GetOrCreateAndGCSResources
	originalGetGCPService := activities.GetGCPService
	defer func() {
		activities.GetOrCreateAndGCSResources = originalGetOrCreateAndGCSResources
		activities.GetGCPService = originalGetGCPService
	}()

	res := []*common.BucketDetails{
		{
			BucketName: "test-bucket",
		},
	}

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}
	activities.GetOrCreateAndGCSResources = func(gcpServices hyperscaler.GoogleServices, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType string) (*iam.ServiceAccount, []*common.BucketDetails, error) {
		return nil, res, nil
	}
	activity := activities.VolumeCreateActivity{}
	bucketDetails, err := activity.CreateBucket(context.Background(), resourceName, tenancyDetails, region)

	assert.NoError(t, err)
	assert.NotNil(t, bucketDetails)
	mockGcpService.AssertExpectations(t)
}

func TestCreateBucketGetGcpServiceFails(t *testing.T) {
	mockGcpService := hyperscaler.NewMockGoogleServices(t)
	resourceName := &common.ResourceNames{
		ServiceAccountId: "test-service-account",
		Email:            "test-email",
		BucketName:       "test-bucket",
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	region := "us-central1"
	originalGetGCPService := activities.GetGCPService
	defer func() {
		activities.GetGCPService = originalGetGCPService
	}()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}
	activity := activities.VolumeCreateActivity{}
	bucketDetails, err := activity.CreateBucket(context.Background(), resourceName, tenancyDetails, region)

	assert.Error(t, err)
	assert.Nil(t, bucketDetails)
	mockGcpService.AssertExpectations(t)
}

func TestCreateSnapshotPolicyInONTAP(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }()

	activities.GetProviderByNode = func(node *models.Node) vsa.Provider {
		return mockProvider
	}

	ctx := context.Background()
	activity := activities.VolumeCreateActivity{}
	node := &models.Node{}
	volume := &datamodel.Volume{
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name:      "policy1",
			IsEnabled: true,
			Schedules: []*datamodel.SnapshotPolicySchedule{
				{
					DaysOfMonth:     []int{1, 15},
					DaysOfWeek:      []int{2},
					Hours:           []int{3},
					Minutes:         []int{0},
					SnapmirrorLabel: "label1",
					Count:           5,
				},
			},
		},
	}

	t.Run("Success", func(tt *testing.T) {
		mockProvider.On("CreateSnapshotPolicy", mock.Anything).Return(nil)
		err := activity.CreateSnapshotPolicyInONTAP(ctx, volume, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("NilNodeOrVolumeOrPolicy", func(tt *testing.T) {
		err := activity.CreateSnapshotPolicyInONTAP(ctx, nil, node)
		assert.NoError(tt, err)
		err = activity.CreateSnapshotPolicyInONTAP(ctx, volume, nil)
		assert.NoError(tt, err)
		volNoPolicy := &datamodel.Volume{}
		err = activity.CreateSnapshotPolicyInONTAP(ctx, volNoPolicy, node)
		assert.NoError(tt, err)
	})
}

func TestConvertToVSASnapshotPolicySchedules(t *testing.T) {
	t.Run("NilSchedules", func(tt *testing.T) {
		result := activities.ConvertToVSASnapshotPolicySchedules(nil)
		assert.Nil(tt, result)
	})

	t.Run("EmptySchedules", func(tt *testing.T) {
		result := activities.ConvertToVSASnapshotPolicySchedules([]*datamodel.SnapshotPolicySchedule{})
		assert.Nil(tt, result)
		assert.Len(tt, result, 0)
	})

	t.Run("PopulatedSchedules", func(tt *testing.T) {
		schedules := []*datamodel.SnapshotPolicySchedule{
			{
				DaysOfMonth:     []int{1, 2},
				DaysOfWeek:      []int{3, 4},
				Hours:           []int{5},
				Minutes:         []int{0, 30},
				SnapmirrorLabel: "labelA",
				Count:           7,
			},
		}
		result := activities.ConvertToVSASnapshotPolicySchedules(schedules)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "labelA", result[0].Prefix)
		assert.Equal(tt, int64(7), result[0].Count)
		assert.Equal(tt, []int{1, 2}, result[0].Schedule.DaysOfMonth)
		assert.Equal(tt, []int{3, 4}, result[0].Schedule.DaysOfWeek)
		assert.Equal(tt, []int{5}, result[0].Schedule.Hours)
		assert.Equal(tt, []int{0, 30}, result[0].Schedule.Minutes)
	})
}
