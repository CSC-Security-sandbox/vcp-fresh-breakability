package activities_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_policy"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi/backup_vault"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	ontapModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscaler "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/mocks"
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
	t.Run("TestCreateVolumeInONTAP_DefaultConfig_Success", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			Name:    "test-volume",
			Svm:     &datamodel.Svm{Name: "test-svm"},
			Account: &datamodel.Account{Name: "test-account"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection:  false,
				SnapshotDirectory: true,
			},
		}
		node := &models.Node{}
		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method
		mockProvider.On("CreateVolume", mock.Anything).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WhenAutoTieringIsFalse_DefaultConfigIsSet", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			Name:               "test-volume",
			Svm:                &datamodel.Svm{Name: "test-svm"},
			Account:            &datamodel.Account{Name: "test-account"},
			AutoTieringEnabled: false,
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
				SnapshotDirectory: false,
			},
		}

		node := &models.Node{}
		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.TieringPolicy.CoolAccessTieringPolicy == ontapModels.VolumeInlineTieringPolicyNone
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WhenAutoTieringIsTrue_AutoTierConfigIsSet", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		volume := &datamodel.Volume{
			Name:               "test-volume",
			Svm:                &datamodel.Svm{Name: "test-svm"},
			Account:            &datamodel.Account{Name: "test-account"},
			AutoTieringEnabled: true,
			AutoTieringPolicy: &datamodel.AutoTieringPolicy{
				TieringPolicy:        "auto",
				RetrievalPolicy:      "onread",
				CoolingThresholdDays: 45,
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		node := &models.Node{}

		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.TieringPolicy.CoolAccessTieringPolicy == ontapModels.VolumeInlineTieringPolicyAuto &&
				params.TieringPolicy.CoolAccessRetrievalPolicy == "onread" &&
				params.TieringPolicy.CoolnessPeriod == 45
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WithFileProperties_ExportPolicyAndJunctionPathAreSet", func(t *testing.T) {
		// Setup file protocol support for this test
		utils.SetFileProtocolSupportedForTesting(true)
		utils.SetFileProtocolAllowlistedAccountsForTesting("test-account")
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
			utils.SetFileProtocolAllowlistedAccountsForTesting("")
		}()

		// Arrange
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volume := &datamodel.Volume{
			Name:    "test-volume",
			Svm:     &datamodel.Svm{Name: "test-svm"},
			Account: &datamodel.Account{Name: "test-account"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
				FileProperties: &datamodel.FileProperties{
					JunctionPath: "/test/junction/path",
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "test-export-policy",
					},
				},
			},
		}
		node := &models.Node{}
		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method and verify ExportPolicy and JunctionPath are set
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.ExportPolicy != nil && *params.ExportPolicy == "test-export-policy" &&
				params.JunctionPath != nil && *params.JunctionPath == "/test/junction/path"
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WhenLargeCapacityWithConstituentCount_FlexGroupStyleAndAggregatesAreSet", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		constituentCount := int32(8)
		volume := &datamodel.Volume{
			Name:    "test-large-volume",
			Svm:     &datamodel.Svm{Name: "test-svm"},
			Account: &datamodel.Account{Name: "test-account"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		volume.LargeVolumeAttributes = &datamodel.LargeVolumeAttributes{
			LargeCapacity:               true,
			LargeVolumeConstituentCount: &constituentCount,
		}
		node := &models.Node{}

		aggrs := &models.AggregateDistributionResult{
			Aggregates:     []string{"aggr1", "aggr2", "aggr3", "aggr4"},
			AggrMultiplier: 2,
		}

		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method with specific checks for large capacity parameters
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.Style != nil &&
				*params.Style == "flexgroup" &&
				len(params.Aggregates) == len(aggrs.Aggregates) &&
				params.ConstituentsPerAggregate != nil &&
				*params.ConstituentsPerAggregate == aggrs.AggrMultiplier &&
				params.TieringSupported == nil // Should not set TieringSupported for constituent count mode
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, aggrs)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WhenLargeCapacityWithAutoProvisioning_FlexGroupStyleAndTieringSupportedAreSet", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volume := &datamodel.Volume{
			Name:                  "test-large-volume-auto",
			Svm:                   &datamodel.Svm{Name: "test-svm"},
			Account:               &datamodel.Account{Name: "test-account"},
			LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{LargeCapacity: true}, // LargeVolumeConstituentCount is nil
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		node := &models.Node{}

		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method with specific checks for auto-provisioning
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.Style != nil &&
				*params.Style == "flexgroup" &&
				params.TieringSupported != nil &&
				*params.TieringSupported == true && // Should set TieringSupported for auto-provisioning
				params.ConstituentsPerAggregate == nil // Should not set ConstituentsPerAggregate for auto-provisioning
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestCreateVolumeInONTAP_WhenNotLargeCapacity_RegularAggregateIsSet", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider) // Use the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

		// Mock GetProviderByNode to return the mock provider
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		activity := activities.VolumeCreateActivity{
			SE: database.NewMockStorage(t),
		}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		volume := &datamodel.Volume{
			Name:    "test-regular-volume",
			Svm:     &datamodel.Svm{Name: "test-svm"},
			Account: &datamodel.Account{Name: "test-account"},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: false,
			},
		}
		node := &models.Node{}

		expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024}

		// Mock the CreateVolume method with specific checks for regular volumes
		mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
			return params.Style == nil && // Should not set Style for regular volumes
				len(params.Aggregates) == 1 &&
				params.Aggregates[0] == "aggr1" && // Should use the default aggregate name
				params.ConstituentsPerAggregate == nil && // Should not set ConstituentsPerAggregate for regular volumes
				params.TieringSupported == nil // Should not set TieringSupported for regular volumes
		})).Return(expectedResponse, nil)

		// Act
		result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})
}

func TestCreateVolumeInONTAP_Success_AlreadyCreated(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		Account: &datamodel.Account{Name: "account"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
	node := &models.Node{}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 1024, State: "online"}

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, utilErrors.NewConflictErr("volume already exists"))
	mockProvider.On("GetVolume", vsa.GetVolumeParams{UUID: "", VolumeName: "test-volume", SvmName: "test-svm", IsRestore: false}).Return(expectedResponse, nil)

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)
	mockProvider.AssertExpectations(t)
}

func TestCreateVolumeInONTAP_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		Account: &datamodel.Account{Name: "account"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
	node := &models.Node{}
	expectedError := errors.New("failed to create volume in ONTAP")

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, expectedError)

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.EqualError(t, err, expectedError.Error())
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	mockProvider.On("IgroupExists", "host1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	mockProvider.On("IgroupExists", "host1", nillable.GetStringPtr("test-svm")).Return(true, nil, nil)

	// Act
	err := activity.CreateIgroup(ctx, volume, hostParams, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestCreateIgroup_Failure_IgroupExists(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	mockProvider.On("IgroupExists", "host1", nillable.GetStringPtr("test-svm")).Return(false, nil, errors.New("failed to check igroup existence"))

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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	mockProvider.On("IgroupExists", "host1", nillable.GetStringPtr("test-svm")).Return(false, nil, nil)
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

func TestCreateLun_WithBlockDevices_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	blockDevices := []datamodel.BlockDevice{
		{
			Name:   "custom-lun-name",
			OSType: "LINUX",
			HostGroupDetails: []datamodel.HostGroupDetail{
				{
					HostGroupUUID: "hg-uuid-1",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
				},
			},
		},
	}

	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &blockDevices,
		},
	}
	node := &models.Node{}
	availableSpace := int64(107374182400) // 100 GiB
	expectedResponse := &vsa.LunResponse{}

	mockProvider.On("LunCreate", mock.Anything).Return(expectedResponse, nil)

	// Act
	result, err := activity.CreateLun(ctx, volume, node, availableSpace)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)

	// Verify that the LUN name from BlockDevice was used instead of generated name
	mockProvider.AssertCalled(t, "LunCreate", vsa.LunCreateParams{
		LunName:    "custom-lun-name", // Should use BlockDevice.Name
		VolumeName: "test-volume",
		SvmName:    "test-svm",
		OsType:     "LINUX", // Should use BlockDevice.OSType
		Size:       availableSpace,
	})
	mockProvider.AssertExpectations(t)
}

func TestCreateLun_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockProperties: &datamodel.BlockProperties{OSType: "linux"},
		},
		SizeInBytes: 107374182400, // minimum value 100 GiB
	}
	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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

func TestGetHosts_WithBlockDevices_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	blockDevices := []datamodel.BlockDevice{
		{
			Name:   "test-lun",
			OSType: "LINUX",
			HostGroupDetails: []datamodel.HostGroupDetail{
				{
					HostGroupUUID: "hg-uuid-1",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
				},
				{
					HostGroupUUID: "hg-uuid-2",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host2"},
				},
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &blockDevices,
		},
	}

	expectedHostGroups := []*datamodel.HostGroup{
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-1"},
			Name:      "hg1",
			State:     "READY",
		},
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-2"},
			Name:      "hg2",
			State:     "READY",
		},
	}

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"hg-uuid-1", "hg-uuid-2"}, int64(1)).Return(expectedHostGroups, nil)

	// Act
	result, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedHostGroups, result)
	mockStorage.AssertExpectations(t)
}

func TestGetHosts_WithBlockDevices_HostGroupsNotFound(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	blockDevices := []datamodel.BlockDevice{
		{
			Name:   "test-lun",
			OSType: "LINUX",
			HostGroupDetails: []datamodel.HostGroupDetail{
				{
					HostGroupUUID: "hg-uuid-1",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
				},
				{
					HostGroupUUID: "hg-uuid-2",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host2"},
				},
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &blockDevices,
		},
	}

	// Return fewer host groups than requested
	expectedHostGroups := []*datamodel.HostGroup{
		{
			BaseModel: datamodel.BaseModel{UUID: "hg-uuid-1"},
			Name:      "hg1",
			State:     "READY",
		},
		// Missing hg-uuid-2
	}
	_, err := vsaerrors.NewErrorHandler()
	if err != nil {
		t.Fatalf("Failed to create error handler: %v", err)
	}

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"hg-uuid-1", "hg-uuid-2"}, int64(1)).Return(expectedHostGroups, nil)

	// Act
	result, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "All host groups could not be found")
	mockStorage.AssertExpectations(t)
}

func TestGetHosts_WithBlockDevices_GetMultipleHostGroupsError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	blockDevices := []datamodel.BlockDevice{
		{
			Name:   "test-lun",
			OSType: "LINUX",
			HostGroupDetails: []datamodel.HostGroupDetail{
				{
					HostGroupUUID: "hg-uuid-1",
					HostQNs:       []string{"iqn.1998-01.com.vmware:host1"},
				},
			},
		},
	}

	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1},
		AccountID: 1,
		VolumeAttributes: &datamodel.VolumeAttributes{
			BlockDevices: &blockDevices,
		},
	}

	expectedError := errors.New("database error")

	mockStorage.On("GetMultipleHostGroups", ctx, []string{"hg-uuid-1"}, int64(1)).Return(nil, expectedError)

	// Act
	result, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Equal(t, expectedError, err)
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
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "uuid1",
					},
					{
						HostGroupUUID: "uuid2",
					},
				},
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
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "uuid1",
					},
					{
						HostGroupUUID: "uuid2",
					},
				},
			},
		},
		AccountID: 123,
	}
	_, err := vsaerrors.NewErrorHandler()
	if err != nil {
		t.Fatalf("Failed to create error handler: %v", err)
	}
	mockStorage.On("GetMultipleHostGroups", ctx, []string{"uuid1", "uuid2"}, int64(123)).Return([]*datamodel.HostGroup{}, nil)

	// Act
	hostGroups, err := activity.GetHosts(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, hostGroups)
	assert.Contains(t, err.Error(), "All host groups could not be found")
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
				HostGroupDetails: []datamodel.HostGroupDetail{
					{
						HostGroupUUID: "uuid1",
					},
					{
						HostGroupUUID: "uuid2",
					},
				},
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{Name: "test-volume", Svm: &datamodel.Svm{Name: "test-svm"},
		Account: &datamodel.Account{Name: "account"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: false,
		}}
	node := &models.Node{}

	mockProvider.On("CreateVolume", mock.Anything).Return(nil, utilErrors.NewConflictErr("volume already exists"))
	mockProvider.On("GetVolume", vsa.GetVolumeParams{UUID: "", VolumeName: "test-volume", SvmName: "test-svm", IsRestore: false}).Return(nil, errors.New("volume not found"))

	// Act
	result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

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
		IsRestore:  false,
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
		IsRestore:  false,
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
		IsRestore:  false,
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
		IsRestore:  false,
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name:    "dp-volume",
		Svm:     &datamodel.Svm{Name: "test-svm"},
		Account: &datamodel.Account{Name: "account"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	node := &models.Node{}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 2048}

	mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
		return params.VolumeType == "dp"
	})).Return(expectedResponse, nil)

	result, err := activity.CreateVolumeInONTAP(ctx, volume, node, nil, nil, nil)

	assert.NoError(t, err)
	assert.Equal(t, expectedResponse, result)
	mockProvider.AssertExpectations(t)
}

func TestCreateVolumeInONTAP_ClonedVolume(t *testing.T) {
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name:    "dp-volume",
		Svm:     &datamodel.Svm{Name: "test-svm"},
		Account: &datamodel.Account{Name: "account"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			IsDataProtection: true,
		},
	}
	node := &models.Node{}
	snapshot := &datamodel.Snapshot{
		Name: "snapshot-1",
		Volume: &datamodel.Volume{
			Name:             "source-volume",
			Svm:              &datamodel.Svm{Name: "test-svm"},
			Account:          &datamodel.Account{Name: "test-account"},
			VolumeAttributes: &datamodel.VolumeAttributes{ExternalUUID: "uuid-123"},
		},
		SnapshotAttributes: &datamodel.SnapshotAttributes{ExternalUUID: "uuid-123"},
	}
	expectedResponse := &vsa.VolumeResponse{ProviderResponse: vsa.ProviderResponse{ExternalUUID: "uuid-123"}, AvailableSpace: 2048}

	mockProvider.On("CreateVolume", mock.MatchedBy(func(params vsa.CreateVolumeParams) bool {
		return params.RestoreFromSnapshot != nil
	})).Return(expectedResponse, nil)

	result, err := activity.CreateVolumeInONTAP(ctx, volume, node, snapshot, nil, nil)

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
			AccountID: 123,
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

		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(backupVault, nil)

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
			AccountID: 123,
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

		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(backupVault, nil)

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
			AccountID:      123,
		}

		expectedError := errors.New("failed to fetch backup vault")
		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(nil, expectedError)

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
			AccountID:      123,
		}

		mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(nil, errors.New("backup vault not found"))

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
		BucketName:          "bucket-name1",
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
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(backupVault, nil)
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
		BucketName:          "bucket-name1",
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

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(backupVault, nil)
	expectedError := errors.New("failed to update backup vault")
	mockStorage.On("UpdateBackupVault", ctx, backupVault).Return(expectedError)

	err := activity.UpdateBackupVaultWithBucketDetails(ctx, volume, bucketDetails)

	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExists_ReturnsUnexpectedError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(nil, errors.New("Unexpected DB Error"))

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExists_ReturnsCrossRegionError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(&datamodel.BackupVault{BackupVaultType: "CROSS_REGION"}, nil)

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	assert.EqualError(t, err, "Cross region backup vaults are not supported for ISCSI volumes")
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExistsSDE_ReturnsCrossRegionError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockClient := backup_vault.NewMockClientService(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(nil, errors.New("backup vault not found"))

	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := activities.CvpCreateClient
	defer func() { activities.CvpCreateClient = originalCreateClient }()
	activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	bvName := "bv-1"
	bvType := "CROSS_REGION"
	res := []*cvpModels.BackupVaultV1beta{
		{
			ResourceID:      &bvName,
			BackupRegion:    nillable.GetStringPtr("CROSS_REGION"),
			BackupVaultID:   "vault-id",
			State:           "CREATING",
			StateDetails:    "Creation in progress",
			BackupVaultType: &bvType,
		},
	}
	result := backup_vault.V1betaListBackupVaultsOK{Payload: &backup_vault.V1betaListBackupVaultsOKBody{
		BackupVaults: res,
	}}
	mockClient.On("V1betaListBackupVaults", mock.Anything).Return(&result, nil)

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	assert.EqualError(t, err, "Cross region backup vaults are not supported for ISCSI volumes")
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExists_ReturnsImmutableBVError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}
	mrd := int64(1)
	immutableFields := &datamodel.BackupVault{ImmutableAttributes: &datamodel.ImmutableAttributes{BackupMinimumEnforcedRetentionDuration: &mrd}}
	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(immutableFields, nil)
	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	assert.EqualError(t, err, "Immutable backup vaults are not supported for ISCSI volumes")
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultExistsSDE_ReturnsImmutableBVError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", volume.AccountID).Return(nil, errors.New("backup vault not found"))

	mockClient := backup_vault.NewMockClientService(t)
	cvpClient := &cvpapi.Cvp{BackupVault: mockClient}
	originalCreateClient := activities.CvpCreateClient
	defer func() { activities.CvpCreateClient = originalCreateClient }()
	activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
		return *cvpClient
	}
	bvName := "bv-1"
	res := []*cvpModels.BackupVaultV1beta{
		{
			ResourceID:            &bvName,
			BackupRetentionPolicy: &cvpModels.BackupRetentionPolicyV1beta{BackupMinimumEnforcedRetentionDays: nillable.GetInt64Ptr(1)},
			BackupVaultID:         "vault-id",
			State:                 "CREATING",
			StateDetails:          "Creation in progress",
		},
	}

	result := backup_vault.V1betaListBackupVaultsOK{Payload: &backup_vault.V1betaListBackupVaultsOKBody{
		BackupVaults: res,
	}}

	mockClient.On("V1betaListBackupVaults", mock.Anything).Return(&result, nil)

	err := activity.CheckBackupVaultExistsInVCP(ctx, volume, "region")

	assert.Error(t, err)
	assert.EqualError(t, err, "Immutable backup vaults are not supported for ISCSI volumes")
	mockStorage.AssertExpectations(t)
}

func TestBackupVaultVCPError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupVaultID: "vault-id"},
		Account:        &datamodel.Account{Name: "project-number"},
		AccountID:      123,
	}

	mockStorage.On("GetBackupVaultByUUIDndOwnerID", ctx, "vault-id", int64(123)).Return(nil, errors.New("some error"))

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
		mgs := hyperscaler2.NewMockGoogleServices(tt)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("", errors.New("Error finding tenancy unit"))

		tenancyInfo, err := activities.FindTenancy(mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetTenantProjectSucceeds", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("tp-projct", nil)

		tenancyInfo, err := activities.FindTenancy(mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.NoError(tt, err)
		assert.NotNil(tt, tenancyInfo)
	})
	t.Run("WhenGetTenantProjectSucceedsWithEmptyTPRegion", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, "").Return("tp-projct", nil)

		tenancyInfo, err := activities.FindTenancy(mgs, consumerVPC, customerProjectNumber, nil)
		assert.NoError(tt, err)
		assert.NotNil(tt, tenancyInfo)
	})
	t.Run("WhenGetGCPServiceFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
	mockGcpService := hyperscaler2.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(&hyperscaler.ServiceAccount{}, nil)
	mockGcpService.On("AttachOrUpdateRolesForServiceAccounts", mock.Anything, email, projectNumber).Return(nil)
	mockGcpService.On("CreateBucketIfNotExists", mock.Anything, projectNumber, bucketName, tenantProjectRegion).Return(nil)

	account, bucketDetails, err := activities.GetOrCreateAndGCSResources(mockGcpService, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType)

	assert.NoError(t, err)
	assert.NotNil(t, account)
	assert.NotNil(t, bucketDetails)
	assert.Equal(t, bucketName, bucketDetails[0].BucketName)
}

func TestServiceAccountCreationFails(t *testing.T) {
	mockGcpService := hyperscaler2.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(nil, errors.New("service account not found"))
	mockGcpService.On("CreateServiceAccount", &hyperscaler.CreateServiceAccountRequest{
		AccountId: serviceAccountId,
		ServiceAccount: &hyperscaler.ServiceAccount{
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
	mockGcpService := hyperscaler2.NewMockGoogleServices(t)
	serviceAccountId := "test-service-account"
	projectNumber := "test-project"
	email := "test-email"
	bucketName := "test-bucket"
	tenantProjectRegion := "region"
	locationType := "region"

	mockGcpService.On("GetServiceAccount", projectNumber, email).Return(&hyperscaler.ServiceAccount{}, nil)
	mockGcpService.On("AttachOrUpdateRolesForServiceAccounts", mock.Anything, email, projectNumber).Return(nil)
	mockGcpService.On("CreateBucketIfNotExists", mock.Anything, projectNumber, bucketName, tenantProjectRegion).Return(errors.New("failed to create bucket"))

	account, bucketDetails, err := activities.GetOrCreateAndGCSResources(mockGcpService, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType)

	assert.Error(t, err)
	assert.Nil(t, account)
	assert.Nil(t, bucketDetails)
	assert.EqualError(t, err, "failed to create bucket")
}

func TestCreateBucketSuccess(t *testing.T) {
	mockGcpService := hyperscaler2.NewMockGoogleServices(t)
	resourceName := &common.ResourceNames{
		ServiceAccountId: "test-service-account",
		Email:            "test-email",
		BucketName:       "test-bucket",
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	region := "us-central1"

	originalGetOrCreateAndGCSResources := activities.GetOrCreateAndGCSResources
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.GetOrCreateAndGCSResources = originalGetOrCreateAndGCSResources
		hyperscaler2.GetGCPService = originalGetGCPService
	}()

	res := []*common.BucketDetails{
		{
			BucketName: "test-bucket",
		},
	}

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}
	activities.GetOrCreateAndGCSResources = func(gcpServices hyperscaler2.GoogleServices, serviceAccountId, projectNumber, email, bucketName, tenantProjectRegion, locationType string) (*hyperscaler.ServiceAccount, []*common.BucketDetails, error) {
		return nil, res, nil
	}
	activity := activities.VolumeCreateActivity{}
	bucketDetails, err := activity.CreateBucket(context.Background(), resourceName, tenancyDetails, region)

	assert.NoError(t, err)
	assert.NotNil(t, bucketDetails)
	mockGcpService.AssertExpectations(t)
}

func TestCreateBucketGetGcpServiceFails(t *testing.T) {
	mockGcpService := hyperscaler2.NewMockGoogleServices(t)
	resourceName := &common.ResourceNames{
		ServiceAccountId: "test-service-account",
		Email:            "test-email",
		BucketName:       "test-bucket",
	}
	tenancyDetails := &common.TenancyInfo{RegionalTenantProject: "test-project"}
	region := "us-central1"
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
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

func TestUpdateVolumeStateInDB_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-1"
	state := "READY"
	stateDetails := "Available"

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"state":         state,
		"state_details": stateDetails,
	}).Return(nil)

	err := activity.UpdateVolumeStateInDB(ctx, volumeUUID, state, stateDetails)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeStateInDB_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-2"
	state := "FAILED"
	stateDetails := "Error"
	expectedErr := errors.New("db error")

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"state":         state,
		"state_details": stateDetails,
	}).Return(expectedErr)

	err := activity.UpdateVolumeStateInDB(ctx, volumeUUID, state, stateDetails)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockStorage.AssertExpectations(t)
}

func TestInitiateSplitOnVolumeInONTAP(t *testing.T) {
	ctx := context.Background()
	activity := activities.VolumeCreateActivity{}
	node := &models.Node{}
	snapshot := &datamodel.Snapshot{BaseModel: datamodel.BaseModel{UUID: "snapshot-uuid"}}
	volume := &datamodel.Volume{
		Name:        "test-volume",
		SizeInBytes: 107374182400, // 100 GiB
		Svm:         &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "vol-uuid-1",
			SnapReserve:  5,
		},
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

	t.Run("GetProviderByNode Error", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}
		err := activity.InitiateSplitForVolume(ctx, volume, node, snapshot)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("Success", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock the second UpdateVolume call for initiating split
		mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
			return params.UUID == "vol-uuid-1" &&
				params.InitiateSplit == true &&
				params.Size == 0 &&
				params.SnapshotPolicyName == "" &&
				params.SnapReserve == nil
		})).Return(nil).Once()

		err := activity.InitiateSplitForVolume(ctx, volume, node, snapshot)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("UpdateVolumeError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock the second UpdateVolume call (split initiation) to fail
		mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
			return params.InitiateSplit == true
		})).Return(errors.New("failed to initiate split")).Once()

		err := activity.InitiateSplitForVolume(ctx, volume, node, snapshot)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to initiate split")
		mockProvider.AssertExpectations(tt)
	})
}
func TestUpdateClonedVolumeBeforeSplit_WithFileVolumeAndExportPolicy_Success(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	// Set up file protocol support for testing
	utils.SetFileProtocolSupportedForTesting(true)
	utils.SetFileProtocolAllowlistedAccountsForTesting("test-account")
	defer func() {
		// Clean up environment variables after test
		utils.SetFileProtocolSupportedForTesting(false)
		utils.SetFileProtocolAllowlistedAccountsForTesting("")
	}()

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data
	exportPolicyName := "test-export-policy"
	junctionPath := "/test/junction/path"

	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account", // This should be a file protocol account
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:      "test-external-uuid",
			SnapReserve:       10,
			Protocols:         []string{"NFS"}, // Add this line - NAS protocol for file volume
			SnapshotDirectory: true,
			FileProperties: &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: exportPolicyName,
				},
				JunctionPath: junctionPath,
			},
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{
		Name:            "test-node",
		ExternalUUID:    "test-node-uuid",
		EndpointAddress: "test-endpoint",
		State:           "online",
	}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)

	// Mock first UpdateVolume call (SnapReserve = 0)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.SnapReserve != nil && *params.SnapReserve == 0 &&
			params.Size == 0 &&
			params.SnapshotPolicyName == ""
	})).Return(nil)

	// Mock second UpdateVolume call (with size, snapshot policy, export policy, junction path)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.Size == volume.SizeInBytes &&
			params.SnapshotPolicyName == volume.SnapshotPolicy.Name &&
			params.SnapReserve != nil && *params.SnapReserve == volume.VolumeAttributes.SnapReserve &&
			params.ExportPolicy != nil && *params.ExportPolicy == exportPolicyName &&
			params.JunctionPath != nil && *params.JunctionPath == junctionPath
	})).Return(nil)

	// Mock GetVolume call that happens after UpdateVolume
	expectedVolumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
		},
		State: "online",
	}
	mockProvider.On("GetVolume", mock.MatchedBy(func(params vsa.GetVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.VolumeName == volume.Name &&
			params.SvmName == volume.Svm.Name
	})).Return(expectedVolumeResponse, nil)

	// Mock hyperscaler2.GetProviderByNode (note: use hyperscaler2, not hyperscaler)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.NoError(t, err)

	// Verify that UpdateVolume was called twice - once for SnapReserve and once for other parameters
	// The mock setup above already handles the expectations

	mockProvider.AssertExpectations(t)
}

func TestUpdateClonedVolumeBeforeSplit_WithNonFileVolume_Success(t *testing.T) {
	// This test covers the case where file protocol is not supported
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data for non-file volume
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "block-account", // This should be a block protocol account
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10,
			// No FileProperties for block volume
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{
		Name:            "test-node",
		ExternalUUID:    "test-node-uuid",
		EndpointAddress: "test-endpoint",
		State:           "online",
	}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)

	// Mock first UpdateVolume call (SnapReserve = 0)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.SnapReserve != nil && *params.SnapReserve == 0 &&
			params.Size == 0 &&
			params.SnapshotPolicyName == ""
	})).Return(nil)

	// Mock second UpdateVolume call (with size, snapshot policy, no export policy or junction path for block volume)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.Size == volume.SizeInBytes &&
			params.SnapshotPolicyName == volume.SnapshotPolicy.Name &&
			params.SnapReserve != nil && *params.SnapReserve == volume.VolumeAttributes.SnapReserve &&
			params.ExportPolicy == nil &&
			params.JunctionPath == nil
	})).Return(nil)

	// Mock GetVolume call that happens after UpdateVolume
	expectedVolumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
		},
		State: "online",
	}
	mockProvider.On("GetVolume", mock.MatchedBy(func(params vsa.GetVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.VolumeName == volume.Name &&
			params.SvmName == volume.Svm.Name
	})).Return(expectedVolumeResponse, nil)

	// Mock hyperscaler2.GetProviderByNode
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.NoError(t, err)

	// Verify that UpdateVolume was called twice - once for SnapReserve and once for other parameters (without ExportPolicy and JunctionPath)
	// The mock setup above already handles the expectations

	mockProvider.AssertExpectations(t)
}

func TestUpdateClonedVolumeBeforeSplit_WithFileVolumeButNoExportPolicy_Success(t *testing.T) {
	// This test covers the case where file protocol is supported but ExportPolicy is nil
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data for file volume without ExportPolicy
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{UUID: "test-account-uuid"},
			Name:      "test-account", // This should be a file protocol account
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10,
			FileProperties: &datamodel.FileProperties{
				// ExportPolicy is nil
				JunctionPath: "/test/junction/path",
			},
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{
		Name:            "test-node",
		ExternalUUID:    "test-node-uuid",
		EndpointAddress: "test-endpoint",
		State:           "online",
	}
	// Mock the provider
	mockProvider := new(vsa.MockProvider)

	// Mock first UpdateVolume call (SnapReserve = 0)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.SnapReserve != nil && *params.SnapReserve == 0 &&
			params.Size == 0 &&
			params.SnapshotPolicyName == ""
	})).Return(nil)

	// Mock second UpdateVolume call (with size, snapshot policy, no export policy or junction path for file volume without export policy)
	mockProvider.On("UpdateVolume", mock.MatchedBy(func(params vsa.UpdateVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.Size == volume.SizeInBytes &&
			params.SnapshotPolicyName == volume.SnapshotPolicy.Name &&
			params.SnapReserve != nil && *params.SnapReserve == volume.VolumeAttributes.SnapReserve &&
			params.ExportPolicy == nil &&
			params.JunctionPath == nil
	})).Return(nil)

	// Mock GetVolume call that happens after UpdateVolume
	expectedVolumeResponse := &vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
		},
		State: "online",
	}
	mockProvider.On("GetVolume", mock.MatchedBy(func(params vsa.GetVolumeParams) bool {
		return params.UUID == volume.VolumeAttributes.ExternalUUID &&
			params.VolumeName == volume.Name &&
			params.SvmName == volume.Svm.Name
	})).Return(expectedVolumeResponse, nil)

	// Mock hyperscaler2.GetProviderByNode
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.NoError(t, err)

	// Verify that UpdateVolume was called twice - once for SnapReserve and once for other parameters (without ExportPolicy and JunctionPath)
	// The mock setup above already handles the expectations

	mockProvider.AssertExpectations(t)
}

func TestUpdateClonedVolumeBeforeSplit_ProviderError(t *testing.T) {
	// This test covers the case where GetProviderByNode returns an error (line 822)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10,
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{}

	// Mock hyperscaler2.GetProviderByNode to return an error
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider error")
}

func TestUpdateClonedVolumeBeforeSplit_UpdateVolumeError(t *testing.T) {
	// This test covers the case where updateVolume returns an error (line 837)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10,
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)
	mockProvider.On("UpdateVolume", mock.AnythingOfType("vsa.UpdateVolumeParams")).Return(errors.New("update volume failed"))

	// Mock hyperscaler2.GetProviderByNode
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "update volume failed")
	mockProvider.AssertExpectations(t)
}

func TestUpdateClonedVolumeBeforeSplit_GetVolumeError(t *testing.T) {
	// This test covers the case where GetVolume returns an error (lines 847-848)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{"key": "value"})

	mockStorage := database.NewMockStorage(t)
	mockScheduler := &scheduler.TemporalScheduler{}
	activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: mockScheduler}

	// Create test data
	volume := &datamodel.Volume{
		BaseModel:   datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:        "test-volume",
		SizeInBytes: 1073741824, // 1GB
		Svm: &datamodel.Svm{
			Name: "test-svm",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "test-external-uuid",
			SnapReserve:  10,
		},
		SnapshotPolicy: &datamodel.SnapshotPolicy{
			Name: "test-snapshot-policy",
		},
	}

	node := &models.Node{}

	// Mock the provider
	mockProvider := new(vsa.MockProvider)
	mockProvider.On("UpdateVolume", mock.AnythingOfType("vsa.UpdateVolumeParams")).Return(nil)
	mockProvider.On("GetVolume", vsa.GetVolumeParams{
		UUID:       "test-external-uuid",
		VolumeName: "test-volume",
		SvmName:    "test-svm",
	}).Return(nil, errors.New("get volume failed"))

	// Mock hyperscaler2.GetProviderByNode
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() {
		hyperscaler2.GetProviderByNode = originalGetProviderByNode
	}()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	// Act
	_, err := activity.UpdateClonedVolumeBeforeSplit(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "get volume failed")
	mockProvider.AssertExpectations(t)
}

func TestUpdateLunName(t *testing.T) {
	t.Run("TestUpdateLunNameSuccess", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		ctx := context.Background()
		activity := activities.VolumeCreateActivity{}
		node := &models.Node{}
		volume := &datamodel.Volume{
			Name: "test-volume",
			Svm:  &datamodel.Svm{Name: "test-svm"},
		}
		lunResponse := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "lun-uuid-123",
			},
			SerialNumber: "lun_test-volume",
		}

		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil)
		mockProvider.On("LunUpdate", mock.Anything).Return(nil)
		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil)
		ontapRes := &vsa.VolumeResponse{
			Size:         1073741824,
			AFSSize:      1073741824,
			MetadataSize: 12345,
		}
		lun, err := activity.UpdateLunName(ctx, volume, node, ontapRes)

		assert.NoError(t, err)
		assert.NotNil(t, lun)
		assert.Equal(t, lunResponse, lun)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestUpdateLunNameLunNotFoundInitially", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		ctx := context.Background()
		activity := activities.VolumeCreateActivity{}
		node := &models.Node{}
		volume := &datamodel.Volume{
			Name: "test-volume",
			Svm:  &datamodel.Svm{Name: "test-svm"},
		}

		mockProvider.On("LunGet", mock.Anything).Return(nil, errors.New("lun not found"))
		ontapRes := &vsa.VolumeResponse{
			Size:         1073741824,
			AFSSize:      1073741824,
			MetadataSize: 12345,
		}
		lun, err := activity.UpdateLunName(ctx, volume, node, ontapRes)

		assert.Error(t, err)
		assert.Nil(t, lun)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestUpdateLunNameLunUpdateFails", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		ctx := context.Background()
		activity := activities.VolumeCreateActivity{}
		node := &models.Node{}
		volume := &datamodel.Volume{
			Name: "test-volume",
			Svm:  &datamodel.Svm{Name: "test-svm"},
		}
		lunResponse := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "lun-uuid-123",
			},
			SerialNumber: "lun_test-volume",
		}
		ontapRes := &vsa.VolumeResponse{
			Size:         1073741824,
			AFSSize:      1073741824,
			MetadataSize: 12345,
		}
		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil)
		mockProvider.On("LunUpdate", mock.Anything).Return(errors.New("failed to update lun"))

		lun, err := activity.UpdateLunName(ctx, volume, node, ontapRes)

		assert.Error(t, err)
		assert.Nil(t, lun)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestUpdateLunNameLunNotFoundAfterUpdate", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		ctx := context.Background()
		activity := activities.VolumeCreateActivity{}
		node := &models.Node{}
		volume := &datamodel.Volume{
			Name: "test-volume",
			Svm:  &datamodel.Svm{Name: "test-svm"},
		}
		lunResponse := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "lun-uuid-123",
			},
			SerialNumber: "lun_test-volume",
		}
		ontapRes := &vsa.VolumeResponse{
			Size:         1073741824,
			AFSSize:      1073741824,
			MetadataSize: 12345,
		}

		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil).Once()
		mockProvider.On("LunUpdate", mock.Anything).Return(nil)
		mockProvider.On("LunGet", mock.Anything).Return(nil, errors.New("lun not found"))

		lun, err := activity.UpdateLunName(ctx, volume, node, ontapRes)

		assert.Error(t, err)
		assert.Nil(t, lun)
		mockProvider.AssertExpectations(t)
	})

	t.Run("TestUpdateLunNameWhenLunSpaceLessThanLunSize", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		ctx := context.Background()
		activity := activities.VolumeCreateActivity{}
		node := &models.Node{}
		volume := &datamodel.Volume{
			Name: "test-volume",
			Svm:  &datamodel.Svm{Name: "test-svm"},
		}
		lunResponse := &vsa.LunResponse{
			ProviderResponse: vsa.ProviderResponse{
				ExternalUUID: "lun-uuid-123",
			},
			Size: 2048,
		}
		ontapRes := &vsa.VolumeResponse{
			Size:         10737418240, // 10GB
			AFSSize:      10737418240,
			MetadataSize: 12345,
		}
		// Calculate expected size based on actual implementation: AFSSize - MetadataSize
		expectedLunSize := ontapRes.AFSSize - ontapRes.MetadataSize

		// Mock the first LunGet call
		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil).Once()
		// Mock LunUpdate with expectedLunSize
		mockProvider.On("LunUpdate", vsa.LunUpdateParams{
			UUID:       "lun-uuid-123",
			LunName:    "lun_test-volume",
			VolumeName: "test-volume",
			SvmName:    "test-svm",
			Size:       expectedLunSize,
		}).Return(nil)
		// Mock the second LunGet call
		mockProvider.On("LunGet", mock.Anything).Return(lunResponse, nil)

		// Act
		lun, err := activity.UpdateLunName(ctx, volume, node, ontapRes)

		// Assert
		assert.NoError(tt, err)
		assert.NotNil(tt, lun)
		assert.Equal(tt, lunResponse, lun)
		mockProvider.AssertExpectations(tt)
	})
}

func TestCheckIfBackupPolicyExistsInVCP(t *testing.T) {
	ctx := context.Background()
	backupPolicyUUID := "test-uuid"
	accountId := int64(123)

	t.Run("ReturnsTrueIfBackupPolicyExists", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockBackupPolicy := &datamodel.BackupPolicy{}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).Return(mockBackupPolicy, nil)
		ok, err := activity.CheckIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.NoError(t, err)
		assert.True(t, ok)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReturnsFalseIfBackupPolicyNotFound", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).
			Return(nil, utilErrors.NewNotFoundErr("backup policy", &backupPolicyUUID))
		ok, err := activity.CheckIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.NoError(t, err)
		assert.False(t, ok)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReturnsErrorIfOtherError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockStorage.On("GetBackupPolicyByUUIDAndOwnerID", ctx, backupPolicyUUID, accountId).
			Return(nil, utilErrors.New("db error"))
		ok, err := activity.CheckIfBackupPolicyExistsInVCP(ctx, backupPolicyUUID, accountId)
		assert.Error(t, err)
		assert.False(t, ok)
		mockStorage.AssertExpectations(t)
	})
}

func TestCreateBackupPolicyFetchedFromSDESucceeds(t *testing.T) {
	ctx := context.Background()
	region := "us-central1"
	backupPolicyUUID := "test-backup-policy-uuid"
	accountName := "test-account"
	accountID := int64(123)

	volume := &datamodel.Volume{
		DataProtection: &datamodel.DataProtection{BackupPolicyID: backupPolicyUUID},
		Account:        &datamodel.Account{Name: accountName},
		AccountID:      accountID,
	}

	t.Run("CreateBackupPolicyFetchedFromSDESucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := activities.CvpCreateClient
		defer func() { activities.CvpCreateClient = originalCreateClient }()
		activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockBackupPolicy := &cvpModels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpModels.BackupPolicyV1beta{
				BackupPolicyID: backupPolicyUUID,
				State:          models.LifeCycleStateREADY,
			},
		}
		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: mockBackupPolicy,
		}, nil)
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(&datamodel.BackupPolicy{BaseModel: datamodel.BaseModel{UUID: backupPolicyUUID}, AccountID: accountID, LifeCycleState: models.LifeCycleStateREADY, LifeCycleStateDetails: models.LifeCycleStateAvailableDetails}, nil)

		res, err := activity.CreateBackupPolicyFetchedFromSDE(ctx, volume, region)
		assert.NoError(t, err)
		assert.NotNil(t, res)
		assert.Equal(t, backupPolicyUUID, res.UUID)
		assert.Equal(t, accountID, res.AccountID)
	})

	t.Run("CreateBackupPolicyFetchedFromSDEFailsWhenCVPReturnsError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := activities.CvpCreateClient
		defer func() { activities.CvpCreateClient = originalCreateClient }()
		activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(nil, errors.New("cvp error"))
		res, err := activity.CreateBackupPolicyFetchedFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("CreateBackupPolicyFetchedFromSDEFailsWhenCVPReturnsNil", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)
		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := activities.CvpCreateClient
		defer func() { activities.CvpCreateClient = originalCreateClient }()
		activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: nil,
		}, nil)

		res, err := activity.CreateBackupPolicyFetchedFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})

	t.Run("CreateBackupPolicyFetchedFromSDEFailsWithDBError", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockStorage}
		mockClient := backup_policy.NewMockClientService(t)

		cvpClient := &cvpapi.Cvp{BackupPolicy: mockClient}
		originalCreateClient := activities.CvpCreateClient
		defer func() { activities.CvpCreateClient = originalCreateClient }()
		activities.CvpCreateClient = func(logger log.Logger, jwtToken string) cvpapi.Cvp {
			return *cvpClient
		}

		mockBackupPolicy := &cvpModels.BackupPolicyDetailsV1beta{
			BackupPolicyV1beta: cvpModels.BackupPolicyV1beta{
				BackupPolicyID: backupPolicyUUID,
				State:          models.LifeCycleStateREADY,
			},
		}
		mockClient.On("V1betaDescribeBackupPolicy", mock.Anything).Return(&backup_policy.V1betaDescribeBackupPolicyOK{
			Payload: mockBackupPolicy,
		}, nil)
		mockStorage.On("CreateBackupPolicyEntryInVCP", ctx, mock.Anything).Return(nil, errors.New("db error"))

		res, err := activity.CreateBackupPolicyFetchedFromSDE(ctx, volume, region)
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}

func TestCreateExportPolicyInOntap(t *testing.T) {
	t.Run("Success_FileVolume", func(t *testing.T) {
		// Mock setup
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock provider setup
		mockProvider := vsa.NewMockProvider(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Test data - Volume with file properties
		volume := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				FileProperties: &datamodel.FileProperties{
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "test-export-policy",
						ExportRules: []*datamodel.ExportRule{
							{
								AllowedClients: "10.0.0.0/8",
								AccessType:     "ReadWrite",
								UnixReadOnly:   false,
								UnixReadWrite:  true,
							},
						},
					},
				},
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name:            "test-node",
			EndpointAddress: "192.168.1.100",
		}

		// Mock expectations
		expectedExportPolicy := &vsa.ExportPolicy{
			ExportPolicyName: "test-export-policy",
			SvmName:          "test-svm",
			ExportRules: []*vsa.ExportRule{
				{
					AllowedClients: "10.0.0.0/8",
					AccessType:     "ReadWrite",
				},
			},
		}
		mockProvider.EXPECT().CreateExportPolicy(expectedExportPolicy).Return(nil)

		// Execute test
		err := activity.CreateExportPolicyInOntap(ctx, volume, node)

		// Assertions
		assert.NoError(t, err)
	})

	t.Run("Skip_NonFileVolume", func(t *testing.T) {
		// Mock setup
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Test data for block volume (no FileProperties)
		volume := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				FileProperties: nil, // Block volume has no file properties
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name:            "test-node",
			EndpointAddress: "192.168.1.100",
		}

		// Execute test
		err := activity.CreateExportPolicyInOntap(ctx, volume, node)

		// Assertions - should return nil for non-file volumes
		assert.NoError(t, err)
	})

	t.Run("Success_ExportPolicyConflict", func(t *testing.T) {
		// Mock setup
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock provider setup
		mockProvider := vsa.NewMockProvider(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Test data
		volume := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				FileProperties: &datamodel.FileProperties{
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "existing-export-policy",
						ExportRules: []*datamodel.ExportRule{
							{
								AllowedClients: "192.168.1.0/24",
								AccessType:     "ReadOnly",
							},
						},
					},
				},
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name:            "test-node",
			EndpointAddress: "192.168.1.100",
		}

		// Mock expectations - simulate conflict error
		conflictError := utilErrors.NewConflictErr("export policy already exists")
		mockProvider.EXPECT().CreateExportPolicy(mock.Anything).Return(conflictError)

		// Execute test
		err := activity.CreateExportPolicyInOntap(ctx, volume, node)

		// Assertions - should return nil on conflict (graceful handling)
		assert.NoError(t, err)
	})

	t.Run("Error_ProviderError", func(t *testing.T) {
		// Mock setup
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock provider setup
		mockProvider := vsa.NewMockProvider(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Test data
		volume := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				FileProperties: &datamodel.FileProperties{
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "test-export-policy",
						ExportRules: []*datamodel.ExportRule{
							{
								AllowedClients: "10.0.0.0/8",
								AccessType:     "ReadWrite",
							},
						},
					},
				},
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name:            "test-node",
			EndpointAddress: "192.168.1.100",
		}

		// Mock expectations - simulate general error (not conflict)
		providerError := errors.New("provider connection failed")
		mockProvider.EXPECT().CreateExportPolicy(mock.Anything).Return(providerError)

		// Execute test
		err := activity.CreateExportPolicyInOntap(ctx, volume, node)

		// Assertions - should return the provider error
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "provider connection failed")
	})

	t.Run("Success_MultipleExportRules", func(t *testing.T) {
		// Mock setup
		mockStorage := database.NewMockStorage(t)
		activity := activities.VolumeCreateActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

		// Mock provider setup
		mockProvider := vsa.NewMockProvider(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Test data with multiple export rules
		volume := &datamodel.Volume{
			VolumeAttributes: &datamodel.VolumeAttributes{
				FileProperties: &datamodel.FileProperties{
					ExportPolicy: &datamodel.ExportPolicy{
						ExportPolicyName: "multi-rule-policy",
						ExportRules: []*datamodel.ExportRule{
							{
								AllowedClients: "10.0.0.0/8",
								AccessType:     "ReadWrite",
								CIFS:           false,
								NFSv3:          true,
								NFSv4:          true,
								Index:          1,
								AnonymousUser:  "nobody",
							},
							{
								AllowedClients: "192.168.1.0/24",
								AccessType:     "ReadOnly",
								CIFS:           true,
								NFSv3:          false,
								NFSv4:          true,
								Index:          2,
								AnonymousUser:  "anonymous",
							},
						},
					},
				},
			},
			Svm: &datamodel.Svm{
				Name: "test-svm",
			},
		}

		node := &models.Node{
			Name:            "test-node",
			EndpointAddress: "192.168.1.100",
		}

		// Mock expectations
		expectedExportPolicy := &vsa.ExportPolicy{
			ExportPolicyName: "multi-rule-policy",
			SvmName:          "test-svm",
			ExportRules: []*vsa.ExportRule{
				{
					AllowedClients: "10.0.0.0/8",
					AccessType:     "ReadWrite",
					CIFS:           false,
					NFSv3:          true,
					NFSv4:          true,
					Index:          1,
					AnonymousUser:  "nobody",
				},
				{
					AllowedClients: "192.168.1.0/24",
					AccessType:     "ReadOnly",
					CIFS:           true,
					NFSv3:          false,
					NFSv4:          true,
					Index:          2,
					AnonymousUser:  "anonymous",
				},
			},
		}
		mockProvider.EXPECT().CreateExportPolicy(expectedExportPolicy).Return(nil)

		// Execute test
		err := activity.CreateExportPolicyInOntap(ctx, volume, node)

		// Assertions
		assert.NoError(t, err)
	})
}

func TestCreateBackupPolicySchedule(t *testing.T) {
	t.Run("CreateBackupPolicyScheduleSucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockClient := &mocks.ScheduleClient{}
		temporalScheduler := scheduler.NewTemporalScheduler(mockClient)
		activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: temporalScheduler}

		ctx := context.Background()
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "policy-uuid",
			},
			Name: "test-policy",
		}

		schedulerHandle := &mocks.ScheduleHandle{}
		schedulerHandle.On("GetID").Return("schedule-id")
		mockClient.On("Create", ctx, mock.Anything).Return(schedulerHandle, nil).Once()

		err := activity.CreateBackupPolicySchedule(ctx, backupPolicy, "")
		assert.NoError(t, err)
	})
	t.Run("CreateBackupPolicyScheduleFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockClient := &mocks.ScheduleClient{}
		temporalScheduler := scheduler.NewTemporalScheduler(mockClient)
		activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: temporalScheduler}

		ctx := context.Background()
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "policy-uuid",
			},
			Name: "test-policy",
		}

		schedulerHandle := &mocks.ScheduleHandle{}
		schedulerHandle.On("GetID").Return("schedule-id")
		mockClient.On("Create", ctx, mock.Anything).Return(nil, errors.New("failed to create schedule")).Times(scheduler.DefaultMaxRetries)

		err := activity.CreateBackupPolicySchedule(ctx, backupPolicy, "")
		assert.Error(t, err, "failed to create schedule")
	})
	t.Run("CreateBackupPolicyScheduleWithCustomScheduleSucceeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockClient := &mocks.ScheduleClient{}
		temporalScheduler := scheduler.NewTemporalScheduler(mockClient)
		activity := activities.VolumeCreateActivity{SE: mockStorage, Scheduler: temporalScheduler}

		ctx := context.Background()
		backupPolicy := &datamodel.BackupPolicy{
			BaseModel: datamodel.BaseModel{
				UUID: "policy-uuid",
			},
			Name: "test-policy",
		}
		customSchedule := "0 2 * * *" // Daily at 2 AM

		schedulerHandle := &mocks.ScheduleHandle{}
		schedulerHandle.On("GetID").Return("schedule-id")
		mockClient.On("Create", ctx, mock.Anything).Return(schedulerHandle, nil).Once()

		err := activity.CreateBackupPolicySchedule(ctx, backupPolicy, customSchedule)
		assert.NoError(t, err)
	})
}

func TestGetVolumesByPoolID(t *testing.T) {
	t.Run("WhenGetVolumesByPoolIdReturnsVolumes", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockSE}
		ctx := context.Background()
		poolID := int64(1)
		vol1 := &datamodel.Volume{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		var volumes []*datamodel.Volume
		volumes = append(volumes, vol1)

		mockSE.On("GetVolumesByPoolID", ctx, poolID).Return(volumes, nil)
		result, err := activity.GetVolumesByPoolID(ctx, poolID)
		assert.NoError(t, err)
		assert.Equal(t, volumes, result)
	})
	t.Run("WhenGetVolumesByPoolIdReturnsError", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.VolumeCreateActivity{SE: mockSE}
		ctx := context.Background()
		poolID := int64(1)

		mockSE.On("GetVolumesByPoolID", ctx, poolID).Return(nil, vsaerrors.WrapAsTemporalApplicationError(errors.New("get volumes ran into error")))
		result, err := activity.GetVolumesByPoolID(ctx, poolID)
		assert.Error(t, err)
		assert.EqualError(t, err, "get volumes ran into error")
		assert.Nil(t, result)
	})
}

func TestUpdateVolumeAttributesInDB_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-3"
	volumeAttributes := &datamodel.VolumeAttributes{
		ExternalUUID: "ext-uuid-123",
		Protocols:    []string{"iscsi"},
		SnapReserve:  10,
	}

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": volumeAttributes,
	}).Return(nil)

	err := activity.UpdateVolumeAttributesInDB(ctx, volumeUUID, volumeAttributes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeAttributesInDB_WithNilAttributes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-4"
	var volumeAttributes *datamodel.VolumeAttributes = nil

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": volumeAttributes,
	}).Return(nil)

	err := activity.UpdateVolumeAttributesInDB(ctx, volumeUUID, volumeAttributes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeAttributesInDB_Error(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := "vol-uuid-5"
	volumeAttributes := &datamodel.VolumeAttributes{
		ExternalUUID: "ext-uuid-456",
		Protocols:    []string{"nfs"},
		SnapReserve:  5,
	}
	expectedErr := errors.New("database update failed")

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": volumeAttributes,
	}).Return(expectedErr)

	err := activity.UpdateVolumeAttributesInDB(ctx, volumeUUID, volumeAttributes)
	assert.Error(t, err)
	// Check that the error is wrapped as a temporal application error
	assert.Contains(t, err.Error(), "database update failed")
	mockStorage.AssertExpectations(t)
}

func TestUpdateVolumeAttributesInDB_EmptyUUID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	volumeUUID := ""
	volumeAttributes := &datamodel.VolumeAttributes{
		ExternalUUID: "ext-uuid-789",
		Protocols:    []string{"smb"},
	}

	mockStorage.On("UpdateVolumeFields", ctx, volumeUUID, map[string]interface{}{
		"volume_attributes": volumeAttributes,
	}).Return(nil)

	err := activity.UpdateVolumeAttributesInDB(ctx, volumeUUID, volumeAttributes)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetAggregatesFromOntap(t *testing.T) {
	// Setup
	mockStorage := database.NewMockStorage(t)
	activity := &activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.Background()
	node := &models.Node{EndpointAddress: "127.0.0.1"}

	// Original function to restore after tests
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	t.Run("Success_WithLargeVolumeConstituentCount", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider)
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Setup mock volume
		constituentCount := int32(8)
		volume := &datamodel.Volume{
			SizeInBytes: 8 * utils.TiBInBytes,
			LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
				LargeCapacity:               true,
				LargeVolumeConstituentCount: &constituentCount,
			},
			Pool: &datamodel.Pool{AllowAutoTiering: false},
		}

		// Create mock aggregates - exactly 6 aggregates as required
		mockAggregates := make([]*vsa.Aggregate, 0, 6)
		for i := 1; i <= 6; i++ {
			mockAggregates = append(mockAggregates, &vsa.Aggregate{
				Name:        fmt.Sprintf("aggr%d", i),
				State:       "online",
				VolumeCount: 10,
				Size:        2 * utils.TiBInBytes,
			})
		}

		// Setup expected result
		expectedResult := &models.AggregateDistributionResult{
			Aggregates:     []string{"aggr1", "aggr2", "aggr3", "aggr4", "aggr5", "aggr6", "aggr1", "aggr2"},
			AggrMultiplier: 1,
		}

		// Mock provider.GetAggregates to return our mock aggregates
		mockProvider.On("GetAggregates").Return(mockAggregates, nil)

		// Act
		result, err := activity.GetAggregatesFromOntap(ctx, volume, node, 12)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(8), int64(*volume.LargeVolumeAttributes.LargeVolumeConstituentCount))
		assert.Len(t, result.Aggregates, len(expectedResult.Aggregates))
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_GetProviderByNodeFails", func(t *testing.T) {
		// Arrange
		expectedErr := errors.New("failed to get provider")
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, expectedErr
		}

		volume := &datamodel.Volume{}

		// Act
		result, err := activity.GetAggregatesFromOntap(ctx, volume, node, 12)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, expectedErr.Error())
	})

	t.Run("Error_GetAggregatesFails", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider)
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		expectedErr := errors.New("failed to get aggregates")
		mockProvider.On("GetAggregates").Return(nil, expectedErr)

		volume := &datamodel.Volume{}

		// Act
		result, err := activity.GetAggregatesFromOntap(ctx, volume, node, 12)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.EqualError(t, err, expectedErr.Error())
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_CalculateAggregatesForConstituentVolumesFails", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider)
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Create mock aggregates - only 5 aggregates (not the expected 6)
		mockAggregates := make([]*vsa.Aggregate, 0, 5)
		for i := 1; i <= 5; i++ {
			mockAggregates = append(mockAggregates, &vsa.Aggregate{
				Name:        fmt.Sprintf("aggr%d", i),
				State:       "online",
				VolumeCount: 10,
			})
		}

		mockProvider.On("GetAggregates").Return(mockAggregates, nil)

		constituentCount := int32(8)
		volume := &datamodel.Volume{
			LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
				LargeCapacity:               true,
				LargeVolumeConstituentCount: &constituentCount,
			},
			Pool: &datamodel.Pool{AllowAutoTiering: false},
		}

		// Act
		result, err := activity.GetAggregatesFromOntap(ctx, volume, node, 12)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "expected exactly 6 aggregates")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error_AggregateNotOnline", func(t *testing.T) {
		// Arrange
		mockProvider := new(vsa.MockProvider)
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Create mock aggregates with one offline aggregate
		mockAggregates := make([]*vsa.Aggregate, 0, 6)
		for i := 1; i <= 6; i++ {
			state := "online"
			if i == 3 {
				state = "offline" // Make one aggregate offline
			}
			mockAggregates = append(mockAggregates, &vsa.Aggregate{
				Name:          fmt.Sprintf("aggr%d", i),
				State:         state,
				VolumeCount:   10,
				AvailableSize: utils.TiBInBytes,
			})
		}

		mockProvider.On("GetAggregates").Return(mockAggregates, nil)

		constituentCount := int32(8)
		volume := &datamodel.Volume{
			SizeInBytes: utils.TiBInBytes,
			LargeVolumeAttributes: &datamodel.LargeVolumeAttributes{
				LargeCapacity:               true,
				LargeVolumeConstituentCount: &constituentCount,
			},
			Pool: &datamodel.Pool{AllowAutoTiering: false},
		}

		// Act
		result, err := activity.GetAggregatesFromOntap(ctx, volume, node, 12)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "is not online")
		mockProvider.AssertExpectations(t)
	})
}

func TestLunSizeUpdateValidation_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 0,
		},
		SizeInBytes: int64(1024),
	}
	node := &models.Node{}
	lunSize := int64(1024) // 1GB (smaller than available space)

	// Mock LunGet to return a lun with smaller size
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "",
	}).Return(&vsa.LunResponse{
		Size: lunSize,
	}, nil)

	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func TestLunSizeUpdateValidation_ProviderError(t *testing.T) {
	// Arrange
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return nil, errors.New("provider error")
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 0,
		},
	}
	node := &models.Node{}
	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "provider error")
}

func TestLunSizeUpdateValidation_LunNotFound(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 0,
		},
	}
	node := &models.Node{}

	// Mock LunGet to return error (lun not found)
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "",
	}).Return(nil, errors.New("lun not found"))

	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "lun not found")
	mockProvider.AssertExpectations(t)
}

func TestLunSizeUpdateValidation_SizeReductionWithSnapReserveZero(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 0,
		},
	}
	node := &models.Node{}
	lunSize := int64(2048) // 2GB (larger than available space)

	// Mock LunGet to return a lun with larger size
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "",
	}).Return(&vsa.LunResponse{
		Size: lunSize,
	}, nil)

	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error restoring volume - Cannot restore the Volume with the specified size. Please increase the volume size")
	mockProvider.AssertExpectations(t)
}

func TestLunSizeUpdateValidation_SizeReductionWithSnapReserveGreaterThanZero(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 20, // 20% snap reserve
		},
	}
	node := &models.Node{}
	lunSize := int64(2048) // 2GB (larger than available space)

	// Mock LunGet to return a lun with larger size
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "",
	}).Return(&vsa.LunResponse{
		Size: lunSize,
	}, nil)

	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error restoring volume - Cannot restore the Volume with the specified size. Please increase the volume size")
	mockProvider.AssertExpectations(t)
}

func TestLunSizeUpdateValidation_ExactSizeMatch(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider)
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.VolumeCreateActivity{
		SE: database.NewMockStorage(t),
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		Name: "test-volume",
		Svm:  &datamodel.Svm{Name: "test-svm"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			SnapReserve: 0,
		},
		SizeInBytes: int64(1024),
	}
	node := &models.Node{}
	lunSize := int64(1024) // 1GB (exact match)

	// Mock LunGet to return a lun with same size
	mockProvider.On("LunGet", vsa.LunGetParams{
		SvmName:    "test-svm",
		VolumeName: "test-volume",
		LunName:    "",
	}).Return(&vsa.LunResponse{
		Size: lunSize,
	}, nil)

	// Act
	err := activity.LunSizeUpdateValidation(ctx, volume, node)

	// Assert
	assert.NoError(t, err)
	mockProvider.AssertExpectations(t)
}

func Test_grantStorageObjectAdminRole(t *testing.T) {
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		serviceAccountEmail := "adc-sa@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}

		mockGCPService := new(hyperscaler2.MockGoogleServices)
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", roles, serviceAccountEmail, projectID).Return(nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return mockGCPService, nil
		}
		err := activities.GrantStorageObjectAdminRole(ctx, serviceAccountEmail, projectID)
		assert.NoError(t, err)
	})
	t.Run("GetCloudServiceReturnError", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		serviceAccountEmail := "adc-sa@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}

		mockGCPService := new(hyperscaler2.MockGoogleServices)
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", roles, serviceAccountEmail, projectID).Return(nil)

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return nil, errors.New("failed to get cloud service")
		}
		err := activities.GrantStorageObjectAdminRole(ctx, serviceAccountEmail, projectID)
		assert.Error(t, err)
	})
	t.Run("OnSuccess", func(t *testing.T) {
		ctx := context.Background()
		projectID := "test-project"
		serviceAccountEmail := "adc-sa@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}

		mockGCPService := new(hyperscaler2.MockGoogleServices)
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", roles, serviceAccountEmail, projectID).Return(errors.New("failed to attach role"))

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return mockGCPService, nil
		}
		err := activities.GrantStorageObjectAdminRole(ctx, serviceAccountEmail, projectID)
		assert.Error(t, err)
	})
}

func TestDeleteRolesForServiceAccountInBackupTenantProject(t *testing.T) {
	t.Run("WhenGetPoolServiceAccountNameFails", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: ""}, // Empty project will cause error
		}
		backup := &datamodel.Backup{}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup not found")
	})

	t.Run("WhenGetBackupTenantProjectFails", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "different-bucket", // Mismatch will cause error
						TenantProjectNumber: "backup-tenant-project",
					},
				},
			},
		}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup")
	})

	t.Run("WhenBackupAttributesIsNil", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backup := &datamodel.Backup{
			Attributes: nil, // Nil attributes will cause error
		}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup")
	})

	t.Run("WhenBackupVaultIsNil", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: nil, // Nil backup vault will cause error
		}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup")
	})

	t.Run("WhenBucketDetailsIsEmpty", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}

		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{}, // Empty bucket details will cause error
			},
		}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)

		// Assertions
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup")
	})
	t.Run("WhenSuccess", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}
		mockGCPService := new(hyperscaler2.MockGoogleServices)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backupTp := "backup-tenant-project"
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket", // Mismatch will cause error
						TenantProjectNumber: backupTp,
					},
				},
			},
		}

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return mockGCPService, nil
		}

		serviceAccountEmail := "test-service-account-id@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}

		mockGCPService.On("RemoveRolesFromServiceAccounts", roles, serviceAccountEmail, backupTp).Return(nil)

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)
		assert.NoError(t, err)
	})
	t.Run("WhenGCPServiceError", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backupTp := "backup-tenant-project"
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket", // Mismatch will cause error
						TenantProjectNumber: backupTp,
					},
				},
			},
		}

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return nil, errors.New("failed to get cloud service")
		}

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)
		assert.Error(t, err)
	})
	t.Run("WhenSuccess", func(t *testing.T) {
		// Mock dependencies
		activity := activities.VolumeCreateActivity{}
		mockGCPService := new(hyperscaler2.MockGoogleServices)
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "test-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backupTp := "backup-tenant-project"
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket", // Mismatch will cause error
						TenantProjectNumber: backupTp,
					},
				},
			},
		}

		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return mockGCPService, nil
		}

		serviceAccountEmail := "test-service-account-id@test-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}

		mockGCPService.On("RemoveRolesFromServiceAccounts", roles, serviceAccountEmail, backupTp).Return(errors.New("failed to remove roles"))

		// Execute the function
		err := activity.DeleteRolesForServiceAccountInBackupTenantProject(ctx, targetPool, backup)
		assert.Error(t, err)
	})
}

func TestCrossPoolOrVPCRestorationActivity(t *testing.T) {
	t.Run("WhenSameTenantProject_ThenNoActionNeeded", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "same-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "same-project",
					},
				},
			},
		}

		// Act
		err := activity.CrossPoolOrVPCRestorationActivity(ctx, targetPool, backup)

		// Assert
		assert.NoError(t, err)
	})

	t.Run("WhenGetPoolTenantProjectFails_ThenReturnError", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: ""}, // Empty project will cause error
		}
		backup := &datamodel.Backup{}

		// Act
		err := activity.CrossPoolOrVPCRestorationActivity(ctx, targetPool, backup)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from pool")
	})

	t.Run("WhenGetBackupTenantProjectFails_ThenReturnError", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "different-bucket", // Mismatch will cause error
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}

		// Act
		err := activity.CrossPoolOrVPCRestorationActivity(ctx, targetPool, backup)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "tenant project number from backup")
	})

	t.Run("WhenSetupCrossTenantProjectPermissionsFails_ThenReturnError", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails:   datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
			ServiceAccountId: "test-service-account-id",
		}
		backupTP := "backup-project"
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: backupTP,
					},
				},
			},
		}
		mockGCPService := new(hyperscaler2.MockGoogleServices)

		// Mock GetGCPService to return error
		activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
			return mockGCPService, nil
		}
		projectID := "backup-project"
		serviceAccountEmail := "test-service-account-id@target-project.iam.gserviceaccount.com"
		roles := []string{"roles/storage.objectAdmin"}
		mockGCPService.On("AttachOrUpdateRolesForServiceAccounts", roles, serviceAccountEmail, projectID).Return(errors.New("failed to attach role"))

		// Act
		err := activity.CrossPoolOrVPCRestorationActivity(ctx, targetPool, backup)

		// Assert
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to attach role")
	})
}

func TestGetPoolServiceAccount(t *testing.T) {
	t.Run("WhenSuccessful_ThenReturnServiceAccountEmail", func(t *testing.T) {
		// Arrange
		pool := &datamodel.Pool{
			ServiceAccountId: "test-service-account-id",
		}
		projectID := "test-project"

		// Act
		result, err := activities.GetPoolServiceAccountName(pool, projectID)

		// Assert
		assert.NoError(t, err)
		expectedEmail := "test-service-account-id@test-project.iam.gserviceaccount.com"
		assert.Equal(t, expectedEmail, result)
	})

	t.Run("WhenServiceAccountIdIsEmpty_ThenReturnEmptyEmail", func(t *testing.T) {
		// Arrange
		pool := &datamodel.Pool{
			ServiceAccountId: "",
		}
		projectID := "test-project"

		// Act
		result, err := activities.GetPoolServiceAccountName(pool, projectID)

		// Assert
		assert.NoError(t, err)
		expectedEmail := "@test-project.iam.gserviceaccount.com"
		assert.Equal(t, expectedEmail, result)
	})
}

func TestDeleteObjectStoreForCrossVPC(t *testing.T) {
	t.Run("WhenSameTenantProject_ThenReturnNil", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "same-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "same-project",
					},
				},
			},
		}
		node := &models.Node{}

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenGetPoolTenantProjectFails_ThenReturnNil", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: ""}, // Empty will cause error
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}
		node := &models.Node{}

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenGetBackupTenantProjectFails_ThenReturnNil", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "different-bucket", // Mismatch will cause error
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}
		node := &models.Node{}

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}
		node := &models.Node{}

		// Mock GetProviderByNode to return error
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return nil, errors.New("failed to get provider")
		}

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get provider")
	})

	t.Run("WhenCloudTargetGetReturnsError_ThenReturnNil", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}
		node := &models.Node{}
		mockProvider := new(vsa.MockProvider)

		// Mock GetProviderByNode to return the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock CloudTargetGet to return error
		mockProvider.On("CloudTargetGet", mock.Anything).Return(nil, errors.New("object store not found"))

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("WhenCloudTargetGetReturnsNil_ThenReturnNil", func(t *testing.T) {
		// Arrange
		activity := activities.VolumeCreateActivity{}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		targetPool := &datamodel.Pool{
			ClusterDetails: datamodel.ClusterDetails{RegionalTenantProject: "target-project"},
		}
		backup := &datamodel.Backup{
			Attributes: &datamodel.BackupAttributes{
				BucketName: "test-bucket",
			},
			BackupVault: &datamodel.BackupVault{
				BucketDetails: []*datamodel.BucketDetails{
					{
						BucketName:          "test-bucket",
						TenantProjectNumber: "backup-project",
					},
				},
			},
		}
		node := &models.Node{}
		mockProvider := new(vsa.MockProvider)

		// Mock GetProviderByNode to return the mock provider
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock CloudTargetGet to return nil
		mockProvider.On("CloudTargetGet", mock.Anything).Return(nil, nil)

		// Act
		result, err := activity.DeleteObjectStoreForCrossVPC(ctx, targetPool, backup, node, "test-name")

		// Assert
		assert.NoError(t, err)
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})
}

func TestFinaliseRestoredVolume_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-volume",
		State:     models.LifeCycleStateRestoring, // Initial state
	}

	// Mock UpdateVolume to succeed
	mockStorage.On("UpdateVolume", ctx, volume).Return(nil)

	// Act
	err := activity.FinaliseRestoredVolume(ctx, volume)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, models.LifeCycleStateREADY, volume.State)
	assert.Equal(t, models.LifeCycleStateAvailableDetails, volume.StateDetails)
	mockStorage.AssertExpectations(t)
}

func TestFinaliseRestoredVolume_UpdateVolumeError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.VolumeCreateActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-volume",
		State:     models.LifeCycleStateRestoring, // Initial state
	}
	expectedError := errors.New("database update failed")

	// Mock UpdateVolume to fail
	mockStorage.On("UpdateVolume", ctx, volume).Return(expectedError)

	// Act
	err := activity.FinaliseRestoredVolume(ctx, volume)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, expectedError.Error())
	// State should still be updated even if database update fails
	assert.Equal(t, models.LifeCycleStateREADY, volume.State)
	assert.Equal(t, models.LifeCycleStateAvailableDetails, volume.StateDetails)
	mockStorage.AssertExpectations(t)
}
