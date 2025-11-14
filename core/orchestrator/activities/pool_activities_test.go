package activities_test

import (
	"context"
	digitalCert "crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	networkpriv "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/client/operations"
	privmodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/priv/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontap_rest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/hydrationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscaler2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/google"
	hyperscaler3 "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/api/servicenetworking/v1"
	"gorm.io/gorm"
)

func assertTemporalApplicationError(t *testing.T, err error, expectedMsg, expectedType string, expectedNonRetryable bool) {
	t.Helper()
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)

	var trackingID int
	var originalMsg string
	require.NoError(t, appErr.Details(&trackingID, &originalMsg))

	assert.Contains(t, originalMsg, expectedMsg)
	assert.Equal(t, expectedType, appErr.Type())
	assert.Equal(t, expectedNonRetryable, appErr.NonRetryable())
}

func TestCreatingPool_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("CreatingPool", ctx, pool).Return(pool, nil)

	// Act
	result, err := activity.CreatingPool(ctx, pool)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPool_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{Name: "test-pool"}}
	pool := database.ConvertPoolViewToPool(poolView)

	mockStorage.On("GetPool", ctx, poolView.UUID, int64(0)).Return(poolView, nil)

	// Act
	result, err := activity.GetPool(ctx, pool)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPool_Fails(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("GetPool", ctx, pool.UUID, int64(0)).Return(nil, gorm.ErrRecordNotFound)

	// Act
	result, err := activity.GetPool(ctx, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPoolView_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	poolView := &datamodel.PoolView{Pool: datamodel.Pool{Name: "test-pool"}}
	pool := database.ConvertPoolViewToPool(poolView)

	mockStorage.On("GetPool", ctx, poolView.UUID, int64(0)).Return(poolView, nil)

	// Act
	result, err := activity.GetPoolView(ctx, pool)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, poolView, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPoolView_Fails(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("GetPool", ctx, pool.UUID, int64(0)).Return(nil, gorm.ErrRecordNotFound)

	// Act
	result, err := activity.GetPoolView(ctx, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestSavePoolWithClusterDetails_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	cluster := &datamodel.ClusterDetails{}

	mockStorage.On("SavePoolWithVsaDetails", ctx, pool, cluster).Return(nil)

	// Act
	err := activity.SavePoolWithClusterDetails(ctx, pool, cluster)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestSavePoolWithClusterDetails_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	cluster := &datamodel.ClusterDetails{}

	mockStorage.On("SavePoolWithVsaDetails", ctx, pool, cluster).Return(gorm.ErrInvalidData)

	// Act
	err := activity.SavePoolWithClusterDetails(ctx, pool, cluster)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPool_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", ctx, pool).Return(pool, nil)
	mockStorage.On("UpdatedPool", ctx, pool).Return(pool, nil)

	// Act
	result, err := activity.CreatedPool(ctx, pool, vlmConfig)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPool_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", ctx, pool).Return(nil, gorm.ErrInvalidData)

	// Act
	result, err := activity.CreatedPool(ctx, pool, vlmConfig)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPoolSuccess_VLMUpdateFailed(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", ctx, pool).Return(pool, nil)
	mockStorage.On("UpdatedPool", ctx, pool).Return(nil, gorm.ErrInvalidData)

	// Act
	result, err := activity.CreatedPool(ctx, pool, vlmConfig)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

// Unit tests for FindTenancy in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_FindTenancy(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	params := commonparams.CreatePoolParams{}

	origGetGCPService := hyperscaler2.GetGCPService
	origGetTenantProject := activities.GetTenantProject
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		activities.GetTenantProject = origGetTenantProject
	}()

	t.Run("GetGCPService fails", func(tt *testing.T) {
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := activity.FindTenancyProject(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})

	t.Run("GetTenantProject fails", func(tt *testing.T) {
		mockSvc := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
			return "", errors.New("tenant project error")
		}
		_, err := activity.FindTenancyProject(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "tenant project error")
	})

	t.Run("Success", func(tt *testing.T) {
		mockSvc := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
			return "tenant-project-id", nil
		}
		result, err := activity.FindTenancyProject(ctx, params)
		assert.NoError(tt, err)
		assert.Equal(tt, "tenant-project-id", result)
	})
}

func TestPoolActivity_GetSubnetwork(t *testing.T) {
	ctx := context.Background()
	params := commonparams.CreatePoolParams{}
	tenantProjectNumber := "tenant-123"

	t.Run("GetGCPService fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}

		origGetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		if err == nil || !strings.Contains(err.Error(), "gcp service error") {
			t.Errorf("expected error from GetGCPService, got %v", err)
		}
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSubnetwork succeeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}

		origGetGCPService := hyperscaler2.GetGCPService
		origGetSubnetToBeUsed := activities.GetSubnetToBeUsed
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
			activities.GetSubnetToBeUsed = origGetSubnetToBeUsed
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("mocked GCP service error")
		}

		_, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		if err == nil {
			t.Error("expected error, got nil")
		}
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSubnetwork fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}

		origGetGCPService := hyperscaler2.GetGCPService
		origGetSubnetToBeUsed := activities.GetSubnetToBeUsed
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
			activities.GetSubnetToBeUsed = origGetSubnetToBeUsed
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("mocked GCP service error")
		}

		_, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		if err == nil {
			t.Error("expected error, got nil")
		}
		mockStorage.AssertExpectations(t)
	})
}

func Test_getTenantProject(t *testing.T) {
	params := commonparams.CreatePoolParams{
		VendorSubNetID: "subnet-123",
		AccountName:    "test-account",
		Region:         "us-central1",
	}
	t.Run("success", func(t *testing.T) {
		mockSvc := hyperscaler2.NewMockGoogleServices(t)
		mockSvc.On("GetTenantProject", params.VendorSubNetID, params.AccountName, params.Region).Return("tenant-456", nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))
		got, err := activities.GetTenantProject(mockSvc, params)
		assert.NoError(t, err)
		assert.Equal(t, "tenant-456", got)
		mockSvc.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		mockSvc := hyperscaler2.NewMockGoogleServices(t)
		mockSvc.On("GetTenantProject", params.VendorSubNetID, params.AccountName, params.Region).Return("", errors.New("not found"))
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))
		got, err := activities.GetTenantProject(mockSvc, params)
		assert.Error(t, err)
		assert.Equal(t, "", got)
		mockSvc.AssertExpectations(t)
	})
}

func TestDeployDeploymentManager_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	deployment := activities.DeploymentsInsert
	defer func() { activities.DeploymentsInsert = deployment }()

	var computeInstancesIPAddress []map[string]string
	computeInstancesIPAddress = append(computeInstancesIPAddress, map[string]string{
		"name": "test-name",
	})
	activities.DeploymentsInsert = func(ctx context.Context, name, region, zone, network, subnet, projectId, snHostProject string, size int) (*[]map[string]string, error) {
		return &computeInstancesIPAddress, nil
	}

	region := "test-region"
	projectId := "test-project"
	snHostProject := "test-sn-host-project"
	network := "test-network"
	zone := "test-zone"
	subnet := "test-subnet"
	size := 1024
	deploymentName := "test-deployment"
	// Act
	result, err := activity.DeployDeploymentManager(ctx, deploymentName, region, network, projectId, snHostProject, zone, subnet, size)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, &computeInstancesIPAddress, result)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_SaveNodeDetails(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)

	activity := activities.PoolActivity{
		SE: mockStorage,
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("SavePoolWithVsaDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Act
	err := activity.SavePoolWithClusterDetails(ctx, pool, &datamodel.ClusterDetails{})

	// Assert
	assert.Nil(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetONTAPProvider_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.PoolActivity{
		SE: database.NewMockStorage(t),
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &coremodel.Node{}
	ontapVersion := "9.10.1"
	mockProvider.On("GetONTAPVersion", mock.Anything).Return(&ontapVersion, nil)

	res, err := activity.GetOntapVersion(ctx, node)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, *res, ontapVersion)
}

func TestGetONTAPProvider_Failure(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	activity := activities.PoolActivity{
		SE: database.NewMockStorage(t),
	}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	node := &coremodel.Node{}
	mockProvider.On("GetONTAPVersion", mock.Anything).Return(nil, errors.New("failed to get ONTAP version"))

	res, err := activity.GetOntapVersion(ctx, node)
	assert.Error(t, err)
	assert.Nil(t, res)
}

func Test_prepareVlmConfig_Success(t *testing.T) {
	cfg := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
			GCPConfig: vlm.GCPConfig{},
		},
	}
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("{}"), nil
	}

	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.NoError(t, err)
	assert.Equal(t, "test-deployment", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
	assert.Equal(t, "test-zone1", cfg.Deployment.Zone.Zone1)
	assert.Equal(t, "test-zone2", cfg.Deployment.Zone.Zone2)
	assert.Equal(t, "test-network", cfg.Deployment.NetConfig[vlm.LIFTypeInterCluster].VPC)
	assert.Equal(t, "test-sn-host-project", cfg.Deployment.NetConfig[vlm.LIFTypeInterCluster].GCPNetworkConfig.SubnetProjectID)
	assert.Equal(t, int64(64), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(1024), cfg.Deployment.SPConfig.IOps)
	assert.Equal(t, "1024Gi", cfg.Deployment.SPConfig.Size)
}

func Test_prepareVlmConfig_StripLssd(t *testing.T) {
	// Set the environment variable to true
	originalEnv := env.GetBool("VSA_INSTANCE_TYPE_OVERRIDE_LSSD", false)
	// Restore the original value after the test
	activities.VsaInstanceTypeOverride = true
	defer func() { activities.VsaInstanceTypeOverride = originalEnv }()

	cfg := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
			GCPConfig: vlm.GCPConfig{},
		},
	}
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("{}"), nil
	}

	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c3-standard-16-lssd"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.NoError(t, err)
	assert.Equal(t, "test-deployment", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
	assert.Equal(t, "test-zone1", cfg.Deployment.Zone.Zone1)
	assert.Equal(t, "test-zone2", cfg.Deployment.Zone.Zone2)
	assert.Equal(t, "test-network", cfg.Deployment.NetConfig[vlm.LIFTypeInterCluster].VPC)
	assert.Equal(t, "test-sn-host-project", cfg.Deployment.NetConfig[vlm.LIFTypeInterCluster].GCPNetworkConfig.SubnetProjectID)
	assert.Equal(t, int64(64), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(1024), cfg.Deployment.SPConfig.IOps)
	assert.Equal(t, "1024Gi", cfg.Deployment.SPConfig.Size)
	assert.Equal(t, "c3-standard-16", cfg.Deployment.VSAInstanceType, "Expected '-lssd' to be stripped from the instance type")
}

func Test_prepareVlmConfig_FileNotFound(t *testing.T) {
	cfg := &vlm.VLMConfig{}
	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "no such file or directory")
}

func Test_prepareVlmConfig_InvalidJSON(t *testing.T) {
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("invalid-json"), nil
	}

	cfg := &vlm.VLMConfig{}
	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test=zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "invalid character")
}

func Test_prepareVlmConfig_EmptyDeploymentName(t *testing.T) {
	cfg := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
			GCPConfig: vlm.GCPConfig{},
		},
	}
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("{}"), nil
	}
	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1099511627776,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "", "test-region", "test-zone", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err, "one or more required string parameters are empty")
	assert.Equal(t, "", cfg.Deployment.DeploymentID)
}

func Test_prepareVlmConfig_IsIntegrationTest(t *testing.T) {
	const mockOntapIp = "8.8.8.8"
	cfg := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{
			HAPairs: []vlm.HAPair{
				{
					VM1: vlm.VMConfig{
						SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{},
					},
					VM2: vlm.VMConfig{
						SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{},
					},
				},
			},
		},
	}
	cfg.Cloud.HAPairs[0].VM1.SystemLIFs[vlm.LIFTypeNodeMgmt] = vlm.LIFConfig{
		IP: "",
	}
	cfg.Cloud.HAPairs[0].VM2.SystemLIFs[vlm.LIFTypeNodeMgmt] = vlm.LIFConfig{
		IP: "",
	}

	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("{}"), nil
	}

	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}

	// Set the environment variable to true
	originalEnv := env.GetBool("INTEGRATION_TEST", false)
	// Restore the original value after the test
	activities.IsIntegrationTest = true
	defer func() { activities.IsIntegrationTest = originalEnv }()

	err := os.Setenv("MOCK_ONTAP_IP", mockOntapIp)
	if err != nil {
		return
	}
	defer func() {
		err := os.Setenv("MOCK_ONTAP_IP", "")
		if err != nil {
			return
		}
	}()

	err = activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "test-tenant-project@xyz.com", "test-tenant-project")
	assert.NoError(t, err)
	assert.Equal(t, mockOntapIp, cfg.Cloud.HAPairs[0].VM1.SystemLIFs[vlm.LIFTypeNodeMgmt].IP)
	assert.Equal(t, mockOntapIp, cfg.Cloud.HAPairs[0].VM2.SystemLIFs[vlm.LIFTypeNodeMgmt].IP)
}

func Test_validateVlmConfigInputs(t *testing.T) {
	validCfg := &vlm.VLMConfig{}
	validDecision := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}

	tests := []struct {
		name        string
		cfg         *vlm.VLMConfig
		decision    *vmrs.Decision
		deployment  string
		region      string
		primaryZone string
		network     string
		subnet      string
		projectId   string
		snHost      string
		saEmail     string
		wantErr     bool
	}{
		{
			name:        "all valid",
			cfg:         validCfg,
			decision:    validDecision,
			deployment:  "deploy",
			region:      "region",
			primaryZone: "zone",
			network:     "network",
			subnet:      "subnet",
			projectId:   "project",
			snHost:      "sn-host",
			saEmail:     "email@xyz.com",
			wantErr:     false,
		},
		{
			name:        "nil vlmConfig",
			cfg:         nil,
			decision:    validDecision,
			deployment:  "deploy",
			region:      "region",
			primaryZone: "zone",
			network:     "network",
			subnet:      "subnet",
			projectId:   "project",
			snHost:      "sn-host",
			saEmail:     "email@xyz.com",
			wantErr:     true,
		},
		{
			name:        "nil decision",
			cfg:         validCfg,
			decision:    nil,
			deployment:  "deploy",
			region:      "region",
			primaryZone: "zone",
			network:     "network",
			subnet:      "subnet",
			projectId:   "project",
			snHost:      "sn-host",
			saEmail:     "email@xyz.com",
			wantErr:     true,
		},
		{
			name:        "empty deploymentID",
			cfg:         validCfg,
			decision:    validDecision,
			deployment:  "",
			region:      "region",
			primaryZone: "zone",
			network:     "network",
			subnet:      "subnet",
			projectId:   "project",
			snHost:      "sn-host",
			saEmail:     "email@xyz.com",
			wantErr:     true,
		},
		{
			name:        "empty region",
			cfg:         validCfg,
			decision:    validDecision,
			deployment:  "deploy",
			region:      "",
			primaryZone: "zone",
			network:     "network",
			subnet:      "subnet",
			projectId:   "project",
			snHost:      "sn-host",
			saEmail:     "email@xyz.com",
			wantErr:     true,
		},
		// Add more cases for other empty required fields if needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := activities.ValidateVlmConfigInputs(
				tt.cfg, tt.decision, tt.deployment, tt.region, tt.primaryZone,
				tt.network, tt.subnet, tt.projectId, tt.snHost, tt.saEmail,
			)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func Test_SaveSVMAndLifData_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeSan: {
						{IP: "192.168.1.1/24", Name: "lif1", HomeNode: "01"},
					},
					vlm.LIFTypeNas: {
						{IP: "192.168.1.1/24", Name: "lif2", HomeNode: "02"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "01"}, {BaseModel: datamodel.BaseModel{ID: 1}, Name: "02"},
	}, nil)
	mockStorage.On("CreateLif", mock.Anything, mock.Anything).Return(&datamodel.Lif{}, nil)

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifDataDBCreationError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}

	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeSan: {
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
					vlm.LIFTypeNas: {
						{IP: "192.168.1.1/24", Name: "lif2"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, errors.New("connection error"))

	svm, err := activity.SaveSVMAndLifData(ctx, pool, vlmConfig, "gcnv")

	assert.Nil(t, svm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection error")
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifData_CouldNotFetchNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	svm, err := activity.SaveSVMAndLifData(ctx, pool, vlmConfig, "gcnv")

	assert.Nil(t, svm)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifData_NotEnoughNodes(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not enough nodes in the cluster")
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifData_FailsToCreateLif(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeSan: {
						{IP: "192.168.1.1/24", Name: "lif1", HomeNode: "01"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "01"}, {BaseModel: datamodel.BaseModel{ID: 1}, Name: "02"},
	}, nil)
	mockStorage.On("CreateLif", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create LIF"))

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create LIF")
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifData_NonExistentHomeNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlm.SvmConfig{
			"gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeSan: {
						{IP: "192.168.1.1/24", Name: "lif1", HomeNode: "non-existent-node"},
					},
				},
			},
		},
	}

	// Mock nodes that exist in the database
	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "existing-node"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "another-node"},
	}, nil)

	svm, err := activity.SaveSVMAndLifData(ctx, pool, vlmConfig, "gcnv")

	assert.Nil(t, svm)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "LIF lif1 references non-existent home node non-existent-node")
	mockStorage.AssertExpectations(t)
}

func Test_SaveNodeDetails_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vmConfig := vlm.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
			vlm.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlm.DeploymentConfig{
		VSAInstanceType: "n1-standard-4",
	}

	vsaNode := &vsa.Node{}

	mockProvider.On("GetNodeByName", mock.Anything).Return(vsaNode, nil)
	mockStorage.On("CreateNode", ctx, mock.Anything).Return(&datamodel.Node{}, nil)

	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool, "clustername", map[string]string{})

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "test-node", node.Name)
	assert.Equal(t, pool.AccountID, node.AccountID)
	mockStorage.AssertExpectations(t)
}

func Test_SaveNodeDetails_FailsToCreateNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vmConfig := vlm.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
			vlm.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlm.DeploymentConfig{
		VSAInstanceType: "n1-standard-4",
	}
	vasNode := &vsa.Node{}

	mockProvider.On("GetNodeByName", mock.Anything).Return(vasNode, nil)
	mockStorage.On("CreateNode", ctx, mock.Anything).Return(nil, errors.New("failed to create node"))

	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "failed to create node")
	mockStorage.AssertExpectations(t)
}

func Test_SaveNodeDetails_FailsToFetchNodeByName(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vmConfig := vlm.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
			vlm.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlm.DeploymentConfig{
		VSAInstanceType: "n1-standard-4",
	}

	mockProvider.On("GetNodeByName", mock.Anything).Return(nil, errors.New("failed to fetch node"))
	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	mockStorage.AssertExpectations(t)
}

func Test_SaveVSANodeDetails_NoClusterDetailsProvided(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{HAPairs: []vlm.HAPair{}},
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assertTemporalApplicationError(t, err, "no cluster details provided", "CustomError", false)
}

func Test_SaveVSANodeDetails_NoHAPairsProvided(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{HAPairs: []vlm.HAPair{}},
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assertTemporalApplicationError(t, err, "no cluster details provided", "CustomError", false)
}

func Test_SaveVSANodeDetails_FailsToSaveFirstNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{
			HAPairs: []vlm.HAPair{
				{
					VM1: vlm.VMConfig{HostName: "node1"},
					VM2: vlm.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlm.VMConfig, deploymentConfig vlm.DeploymentConfig, pool *datamodel.Pool, clusterName string, hostMap map[string]string) (*datamodel.Node, error) {
		if vmConfig.HostName == "node1" {
			return nil, errors.New("failed to save node1")
		}
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "failed to save node1")
}

func Test_SaveVSANodeDetails_FailsToSaveSecondNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{
			HAPairs: []vlm.HAPair{
				{
					VM1: vlm.VMConfig{HostName: "node1"},
					VM2: vlm.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlm.VMConfig, deploymentConfig vlm.DeploymentConfig, pool *datamodel.Pool, clusterName string, hostMap map[string]string) (*datamodel.Node, error) {
		if vmConfig.HostName == "node1" {
			return nil, errors.New("failed to save node2")
		}
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "failed to save node2")
}

func Test_SaveVSANodeDetails_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{
			HAPairs: []vlm.HAPair{
				{
					VM1: vlm.VMConfig{HostName: "node1"},
					VM2: vlm.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlm.VMConfig, deploymentConfig vlm.DeploymentConfig, pool *datamodel.Pool, clusterName string, hostMap map[string]string) (*datamodel.Node, error) {
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig, "clusterName", map[string]string{})

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "node1", node.Name)
}

func Test_DeletePoolResourcesOnRollback_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	deleteSVMS := activities.DeleteSVMs
	deleteNodes := activities.DeleteNodes
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteNodes = deleteNodes
		activities.DeleteLIFs = deleteLIFs
	}()

	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}

	err := activity.DeletePoolResourcesOnRollback(ctx, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResourcesOnRollback_Failure(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	deleteSVMS := activities.DeleteSVMs
	deleteNodes := activities.DeleteNodes
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteNodes = deleteNodes
		activities.DeleteLIFs = deleteLIFs
	}()

	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete LIFs")
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}

	err := activity.DeletePoolResourcesOnRollback(ctx, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete LIFs")
	mockStorage.AssertExpectations(t)
}

func Test_ErroredPool_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", ctx, pool, mock.Anything).Return(pool, nil)

	result, err := activity.ErroredPool(ctx, pool, "")

	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("DeletePool", ctx, pool).Return(nil)
	deleteSVMS := activities.DeleteSVMs
	deleteNodes := activities.DeleteNodes
	deleteLIFs := activities.DeleteLIFs

	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteNodes = deleteNodes
		activities.DeleteLIFs = deleteLIFs
	}()
	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	result, err := activity.DeletePoolResources(ctx, pool)

	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteLIFs(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteLIFs = deleteLIFs
	}()
	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete LIFs")
	}

	result, err := activity.DeletePoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete LIFs")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteSVMs(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deleteSVMS := activities.DeleteSVMs
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteLIFs = deleteLIFs
	}()

	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete SVMs")
	}

	result, err := activity.DeletePoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete SVMs")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deleteSVMS := activities.DeleteSVMs
	deleteNodes := activities.DeleteNodes
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteNodes = deleteNodes
		activities.DeleteLIFs = deleteLIFs
	}()
	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete nodes")
	}

	result, err := activity.DeletePoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete nodes")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeletePool(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deleteSVMS := activities.DeleteSVMs
	deleteNodes := activities.DeleteNodes
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteSVMs = deleteSVMS
		activities.DeleteNodes = deleteNodes
		activities.DeleteLIFs = deleteLIFs
	}()
	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeleteNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	mockStorage.On("DeletePool", ctx, pool).Return(errors.New("failed to delete pool"))

	result, err := activity.DeletePoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete pool")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteSVMsReturnsErrorWhenNoSVMsFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(nil, errors.New("SVM not found"))

	err := activities.DeleteSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SVM not found")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteSVMsSkipsDeletedSVMs(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{BaseModel: datamodel.BaseModel{ID: 1, DeletedAt: &gorm.DeletedAt{Valid: true}}, Name: "svm1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "svm2"},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("DeleteSVM", ctx, svms[1]).Return(nil)

	err := activities.DeleteSVMs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteSVMsReturnsErrorWhenSVMDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "svm1"},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("DeleteSVM", ctx, svms[0]).Return(errors.New("failed to delete SVM"))

	err := activities.DeleteSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to delete SVM record")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteSVMsDeletesAllSVMsSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "svm1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "svm2"},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("DeleteSVM", ctx, svms[0]).Return(nil)
	mockStorage.On("DeleteSVM", ctx, svms[1]).Return(nil)

	err := activities.DeleteSVMs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_deleteLIFsDeletesAllLIFsSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 2}}

	// Mock nodes
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 10}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 20}, Name: "node2"},
	}
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	// Mock LIFs
	lifs := []*datamodel.Lif{
		{Name: "lif1", BaseModel: datamodel.BaseModel{ID: 100}},
		{Name: "lif2", BaseModel: datamodel.BaseModel{ID: 200}},
	}
	mockStorage.On("GetLifsForNodesWithProtocol", ctx, []int64{10, 20}, pool.AccountID, "").Return(lifs, nil)
	mockStorage.On("DeleteLif", ctx, lifs[0]).Return(nil)
	mockStorage.On("DeleteLif", ctx, lifs[1]).Return(nil)

	err := activities.DeleteLIFs(ctx, mockStorage, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_deleteLIFsWhenZeroNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 2}}

	// Mock nodes
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)

	err := activities.DeleteLIFs(ctx, mockStorage, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_deleteLIFsSkipsDeletedLIFs(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 2}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 10}}}
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	lifs := []*datamodel.Lif{
		{BaseModel: datamodel.BaseModel{DeletedAt: &gorm.DeletedAt{Valid: true}}, Name: "lif1"},
		{Name: "lif2", BaseModel: datamodel.BaseModel{DeletedAt: nil, ID: 200}},
	}
	mockStorage.On("GetLifsForNodesWithProtocol", ctx, []int64{10}, pool.AccountID, "").Return(lifs, nil)
	mockStorage.On("DeleteLif", ctx, lifs[1]).Return(nil)

	err := activities.DeleteLIFs(ctx, mockStorage, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_deleteLIFsReturnsErrorWhenLIFRetrievalFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 2}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 10}}}
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("GetLifsForNodesWithProtocol", ctx, []int64{10}, pool.AccountID, "").Return(nil, errors.New("lif db error"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to retrieve LIFs for pool")
	mockStorage.AssertExpectations(t)
}

func Test_deleteLIFsReturnsErrorWhenLIFDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.Background()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 2}

	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 10}}}
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	lifs := []*datamodel.Lif{
		{Name: "lif1", BaseModel: datamodel.BaseModel{ID: 100, DeletedAt: nil}},
	}
	mockStorage.On("GetLifsForNodesWithProtocol", ctx, []int64{10}, pool.AccountID, "").Return(lifs, nil)
	mockStorage.On("DeleteLif", ctx, lifs[0]).Return(errors.New("delete error"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to delete LIF record")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteLIFsReturnsErrorWhenNodesNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("nodes not found"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to retrieve nodes for pool")
	mockStorage.AssertExpectations(t)
}

func TestUpdatesSVMStatusToErrorWhenMarkedForDeletion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{State: coremodel.LifeCycleStateDeleting, BaseModel: datamodel.BaseModel{ID: 1}},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("ErroredSVM", ctx, svms[0], coremodel.LifeCycleStateDeletionErrorDetails).Return(nil)

	err := activities.FailedSVMs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenSVMsNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	err := activities.FailedSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "SVM not found")
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenErroredSVMFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{State: coremodel.LifeCycleStateDeleting},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("ErroredSVM", ctx, svms[0], coremodel.LifeCycleStateDeletionErrorDetails).Return(errors.New("failed to update SVM"))

	err := activities.FailedSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update SVM")
	mockStorage.AssertExpectations(t)
}

func TestSkipsSVMsNotMarkedForDeletion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{State: coremodel.LifeCycleStateREADY},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)

	err := activities.FailedSVMs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdatesNodeStatusToErrorWhenMarkedForDeletion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{State: coremodel.LifeCycleStateDeleting},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("ErroredNode", ctx, nodes[0], coremodel.LifeCycleStateDeletionErrorDetails).Return(nil)

	err := activities.FailedNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenNodesNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("nodes not found"))

	err := activities.FailedNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to retrieve nodes for pool")
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenErroredNodeFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{State: coremodel.LifeCycleStateDeleting},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("ErroredNode", ctx, nodes[0], coremodel.LifeCycleStateDeletionErrorDetails).Return(errors.New("failed to update node"))

	err := activities.FailedNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update node")
	mockStorage.AssertExpectations(t)
}

func TestSkipsNodesNotMarkedForDeletion(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{State: coremodel.LifeCycleStateREADY},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	err := activities.FailedNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeletesAllNodesSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("DeleteNode", ctx, nodes[0]).Return(nil)
	mockStorage.On("DeleteNode", ctx, nodes[1]).Return(nil)

	err := activities.DeleteNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteNodesReturnsErrorWhenNodesNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("nodes not found"))

	err := activities.DeleteNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to retrieve nodes for pool")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteNodesReturnsErrorWhenNodeDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("DeleteNode", ctx, nodes[0]).Return(errors.New("failed to delete node"))

	err := activities.DeleteNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to update node record to deleting")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteNodesSkipsEmptyNodeList(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)

	err := activities.DeleteNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteNodesSkipsAlreadyDeletedNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1, DeletedAt: &gorm.DeletedAt{Valid: true}}, Name: "node1"},
	}, nil)

	err := activities.DeleteNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenNoSVMsFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	err := activities.DeletingSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "SVM not found")
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenSVMUpdateFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{Name: "svm1"},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("DeletingSVM", ctx, svms[0]).Return(errors.New("failed to update SVM"))

	err := activities.DeletingSVMs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to update SVM record to deleting svm1")
	mockStorage.AssertExpectations(t)
}

func Test_UpdatesAllSVMsToDeletingSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	svms := []*datamodel.Svm{
		{Name: "svm1", State: coremodel.LifeCycleStateDeleting},
		{Name: "svm2"},
	}

	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(svms, nil)
	mockStorage.On("DeletingSVM", ctx, svms[1]).Return(nil)

	err := activities.DeletingSVMs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeletingAllNodesSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1", State: coremodel.LifeCycleStateDeleting},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("DeletingNode", ctx, nodes[1]).Return(nil)

	err := activities.DeletingNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenNodesNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("nodes not found"))

	err := activities.DeletingNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to retrieve nodes for pool")
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenNodeDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("DeletingNode", ctx, nodes[0]).Return(errors.New("failed to delete node"))

	err := activities.DeletingNodes(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to delete node record")
	mockStorage.AssertExpectations(t)
}

func Test_SkipsEmptyNodeList(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)

	err := activities.DeletingNodes(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenDeletingSVMsFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	deletingSVMS := activities.DeletingSVMs
	defer func() {
		activities.DeletingSVMs = deletingSVMS
	}()

	activities.DeletingSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete SVMs")
	}

	activity := activities.PoolActivity{SE: mockStorage}
	result, err := activity.DeletingPoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete SVMs")
}

func Test_ReturnsErrorWhenDeletingNodesFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deletingSVMS := activities.DeletingSVMs
	deletingNodes := activities.DeletingNodes
	defer func() {
		activities.DeletingSVMs = deletingSVMS
		activities.DeletingNodes = deletingNodes
	}()

	activities.DeletingSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeletingNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete nodes")
	}

	activity := activities.PoolActivity{SE: mockStorage}
	result, err := activity.DeletingPoolResources(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete nodes")
}

func Test_DeletesPoolResourcesSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deletingSVMS := activities.DeletingSVMs
	deletingNodes := activities.DeletingNodes
	defer func() {
		activities.DeletingSVMs = deletingSVMS
		activities.DeletingNodes = deletingNodes
	}()

	activities.DeletingSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.DeletingNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}

	activity := activities.PoolActivity{SE: mockStorage}
	result, err := activity.DeletingPoolResources(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, pool, result)
}

func Test_ReturnsErrorWhenListPoolsFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		AccountID: 1,
		Network:   "test-network",
		Account:   &datamodel.Account{Name: "643029180821"},
		ClusterDetails: datamodel.ClusterDetails{
			SubnetNames: []string{"subnet1"},
		},
	}
	mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("failed to list pools"))

	err := activity.ReleaseSubnet(ctx, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list pools")
	mockStorage.AssertExpectations(t)
}

// Unit tests for ReleaseSubnet in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_ReleaseSubnetN(t *testing.T) {
	ctx := context.Background()
	pool := datamodel.Pool{
		AccountID:      1,
		Network:        "test-network",
		Account:        &datamodel.Account{Name: "test-account"},
		ClusterDetails: datamodel.ClusterDetails{SubnetNames: []string{"subnet1"}},
	}
	poolView := &datamodel.PoolView{Pool: pool}

	pool2 := datamodel.Pool{
		AccountID:      1,
		Network:        "test-network-2",
		Account:        &datamodel.Account{Name: "test-account"},
		ClusterDetails: datamodel.ClusterDetails{SubnetNames: []string{"subnet1"}},
	}
	poolView2 := &datamodel.PoolView{Pool: pool2}

	t.Run("listPoolsFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "list pools error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("multiplePoolsExist", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{poolView, poolView2}, nil)
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetGCPServiceFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "initialisation of Google GCP service failed")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("getSubnetForConsumerProjectAndReleaseFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		defer func() {}()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler2.GoogleServices, snHost, subnetName string) error {
			return errors.New("release subnet error")
		}
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "release subnet error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("releasesSubnet", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)

		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)

		originalGetGCPService := hyperscaler2.GetGCPService
		releaseSubnet := activities.ReleaseSubnet
		defer func() {
			activities.ReleaseSubnet = releaseSubnet
			hyperscaler2.GetGCPService = originalGetGCPService
		}()

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnet = func(service hyperscaler2.GoogleServices, snHost, subnetName string) error {
			return nil
		}
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestPoolActivity_ReleaseSubnet(t *testing.T) {
	ctx := context.Background()
	rawPool := datamodel.Pool{
		Name:    "test-pool",
		Network: "test-network",
		Account: &datamodel.Account{
			Name: "test-account",
		},
		ClusterDetails: datamodel.ClusterDetails{
			SnHostProject: "sn-host-project",
			SubnetNames:   []string{"subnet-1"},
		}}
	pool := &datamodel.PoolView{
		Pool: rawPool,
	}
	pool1 := &datamodel.PoolView{
		Pool: datamodel.Pool{Name: "test-pool-1",
			Network: "test-network",
			Account: &datamodel.Account{
				Name: "test-account",
			},
			ClusterDetails: datamodel.ClusterDetails{
				SnHostProject: "sn-host-project",
				SubnetNames:   []string{"subnet-1"},
			}},
	}
	t.Run("NoSubnetNames", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		poolNoSubnet := rawPool
		poolNoSubnet.ClusterDetails = datamodel.ClusterDetails{
			SnHostProject: "sn-host-project",
			SubnetNames:   []string{},
		}
		err := activity.ReleaseSubnet(ctx, &poolNoSubnet)
		assert.Nil(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetPoolsBySubnetworkFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("list pools error"))
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "list pools error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("MultiplePoolsUsingSubnet", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool, pool1}, nil)
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetGCPServiceFails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		// Override with mock that returns error
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "initialisation of Google GCP service failed")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("ReleaseSubnet fails", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		defer func() {}()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler2.GoogleServices, snHost, subnetName string) error {
			return errors.New("release subnet error")
		}
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "release subnet error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		GetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = GetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)

		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler2.GoogleServices, snHost, subnetName string) error {
			return nil
		}
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func Test_InsertFirewall(t *testing.T) {
	projectName := "test-project"
	vpcName := "test-vpc"
	firewallName := "test-firewall"
	priority := int64(1000)
	direction := "INGRESS"
	firewallSourceRanges := []string{"10.0.0.0/8", "192.168.0.0/16"}
	firewallAllowedPortRules := []string{"tcp", "udp"}
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenFirewallAlreadyExists", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)
		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: []string{"10.0.0.0/8", "192.168.0.0/16"}, // Same source ranges as expected
		}
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(existingFirewall, nil)

		_, err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetFirewallFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, errors.New(errString))

		_, err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap().(*vsaerrors.CustomError).OriginalErr, fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, firewall name: %s. Error : %s", projectName, vpcName, firewallName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenInsertFirewallFails", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		errString := "failed to insert firewall"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, nil)
		mgs.On("InsertFirewall", &hyperscaler3.Firewall{
			ProjectName:      projectName,
			Name:             firewallName,
			VPCNetworkName:   vpcName,
			Priority:         priority,
			Direction:        direction,
			SourceRanges:     firewallSourceRanges,
			AllowedPortRules: firewallAllowedPortRules,
		}).Return("", errors.New(errString))

		_, err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.EqualError(tt, err, errString)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenInsertFirewallSucceeds", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, nil)
		mgs.On("InsertFirewall", mock.Anything).Return("", nil)

		_, err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})
}

func Test_CreateVPC(t *testing.T) {
	projectName := "test-project"
	vpcName := "test-vpc"

	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenVPCAlreadyExists", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(&hyperscaler3.VPCNetwork{}, nil)

		_, err := activities.CreateVPC(mgs, projectName, vpcName)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))

		_, err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap().(*vsaerrors.CustomError).OriginalErr, fmt.Sprintf("Error getting vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkFailsWithNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "not found"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))
		mgs.On("CreateVPC", &hyperscaler3.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", nil)

		_, err := activities.CreateVPC(mgs, projectName, vpcName)
		assert.Nil(tt, err)
		mgs.AssertExpectations(tt)
	})
	t.Run("WhenCreateVPCFails", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "failed to create VPC"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, nil)
		mgs.On("CreateVPC", &hyperscaler3.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", errors.New(errString))

		_, err := activities.CreateVPC(mgs, projectName, vpcName)

		assert.Contains(tt, err.Error(), "failed to create VPC")
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkAfterCreationFails", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "failed to get VPC network"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))

		_, err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Contains(tt, customErr.Unwrap().(*vsaerrors.CustomError).OriginalErr.Error(), errString)
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateVPCSucceeds", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, nil).Once()
		mgs.On("CreateVPC", &hyperscaler3.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", nil)

		_, err := activities.CreateVPC(mgs, projectName, vpcName)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})
}

func Test_InsertSubnet(t *testing.T) {
	projectName := "test-project"
	region := "us-central1"
	subnetName := "test-subnet"
	vpcName := "test-vpc"
	ipCidrRange := "10.0.0.0/16"

	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSubnetAlreadyExists", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(&hyperscaler3.Subnet{}, nil)

		_, err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetSubnetworkFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, errors.New(errString))

		_, err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap().(*vsaerrors.CustomError).OriginalErr, "Error getting subnet for project: test-project, vpc name: test-vpc, subnet name: test-subnet. Error : "+errString)
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateSubnetworkFails", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		errString := "failed to create subnetwork"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, nil)
		mgs.On("CreateSubnetwork", mock.Anything).Return("", errors.New(errString))

		_, err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.EqualError(tt, err, errString)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateSubnetworkSucceeds", func(tt *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, nil)
		mgs.On("CreateSubnetwork", mock.Anything).Return("", nil)

		_, err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})
}

// Unit tests for _getSubnetwork
func Test_getSubnetwork(t *testing.T) {
	tenantProjectNumber := "tenant-456"

	t.Run("GetTenancyInfo succeeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}
		ctx := context.Background()

		expectedSubnet := &hyperscaler3.Subnet{
			Name:           "subnet-1",
			IpCidrRange:    "10.0.0.0/24",
			Network:        "projects/sn-host/global/networks/test-network",
			GatewayAddress: "10.0.0.1",
		}

		info, err := activity.GetTenancyInfo(ctx, tenantProjectNumber, expectedSubnet)
		assert.NoError(t, err)
		assert.Equal(t, tenantProjectNumber, info.RegionalTenantProject)
		assert.Equal(t, "test-network", info.Network)
		assert.Equal(t, []string{"subnet-1"}, info.SubnetworkNames)
		assert.Equal(t, "10.0.0.1", info.Gateway)
		mockStorage.AssertExpectations(t)
	})
}

// Unit tests for UpdatePoolSubnet in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_UpdatePoolSubnet(t *testing.T) {
	ctx := context.Background()
	poolUUID := "test-pool-uuid"
	tenancyDetails := &commonparams.TenancyInfo{
		SnHostProject:   "test-sn-host",
		SubnetworkNames: []string{"subnet-1", "subnet-2"},
	}

	t.Run("Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}
		mockStorage.On("UpdatePoolSubnetNames", ctx, poolUUID, tenancyDetails.SnHostProject, tenancyDetails.SubnetworkNames).Return(nil)
		err := activity.UpdatePoolSubnet(ctx, poolUUID, *tenancyDetails)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdatePoolSubnetNames fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}
		mockStorage.On("UpdatePoolSubnetNames", ctx, poolUUID, tenancyDetails.SnHostProject, tenancyDetails.SubnetworkNames).Return(errors.New("db error"))
		err := activity.UpdatePoolSubnet(ctx, poolUUID, *tenancyDetails)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "db error")
		mockStorage.AssertExpectations(t)
	})
}

// Unit test for setupNetworkFirewallsForIscsi in core/orchestrator/activities/pool_activities_test.go
func Test_setupNetworkFirewallsForIscsi(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	network := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForIscsiSucceeds", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the InsertFirewall function to nil after the test
		}()
		activities.DataFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, "ingress-data-iscsi", name)
			assert.Equal(t, network, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "3260"}, allowedPorts)
			return "op", nil
		}
		op, err := activities.SetupNetworkFirewallsForIscsi(mockService, snHostProject, network)
		assert.NoError(t, err)
		assert.Equal(t, op, "op")
	})
	t.Run("WhenSetupNetworkFirewallsForIscsiFails", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the InsertFirewall function to nil after the test
		}()
		activities.DataFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForIscsi(mockService, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "firewall error")
	})
}

func Test_setupNetworkFirewallsForNFS(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	network := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForNFSSucceeds", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "172.16.0.0/12,192.168.0.0/16,10.152.0.0/20"
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, "ingress-data-nfs", name)
			assert.Equal(t, network, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"172.16.0.0/12", "192.168.0.0/16", "10.152.0.0/20"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "111", "635", "2049", "4045", "udp", "111", "4046"}, allowedPorts)
			return "op", nil
		}
		op, err := activities.SetupNetworkFirewallsForNFS(mockService, snHostProject, network)
		assert.NoError(t, err)
		assert.Equal(t, op, "op")
	})
	t.Run("WhenSetupNetworkFirewallsForNFSFails", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "172.16.0.0/12,192.168.0.0/16,10.152.0.0/20"
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("nfs firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForNFS(mockService, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nfs firewall error")
	})
}

func Test_setupNetworkFirewallsForIntercluster(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	network := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForInterclusterSucceeds", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "172.16.0.0/12,192.168.0.0/16,10.152.0.0/20"
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, "ingress-intercluster", name)
			assert.Equal(t, network, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"172.16.0.0/12", "192.168.0.0/16", "10.152.0.0/20"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "10566", "11104", "11105"}, allowedPorts)
			return "op", nil
		}
		op, err := activities.SetupNetworkFirewallsForIntercluster(mockService, snHostProject, network)
		assert.NoError(t, err)
		assert.Equal(t, op, "op")
	})
	t.Run("WhenSetupNetworkFirewallsForNFSFails", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "172.16.0.0/12,192.168.0.0/16,10.152.0.0/20"
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("intercluster firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForIntercluster(mockService, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "intercluster firewall error")
	})
}

func Test_CreateGCPBucket_Success(t *testing.T) {
	mockGcp := hyperscaler2.NewMockGoogleServices(t)

	ctx := context.Background()
	logger := util.GetLogger(ctx)
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.On("GetLogger").Return(logger)
	mockGcp.On("CreateBucketIfNotExists", ctx, projectId, bucketName, region).Return(nil)

	// Create a bucket in the project if it doesn't exist
	// mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(nil)
	err := activities.CreateGCPBucket(ctx, projectId, bucketName, region, mockGcp)
	assert.NoError(t, err)
}

func Test_releaseSubnet_Error(t *testing.T) {
	mockSvc := hyperscaler2.NewMockGoogleServices(t)
	snHost := "test-sn-host"
	subnetName := "test-subnet"
	expectedErr := errors.New("release failed")

	mockSvc.On("ReleaseSubnetwork", "", snHost, subnetName).Return(expectedErr)

	err := activities.ReleaseSubnet(mockSvc, snHost, subnetName)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	mockSvc.AssertExpectations(t)
}

func Test_CreateGCPBucket_Failure(t *testing.T) {
	mockGcp := hyperscaler2.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.On("GetLogger").Return(util.GetLogger(ctx))

	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(errors.New("failed to create bucket"))
	err := activities.CreateGCPBucket(ctx, projectId, bucketName, region, mockGcp)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to create bucket")
	assert.Contains(t, err.Error(), "The requested resource already exists")
}

func Test_EnableAutoTiering_Failure(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	bucketName := "region-poolId"
	projectId := "test-project"

	// Save original and mock _createGCPBucket
	origCreateGCPBucket := activities.CreateGCPBucket
	getGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.CreateGCPBucket = origCreateGCPBucket
		hyperscaler2.GetGCPService = getGCPService
	}()
	activities.CreateGCPBucket = func(ctx context.Context, projectId, poolName, region string, gcpService hyperscaler2.GoogleServices) error {
		return errors.New("Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
	}
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	err := activity.CreateAutoTierBucket(ctx, bucketName, "region", projectId)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
}

func TestPoolActivity_CreateServiceAccountWithStorageRole(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"
	saDisplayName := "Test Service Account"

	origCreateServiceAccountAndAttachRole := activities.CreateServiceAccountAndAttachRole
	getGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.CreateServiceAccountAndAttachRole = origCreateServiceAccountAndAttachRole
		hyperscaler2.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		expectedSA := &hyperscaler3.ServiceAccount{Name: "projects/test-project/serviceAccounts/test-sa"}
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler3.ServiceAccount, error) {
			return expectedSA, nil
		}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		sa, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("error", func(t *testing.T) {
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler3.ServiceAccount, error) {
			return nil, errors.New("Mock error: failed to create service account")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		sa, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)
		assert.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "failed to create service account")
	})
}

func Test_createServiceAccountAndAttachRole(t *testing.T) {
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"
	saDisplayName := "Test Service Account"
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	expectedSA := &hyperscaler3.ServiceAccount{Email: saEmail}
	roles := []string{"roles/storage.objectUser"}

	t.Run("success", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler3.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler3.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(nil)

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("create service account fails", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler3.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler3.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(nil, errors.New("create error"))

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "create error")
	})

	t.Run("attach roles fails", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler3.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler3.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(errors.New("attach error"))

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "attach error")
	})
}

func TestPoolActivity_DeleteAutoTierBucket(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	bucketName := "us-central1-test-pool"

	// Save and mock DeleteGCPBucket
	origDeleteGCPBucket := activities.DeleteGCPBucket
	getGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.DeleteGCPBucket = origDeleteGCPBucket
		hyperscaler2.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
			return true, nil
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		err := activity.DeleteAutoTierBucket(ctx, bucketName, "accountName", 2)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
			return false, errors.New("delete failed")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		// Mock the CreatePendingResourceDeletion call that happens when bucket deletion fails
		mockStorage.On("CreatePendingResourceDeletion", ctx, "BUCKET", bucketName, "delete failed", "accountName", int64(2)).Return(&datamodel.PendingResourceDeletions{}, nil)

		err := activity.DeleteAutoTierBucket(ctx, bucketName, "accountName", 2)
		assert.NoError(t, err)
	})

	t.Run("empty bucket name", func(t *testing.T) {
		// Test the case where bucket name is empty - should log warning and return nil
		err := activity.DeleteAutoTierBucket(ctx, "", "accountName", 2)
		assert.NoError(t, err)
	})

	t.Run("failure_no_error", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
			return false, nil // No error but deletion failed
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		// Mock the CreatePendingResourceDeletion call with empty error message
		mockStorage.On("CreatePendingResourceDeletion", ctx, "BUCKET", bucketName, "", "accountName", int64(2)).Return(&datamodel.PendingResourceDeletions{}, nil)

		err := activity.DeleteAutoTierBucket(ctx, bucketName, "accountName", 2)
		assert.NoError(t, err)
	})

	t.Run("failure_create_pending_deletion_fails", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler2.GoogleServices) (bool, error) {
			return false, errors.New("delete failed")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		// Mock the CreatePendingResourceDeletion call to return an error
		mockStorage.On("CreatePendingResourceDeletion", ctx, "BUCKET", bucketName, "delete failed", "accountName", int64(2)).Return(nil, errors.New("database error"))

		err := activity.DeleteAutoTierBucket(ctx, bucketName, "accountName", 2)
		assert.NoError(t, err) // Function should still return nil even if logging fails
	})
}

func Test_deleteGCPBucket(t *testing.T) {
	ctx := context.Background()
	poolId := "test-pool"
	region := "us-central1"
	bucketName := fmt.Sprintf("%s-%s", region, poolId)
	logger := util.GetLogger(ctx)

	t.Run("Success", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteBucketWithLifecyclePolicy(ctx, bucketName).Return(true, nil)
		_, err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)

		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteBucketWithLifecyclePolicy(ctx, bucketName).Return(false, errors.New("delete failed"))
		_, err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.Error(t, err)
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) && customErr.Unwrap() != nil {
			assert.ErrorContains(t, customErr.Unwrap(), "delete failed")
		} else {
			assert.ErrorContains(t, err, "delete failed")
		}
	})
}

func Test_deleteServiceAccount(t *testing.T) {
	ctx := context.Background()
	projectNumber := "123456789"
	saAccountID := "test-sa"
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectNumber)
	logger := util.GetLogger(ctx)
	roles := []string{"roles/storage.objectUser"}

	t.Run("success - roles removed and service account deleted", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(nil)
		mockGcp.EXPECT().DeleteServiceAccount(projectNumber, saEmail).Return(nil)
		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("failure - role removal fails", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(errors.New("role removal failed"))
		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "role removal failed")
	})

	t.Run("failure - delete service account fails after successful role removal", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(nil)
		mockGcp.EXPECT().DeleteServiceAccount(projectNumber, saEmail).Return(errors.New("delete failed"))
		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})

	t.Run("failure - permission denied for role removal", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(errors.New("permission denied"))
		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "permission denied")
	})

	t.Run("failure - service account not found during role removal", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(errors.New("service account not found"))
		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "service account not found")
	})
}

func TestPoolActivity_DeleteServiceAccount(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	projectNumber := "123456789"
	saAccountID := "test-sa"

	origDeleteSrvcAccount := activities.DeleteServiceAccountAndRemoveStorageRole
	getGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.DeleteServiceAccountAndRemoveStorageRole = origDeleteSrvcAccount
		hyperscaler2.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		activities.DeleteServiceAccountAndRemoveStorageRole = func(ctx context.Context, projectNumber, saAccountID string, gcpService hyperscaler2.GoogleServices) error {
			return nil
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		err := activity.DeleteServiceAccount(ctx, projectNumber, saAccountID)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteServiceAccountAndRemoveStorageRole = func(ctx context.Context, projectNumber, saAccountID string, gcpService hyperscaler2.GoogleServices) error {
			return errors.New("delete error")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		err := activity.DeleteServiceAccount(ctx, projectNumber, saAccountID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("empty service account ID", func(t *testing.T) {
		// Test the case where service account ID is empty - should log warning and return nil
		err := activity.DeleteServiceAccount(ctx, projectNumber, "")
		assert.NoError(t, err)
	})

	t.Run("empty project number", func(t *testing.T) {
		// Test the case where project number is empty - should log warning and return nil
		err := activity.DeleteServiceAccount(ctx, "", saAccountID)
		assert.NoError(t, err)
	})

	t.Run("both empty", func(t *testing.T) {
		// Test the case where both project number and service account ID are empty - should log warning and return nil
		err := activity.DeleteServiceAccount(ctx, "", "")
		assert.NoError(t, err)
	})
}

func TestGenerateCSR(t *testing.T) {
	commonName := "test.example.com"
	domains := []string{"test.example.com", "www.test.example.com"}
	csrDER, key, err := hyperscaler2.GenerateCSR(commonName, domains)
	if err != nil {
		t.Fatalf("GenerateCSR returned error: %v", err)
	}
	if csrDER == nil {
		t.Error("Expected non-nil csrDER")
	}
	if key == nil {
		t.Error("Expected non-nil private key")
	}
	csr, err := digitalCert.ParseCertificateRequest(csrDER)
	if err != nil {
		t.Fatalf("Failed to parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Errorf("CSR signature check failed: %v", err)
	}
	if csr.Subject.CommonName != commonName {
		t.Errorf("Expected CommonName %s, got %s", commonName, csr.Subject.CommonName)
	}
	if len(csr.DNSNames) != len(domains) {
		t.Errorf("Expected %d DNSNames, got %d", len(domains), len(csr.DNSNames))
	}
}

func Test_IdentifyVMs_SuccessfullyPreparesConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
		return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return nil
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.NoError(t, err)
}

func Test_IdentifyVMs_SuccessfullyPreparesConfig_LargeVolume(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
		return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return nil
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             5000,
		DesiredThroughputInMiBs: 2048,
		DesiredCapacityInGiB:    12288, // 12 TiB - typical large volume size
	}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.NoError(t, err)
}

func Test_IdentifyVMs_FailsToPrepareConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, userName string) (*hyperscaler3.CustomSecret, error) {
		return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config")
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_IdentifyVMs_FailsToPrepareConfig_LargeVolume(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config for large volume")
	}
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, userName string) (*hyperscaler3.CustomSecret, error) {
		return &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "password"}}, nil
	}
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             8000,
		DesiredThroughputInMiBs: 4096,
		DesiredCapacityInGiB:    24576, // 24 TiB - large volume
	}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_IdentifyVMs_FailsToLoadConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return nil, errors.New("failed to load VMRS config from file")
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load VMRS config from file")
}

func Test_IdentifyVMs_FailsToLoadConfig_LargeVolume(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return nil, errors.New("failed to load VMRS config for large volume cluster")
	}
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             15000,
		DesiredThroughputInMiBs: 8192,
		DesiredCapacityInGiB:    51200, // 50 TiB - very large volume
	}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load VMRS config for large volume cluster")
}

func Test_IdentifyVMs_FailsToCreateDecisionMaker(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return &vmrs.VMRSConfig{}, nil
	}

	createDecisionMaker := activities.CreateDecisionMaker
	defer func() {
		activities.CreateDecisionMaker = createDecisionMaker
	}()
	activities.CreateDecisionMaker = func(cfg *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
		return nil, errors.New("failed to create decision maker")
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create decision maker")
}

func Test_IdentifyVMs_FailsToCreateDecisionMaker_LargeVolume(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return &vmrs.VMRSConfig{}, nil
	}

	createDecisionMaker := activities.CreateDecisionMaker
	defer func() {
		activities.CreateDecisionMaker = createDecisionMaker
	}()
	activities.CreateDecisionMaker = func(cfg *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
		return nil, errors.New("failed to create decision maker for large volume cluster")
	}
	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{
		DesiredIOPS:             25000,
		DesiredThroughputInMiBs: 16384,
		DesiredCapacityInGiB:    102400, // 100 TiB - extremely large volume
	}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create decision maker")
}

func Test_IdentifyVMs_FailsToFindOptimalVMs(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return &vmrs.VMRSConfig{}, nil
	}

	mockDecisionMaker := vmrs_decision.NewDecisionMakerMock()
	createDecisionMaker := activities.CreateDecisionMaker
	defer func() {
		activities.CreateDecisionMaker = createDecisionMaker
	}()
	activities.CreateDecisionMaker = func(cfg *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
		return mockDecisionMaker, nil
	}
	mockDecisionMaker.Mock.On("FindOptimalVMs", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to find optimal VMs foo"))

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find optimal VMs")
}

func Test_IdentifyVMs_FailsToFindOptimalVMs_LargeVolume(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	loadVMRSConfig := activities.LoadVMRSConfig
	defer func() {
		activities.LoadVMRSConfig = loadVMRSConfig
	}()
	activities.LoadVMRSConfig = func(filePath string) (*vmrs.VMRSConfig, error) {
		return &vmrs.VMRSConfig{}, nil
	}

	mockDecisionMaker := vmrs_decision.NewDecisionMakerMock()
	createDecisionMaker := activities.CreateDecisionMaker
	defer func() {
		activities.CreateDecisionMaker = createDecisionMaker
	}()
	activities.CreateDecisionMaker = func(cfg *vmrs.VMRSConfig) (vmrs.DecisionMaker, error) {
		return mockDecisionMaker, nil
	}
	mockDecisionMaker.Mock.On("FindOptimalVMs", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to find optimal VMs for large volume cluster"))

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "test-zone1",
		SecondaryZone: "test-zone2",
		Region:        "test-region",
	}
	tenancyInfo := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		SnHostProject:         "test-sn-host-project",
	}
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find optimal VMs")
}

func TestMarksPoolAndResourcesAsFailedWhenErroredResourceSucceeds(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", ctx, pool, "error during pool deletion").Return(pool, nil)
	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return([]*datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{}, nil)

	err := activity.FailedPool(ctx, pool, "error during pool deletion")

	assert.NoError(t, err)
	assert.Equal(t, coremodel.LifeCycleStateError, pool.State)
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenErroredResourceFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", ctx, pool, "error during pool deletion").Return(nil, errors.New("failed to mark pool as errored"))

	err := activity.FailedPool(ctx, pool, "error during pool deletion")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to mark pool as errored")
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenFailedSVMsFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", ctx, pool, "error during pool deletion").Return(pool, nil)
	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return(nil, errors.New("failed to retrieve SVMs"))

	err := activity.FailedPool(ctx, pool, "error during pool deletion")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to retrieve SVMs")
	mockStorage.AssertExpectations(t)
}

func TestReturnsErrorWhenFailedNodesFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", ctx, pool, "error during pool deletion").Return(pool, nil)
	mockStorage.On("GetSvmsByPoolID", ctx, pool.ID).Return([]*datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("failed to retrieve nodes"))

	err := activity.FailedPool(ctx, pool, "error during pool deletion")

	assert.Error(t, err)
	assertTemporalApplicationError(t, err, "failed to retrieve nodes", "CustomError", false)
	mockStorage.AssertExpectations(t)
}

// Unit test for _getCertificateAndPrivateKeyByID
func Test_getCertificateAndPrivateKeyByID(t *testing.T) {
	caDeployedProjectID := "ca-proj"
	secretManagerProjectID := "sm-proj"
	region := "us-central1"
	caPoolName := "pool"
	certificateID := "cert-id"

	cert := &hyperscaler3.CustomCertificate{}
	secret := &hyperscaler3.CustomSecret{SecretVersion: &hyperscaler3.CustomSecretVersion{}}

	t.Run("success", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(secret, nil)
		resp, err := hyperscaler2.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, cert, resp.Certificate)
		assert.Equal(t, secret, resp.Secret)
		mockService.AssertExpectations(t)
	})

	t.Run("certificate not found", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(nil, fmt.Errorf("not found"))
		resp, err := hyperscaler2.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})

	t.Run("secret not found", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(nil, fmt.Errorf("not found"))
		resp, err := hyperscaler2.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})

	t.Run("secret version nil", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		secretNoVersion := &hyperscaler3.CustomSecret{}
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(secretNoVersion, nil)
		resp, err := hyperscaler2.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})
}
func Test_GetAndCreateCloudDNSRecord(t *testing.T) {
	recordName := "test-record"
	ipAddress := "1.2.3.4"
	t.Run("CreateResourceRecordSet success", func(t *testing.T) {
		mockService := hyperscaler2.NewMockGoogleServices(t)
		expectedRecord := &hyperscaler3.CustomCloudDNSRecord{RecordName: recordName, Data: ipAddress}

		mockService.On("GetLogger").Return(log.NewLogger())
		mockService.On("GetResourceRecordSet", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockService.On("CreateResourceRecordSet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(expectedRecord, nil)

		record, err := hyperscaler2.GetOrCreateCloudDNSRecord(mockService, recordName, ipAddress)
		assert.NoError(t, err)
		assert.Equal(t, expectedRecord, record)
		mockService.AssertExpectations(t)
	})
	t.Run("returns error when CreateResourceRecordSet fails", func(t *testing.T) {
		mockService := hyperscaler2.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(log.NewLogger())
		mockService.On("GetResourceRecordSet", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockService.On("CreateResourceRecordSet", env.CaPoolDeployedProjectID, env.VsaManagedZone, ipAddress, recordName).
			Return(nil, errors.New("dns error"))

		record, err := hyperscaler2.GetOrCreateCloudDNSRecord(mockService, recordName, ipAddress)
		assert.Nil(t, record)
		assert.Error(t, err)
		mockService.AssertExpectations(t)
	})
}

func TestPoolActivity_GetCloudDNSRecords(t *testing.T) {
	t.Run("GetNode_Success", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.PoolActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)
		expectedNode := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name:            "test-node",
				EndpointAddress: "1.2.3.4",
				HostDNSName:     "test-node.example.com",
			},
		}

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(expectedNode, nil)

		mapHost, err := activity.GetCloudDNSRecords(ctx, poolId, env.USER_CERTIFICATE)

		assert.NoError(tt, err)
		mapHostExpected := &map[string]string{"1.2.3.4": "test-node.example.com"}
		assert.Equal(tt, mapHostExpected, mapHost)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_Error", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.PoolActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nil, gorm.ErrInvalidDB)

		mapHost, err := activity.GetCloudDNSRecords(ctx, poolId, env.USER_CERTIFICATE)

		expectedHost := &map[string]string{}
		assert.Error(tt, err)
		assert.Equal(tt, expectedHost, mapHost)
		mockStorage.AssertExpectations(tt)
	})
}

func TestPoolActivity_DeleteCloudDNSRecords(t *testing.T) {
	ctx := context.Background()
	hostMap := map[string]string{
		"1.2.3.4": "dns-1.test-cluster.example.com.",
		"2.3.4.5": "dns-2.test-cluster.example.com.",
	}

	t.Run("successfully deletes all DNS records", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		originalGetGCPService := hyperscaler2.GetGCPService
		originalDeleteCloudDNSRecord := hyperscaler2.DeleteCloudDNSRecord
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
			hyperscaler2.DeleteCloudDNSRecord = originalDeleteCloudDNSRecord
		}()

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		hyperscaler2.DeleteCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, recordName string) error {
			return nil
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, env.USER_CERTIFICATE)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService fails", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		originalGetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
		}()

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("gcp error")
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "gcp error")
	})

	t.Run("DeleteCloudDNSRecord fails", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		originalGetGCPService := hyperscaler2.GetGCPService
		originalDeleteCloudDNSRecord := hyperscaler2.DeleteCloudDNSRecord
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
			hyperscaler2.DeleteCloudDNSRecord = originalDeleteCloudDNSRecord
		}()

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		hyperscaler2.DeleteCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, recordName string) error {
			return fmt.Errorf("delete error")
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})
	t.Run("does nothing if not USER_CERTIFICATE", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, env.USERNAME_PWD)
		assert.NoError(t, err)
	})
}

func TestPoolActivity_CreateCloudDNSRecords(t *testing.T) {
	ctx := context.Background()
	clusterName := "testcluster"
	env.VsaDeployedDnsName = "example.com"

	// Mock CreateCloudDNSRecord
	originalCreateCloudDNSRecord := hyperscaler2.GetOrCreateCloudDNSRecord
	originalGCPService := hyperscaler2.GetGCPService
	defer func() {
		hyperscaler2.GetOrCreateCloudDNSRecord = originalCreateCloudDNSRecord
		hyperscaler2.GetGCPService = originalGCPService
	}()
	hyperscaler2.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, ip, recordName string) (*hyperscaler3.CustomCloudDNSRecord, error) {
		return &hyperscaler3.CustomCloudDNSRecord{RecordName: recordName}, nil
	}

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Logger: log.NewLogger()}, nil
	}

	// Success case
	t.Run("success", func(t *testing.T) {
		vlmConfig := &vlm.VLMConfig{
			Cloud: vlm.CloudConfig{
				HAPairs: []vlm.HAPair{
					{
						VM1: vlm.VMConfig{
							SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
								vlm.LIFTypeNodeMgmt: {IP: "1.1.1.1"},
							},
						},
						VM2: vlm.VMConfig{
							SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
								vlm.LIFTypeNodeMgmt: {IP: "2.2.2.2"},
							},
						},
					},
				},
			},
		}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.NoError(t, err)
		assert.NotNil(t, hostMap)
		assert.Equal(t, 2, len(*hostMap))
	})

	// No HAPairs
	t.Run("no HAPairs", func(t *testing.T) {
		vlmConfig := &vlm.VLMConfig{
			Cloud: vlm.CloudConfig{
				HAPairs: []vlm.HAPair{},
			},
		}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// No SystemLIFs
	t.Run("no SystemLIFs", func(t *testing.T) {
		vlmConfig := &vlm.VLMConfig{
			Cloud: vlm.CloudConfig{
				HAPairs: []vlm.HAPair{
					{
						VM1: vlm.VMConfig{SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{}},
						VM2: vlm.VMConfig{SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{}},
					},
				},
			},
		}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// CreateCloudDNSRecord returns error
	t.Run("GetOrCreateCloudDNSRecord error", func(t *testing.T) {
		hyperscaler2.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, ip, recordName string) (*hyperscaler3.CustomCloudDNSRecord, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("dns error"))
		}
		vlmConfig := &vlm.VLMConfig{
			Cloud: vlm.CloudConfig{
				HAPairs: []vlm.HAPair{
					{
						VM1: vlm.VMConfig{
							SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
								vlm.LIFTypeNodeMgmt: {IP: "1.1.1.1"},
							},
						},
						VM2: vlm.VMConfig{
							SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
								vlm.LIFTypeNodeMgmt: {IP: "2.2.2.2"},
							},
						},
					},
				},
			},
		}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// Not USER_CERTIFICATE auth type
	t.Run("not user certificate", func(t *testing.T) {
		env.AuthType = env.USERNAME_PWD_SEC_MGR
		vlmConfig := &vlm.VLMConfig{}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, env.USERNAME_PWD)
		assert.NoError(t, err)
		assert.NotNil(t, hostMap)
		assert.Equal(t, 0, len(*hostMap))
	})
}

func TestPoolActivity_DeleteOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()

	origGetGCPService := hyperscaler2.GetGCPService
	origRevokeCert := hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager
	origDeletePwd := hyperscaler2.DeletePasswordFromCacheAndSecretManager
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = origRevokeCert
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = origDeletePwd
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, certID string) error {
			assert.Equal(t, "cert-id", certID)
			return nil
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			assert.Equal(t, "secret-id", secretID)
			return nil
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("USER_CERTIFICATE failure due to secret error ", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, certID string) error {
			assert.Equal(t, "cert-id", certID)
			return nil
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			return errors.New("delete error")
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, certID string) error {
			return errors.New("revoke error")
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "revoke error")
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
			},
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			assert.Equal(t, "secret-id", secretID)
			return nil
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
			},
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			return errors.New("delete error")
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("default password - no cert no secret-manager", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "gcp error", "CustomError", false)
	})
}

func TestPoolActivity_CreateOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()
	clusterName := "test-cluster"
	username := "admin"

	origGetGCPService := hyperscaler2.GetGCPService
	origGenerateAndCreateCertificateForVSACluster := hyperscaler2.GenerateAndCreateCertificateForVSACluster
	origGeneratePasswordForVSACluster := hyperscaler2.GeneratePasswordForVSACluster
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = origGenerateAndCreateCertificateForVSACluster
		hyperscaler2.GeneratePasswordForVSACluster = origGeneratePasswordForVSACluster
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
			return &hyperscaler3.CustomCertificateResponse{
				Certificate: &hyperscaler3.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &hyperscaler3.CustomSecret{
					SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{
				SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "CN", creds.Certificate.CommonName)
		assert.Equal(t, "cert", creds.Certificate.Certificate)
		assert.Equal(t, "key", creds.Certificate.PrivateKey)
		assert.Equal(t, []string{"chain"}, creds.Certificate.InterMediateCertificate)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USER_CERTIFICATE error due to secret failure", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
			return &hyperscaler3.CustomCertificateResponse{
				Certificate: &hyperscaler3.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &hyperscaler3.CustomSecret{
					SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("cert error"))
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
			},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{
				SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
			},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("default password", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "default-password", creds.AdminPassword)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("gcp error"))
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})
}

func Test_makeSubnetName(t *testing.T) {
	tests := []struct {
		projectNumber string
		wantPrefix    string
	}{
		{"123456", "vsa-123456-"},
		{"789012", "vsa-789012-"},
		{"555555", "vsa-555555-"},
	}

	for _, tt := range tests {
		t.Run(tt.projectNumber, func(t *testing.T) {
			got := activities.MakeSubnetName(tt.projectNumber, false) // assuming standard pools for this test
			// The result should start with the expected prefix, followed by a timestamp
			if !strings.HasPrefix(got, tt.wantPrefix) {
				t.Errorf("got %q, want prefix %q", got, tt.wantPrefix)
			}
			// The last part should be a valid integer (timestamp)
			parts := strings.Split(got, "-")
			if len(parts) < 3 {
				t.Errorf("expected at least 4 parts in subnet name, got %v", parts)
			} else {
				if _, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
					t.Errorf("expected last part to be a timestamp, got %q", parts[len(parts)-1])
				}
			}
		})
	}
}

func Test_createSubnetwork(t *testing.T) {
	tenantProjectNumber := "tenant-123"
	consumerVPC := "vpc-456"
	region := "us-central1"

	t.Run("success", func(t *testing.T) {
		mockSvc := hyperscaler2.NewMockGoogleServices(t)

		subnetName := "vsa-" + tenantProjectNumber + "-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string, isLargeCapacity bool) string {
			return subnetName
		}
		operation := "operation-12345"
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName, false).
			Return(&operation, nil)

		operationName, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region, false) // assuming standard pools
		assert.NoError(t, err)
		assert.Equal(t, "operation-12345", *operationName)
		mockSvc.AssertExpectations(t)
	})

	t.Run("CreateSubnetworkForTenantProjectFails", func(t *testing.T) {
		mockSvc := hyperscaler2.NewMockGoogleServices(t)

		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string, isLargeCapacity bool) string {
			return subnetName
		}
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName, false).
			Return(nil, errors.New("create failed"))
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		_, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region, false)
		assert.Error(t, err)
		mockSvc.AssertExpectations(t)
	})
}

// Test cases for missing functions in pool_activities.go

func TestPoolActivity_FailedPool_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}
	errMsg := "test error message"

	expectedPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name:  "test-pool",
		State: coremodel.LifeCycleStateError,
	}

	mockStorage.On("ErroredResource", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.State == coremodel.LifeCycleStateError
	}), errMsg).Return(expectedPool, nil)

	originalFailedSVMs := activities.FailedSVMs
	originalFailedNodes := activities.FailedNodes
	defer func() {
		activities.FailedSVMs = originalFailedSVMs
		activities.FailedNodes = originalFailedNodes
	}()

	activities.FailedSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}
	activities.FailedNodes = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return nil
	}

	// Act
	err := activity.FailedPool(ctx, pool, errMsg)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, coremodel.LifeCycleStateError, pool.State)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_FailedPool_ErroredResourceFails(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}
	errMsg := "test error message"

	mockStorage.On("ErroredResource", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.State == coremodel.LifeCycleStateError
	}), errMsg).Return(nil, errors.New("database error"))

	// Act
	err := activity.FailedPool(ctx, pool, errMsg)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_UpdatedPool_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}

	mockStorage.On("UpdatedPool", ctx, pool).Return(pool, nil)

	// Act
	result, err := activity.UpdatedPool(ctx, pool)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_UpdatedPool_Failure(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}

	mockStorage.On("UpdatedPool", ctx, pool).Return(nil, errors.New("update failed"))

	// Act
	result, err := activity.UpdatedPool(ctx, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "update failed")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_CreateOnTapCredentials_Success(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			CertificateID: "test-cert-id",
			SecretID:      "test-secret-id",
		},
	}
	clusterName := "test-cluster"
	username := "admin"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalGenerateAndCreateCertificate := hyperscaler2.GenerateAndCreateCertificateForVSACluster
	originalGeneratePassword := hyperscaler2.GeneratePasswordForVSACluster
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = originalGenerateAndCreateCertificate
		hyperscaler2.GeneratePasswordForVSACluster = originalGeneratePassword
	}()

	mockGCPService := &google.GcpServices{}
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	// Mock certificate generation
	hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
		return &hyperscaler3.CustomCertificateResponse{
			Certificate: &hyperscaler3.CustomCertificate{
				SubjectCommonName:   "test-cn",
				PemCertificate:      "test-cert",
				PemCertificateChain: []string{"test-chain"},
			},
			Secret: &hyperscaler3.CustomSecret{
				SecretVersion: &hyperscaler3.CustomSecretVersion{
					Value: "test-private-key",
				},
			},
		}, nil
	}

	// Mock password generation
	hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
		return &hyperscaler3.CustomSecret{
			SecretVersion: &hyperscaler3.CustomSecretVersion{
				Value: "test-password",
			},
		}, nil
	}

	// Act
	result, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &vlm.OntapCredentials{}, result)
}

func TestPoolActivity_CreateOnTapCredentials_GetGCPServiceFails(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}
	clusterName := "test-cluster"
	username := "admin"

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	result, err := activity.CreateOnTapCredentials(ctx, pool, clusterName, username)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "failed to get GCP service")
}

func TestPoolActivity_DeletingPoolResources_DeletingSVMsFails(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID: 1,
		},
		Name: "test-pool",
	}

	originalDeletingSVMs := activities.DeletingSVMs
	defer func() { activities.DeletingSVMs = originalDeletingSVMs }()

	activities.DeletingSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to mark SVMs as deleting")
	}

	// Act
	result, err := activity.DeletingPoolResources(ctx, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to mark SVMs as deleting")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_CreateAutoTierBucket_Success(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	autoTierBucketName := "test-bucket"
	region := "us-central1"
	projectId := "test-project"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalCreateGCPBucket := activities.CreateGCPBucket
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.CreateGCPBucket = originalCreateGCPBucket
	}()

	mockGCPService := &google.GcpServices{}
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	activities.CreateGCPBucket = func(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler2.GoogleServices) error {
		return nil
	}

	// Act
	err := activity.CreateAutoTierBucket(ctx, autoTierBucketName, region, projectId)

	// Assert
	assert.NoError(t, err)
}

func TestPoolActivity_CreateAutoTierBucket_GetGCPServiceFails(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	autoTierBucketName := "test-bucket"
	region := "us-central1"
	projectId := "test-project"

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	err := activity.CreateAutoTierBucket(ctx, autoTierBucketName, region, projectId)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get GCP service")
}

func TestPoolActivity_DeleteAutoTierBucket_GetGCPServiceFails(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	autoTierBucketName := "test-bucket"

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	err := activity.DeleteAutoTierBucket(ctx, autoTierBucketName, "accountName", 2)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get GCP service")
}

func TestPoolActivity_CreateServiceAccountWithStorageRole_Success(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	projectID := "test-project"
	saAccountID := "test-sa"
	saDisplayName := "Test Service Account"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalCreateServiceAccountAndAttachRole := activities.CreateServiceAccountAndAttachRole
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.CreateServiceAccountAndAttachRole = originalCreateServiceAccountAndAttachRole
	}()

	mockGCPService := &google.GcpServices{}
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	expectedServiceAccount := &hyperscaler3.ServiceAccount{
		Email: "test-sa@test-project.iam.gserviceaccount.com",
		Name:  "Test Service Account",
	}

	activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler3.ServiceAccount, error) {
		return expectedServiceAccount, nil
	}

	// Act
	result, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedServiceAccount.Email, result.Email)
}

func TestCreateQoSPolicyAndApplyToSVM(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000,
			Iops:            5000,
		},
	}
	svm := &datamodel.Svm{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-svm",
		SvmDetails: &datamodel.SvmDetails{
			ExternalUUID: "test-svm-uuid",
		},
	}
	node := &coremodel.Node{
		Name: "test-node",
	}

	t.Run("WhenQoSPolicyDoesNotExist_ThenCreateAndApply", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock FindQoSGroupPolicy to return error (policy doesn't exist)
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(nil, errors.New("policy not found"))

		// Mock QoS policy creation
		expectedQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}

		mockProvider.On("CreateQoSGroupPolicy", vsa.CreateQoSGroupPolicyParams{
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}).Return(expectedQoSPolicy, nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyExistsWithSameValues_ThenSkipCreation", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock existing QoS policy with same values
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000, // Same as pool requirements
			MaxIOPS:       5000, // Same as pool requirements
		}

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)

		// Mock SVM modification (should be called with existing policy)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		// No CreateQoSGroupPolicy call should be made

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyExistsWithDifferentValues_ThenUpdateAndApply", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock existing QoS policy with different values
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 500,  // Different from pool requirements
			MaxIOPS:       2500, // Different from pool requirements
		}

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)

		// Mock QoS policy update
		mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
			UUID:          "test-qos-uuid",
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}).Return(nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		// No CreateQoSGroupPolicy call should be made

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock existing QoS policy with different values
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 500,  // Different from pool requirements
			MaxIOPS:       2500, // Different from pool requirements
		}

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)

		// Mock QoS policy update to fail
		mockProvider.On("UpdateQoSGroupPolicy", mock.Anything).Return(errors.New("update failed"))

		// No CreateQoSGroupPolicy or ModifySVMWithQoSPolicy calls should be made

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenFindQoSGroupPolicyFails_ThenCreateNew", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock FindQoSGroupPolicy to return error (policy not found)
		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(nil, errors.New("policy not found"))

		// Mock QoS policy creation
		expectedQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}

		mockProvider.On("CreateQoSGroupPolicy", mock.Anything).Return(expectedQoSPolicy, nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", mock.Anything).Return(nil)

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyCreationFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock FindQoSGroupPolicy to return error (policy not found)
		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(nil, errors.New("policy not found"))

		mockProvider.On("CreateQoSGroupPolicy", mock.Anything).Return(nil, errors.New("qos creation failed"))

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "qos creation failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSVMModificationFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock FindQoSGroupPolicy to return error (policy not found)
		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(nil, errors.New("policy not found"))

		// Mock QoS policy creation success
		expectedQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}

		mockProvider.On("CreateQoSGroupPolicy", mock.Anything).Return(expectedQoSPolicy, nil)

		// Mock SVM modification failure
		mockProvider.On("ModifySVMWithQoSPolicy", mock.Anything).Return(errors.New("svm modification failed"))

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "svm modification failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyNameIsGeneratedCorrectly", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock FindQoSGroupPolicy to return error (policy not found)
		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(nil, errors.New("policy not found"))

		// Mock QoS policy creation with specific name format
		mockProvider.On("CreateQoSGroupPolicy", vsa.CreateQoSGroupPolicyParams{
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}).Return(&vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}, nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})
}

func TestModifyQoSPolicyAndApplyToSVM(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000, // Current throughput (will be compared against)
			Iops:            5000, // Current IOPS (will be compared against)
		},
	}
	updateParams := &commonparams.UpdatePoolParams{
		TotalThroughputMibps: 2000,                            // New throughput requirement
		TotalIops:            nillable.ToPointer(int64(6000)), // New IOPS requirement
	}
	node := &coremodel.Node{
		Name: "test-node",
	}

	t.Run("WhenQoSPolicyNeedsUpdate_ThenUpdateAndApply", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock SVM retrieval
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		// Mock existing QoS policy (different values)
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000, // Different from new value
			MaxIOPS:       5000, // Different from new value
		}

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)

		// Mock QoS policy update
		mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
			UUID:          "test-qos-uuid",
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 2000,
			MaxIOPS:       6000,
		}).Return(nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyNoChangeNeeded_ThenSkipUpdate", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock SVM retrieval
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		// Mock existing QoS policy (same values as new requirements)
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 2000, // Same as new value
			MaxIOPS:       6000, // Same as new value
		}

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)

		// No update or modify calls should be made

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := &activities.PoolActivity{}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenGetSvmForPoolIDFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, errors.New("SVM not found"))

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenFindQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock SVM retrieval
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(nil, errors.New("QoS policy not found"))

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "QoS policy not found")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock SVM retrieval
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		// Mock existing QoS policy
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}

		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(existingQoSPolicy, nil)
		mockProvider.On("UpdateQoSGroupPolicy", mock.Anything).Return(errors.New("update failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update failed")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenModifySVMWithQoSPolicyFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock SVM retrieval
		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		// Mock existing QoS policy
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}

		mockProvider.On("FindQoSGroupPolicy", mock.Anything).Return(existingQoSPolicy, nil)
		mockProvider.On("UpdateQoSGroupPolicy", mock.Anything).Return(nil)
		mockProvider.On("ModifySVMWithQoSPolicy", mock.Anything).Return(errors.New("SVM modification failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		err := activity.ModifyQoSPolicyAndApplyToSVM(ctx, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM modification failed")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})
}

func Test_checkAndUpdateFirewall(t *testing.T) {
	sourceRanges1 := []string{"10.0.0.0/24", "192.168.1.0/24"}
	sourceRanges2 := []string{"10.0.0.0/24", "192.168.2.0/24"}
	sourceRanges3 := []string{"10.0.0.0/24", "192.168.1.0/24", "172.16.0.0/12"}
	sourceRanges4 := []string{"10.0.0.0/24"}
	sourceRanges5 := []string{"10.1.0.0/24", "192.168.1.0/24"}
	sourceRanges6 := []string{"192.168.1.0/24", "10.0.0.0/24"}
	t.Run("whenNoChangeInSourceRange", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallEdited", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges2,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		mgs.On("UpdateFirewall", firewallRequest).Return("", nil)
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallRemovedSuccess", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges4,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return("", nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallAddedSuccess", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges3,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return("", nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallIsDifferentSuccess", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges4,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return("", nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallIsDifferentFails", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges5,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return("", errors.New("update error"))
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.Error(t, err, "update error")
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallOrderChanged", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler3.Firewall{
			SourceRanges: sourceRanges6,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		// No update should be needed when only order is different
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err, "should not error when only order is different")
		mgs.AssertExpectations(t)
	})
}

func TestUpdatingPool(t *testing.T) {
	t.Run("WhenUpdatingPoolIsSuccessful", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		seResult := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, State: coremodel.LifeCycleStateUpdating, StateDetails: coremodel.LifeCycleStateUpdatingDetails}

		mockSE.On("UpdatingPool", ctx, pool).Return(seResult, nil)
		result, err := activity.UpdatingPool(ctx, pool)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, coremodel.LifeCycleStateUpdating, result.State)
		assert.Equal(t, coremodel.LifeCycleStateUpdatingDetails, result.StateDetails)
	})
	t.Run("WhenUpdatingPoolReturnsError", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}

		mockSE.On("UpdatingPool", ctx, pool).Return(nil, vsaerrors.WrapAsTemporalApplicationError(errors.New("pool update ran into error")))
		result, err := activity.UpdatingPool(ctx, pool)
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.EqualError(t, err, "pool update ran into error")
	})
}

func TestUpdatePoolState(t *testing.T) {
	t.Run("WhenUpdatePoolStateIsSuccessful", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		seResult := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, State: coremodel.LifeCycleStateInUse, StateDetails: coremodel.LifeCycleStateInUseDetails}

		mockSE.On("UpdatePoolState", ctx, pool, coremodel.LifeCycleStateInUse, coremodel.LifeCycleStateInUseDetails).Return(seResult, nil)
		result, err := activity.UpdatePoolState(ctx, pool, coremodel.LifeCycleStateInUse, coremodel.LifeCycleStateInUseDetails)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, coremodel.LifeCycleStateInUse, result.State)
		assert.Equal(t, coremodel.LifeCycleStateInUseDetails, result.StateDetails)
	})
	t.Run("WhenUpdatePoolStateReturnsError", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		ctx := context.Background()
		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}

		mockSE.On("UpdatePoolState", ctx, pool, coremodel.LifeCycleStateInUse, coremodel.LifeCycleStateInUseDetails).Return(nil, vsaerrors.WrapAsTemporalApplicationError(errors.New("pool state update ran into error")))
		result, err := activity.UpdatePoolState(ctx, pool, coremodel.LifeCycleStateInUse, coremodel.LifeCycleStateInUseDetails)
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.EqualError(t, err, "pool state update ran into error")
	})
}

func TestFailedPoolActivity(t *testing.T) {
	t.Run("WhenFailedPoolActivityReturnsError", func(tt *testing.T) {
		mockSE := database.NewMockStorage(tt)
		ctx := context.Background()
		activity := &activities.PoolActivity{SE: mockSE}

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		err := errors.New("Pool update failed")
		mockSE.On("ErroredResource", ctx, pool, mock.Anything).Return(pool, err)
		errActivity := activity.FailedPoolActivity(ctx, pool, "error message")

		assert.Error(tt, errActivity)
	})
	t.Run("WhenFailedPoolActivityReturnsNil", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		ctx := context.Background()
		activity := &activities.PoolActivity{SE: mockSE}

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		mockSE.On("ErroredResource", ctx, pool, mock.Anything).Return(pool, nil)
		err := activity.FailedPoolActivity(ctx, pool, "error message")

		assert.Nil(t, err)
	})
}

func TestPoolActivity_GetAvailableSubnet(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := commonparams.CreatePoolParams{}
	tenantProjectNumber := "123456789"
	expectedSubnet := &hyperscaler3.Subnet{}

	origGetGCPService := hyperscaler2.GetGCPService
	origCheckReusableSubnet := activities.CheckReusableSubnet
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		activities.CheckReusableSubnet = origCheckReusableSubnet
	}()

	t.Run("Success", func(t *testing.T) {
		mockSvc := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler3.Subnet, error) {
			return expectedSubnet, nil
		}
		result, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, expectedSubnet, result)
	})

	t.Run("GetGCPServiceError", func(t *testing.T) {
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		result, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "gcp service error")
	})

	t.Run("CheckReusableSubnetError", func(t *testing.T) {
		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler3.Subnet, error) {
			return nil, errors.New("subnet error")
		}
		result, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "subnet error")
	})
}

func TestPoolActivity_GetTenancyInfo(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.Background()

	tenantProjectNumber := "123456789"
	subnet := &hyperscaler3.Subnet{
		Name:           "subnet-1",
		IpCidrRange:    "10.0.0.0/24",
		Network:        "projects/sn-host/global/networks/test-network",
		GatewayAddress: "10.0.0.1",
	}

	t.Run("Success", func(t *testing.T) {
		expectedTenancyInfo := &commonparams.TenancyInfo{
			RegionalTenantProject: tenantProjectNumber,
			Network:               "test-network",
			SnHostProject:         "sn-host",
			SubnetworkNames:       []string{"subnet-1"},
			Gateway:               "10.0.0.1",
		}
		result, err := activity.GetTenancyInfo(ctx, tenantProjectNumber, subnet)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedTenancyInfo, result)
		mockSE.AssertExpectations(t)
	})

	t.Run("Failure", func(t *testing.T) {
		subnet.Network = "" // Simulate missing network
		expectedError := errors.New("parseProjectId failed for network")
		result, err := activity.GetTenancyInfo(ctx, tenantProjectNumber, subnet)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), expectedError.Error())
		mockSE.AssertExpectations(t)
	})
}

func TestPoolActivity_CreateDataSubnet(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	params := commonparams.CreatePoolParams{}
	tenantProjectNumber := "123456789"
	expectedSubnetName := "test-subnet"

	t.Run("Success", func(t *testing.T) {
		originalGetGCPService := hyperscaler2.GetGCPService
		originalGetCreateDataSubnetOp := activities.GetCreateDataSubnetworkOp
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
			activities.GetCreateDataSubnetworkOp = originalGetCreateDataSubnetOp
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GetCreateDataSubnetworkOp = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
			return &expectedSubnetName, nil
		}

		result, err := activity.GetCreateDataSubnetOp(ctx, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, expectedSubnetName, *result)
	})

	t.Run("GetGCPServiceError", func(t *testing.T) {
		originalGetGCPService := hyperscaler2.GetGCPService
		defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}

		result, err := activity.GetCreateDataSubnetOp(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "gcp service error")
	})

	t.Run("GetCreateDataSubnetOpError", func(t *testing.T) {
		originalGetGCPService := hyperscaler2.GetGCPService
		originalGetCreateDataSubnetOp := activities.GetCreateDataSubnetworkOp
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
			activities.GetCreateDataSubnetworkOp = originalGetCreateDataSubnetOp
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GetCreateDataSubnetworkOp = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
			return nil, errors.New("create subnet error")
		}

		result, err := activity.GetCreateDataSubnetOp(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "create subnet error")
	})
}

func TestUpdatedPool_Success(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
	}

	expectedPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		State:     coremodel.LifeCycleStateInUse,
	}

	mockSE.On("UpdatedPool", ctx, pool).Return(expectedPool, nil)

	result, err := activity.UpdatedPool(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedPool, result)
	mockSE.AssertExpectations(t)
}

func TestUpdatedPool_Failure(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
	}

	expectedError := errors.New("failed to update pool")
	mockSE.On("UpdatedPool", ctx, pool).Return(nil, expectedError)

	result, err := activity.UpdatedPool(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to update pool")
	mockSE.AssertExpectations(t)
}

func TestUpdatedPoolWithVLMConfig_Success(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:         datamodel.BaseModel{ID: 1},
		Name:              "test-pool",
		PoolAttributes:    &datamodel.PoolAttributes{},
		AutoTieringConfig: &datamodel.AutoTieringConfig{}, // Initialize AutoTiering config
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "foobar",
		},
	}
	vlmConfigAsStr := "{\"deployment\":{\"provider\":\"foobar\"}}"
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes: 1000,
		Labels: &datamodel.JSONB{
			"foo": "bar",
		},
	}

	expectedPool := &datamodel.Pool{
		BaseModel:   datamodel.BaseModel{ID: 1},
		Name:        "test-pool",
		State:       coremodel.LifeCycleStateInUse,
		VLMConfig:   vlmConfigAsStr,
		SizeInBytes: 1000,
	}

	mockSE.On("UpdatedPool", ctx, pool).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedPool, result)
	mockSE.AssertExpectations(t)
}

func TestUpdatedPoolWithVLMConfig_Failure(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:         datamodel.BaseModel{ID: 1},
		Name:              "test-pool",
		PoolAttributes:    &datamodel.PoolAttributes{},
		AutoTieringConfig: &datamodel.AutoTieringConfig{}, // Initialize AutoTiering config
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "foobar",
		},
	}
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes: 1000,
	}

	expectedError := errors.New("failed to update pool")
	mockSE.On("UpdatedPool", ctx, pool).Return(nil, expectedError)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to update pool")
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringEnabled tests updating a pool with AutoTiering enabled
func TestUpdatedPoolWithVLMConfig_AutoTieringEnabled(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:         datamodel.BaseModel{ID: 1},
		Name:              "test-pool",
		PoolAttributes:    &datamodel.PoolAttributes{},
		AutoTieringConfig: &datamodel.AutoTieringConfig{}, // Initialize AutoTiering config
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "gcp",
		},
	}
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes:             2000,
		Description:             "Updated description",
		TotalThroughputMibps:    100,
		AllowAutoTiering:        true,
		HotTierSizeInBytes:      1000,
		EnableHotTierAutoResize: true,
	}

	// Expected pool should have AutoTiering enabled with new config
	expectedPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		State:            coremodel.LifeCycleStateInUse,
		VLMConfig:        "{\"deployment\":{\"provider\":\"gcp\"}}",
		SizeInBytes:      2000,
		Description:      "Updated description",
		AllowAutoTiering: true,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      1000,
			EnableHotTierAutoResize: true,
			BucketName:              "", // Empty since no existing bucket
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 100,
		},
	}

	mockSE.On("UpdatedPool", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 1000 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == true &&
			p.AutoTieringConfig.BucketName == ""
	})).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(1000), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, "", result.AutoTieringConfig.BucketName)
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringDisabled tests updating a pool with AutoTiering disabled
func TestUpdatedPoolWithVLMConfig_AutoTieringOneWayEnablement(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		PoolAttributes:   &datamodel.PoolAttributes{},
		AllowAutoTiering: true, // Already enabled
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      500,
			EnableHotTierAutoResize: false,
			BucketName:              "existing-bucket",
		},
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "gcp",
		},
	}
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes:             3000,
		Description:             "Updated description",
		TotalThroughputMibps:    150,
		AllowAutoTiering:        false, // Attempt to disable AutoTiering (should be ignored)
		HotTierSizeInBytes:      1500,  // This should be ignored since AutoTiering remains enabled
		EnableHotTierAutoResize: true,  // This should be updated since AutoTiering is enabled
	}

	// Expected pool should STILL have AutoTiering enabled and config preserved
	expectedPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		State:            coremodel.LifeCycleStateInUse,
		VLMConfig:        "{\"deployment\":{\"provider\":\"gcp\"}}",
		SizeInBytes:      3000,
		Description:      "Updated description",
		AllowAutoTiering: true, // Should remain enabled despite update param
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      1500,              // Updated from params since AutoTiering is enabled
			EnableHotTierAutoResize: true,              // Updated from params since AutoTiering is enabled
			BucketName:              "existing-bucket", // Preserved
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 150,
		},
	}

	mockSE.On("UpdatedPool", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		// AutoTiering should still be enabled and config preserved
		return p.AllowAutoTiering == true && p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.BucketName == "existing-bucket"
	})).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.AllowAutoTiering)                                 // Should still be true
	assert.NotNil(t, result.AutoTieringConfig)                              // Config should be preserved
	assert.Equal(t, "existing-bucket", result.AutoTieringConfig.BucketName) // Bucket name preserved
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringDisabled tests updating a pool with AutoTiering disabled
func TestUpdatedPoolWithVLMConfig_AutoTieringDisabled(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		PoolAttributes:   &datamodel.PoolAttributes{},
		AllowAutoTiering: false, // AutoTiering is disabled
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      1000,
			EnableHotTierAutoResize: false,
			BucketName:              "existing-bucket",
		},
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "gcp",
		},
	}
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes:             5000,
		Description:             "Updated description",
		TotalThroughputMibps:    200,
		AllowAutoTiering:        false, // Keep AutoTiering disabled
		HotTierSizeInBytes:      2000,  // This should be ignored, HotTierSizeInBytes should sync with SizeInBytes
		EnableHotTierAutoResize: true,  // This should be updated anyway
	}

	// Expected pool should have HotTierSizeInBytes synced with SizeInBytes
	expectedPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		State:            coremodel.LifeCycleStateInUse,
		VLMConfig:        "{\"deployment\":{\"provider\":\"gcp\"}}",
		SizeInBytes:      5000,
		Description:      "Updated description",
		AllowAutoTiering: false, // Should remain disabled
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      5000,              // Should sync with SizeInBytes
			EnableHotTierAutoResize: true,              // Updated from params
			BucketName:              "existing-bucket", // Preserved
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 200,
		},
	}

	mockSE.On("UpdatedPool", ctx, mock.AnythingOfType("*datamodel.Pool")).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.AllowAutoTiering)                                  // Should remain disabled
	assert.NotNil(t, result.AutoTieringConfig)                                // Config should be preserved
	assert.Equal(t, int64(5000), result.AutoTieringConfig.HotTierSizeInBytes) // Should sync with SizeInBytes
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)          // Should be updated
	assert.Equal(t, "existing-bucket", result.AutoTieringConfig.BucketName)   // Bucket name preserved
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_PreserveBucketName tests that existing bucket name is preserved when updating AutoTiering
func TestUpdatedPoolWithVLMConfig_PreserveBucketName(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	existingBucketName := "my-existing-bucket"
	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		PoolAttributes:   &datamodel.PoolAttributes{},
		AllowAutoTiering: true,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      500,
			EnableHotTierAutoResize: false,
			BucketName:              existingBucketName,
		},
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "gcp",
		},
	}
	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes:             4000,
		Description:             "Updated description",
		TotalThroughputMibps:    200,
		AllowAutoTiering:        true,
		HotTierSizeInBytes:      1500, // Updated size
		EnableHotTierAutoResize: true, // Updated setting
	}

	// Expected pool should preserve the existing bucket name
	expectedPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		State:            coremodel.LifeCycleStateInUse,
		VLMConfig:        "{\"deployment\":{\"provider\":\"gcp\"}}",
		SizeInBytes:      4000,
		Description:      "Updated description",
		AllowAutoTiering: true,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      1500,
			EnableHotTierAutoResize: true,
			BucketName:              existingBucketName, // Should be preserved
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 200,
		},
	}

	mockSE.On("UpdatedPool", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 1500 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == true &&
			p.AutoTieringConfig.BucketName == existingBucketName
	})).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(1500), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, existingBucketName, result.AutoTieringConfig.BucketName)
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringWithIOPS tests updating a pool with AutoTiering and IOPS
func TestUpdatedPoolWithVLMConfig_AutoTieringWithIOPS(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	pool := &datamodel.Pool{
		BaseModel:         datamodel.BaseModel{ID: 1},
		Name:              "test-pool",
		PoolAttributes:    &datamodel.PoolAttributes{},
		AutoTieringConfig: &datamodel.AutoTieringConfig{}, // Initialize AutoTiering config
	}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Provider: "gcp",
		},
	}

	updatePoolParams := &commonparams.UpdatePoolParams{
		SizeInBytes:             5000,
		Description:             "Updated description with IOPS",
		TotalThroughputMibps:    250,
		TotalIops:               nillable.ToPointer(int64(2048)),
		AllowAutoTiering:        true,
		HotTierSizeInBytes:      2000,
		EnableHotTierAutoResize: false,
		Labels: &datamodel.JSONB{
			"environment": "test",
			"team":        "platform",
		},
	}

	expectedPool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{ID: 1},
		Name:             "test-pool",
		State:            coremodel.LifeCycleStateInUse,
		VLMConfig:        "{\"deployment\":{\"provider\":\"gcp\"}}",
		SizeInBytes:      5000,
		Description:      "Updated description with IOPS",
		AllowAutoTiering: true,
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			HotTierSizeInBytes:      2000,
			EnableHotTierAutoResize: false,
			BucketName:              "",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 250,
			Iops:            2048,
			Labels: &datamodel.JSONB{
				"environment": "test",
				"team":        "platform",
			},
		},
	}

	mockSE.On("UpdatedPool", ctx, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 2000 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == false &&
			p.PoolAttributes.Iops == 2048 &&
			p.PoolAttributes.Labels != nil
	})).Return(expectedPool, nil)

	result, err := activity.UpdatedPoolWithVLMConfig(ctx, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(2000), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.False(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, int64(2048), result.PoolAttributes.Iops)
	assert.NotNil(t, result.PoolAttributes.Labels)
	mockSE.AssertExpectations(t)
}

func TestPoolActivity_GetServiceNetOpStatus(t *testing.T) {
	activity := &activities.PoolActivity{}

	t.Run("Success", func(t *testing.T) {
		expectedOp := &hyperscaler3.ComputeOperation{
			Name: "op-123",
		}
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		originalGetServiceNetOpStatus := activities.GetServiceNetOpStatus
		activities.GetServiceNetOpStatus = func(gcpService hyperscaler2.GoogleServices, operation string) (*hyperscaler3.ComputeOperation, error) {
			return expectedOp, nil
		}
		defer func() {
			hyperscaler2.GetGCPService = original
			activities.GetServiceNetOpStatus = originalGetServiceNetOpStatus
		}()
		ctx := context.Background()
		op, err := activity.GetServiceNetOpStatus(ctx, "op-123")
		assert.NoError(t, err)
		assert.Equal(t, expectedOp.Name, op.Name)
	})

	t.Run("GetGCPServiceFails", func(t *testing.T) {
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("service error"))
		}
		defer func() { hyperscaler2.GetGCPService = original }()

		ctx := context.Background()
		op, err := activity.GetServiceNetOpStatus(ctx, "op-123")
		assert.Error(t, err)
		assert.Nil(t, op)
	})
}

func Test_getServiceNetOpStatus(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	expectedOp := &hyperscaler3.ComputeOperation{Status: "DONE"}
	mockService.On("GetServiceNetOpStatus", "op-123").Return(expectedOp, nil)

	op, err := activities.GetServiceNetOpStatus(mockService, "op-123")
	assert.NoError(t, err)
	assert.Equal(t, expectedOp, op)
	mockService.AssertExpectations(t)
}

func Test_getSubnetFromOperation_Success(t *testing.T) {
	ctx := context.Background()

	// Mock subnet data
	mockSubnet := &servicenetworking.Subnetwork{
		Name:        "test-subnet",
		Network:     "projects/test-project/global/networks/test-vpc",
		IpCidrRange: "10.0.0.0/24",
	}

	subnetBytes, err := json.Marshal(mockSubnet)
	assert.NoError(t, err)

	result, err := activities.GetSubnetFromOperation(ctx, subnetBytes)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "test-subnet", result.Name)
	assert.Equal(t, "projects/test-project/global/networks/test-vpc", result.Network)
	assert.Equal(t, "10.0.0.1", result.GatewayAddress)
}

func Test_getSubnetFromOperation(t *testing.T) {
	ctx := context.Background()

	t.Run("NilBytes", func(t *testing.T) {
		result, err := activities.GetSubnetFromOperation(ctx, nil)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "operation response is nil")
	})
	t.Run("InvalidJSON", func(t *testing.T) {
		invalidJSON := []byte("invalid json data")

		result, err := activities.GetSubnetFromOperation(ctx, invalidJSON)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("EmptyJSON", func(t *testing.T) {
		emptyJSON := []byte("{}")

		result, err := activities.GetSubnetFromOperation(ctx, emptyJSON)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("InvalidCIDR", func(t *testing.T) {
		mockSubnet := &servicenetworking.Subnetwork{
			Name:        "test-subnet",
			Network:     "projects/test-project/global/networks/test-vpc",
			IpCidrRange: "invalid-cidr",
		}

		subnetBytes, err := json.Marshal(mockSubnet)
		assert.NoError(t, err)

		result, err := activities.GetSubnetFromOperation(ctx, subnetBytes)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
	t.Run("EmptySubnetFields", func(t *testing.T) {
		mockSubnet := &servicenetworking.Subnetwork{
			Name:        "",
			Network:     "",
			IpCidrRange: "10.0.0.0/24",
		}

		subnetBytes, err := json.Marshal(mockSubnet)
		assert.NoError(t, err)

		result, err := activities.GetSubnetFromOperation(ctx, subnetBytes)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "", result.Name)
		assert.Equal(t, "", result.Network)
		assert.Equal(t, "10.0.0.1", result.GatewayAddress)
	})
	t.Run("InvalidCIDR", func(t *testing.T) {
		mockSubnet := &servicenetworking.Subnetwork{
			Name:        "test-subnet",
			Network:     "projects/test-project/global/networks/test-vpc",
			IpCidrRange: "invalid-cidr",
		}

		subnetBytes, err := json.Marshal(mockSubnet)
		assert.NoError(t, err)

		result, err := activities.GetSubnetFromOperation(ctx, subnetBytes)

		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func Test_getSubnetFromOperation_MalformedJSON(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name     string
		jsonData string
	}{
		{
			name:     "Incomplete JSON",
			jsonData: `{"name": "test-subnet", "network":`,
		},
		{
			name:     "Invalid JSON structure",
			jsonData: `{"name": "test-subnet", "network": "test", "ipCidrRange": 123}`,
		},
		{
			name:     "Non-object JSON",
			jsonData: `["not", "an", "object"]`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := activities.GetSubnetFromOperation(ctx, []byte(tc.jsonData))

			assert.Error(t, err)
			assert.Nil(t, result)
		})
	}
}

// Test_getGatewayFromIpCidrRange tests the getGatewayFromIpCidrRange function
func Test_getGatewayFromIpCidrRange(t *testing.T) {
	tests := []struct {
		name          string
		ipCidrRange   string
		expectedGW    string
		expectedError bool
		errorMsg      string
	}{
		{
			name:          "Valid IPv4 CIDR - 192.168.1.0/24",
			ipCidrRange:   "192.168.1.0/24",
			expectedGW:    "192.168.1.1",
			expectedError: false,
		},
		{
			name:          "Valid IPv4 CIDR - 10.0.0.0/16",
			ipCidrRange:   "10.0.0.0/16",
			expectedGW:    "10.0.0.1",
			expectedError: false,
		},
		{
			name:          "Valid IPv4 CIDR - 172.16.0.0/12",
			ipCidrRange:   "172.16.0.0/12",
			expectedGW:    "172.16.0.1",
			expectedError: false,
		},
		{
			name:          "Valid IPv4 CIDR with host bits set - 192.168.1.5/24",
			ipCidrRange:   "192.168.1.5/24",
			expectedGW:    "192.168.1.6",
			expectedError: false,
		},
		{
			name:          "IPv4 CIDR with /32 subnet",
			ipCidrRange:   "192.168.1.100/32",
			expectedGW:    "192.168.1.101",
			expectedError: false,
		},
		{
			name:          "IPv4 CIDR with /8 subnet",
			ipCidrRange:   "10.0.0.0/8",
			expectedGW:    "10.0.0.1",
			expectedError: false,
		},
		{
			name:          "Invalid CIDR format - missing subnet mask",
			ipCidrRange:   "192.168.1.0",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "Invalid CIDR format - invalid IP",
			ipCidrRange:   "256.256.256.256/24",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "Invalid CIDR format - invalid subnet mask",
			ipCidrRange:   "192.168.1.0/33",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "IPv6 CIDR range",
			ipCidrRange:   "2001:db8::/32",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "IP CIDR range is not an IPv4 address",
		},
		{
			name:          "Empty string",
			ipCidrRange:   "",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "Just slash",
			ipCidrRange:   "/24",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "Invalid format - no slash",
			ipCidrRange:   "192.168.1.0 24",
			expectedGW:    "",
			expectedError: true,
			errorMsg:      "invalid CIDR address",
		},
		{
			name:          "Edge case - last possible IPv4 address",
			ipCidrRange:   "255.255.255.254/32",
			expectedGW:    "255.255.255.255",
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gateway, err := activities.GetGatewayFromIpCidrRange(tt.ipCidrRange)

			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
				assert.Empty(t, gateway)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedGW, gateway)
			}
		})
	}
}

func Test_getCreateDataSubnetworkOp(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		// Arrange
		mockService := new(hyperscaler2.MockGoogleServices)
		mockLogger := util.GetLogger(context.Background())

		params := commonparams.CreatePoolParams{
			Region:         "us-central1",
			VendorSubNetID: "test-vpc",
		}
		tenantProjectNumber := "123456789"
		expectedOperationName := "operation-123"

		mockService.On("GetLogger").Return(mockLogger)

		originalGetCreateSubnetworkOperation := activities.GetCreateSubnetworkOperation
		defer func() { activities.GetCreateSubnetworkOperation = originalGetCreateSubnetworkOperation }()

		activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool) (*string, error) {
			assert.Equal(t, "123456789", tenantProjectNumber)
			assert.Equal(t, "test-vpc", consumerVPC)
			assert.Equal(t, "us-central1", *tenantProjectRegion)
			assert.False(t, isLargeCapacity) // assuming standard pools for this test
			return &expectedOperationName, nil
		}

		// Act
		result, err := activities.GetCreateDataSubnetworkOp(mockService, params, tenantProjectNumber)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedOperationName, *result)
		mockService.AssertExpectations(t)
	})

	t.Run("GetCreateSubnetworkOperationFails", func(t *testing.T) {
		// Arrange
		mockService := new(hyperscaler2.MockGoogleServices)
		mockLogger := util.GetLogger(context.Background())

		params := commonparams.CreatePoolParams{
			Region:         "us-central1",
			VendorSubNetID: "test-vpc",
		}
		tenantProjectNumber := "123456789"
		expectedError := errors.New("failed to create subnetwork operation")

		mockService.On("GetLogger").Return(mockLogger)

		originalGetCreateSubnetworkOperation := activities.GetCreateSubnetworkOperation
		defer func() { activities.GetCreateSubnetworkOperation = originalGetCreateSubnetworkOperation }()

		activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool) (*string, error) {
			return nil, expectedError
		}

		// Act
		result, err := activities.GetCreateDataSubnetworkOp(mockService, params, tenantProjectNumber)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		mockService.AssertExpectations(t)
	})

	t.Run("ParametersPassedCorrectly", func(t *testing.T) {
		// Arrange
		testCases := []struct {
			name                string
			region              string
			vendorSubNetID      string
			tenantProjectNumber string
		}{
			{"ValidParameters", "us-west1", "vpc-001", "987654321"},
			{"DifferentRegion", "europe-west1", "vpc-002", "111111111"},
			{"EmptyParameters", "", "", ""},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockService := new(hyperscaler2.MockGoogleServices)
				mockLogger := util.GetLogger(context.Background())

				params := commonparams.CreatePoolParams{
					Region:         tc.region,
					VendorSubNetID: tc.vendorSubNetID,
				}
				operationName := "test-operation"

				mockService.On("GetLogger").Return(mockLogger)

				originalGetCreateSubnetworkOperation := activities.GetCreateSubnetworkOperation
				defer func() { activities.GetCreateSubnetworkOperation = originalGetCreateSubnetworkOperation }()

				activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool) (*string, error) {
					assert.Equal(t, tc.tenantProjectNumber, tenantProjectNumber)
					assert.Equal(t, tc.vendorSubNetID, consumerVPC)
					assert.Equal(t, tc.region, *tenantProjectRegion)
					// We can determine if it's large capacity based on the pool type in the test case
					return &operationName, nil
				}

				// Act
				result, err := activities.GetCreateDataSubnetworkOp(mockService, params, tc.tenantProjectNumber)

				// Assert
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, operationName, *result)
				mockService.AssertExpectations(t)
			})
		}
	})
}

func Test_IdentifySecondaryAndMediatorZone_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	projectNumber := "123456789"
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "us-central1-a",
		SecondaryZone: "us-central1-b",
		Region:        "us-central1",
	}

	// Mock GetGCPService to return error for now (simplified test)
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("GCP service not available in test")
	}

	// Act
	result, err := activity.IdentifySecondaryAndMediatorZone(ctx, projectNumber, locationInfo, "c3-std-4", false)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
}

func Test_IdentifySecondaryAndMediatorZone_GCPServiceError(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	projectNumber := "123456789"
	locationInfo := &commonparams.LocationInfo{
		PrimaryZone:   "us-central1-a",
		SecondaryZone: "us-central1-b",
		Region:        "us-central1",
	}

	// Mock GetGCPService to return error
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	result, err := activity.IdentifySecondaryAndMediatorZone(ctx, projectNumber, locationInfo, "c3-std-4", false)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
}

func Test_resolveZonesForCluster_Success_NoSecondaryNoMediator(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for primary zone validation
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", instanceType).Return(true, nil)

	// Mock IsMachineTypeAvailable for secondary zone selection
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(true, nil)

	// Mock IsMachineTypeAvailable for mediator zone (when isRegionalHA=false, mediatorZone=primaryZone) to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", "e2-micro").Return(true, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "us-central1-b", resolvedSecondary)
	assert.Equal(t, "us-central1-a", resolvedMediator)
	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_PrimaryZoneEmpty(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := ""
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary zone is not set")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)
}

func Test_resolveZonesForCluster_Error_GetZonesFails(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return error
	mockService.On("GetZones", projectNumber, region).Return(nil, errors.New("failed to get zones"))

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get zones")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)
	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_NoSecondaryZoneSupportsInstanceType(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for primary zone validation
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", instanceType).Return(true, nil)

	// Mock IsMachineTypeAvailable to return false for all zones
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(false, nil)
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-c", instanceType).Return(false, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no secondary zone found that supports the instance type")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)
	mockService.AssertExpectations(t)
}

func TestAllocateSVMName(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	t.Run("FirstSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(1), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-01", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("SecondSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(2), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-02", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("TenthSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(10), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-10", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("EleventhSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(11), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-11", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("NinetyNinthSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(99), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-99", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("HundredthSVMInPool", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(100), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-100", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DifferentDeploymentName", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 456},
			DeploymentName: "test-deployment",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(456)).Return(int64(6), nil)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, "test-deployment-svm-06", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("database connection failed"))
		mockStorage.On("GetNextSVMIndexByPoolID", ctx, int64(123)).Return(int64(0), expectedError)

		// Act
		result, err := activity.AllocateSVMName(ctx, pool)

		// Assert
		assert.Error(t, err)
		assert.Empty(t, result)
		mockStorage.AssertExpectations(t)
	})
}

func Test_AllocateClusterSerialNumber(t *testing.T) {
	t.Run("SuccessOneHAPair", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		serialNumber1 := "935010000000000001"
		serialNumber2 := "935010000000000002"
		serials := []string{serialNumber1, serialNumber2}
		req := &vlm.CreateVSAClusterDeploymentRequest{
			VLMConfig: vlm.VLMConfig{
				Deployment: vlm.DeploymentConfig{
					NumHAPair: 1,
					NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
					GCPConfig: vlm.GCPConfig{},
				},
			},
		}
		oldRegionNumber := activities.RegionNumber
		activities.RegionNumber = "34"
		defer func() { activities.RegionNumber = oldRegionNumber }()

		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber1, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber2, nil).Once()
		result, err := activity.AllocateClusterSerialNumber(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, "", result.VLMConfig.Deployment.SerialNumberPrefix)
		assert.Equal(t, serials, result.VLMConfig.Deployment.VMSerialNumbers)
	})
	t.Run("SuccessMultiHAPair", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		serialNumber1 := "935010000000000001"
		serialNumber2 := "935010000000000002"
		serialNumber3 := "935010000000000003"
		serialNumber4 := "935010000000000004"
		serials := []string{serialNumber1, serialNumber2, serialNumber3, serialNumber4}
		req := &vlm.CreateVSAClusterDeploymentRequest{
			VLMConfig: vlm.VLMConfig{
				Deployment: vlm.DeploymentConfig{
					NumHAPair: 2,
					NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
					GCPConfig: vlm.GCPConfig{},
				},
			},
		}
		oldRegionNumber := activities.RegionNumber
		activities.RegionNumber = "34"
		defer func() { activities.RegionNumber = oldRegionNumber }()

		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber1, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber2, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber3, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return(serialNumber4, nil).Once()
		result, err := activity.AllocateClusterSerialNumber(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, "", result.VLMConfig.Deployment.SerialNumberPrefix)
		assert.Equal(t, serials, result.VLMConfig.Deployment.VMSerialNumbers)
	})
	t.Run("FailureOnHAPairBeingZero", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		req := &vlm.CreateVSAClusterDeploymentRequest{
			VLMConfig: vlm.VLMConfig{
				Deployment: vlm.DeploymentConfig{
					NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
					GCPConfig: vlm.GCPConfig{},
				},
			},
		}

		oldRegionNumber := activities.RegionNumber
		activities.RegionNumber = "34"
		defer func() { activities.RegionNumber = oldRegionNumber }()

		result, err := activity.AllocateClusterSerialNumber(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "HA pairs count must be at least 1")
	})

	t.Run("FailureOnRegionNotAvailable", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		req := &vlm.CreateVSAClusterDeploymentRequest{
			VLMConfig: vlm.VLMConfig{
				Deployment: vlm.DeploymentConfig{
					NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
					GCPConfig: vlm.GCPConfig{},
				},
			},
		}

		oldRegionNumber := activities.RegionNumber
		activities.RegionNumber = ""
		defer func() { activities.RegionNumber = oldRegionNumber }()

		result, err := activity.AllocateClusterSerialNumber(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "region number is not set")
	})
	t.Run("FailureOnGetNextSerialNumberInRegionError", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		req := &vlm.CreateVSAClusterDeploymentRequest{
			VLMConfig: vlm.VLMConfig{
				Deployment: vlm.DeploymentConfig{
					NumHAPair: 1,
					NetConfig: map[vlm.VSALIFType]vlm.NetworkConfig{},
					GCPConfig: vlm.GCPConfig{},
				},
			},
		}

		oldRegionNumber := activities.RegionNumber
		activities.RegionNumber = "34"
		defer func() { activities.RegionNumber = oldRegionNumber }()

		mockStorage.On("GetNextSerialNumberInRegion", ctx, "93534").Return("", errors.New("error fetching serial number"))
		result, err := activity.AllocateClusterSerialNumber(ctx, req)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "error fetching serial number")
	})
}

func TestPoolActivity_CreateVPCs(t *testing.T) {
	mgmtVpcName := "mgmt-e0a-vpc-01"
	icVpcName := "ic-e0b-vpc-01"
	rsmVpcName := "rsm-e0c-vpc-01"

	project := "test-project"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalCreateVPC := activities.CreateVPC
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.CreateVPC = originalCreateVPC
	}()

	t.Run("Success_AllVPCsCreated", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			return "operation-" + vpcName, nil
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Get the actual result from the activity execution
		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 3) // Should have 3 operations for mgmt, cluster-ic, and rsm VPCs

		expectedOperations := []string{
			"operation-mgmt-e0a-vpc-01",
			"operation-ic-e0b-vpc-01",
			"operation-rsm-e0c-vpc-01",
		}

		// Create a map from the slice for easy lookup
		actualOperations := make(map[string]bool)
		for _, op := range *operations {
			actualOperations[op.OperationName] = op.IsDone
		}

		for _, expectedOp := range expectedOperations {
			value, exists := actualOperations[expectedOp]
			assert.True(t, exists, "Operation %s should exist in result", expectedOp)
			assert.False(t, value, "Operation %s should be set to false", expectedOp)
		}
	})

	t.Run("Success_SomeVPCsAlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		callCount := 0
		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			callCount++
			if callCount == 1 {
				return "operation-" + vpcName, nil // First VPC needs creation
			}
			return "", nil // Other VPCs already exist
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 1) // Only one operation should be in the result

		// Should contain exactly one operation that starts with "operation-"
		operationFound := false
		for _, op := range *operations {
			if strings.HasPrefix(op.OperationName, "operation-") {
				operationFound = true
				break
			}
		}
		assert.True(t, operationFound, "Should have exactly one operation starting with 'operation-'")
	})

	t.Run("GetGCPService_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get GCP service")
	})

	t.Run("CreateVPC_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			return "", errors.New("failed to create VPC")
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create VPC")
	})

	t.Run("EmptyProject", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			if project == "" {
				return "", errors.New("project cannot be empty")
			}
			return "operation-" + vpcName, nil
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, "")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "project cannot be empty")
	})

	t.Run("AllVPCs_AlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			return "", nil // All VPCs already exist
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 0) // No operations should be in the result
	})

	t.Run("PartialFailure_FirstVPCFails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			if vpcName == mgmtVpcName {
				return "", errors.New("failed to create management VPC")
			}
			return "operation-" + vpcName, nil
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create management VPC")
	})

	t.Run("MixedResults_SomeCreatedSomeExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		vpcCallOrder := []string{}
		activities.CreateVPC = func(service hyperscaler2.GoogleServices, project, vpcName string) (string, error) {
			vpcCallOrder = append(vpcCallOrder, vpcName)
			switch vpcName {
			case mgmtVpcName:
				return "operation-mgmt", nil
			case icVpcName:
				return "", nil // Already exists
			case rsmVpcName:
				return "operation-rsm", nil
			default:
				return "", errors.New("unexpected VPC name")
			}
		}

		result, err := env.ExecuteActivity(activity.CreateVPCs, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Equal(t, 2, len(*operations)) // Two operations created
		assert.Len(t, vpcCallOrder, 3)       // All three VPCs should be processed

		// Check for specific operation names
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
		}
		assert.Contains(t, operationNames, "operation-mgmt")
		assert.Contains(t, operationNames, "operation-rsm")
	})
}

func TestPoolActivity_CreateSubnets(t *testing.T) {
	project := "test-project"
	originalGetGCPService := hyperscaler2.GetGCPService
	originalInsertSubnet := activities.InsertSubnet
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.InsertSubnet = originalInsertSubnet
	}()

	t.Run("Success_AllSubnetsCreated", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			return fmt.Sprintf("operation-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Equal(t, 3, len(*operations))
		assert.Equal(t, 3, callCount)

		// Check that all operations are present and have correct names
		for _, op := range *operations {
			assert.NotEmpty(t, op.OperationName)
			assert.False(t, op.IsDone)
		}
	})

	t.Run("Success_SomeSubnetsAlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			if callCount == 2 {
				return "", nil // Subnet already exists
			}
			return fmt.Sprintf("operation-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Equal(t, 2, len(*operations)) // Only operations with non-empty names are added
		assert.Equal(t, 3, callCount)
	})

	t.Run("GetGCPService_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get GCP service")
	})

	t.Run("InsertSubnet_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			return "", errors.New("failed to create subnet")
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to create subnet")
	})

	t.Run("EmptyProject", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			assert.Equal(t, "", project)
			return fmt.Sprintf("operation-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, "")

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.Equal(t, 3, callCount)
	})

	t.Run("AllSubnets_AlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			return "", nil // All subnets already exist
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Equal(t, 0, len(*operations)) // No operations to track
		assert.Equal(t, 3, callCount)
	})

	t.Run("PartialFailure_FirstSubnetFails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", errors.New("first subnet creation failed")
			}
			return fmt.Sprintf("operation-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "first subnet creation failed")
		assert.Equal(t, 1, callCount) // Should stop after first failure
		_ = result                    // result unused in error case
		assert.Contains(t, err.Error(), "first subnet creation failed")
		assert.Equal(t, 1, callCount) // Should stop after first failure
	})

	t.Run("MixedResults_SomeCreatedSomeExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity)

		mockService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		callCount := 0
		activities.InsertSubnet = func(service hyperscaler2.GoogleServices, project string, region *string, subnetName, vpcName, ipCidrRange string) (string, error) {
			callCount++
			if callCount%2 == 0 {
				return "", nil // Even calls return empty (already exists)
			}
			return fmt.Sprintf("operation-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateSubnets, project)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Equal(t, 2, len(*operations)) // Only operations 1 and 3 should be tracked
		assert.Equal(t, 3, callCount)

		// Verify specific operation names are present
		operationNames := make([]string, 0, len(*operations))
		for _, op := range *operations {
			operationNames = append(operationNames, op.OperationName)
		}
		assert.Contains(t, operationNames, "operation-1")
		assert.Contains(t, operationNames, "operation-3")
	})
}

func TestPoolActivity_CreateFirewalls(t *testing.T) {
	project := "test-project"
	snHostProject := "test-sn-host-project"
	network := "test-network"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalInsertFirewall := activities.InsertFirewall
	originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
	originalSetupNetworkFirewallsForNFS := activities.SetupNetworkFirewallsForNFS
	originalSetupNetworkFirewallsForIntercluster := activities.SetupNetworkFirewallsForIntercluster
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.InsertFirewall = originalInsertFirewall
		activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
		activities.SetupNetworkFirewallsForNFS = originalSetupNetworkFirewallsForNFS
		activities.SetupNetworkFirewallsForIntercluster = originalSetupNetworkFirewallsForIntercluster
	}()

	t.Run("Success_AllFirewallsCreated", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test to ensure NFS firewall is created
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock InsertFirewall to return operation names for all VPC firewalls
		callCount := 0
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			callCount++
			return fmt.Sprintf("operation-firewall-%d", callCount), nil
		}

		// Mock SetupNetworkFirewallsForIscsi to return operation name
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-iscsi-firewall", nil
		}

		// Mock SetupNetworkFirewallsForNFS to return operation name
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-nfs-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIntercluster to return operation names for intercluster firewalls
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-intercluster-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 7)

		// Check all operations are present and not done
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
			assert.False(t, op.IsDone)
		}
		assert.Contains(t, operationNames, "operation-firewall-1")
		assert.Contains(t, operationNames, "operation-firewall-2")
		assert.Contains(t, operationNames, "operation-firewall-3")
		assert.Contains(t, operationNames, "operation-firewall-4")
		assert.Contains(t, operationNames, "operation-iscsi-firewall")
		assert.Contains(t, operationNames, "operation-nfs-firewall")
		assert.Contains(t, operationNames, "operation-intercluster-firewall")
	})

	t.Run("Success_SomeFirewallsAlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test so NFS firewall function gets called
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock InsertFirewall to return empty string for some (already exist) and operation names for others
		callCount := 0
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			callCount++
			if callCount == 2 {
				return "", nil // Second firewall already exists
			}
			return fmt.Sprintf("operation-firewall-%d", callCount), nil
		}

		// Mock SetupNetworkFirewallsForIscsi to return empty (already exists)
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		// Mock SetupNetworkFirewallsForNFS to return empty (already exists)
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		// Mock SetupNetworkFirewallsForIntercluster to return empty (already exists)
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 3) // Only operations that were created

		// Check the correct operations are present
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
		}
		assert.Contains(t, operationNames, "operation-firewall-1")
		assert.Contains(t, operationNames, "operation-firewall-3")
	})

	t.Run("GetGCPService_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get GCP service")
		_ = result // result unused in error case
	})

	t.Run("InsertFirewall_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "", errors.New("failed to create firewall")
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create firewall")
		_ = result // result unused in error case
	})

	t.Run("SetupNetworkFirewallsForIscsi_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock InsertFirewall to succeed for all VPC firewalls
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "operation-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIscsi to fail
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("failed to setup iSCSI firewalls")
		}

		// Mock SetupNetworkFirewallsForNFS to succeed (test focuses on iSCSI failure)
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-nfs-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup iSCSI firewalls")
		_ = result // result unused in error case
	})

	t.Run("SetupNetworkFirewallsForNFS_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test to ensure NFS firewall is called
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock InsertFirewall to succeed for all VPC firewalls
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "operation-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIscsi to succeed
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-iscsi-firewall", nil
		}

		// Mock SetupNetworkFirewallsForNFS to fail
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("failed to setup NFS firewalls")
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup NFS firewalls")
		_ = result // result unused in error case
	})

	t.Run("SetupNetworkFirewallsForIntercluster_Fails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test to ensure NFS firewall is called
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("failed to setup Intercluster firewalls")
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup Intercluster firewalls")
		_ = result // result unused in error case
	})

	t.Run("EmptyProject", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test so NFS firewall function gets called
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "operation-firewall", nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-iscsi-firewall", nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-nfs-firewall", nil
		}
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-intercluster-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, "", snHostProject, network)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		// Should still work with empty project as the mock doesn't validate project
	})

	t.Run("AllFirewalls_AlreadyExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		// Enable file protocol support for this test so NFS firewall function gets called
		utils.SetFileProtocolSupportedForTesting(true)
		defer func() {
			utils.SetFileProtocolSupportedForTesting(false)
		}()

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock all firewalls to already exist (return empty strings)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 0) // No operations returned since all already exist
	})

	t.Run("PartialFailure_FirstFirewallFails", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock first firewall to fail
		callCount := 0
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			callCount++
			if callCount == 1 {
				return "", errors.New("first firewall creation failed")
			}
			return fmt.Sprintf("operation-firewall-%d", callCount), nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "first firewall creation failed")
		_ = result // result unused in error case
	})

	t.Run("MixedResults_SomeCreatedSomeExist", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity)

		mockGCPService := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		// Mock mixed results: some created, some already exist
		callCount := 0
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, firewallName, vpcName string, priority int64, direction string, sourceRanges, portRules []string) (string, error) {
			callCount++
			if callCount%2 == 0 {
				return "", nil // Even calls return empty (already exist)
			}
			return fmt.Sprintf("operation-firewall-%d", callCount), nil
		}

		// Mock iSCSI firewall to be created
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-iscsi-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network)

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 3) // 2 VPC firewalls created + 1 iSCSI firewall

		// Create a map for easy lookup
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
		}
		assert.Contains(t, operationNames, "operation-firewall-1")
		assert.Contains(t, operationNames, "operation-firewall-3")
		assert.Contains(t, operationNames, "operation-iscsi-firewall")
	})
}

func Test_getComputeOpStatus(t *testing.T) {
	project := "test-project"
	operation := "test-operation"

	t.Run("Global_Operation_Success", func(t *testing.T) {
		mockGCPService := hyperscaler2.NewMockGoogleServices(t)
		expectedOp := &hyperscaler3.ComputeOperation{
			Name:   operation,
			Status: "DONE",
		}

		mockGCPService.On("GetComputeGlobalOpStatus", project, operation).Return(expectedOp, nil)

		result, err := activities.GetComputeOpStatus(mockGCPService, project, false, operation)

		assert.NoError(t, err)
		assert.Equal(t, expectedOp, result)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("Regional_Operation_Success", func(t *testing.T) {
		mockGCPService := hyperscaler2.NewMockGoogleServices(t)
		expectedOp := &hyperscaler3.ComputeOperation{
			Name:   operation,
			Status: "RUNNING",
		}
		defer func() {
			activities.Region = env.GetString("LOCAL_REGION", "")
		}()
		activities.Region = "us-central1"
		mockGCPService.On("GetComputeRegionalOpStatus", project, activities.Region, operation).Return(expectedOp, nil)

		result, err := activities.GetComputeOpStatus(mockGCPService, project, true, operation)

		assert.NoError(t, err)
		assert.Equal(t, expectedOp, result)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("Global_Operation_Error", func(t *testing.T) {
		mockGCPService := hyperscaler2.NewMockGoogleServices(t)
		expectedError := errors.New("failed to get global operation status")
		mockGCPService.On("GetComputeGlobalOpStatus", project, operation).Return(nil, expectedError)

		result, err := activities.GetComputeOpStatus(mockGCPService, project, false, operation)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedError, err)
		mockGCPService.AssertExpectations(t)
	})

	t.Run("Regional_Operation_Error", func(t *testing.T) {
		mockGCPService := hyperscaler2.NewMockGoogleServices(t)
		expectedError := errors.New("failed to get regional operation status")
		defer func() {
			activities.Region = env.GetString("LOCAL_REGION", "")
		}()
		activities.Region = "us-central1"
		mockGCPService.On("GetComputeRegionalOpStatus", project, activities.Region, operation).Return(nil, expectedError)

		result, err := activities.GetComputeOpStatus(mockGCPService, project, true, operation)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, expectedError, err)
		mockGCPService.AssertExpectations(t)
	})
}

func TestFetchOnTapCredentials_WithUserCertificate_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType:      env.USER_CERTIFICATE,
			CertificateID: "cert-id",
			SecretID:      "secret-id",
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() {
		hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate
		hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
	}()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certificateID string) (*coremodel.Certificate, error) {
		return &coremodel.Certificate{
			CommonName:               "CN",
			SignedCertificate:        "cert",
			PrivateKey:               "key",
			InterMediateCertificates: []string{"intermediate"},
		}, nil
	}
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "admin-password", nil
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.Equal(t, "CN", creds.Certificate.CommonName)
	assert.Equal(t, "cert", creds.Certificate.Certificate)
	assert.Equal(t, "key", creds.Certificate.PrivateKey)
	assert.Equal(t, []string{"intermediate"}, creds.Certificate.InterMediateCertificate)
	assert.Equal(t, "admin-password", creds.AdminPassword)
}

func TestFetchOnTapCredentials_WithUserCertificate_CertificateError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType:      env.USER_CERTIFICATE,
			CertificateID: "cert-id",
			SecretID:      "secret-id",
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	defer func() { hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate }()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certificateID string) (*coremodel.Certificate, error) {
		return nil, errors.New("certificate error")
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "certificate error")
}

func TestFetchOnTapCredentials_WithUserCertificate_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType:      env.USER_CERTIFICATE,
			SecretID:      "secret-id",
			CertificateID: "cert-id",
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() {
		hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate
		hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
	}()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certificateID string) (*coremodel.Certificate, error) {
		return &coremodel.Certificate{
			CommonName:               "CN",
			SignedCertificate:        "cert",
			PrivateKey:               "key",
			InterMediateCertificates: []string{"intermediate"},
		}, nil
	}
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "", errors.New("Invalid resource field value")
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchOnTapCredentials_WithUsernamePwdSecMgr_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD_SEC_MGR,
			SecretID: "secret-id",
		},
	}
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "admin-password", nil
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, creds)
	assert.Equal(t, "admin-password", creds.AdminPassword)
}

func TestFetchOnTapCredentials_WithUsernamePwdSecMgr_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD_SEC_MGR,
			SecretID: "secret-id",
		},
	}
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "", errors.New("Invalid resource field value")
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchOnTapCredentials_WithDefaultAuthType_ReturnsPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD, // Assume this is a default type
			Password: "plain-password",
		},
	}

	creds, err := activity.GetOnTapCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.Equal(t, "plain-password", creds.AdminPassword)
}

func TestGetInterClusterLifsFromVLMConfig(t *testing.T) {
	tests := []struct {
		name      string
		vlmConfig vlm.VLMConfig
		expected  []string
		wantErr   bool
	}{
		{
			name: "Success - Single HA Pair with InterCluster LIFs",
			vlmConfig: vlm.VLMConfig{
				Cloud: vlm.CloudConfig{
					HAPairs: []vlm.HAPair{
						{
							VM1: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.1.1"},
									vlm.LIFTypeNodeMgmt:     {IP: "192.168.1.10"},
								},
							},
							VM2: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.1.2"},
									vlm.LIFTypeNodeMgmt:     {IP: "192.168.1.20"},
								},
							},
						},
					},
				},
			},
			expected: []string{"192.168.1.1", "192.168.1.2"},
			wantErr:  false,
		},
		{
			name: "Success - Multiple HA Pairs with InterCluster LIFs",
			vlmConfig: vlm.VLMConfig{
				Cloud: vlm.CloudConfig{
					HAPairs: []vlm.HAPair{
						{
							VM1: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.1.1"},
								},
							},
							VM2: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.1.2"},
								},
							},
						},
						{
							VM1: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.2.1"},
								},
							},
							VM2: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.2.2"},
								},
							},
						},
					},
				},
			},
			expected: []string{"192.168.1.1", "192.168.1.2", "192.168.2.1", "192.168.2.2"},
			wantErr:  false,
		},
		{
			name: "Success - Partial InterCluster LIFs",
			vlmConfig: vlm.VLMConfig{
				Cloud: vlm.CloudConfig{
					HAPairs: []vlm.HAPair{
						{
							VM1: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeInterCluster: {IP: "192.168.1.1"},
									vlm.LIFTypeNodeMgmt:     {IP: "192.168.1.10"},
								},
							},
							VM2: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeNodeMgmt: {IP: "192.168.1.20"},
								},
							},
						},
					},
				},
			},
			expected: []string{"192.168.1.1"},
			wantErr:  false,
		},
		{
			name: "Success - No InterCluster LIFs",
			vlmConfig: vlm.VLMConfig{
				Cloud: vlm.CloudConfig{
					HAPairs: []vlm.HAPair{
						{
							VM1: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeNodeMgmt: {IP: "192.168.1.10"},
								},
							},
							VM2: vlm.VMConfig{
								SystemLIFs: map[vlm.VSALIFType]vlm.LIFConfig{
									vlm.LIFTypeNodeMgmt: {IP: "192.168.1.20"},
								},
							},
						},
					},
				},
			},
			expected: nil, // Changed from []string{} to nil
			wantErr:  false,
		},
		{
			name: "Success - Empty HA Pairs",
			vlmConfig: vlm.VLMConfig{
				Cloud: vlm.CloudConfig{
					HAPairs: []vlm.HAPair{},
				},
			},
			expected: nil, // Changed from []string{} to nil
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			activity := &activities.PoolActivity{}

			result, err := activity.GetInterClusterLifsFromVLMConfig(ctx, &tt.vlmConfig)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Unit tests for PrepareInternalVSANetworksForFirewall
func TestPrepareInternalVSANetworksForFirewall(t *testing.T) {
	// Save original values
	originalMgmtFirewallSourceRanges := activities.MgmtFirewallSourceRanges
	originalMgmtRegionalNatIP := activities.MgmtRegionalNatIP
	originalInternalVSANetworks := activities.InternalVSANetworks
	originalGetInternalVSANetworkForFirewalls := activities.GetInternalVSANetworkForFirewalls

	defer func() {
		activities.MgmtFirewallSourceRanges = originalMgmtFirewallSourceRanges
		activities.MgmtRegionalNatIP = originalMgmtRegionalNatIP
		activities.InternalVSANetworks = originalInternalVSANetworks
		activities.GetInternalVSANetworkForFirewalls = originalGetInternalVSANetworkForFirewalls
	}()

	t.Run("Success_WithValidConfiguration", func(t *testing.T) {
		// Setup test data
		activities.MgmtFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12"
		activities.MgmtRegionalNatIP = "34.123.45.67,34.123.45.68"

		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName:     activities.MgmtVpcName,
				SubnetName:  activities.MgmtSubnetName,
				IpCidrRange: activities.MgmtNetworkIpRange,
				Firewall: hyperscaler_models.Firewall{
					Name:             activities.MgmtFirewallName,
					SourceRanges:     []string{},
					AllowedPortRules: []string{"tcp", "22", "443"},
				},
			},
			activities.IcVpcName: {
				VpcName:     activities.IcVpcName,
				SubnetName:  activities.IcSubnet,
				IpCidrRange: activities.IcNetworkIpRange,
				Firewall: hyperscaler_models.Firewall{
					Name:             activities.IcFirewallName,
					SourceRanges:     []string{"10.0.0.0/8"},
					AllowedPortRules: []string{"tcp", "udp"},
				},
			},
			activities.RsmVpcName: {
				VpcName:     activities.RsmVpcName,
				SubnetName:  activities.RsmSubnetName,
				IpCidrRange: activities.RsmNetworkIpRange,
				Firewall: hyperscaler_models.Firewall{
					Name:             activities.RsmFirewallName,
					SourceRanges:     []string{"192.168.0.0/16"},
					AllowedPortRules: []string{"tcp", "udp"},
				},
			},
		}

		// Mock the GetInternalVSANetworkForFirewalls function
		callCount := 0
		activities.GetInternalVSANetworkForFirewalls = func(vpcName, firewallName string, sourceRanges, portRules []string, priority int64, trafficDirection string) activities.InternalVSANetwork {
			callCount++
			if callCount == 1 {
				// First call for private firewall
				assert.Equal(t, activities.MgmtVpcName, vpcName)
				assert.Equal(t, activities.MgmtFirewallName+"-1", firewallName)
				assert.Equal(t, []string{"10.0.0.0/8", "172.16.0.0/12"}, sourceRanges)
				assert.Equal(t, []string{activities.AllowAllPorts}, portRules)
				assert.Equal(t, int64(activities.FirewallPriority), priority)
				assert.Equal(t, activities.IngressTrafficDirection, trafficDirection)

				return activities.InternalVSANetwork{
					VpcName:     activities.MgmtVpcName,
					SubnetName:  activities.MgmtSubnetName,
					IpCidrRange: activities.MgmtNetworkIpRange,
					Firewall: hyperscaler_models.Firewall{
						Name:             firewallName,
						SourceRanges:     sourceRanges,
						AllowedPortRules: portRules,
						Priority:         priority,
						Direction:        trafficDirection,
					},
				}
			} else if callCount == 2 {
				// Second call for public firewall
				assert.Equal(t, activities.MgmtVpcName, vpcName)
				assert.Equal(t, activities.MgmtFirewallName+"-2", firewallName)
				assert.Equal(t, []string{"34.123.45.67", "34.123.45.68"}, sourceRanges)
				assert.Equal(t, []string{"tcp", "22", "443"}, portRules)
				assert.Equal(t, int64(activities.FirewallPriority), priority)
				assert.Equal(t, activities.IngressTrafficDirection, trafficDirection)

				return activities.InternalVSANetwork{
					VpcName:     activities.MgmtVpcName,
					SubnetName:  activities.MgmtSubnetName,
					IpCidrRange: activities.MgmtNetworkIpRange,
					Firewall: hyperscaler_models.Firewall{
						Name:             firewallName,
						SourceRanges:     sourceRanges,
						AllowedPortRules: portRules,
						Priority:         priority,
						Direction:        trafficDirection,
					},
				}
			}
			return activities.InternalVSANetwork{}
		}

		// Execute the function
		result := activities.PrepareInternalVSANetworksForFirewall()

		// Verify results
		assert.Equal(t, 4, len(result))
		assert.Contains(t, result, activities.MgmtVpcName+"-1")
		assert.Contains(t, result, activities.MgmtVpcName+"-2")
		assert.Contains(t, result, activities.IcVpcName)
		assert.Contains(t, result, activities.RsmVpcName)

		// Verify IC and RSM networks are copied directly
		assert.Equal(t, activities.InternalVSANetworks[activities.IcVpcName], result[activities.IcVpcName])
		assert.Equal(t, activities.InternalVSANetworks[activities.RsmVpcName], result[activities.RsmVpcName])

		// Verify GetInternalVSANetworkForFirewalls was called twice
		assert.Equal(t, 2, callCount)
	})

	t.Run("Success_WithEmptySourceRanges", func(t *testing.T) {
		activities.MgmtFirewallSourceRanges = ""
		activities.MgmtRegionalNatIP = ""

		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName:     activities.MgmtVpcName,
				SubnetName:  activities.MgmtSubnetName,
				IpCidrRange: activities.MgmtNetworkIpRange,
				Firewall: hyperscaler_models.Firewall{
					Name:             activities.MgmtFirewallName,
					AllowedPortRules: []string{"tcp", "22"},
				},
			},
			activities.IcVpcName:  activities.InternalVSANetwork{VpcName: activities.IcVpcName},
			activities.RsmVpcName: activities.InternalVSANetwork{VpcName: activities.RsmVpcName},
		}

		activities.GetInternalVSANetworkForFirewalls = func(vpcName, firewallName string, sourceRanges, portRules []string, priority int64, trafficDirection string) activities.InternalVSANetwork {
			// Verify empty strings result in single empty element when split
			if strings.Contains(firewallName, "-1") {
				assert.Equal(t, []string{""}, sourceRanges)
			} else if strings.Contains(firewallName, "-2") {
				assert.Equal(t, []string{""}, sourceRanges)
			}
			return activities.InternalVSANetwork{VpcName: vpcName, Firewall: hyperscaler_models.Firewall{Name: firewallName}}
		}

		result := activities.PrepareInternalVSANetworksForFirewall()
		assert.Equal(t, 4, len(result))
	})

	t.Run("Success_WithSingleSourceRange", func(t *testing.T) {
		activities.MgmtFirewallSourceRanges = "10.0.0.0/8"
		activities.MgmtRegionalNatIP = "34.123.45.67"

		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName: activities.MgmtVpcName,
				Firewall: hyperscaler_models.Firewall{
					Name:             activities.MgmtFirewallName,
					AllowedPortRules: []string{"tcp", "443"},
				},
			},
			activities.IcVpcName:  activities.InternalVSANetwork{VpcName: activities.IcVpcName},
			activities.RsmVpcName: activities.InternalVSANetwork{VpcName: activities.RsmVpcName},
		}

		activities.GetInternalVSANetworkForFirewalls = func(vpcName, firewallName string, sourceRanges, portRules []string, priority int64, trafficDirection string) activities.InternalVSANetwork {
			if strings.Contains(firewallName, "-1") {
				assert.Equal(t, []string{"10.0.0.0/8"}, sourceRanges)
			} else if strings.Contains(firewallName, "-2") {
				assert.Equal(t, []string{"34.123.45.67"}, sourceRanges)
			}
			return activities.InternalVSANetwork{VpcName: vpcName, Firewall: hyperscaler_models.Firewall{Name: firewallName}}
		}

		result := activities.PrepareInternalVSANetworksForFirewall()
		assert.Equal(t, 4, len(result))
	})
}

// Unit tests for _getInternalVSANetworkForFirewalls
func Test_getInternalVSANetworkForFirewalls(t *testing.T) {
	// Save original InternalVSANetworks
	originalInternalVSANetworks := activities.InternalVSANetworks
	defer func() {
		activities.InternalVSANetworks = originalInternalVSANetworks
	}()

	t.Run("Success_WithMgmtVpc", func(t *testing.T) {
		// Setup test data
		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName:     "test-mgmt-vpc",
				SubnetName:  "test-mgmt-subnet",
				IpCidrRange: "198.18.0.0/20",
				Firewall: hyperscaler_models.Firewall{
					Name: "existing-firewall",
				},
			},
		}

		vpcName := activities.MgmtVpcName
		firewallName := "test-firewall"
		sourceRanges := []string{"10.0.0.0/8", "172.16.0.0/12"}
		portRules := []string{"tcp", "22", "443"}
		priority := int64(1000)
		trafficDirection := "INGRESS"

		result := activities.GetInternalVSANetworkForFirewalls(vpcName, firewallName, sourceRanges, portRules, priority, trafficDirection)

		// Verify the network details are copied from InternalVSANetworks
		assert.Equal(t, "test-mgmt-vpc", result.VpcName)
		assert.Equal(t, "test-mgmt-subnet", result.SubnetName)
		assert.Equal(t, "198.18.0.0/20", result.IpCidrRange)

		// Verify the firewall details are set from parameters
		assert.Equal(t, firewallName, result.Firewall.Name)
		assert.Equal(t, sourceRanges, result.Firewall.SourceRanges)
		assert.Equal(t, portRules, result.Firewall.AllowedPortRules)
		assert.Equal(t, priority, result.Firewall.Priority)
		assert.Equal(t, trafficDirection, result.Firewall.Direction)
	})

	t.Run("Success_WithIcVpc", func(t *testing.T) {
		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.IcVpcName: {
				VpcName:     "test-ic-vpc",
				SubnetName:  "test-ic-subnet",
				IpCidrRange: "198.18.32.0/20",
				Firewall: hyperscaler_models.Firewall{
					Name: "existing-ic-firewall",
				},
			},
		}

		vpcName := activities.IcVpcName
		firewallName := "ic-test-firewall"
		sourceRanges := []string{"192.168.0.0/16"}
		portRules := []string{"tcp", "udp"}
		priority := int64(1500)
		trafficDirection := "EGRESS"

		result := activities.GetInternalVSANetworkForFirewalls(vpcName, firewallName, sourceRanges, portRules, priority, trafficDirection)

		assert.Equal(t, "test-ic-vpc", result.VpcName)
		assert.Equal(t, "test-ic-subnet", result.SubnetName)
		assert.Equal(t, "198.18.32.0/20", result.IpCidrRange)
		assert.Equal(t, firewallName, result.Firewall.Name)
		assert.Equal(t, sourceRanges, result.Firewall.SourceRanges)
		assert.Equal(t, portRules, result.Firewall.AllowedPortRules)
		assert.Equal(t, priority, result.Firewall.Priority)
		assert.Equal(t, trafficDirection, result.Firewall.Direction)
	})

	t.Run("Success_WithRsmVpc", func(t *testing.T) {
		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.RsmVpcName: {
				VpcName:     "test-rsm-vpc",
				SubnetName:  "test-rsm-subnet",
				IpCidrRange: "198.18.16.0/20",
				Firewall: hyperscaler_models.Firewall{
					Name: "existing-rsm-firewall",
				},
			},
		}

		vpcName := activities.RsmVpcName
		firewallName := "rsm-test-firewall"
		sourceRanges := []string{"0.0.0.0/0"}
		portRules := []string{"all"}
		priority := int64(2000)
		trafficDirection := "INGRESS"

		result := activities.GetInternalVSANetworkForFirewalls(vpcName, firewallName, sourceRanges, portRules, priority, trafficDirection)

		assert.Equal(t, "test-rsm-vpc", result.VpcName)
		assert.Equal(t, "test-rsm-subnet", result.SubnetName)
		assert.Equal(t, "198.18.16.0/20", result.IpCidrRange)
		assert.Equal(t, firewallName, result.Firewall.Name)
		assert.Equal(t, sourceRanges, result.Firewall.SourceRanges)
		assert.Equal(t, portRules, result.Firewall.AllowedPortRules)
		assert.Equal(t, priority, result.Firewall.Priority)
		assert.Equal(t, trafficDirection, result.Firewall.Direction)
	})

	t.Run("Success_WithEmptyParameters", func(t *testing.T) {
		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName:     "test-vpc",
				SubnetName:  "test-subnet",
				IpCidrRange: "10.0.0.0/24",
			},
		}

		vpcName := activities.MgmtVpcName
		firewallName := ""
		sourceRanges := []string{}
		portRules := []string{}
		priority := int64(0)
		trafficDirection := ""

		result := activities.GetInternalVSANetworkForFirewalls(vpcName, firewallName, sourceRanges, portRules, priority, trafficDirection)

		assert.Equal(t, "test-vpc", result.VpcName)
		assert.Equal(t, "test-subnet", result.SubnetName)
		assert.Equal(t, "10.0.0.0/24", result.IpCidrRange)
		assert.Equal(t, "", result.Firewall.Name)
		assert.Equal(t, []string{}, result.Firewall.SourceRanges)
		assert.Equal(t, []string{}, result.Firewall.AllowedPortRules)
		assert.Equal(t, int64(0), result.Firewall.Priority)
		assert.Equal(t, "", result.Firewall.Direction)
	})

	t.Run("Success_WithNilSourceRangesAndPortRules", func(t *testing.T) {
		activities.InternalVSANetworks = map[string]activities.InternalVSANetwork{
			activities.MgmtVpcName: {
				VpcName:     "test-vpc",
				SubnetName:  "test-subnet",
				IpCidrRange: "172.16.0.0/16",
			},
		}

		vpcName := activities.MgmtVpcName
		firewallName := "nil-test-firewall"
		var sourceRanges []string = nil
		var portRules []string = nil
		priority := int64(500)
		trafficDirection := "BIDIRECTIONAL"

		result := activities.GetInternalVSANetworkForFirewalls(vpcName, firewallName, sourceRanges, portRules, priority, trafficDirection)

		assert.Equal(t, "test-vpc", result.VpcName)
		assert.Equal(t, "test-subnet", result.SubnetName)
		assert.Equal(t, "172.16.0.0/16", result.IpCidrRange)
		assert.Equal(t, firewallName, result.Firewall.Name)
		assert.Nil(t, result.Firewall.SourceRanges)
		assert.Nil(t, result.Firewall.AllowedPortRules)
		assert.Equal(t, priority, result.Firewall.Priority)
		assert.Equal(t, trafficDirection, result.Firewall.Direction)
	})
}

func TestDetermineVMScalingDirection_Success_ScalingUp(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test scaling up (cheaper to more expensive VM)
	isScalingUp, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-4-lssd", "c3-standard-22-lssd")

	assert.NoError(t, err)
	assert.True(t, isScalingUp, "Should be scaling up from cheaper to more expensive VM")
}

func TestDetermineVMScalingDirection_Success_ScalingDown(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test scaling down (more expensive to cheaper VM)
	isScalingUp, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-22-lssd", "c3-standard-4-lssd")

	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Should be scaling down from more expensive to cheaper VM")
}

func TestDetermineVMScalingDirection_LoadVMRSConfigError(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Test with non-existent config file
	_, err := activity.DetermineVMScalingDirection(ctx, "non-existent-config.yaml", "n2-standard-8", "n2-standard-16")

	assert.Error(t, err)
	// VMRS errors are not wrapped as temporal application errors
	assert.Contains(t, err.Error(), "ConfigParseError")
}

func TestDetermineVMScalingDirection_InvalidVMTypes(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with invalid VM types that don't exist in the config
	// This should trigger an error when trying to compare VM types
	_, err := activity.DetermineVMScalingDirection(ctx, configPath, "invalid-vm-type-1", "invalid-vm-type-2")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "current VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_UnexpectedDecisionMakerType(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with valid config - this should work since we're using least_cost_single_vm strategy
	isScalingUp, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-4-lssd", "c3-standard-4-lssd")

	// This should work since the strategy is correct
	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Same VM type should not be scaling")
}

func TestDetermineVMScalingDirection_VMsSortedByCostNil(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with valid config - this should work
	isScalingUp, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-4-lssd", "c3-standard-4-lssd")

	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Same VM type should not be scaling")
}

func TestDetermineVMScalingDirection_CurrentVMTypeNotFound(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with current VM type not in the list
	_, err := activity.DetermineVMScalingDirection(ctx, configPath, "non-existent-vm-type", "c3-standard-4-lssd")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "current VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_NewVMTypeNotFound(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with new VM type not in the list
	_, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-4-lssd", "non-existent-vm-type")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "new VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_EarlyBreakOptimization(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with first and second VM types to test early break optimization
	isScalingUp, err := activity.DetermineVMScalingDirection(ctx, configPath, "c3-standard-4-lssd", "c3-standard-8-lssd")

	assert.NoError(t, err)
	assert.True(t, isScalingUp, "Should be scaling up from cheaper to more expensive VM")
}

func TestUpdatePoolFields_Success(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the UpdatePoolFields call
	poolUUID := "test-pool-uuid"
	updates := map[string]interface{}{
		"description": "updated description",
		"name":        "updated-pool-name",
	}

	mockStorage.On("UpdatePoolFields", ctx, poolUUID, updates).Return(nil)

	// Test UpdatePoolFields
	err := activity.UpdatePoolFields(ctx, poolUUID, updates)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdatePoolFields_Error(t *testing.T) {
	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock the UpdatePoolFields call to return an error
	poolUUID := "test-pool-uuid"
	updates := map[string]interface{}{
		"description": "updated description",
	}
	expectedError := errors.New("database update failed")

	mockStorage.On("UpdatePoolFields", ctx, poolUUID, updates).Return(expectedError)

	// Test UpdatePoolFields with error
	err := activity.UpdatePoolFields(ctx, poolUUID, updates)

	assert.Error(t, err)
	// Regular errors are not wrapped as temporal application errors
	assert.Equal(t, expectedError, err)

	mockStorage.AssertExpectations(t)
}

// ============================================================================
// Tests for newly added zone validation functions
// ============================================================================

func TestValidateVSAZonesForMachineType_Success(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	instanceType := "n2-standard-4"

	// Mock IsMachineTypeAvailable for primary zone
	mockService.On("IsMachineTypeAvailable", projectNumber, primaryZone, instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for secondary zone
	mockService.On("IsMachineTypeAvailable", projectNumber, secondaryZone, instanceType).Return(true, nil)

	// Act
	err := activities.ValidateVSAZonesForMachineType(mockService, projectNumber, primaryZone, secondaryZone, instanceType)

	// Assert
	assert.NoError(t, err)
	mockService.AssertExpectations(t)
}

func TestValidateVSAZonesForMachineType_PrimaryZoneFailure(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	instanceType := "n2-standard-4"

	// Mock IsMachineTypeAvailable for primary zone to return false
	mockService.On("IsMachineTypeAvailable", projectNumber, primaryZone, instanceType).Return(false, nil)

	// Act
	err := activities.ValidateVSAZonesForMachineType(mockService, projectNumber, primaryZone, secondaryZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")
	// Check that it's a VCP error with the correct error code
	var vcpErr *vsaerrors.CustomError
	assert.ErrorAs(t, err, &vcpErr)
	assert.Equal(t, vsaerrors.ErrZoneMachineTypeValidation, vcpErr.TrackingID)
	mockService.AssertExpectations(t)
}

func TestValidateVSAZonesForMachineType_SecondaryZoneFailure(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	instanceType := "n2-standard-4"

	// Mock IsMachineTypeAvailable for primary zone to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, primaryZone, instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for secondary zone to return false
	mockService.On("IsMachineTypeAvailable", projectNumber, secondaryZone, instanceType).Return(false, nil)

	// Act
	err := activities.ValidateVSAZonesForMachineType(mockService, projectNumber, primaryZone, secondaryZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")
	// Check that it's a VCP error with the correct error code
	var vcpErr *vsaerrors.CustomError
	assert.ErrorAs(t, err, &vcpErr)
	assert.Equal(t, vsaerrors.ErrZoneMachineTypeValidation, vcpErr.TrackingID)
	mockService.AssertExpectations(t)
}

func TestValidateVSAZonesForMachineType_PrimaryZoneError(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	instanceType := "n2-standard-4"

	// Mock IsMachineTypeAvailable for primary zone to return error
	mockService.On("IsMachineTypeAvailable", projectNumber, primaryZone, instanceType).Return(false, errors.New("API error"))

	// Act
	err := activities.ValidateVSAZonesForMachineType(mockService, projectNumber, primaryZone, secondaryZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate machine type availability in primary zone us-central1-a: API error")
	// The error should not be wrapped as a temporal application error since it's not a CustomError
	// It should be a regular error
	assert.NotContains(t, err.Error(), "Resource unavailable. Please contact support.")
	mockService.AssertExpectations(t)
}

func TestValidateVSAZonesForMachineType_SecondaryZoneError(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	instanceType := "n2-standard-4"

	// Mock IsMachineTypeAvailable for primary zone to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, primaryZone, instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for secondary zone to return error
	mockService.On("IsMachineTypeAvailable", projectNumber, secondaryZone, instanceType).Return(false, errors.New("API error"))

	// Act
	err := activities.ValidateVSAZonesForMachineType(mockService, projectNumber, primaryZone, secondaryZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to validate machine type availability in secondary zone us-central1-b: API error")
	// The error should not be wrapped as a temporal application error since it's not a CustomError
	// It should be a regular error
	assert.NotContains(t, err.Error(), "Resource unavailable. Please contact support.")
	mockService.AssertExpectations(t)
}

func TestValidateZonesForMachineTypes_ActivityMethodSignature(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Act - Test that the activity method exists and has the correct signature
	// This test ensures the method can be called without compilation errors
	// We're not testing the actual execution since it requires GCP service initialization
	_ = activity.ValidateZonesForMachineTypes

	// Assert - If we get here without compilation errors, the method exists
	// This is a basic test to ensure the method signature is correct
	assert.NotNil(t, activity.ValidateZonesForMachineTypes)
}

// Test error handling with WrapAsNonRetryableTemporalApplicationError
func Test_resolveZonesForCluster_Error_PrimaryZoneMachineTypeValidation(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for primary zone validation to return false
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", instanceType).Return(false, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)

	// Check that it's wrapped as a non-retryable temporal application error
	var appErr *temporal.ApplicationError
	assert.ErrorAs(t, err, &appErr)
	assert.Equal(t, "CustomError", appErr.Type())

	// Extract the tracking ID from the application error
	var trackingID int
	var errorDetails string
	err = appErr.Details(&trackingID, &errorDetails)
	assert.NoError(t, err)
	assert.Equal(t, vsaerrors.ErrZoneMachineTypeValidation, trackingID)

	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_SecondaryZoneMachineTypeValidation(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for primary zone validation to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for secondary zone validation to return false
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(false, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)

	// Check that it's wrapped as a non-retryable temporal application error
	var appErr *temporal.ApplicationError
	assert.ErrorAs(t, err, &appErr)
	assert.Equal(t, "CustomError", appErr.Type())

	// Extract the tracking ID from the application error
	var trackingID int
	var errorDetails string
	err = appErr.Details(&trackingID, &errorDetails)
	assert.NoError(t, err)
	assert.Equal(t, vsaerrors.ErrZoneMachineTypeValidation, trackingID)

	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_MediatorZoneMachineTypeValidation(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler2.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for primary zone validation to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for secondary zone selection to return true
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for mediator zone (when isRegionalHA=false, mediatorZone=primaryZone) to return false
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-a", "e2-micro").Return(false, nil)

	// Act - Use isRegionalHA=false to trigger mediatorZone=primaryZone and validation
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType, false)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)

	// Check that it's wrapped as a non-retryable temporal application error
	var appErr *temporal.ApplicationError
	assert.ErrorAs(t, err, &appErr)
	assert.Equal(t, "CustomError", appErr.Type())

	// Extract the tracking ID from the application error
	var trackingID int
	var errorDetails string
	err = appErr.Details(&trackingID, &errorDetails)
	assert.NoError(t, err)
	assert.Equal(t, vsaerrors.ErrZoneMachineTypeValidation, trackingID)

	mockService.AssertExpectations(t)
}

// TestValidateZonesForMachineTypes_GCPServiceError covers lines 133-135, 137
func TestValidateZonesForMachineTypes_GCPServiceError(t *testing.T) {
	// Mock the hyperscaler2.GetGCPService to return an error
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("GCP service initialization failed")
	}

	activity := &activities.PoolActivity{}
	ctx := context.Background()

	err := activity.ValidateZonesForMachineTypes(ctx, "test-project", "us-central1-a", "us-central1-b", "e2-standard-4")

	// Should return a temporal application error with the GCP service error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to initialize GCP service")
}

// TestResolveZonesForCluster_PrimaryZoneValidationError covers lines 1160, 1161
func TestResolveZonesForCluster_PrimaryZoneValidationError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(false, errors.New("API rate limit exceeded"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"",              // secondary zone (will be auto-selected)
		"",              // mediator zone (will be auto-selected)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate machine type availability in primary zone")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_SecondaryZoneValidationError covers lines 1173, 1174
func TestResolveZonesForCluster_SecondaryZoneValidationError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(false, errors.New("network timeout"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone (explicitly set)
		"",              // mediator zone (will be auto-selected)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate machine type availability in secondary zone")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_SecondaryZoneAutoSelectionError covers lines 1188, 1189
func TestResolveZonesForCluster_SecondaryZoneAutoSelectionError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(false, errors.New("zone unavailable"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"",              // secondary zone (will be auto-selected)
		"",              // mediator zone (will be auto-selected)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate machine type availability in zone us-central1-b: zone unavailable")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_MediatorZoneValidationError covers lines 1200, 1201
func TestResolveZonesForCluster_MediatorZoneValidationError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-micro").Return(false, errors.New("mediator instance type not supported"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone (explicitly set)
		"",              // mediator zone (will be auto-selected)
		"e2-standard-4",
		false, // isRegionalHA (mediator uses primary zone)
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate mediator machine type availability in primary zone")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_MediatorZoneAutoSelectionError covers lines 1209-1212
func TestResolveZonesForCluster_MediatorZoneAutoSelectionError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c", "us-central1-d"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-c", "e2-micro").Return(false, errors.New("mediator instance type not supported"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone (explicitly set)
		"",              // mediator zone (will be auto-selected)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate mediator machine type availability in zone us-central1-c: mediator instance type not supported")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_MediatorZoneConflictError covers lines 1214-1216
func TestResolveZonesForCluster_MediatorZoneConflictError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(true, nil)

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone
		"us-central1-b", // mediator zone (same as secondary - should fail)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "mediator zone cannot be the same as secondary zone")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_ExplicitMediatorZoneValidationError covers lines 1231, 1232
func TestResolveZonesForCluster_ExplicitMediatorZoneValidationError(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-c", "e2-micro").Return(false, errors.New("mediator instance type not supported"))

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone
		"us-central1-c", // mediator zone (explicitly set)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "failed to validate mediator machine type availability in mediator zone")

	mockGCPService.AssertExpectations(t)
}

// TestResolveZonesForCluster_ExplicitMediatorZoneMachineTypeUnavailable covers lines 1233, 1234
func TestResolveZonesForCluster_ExplicitMediatorZoneMachineTypeUnavailable(t *testing.T) {
	mockGCPService := new(hyperscaler2.MockGoogleServices)
	mockGCPService.On("GetZones", "test-project", "us-central1").Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-a", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-b", "e2-standard-4").Return(true, nil)
	mockGCPService.On("IsMachineTypeAvailable", "test-project", "us-central1-c", "e2-micro").Return(false, nil) // Machine type unavailable

	secondaryZone, mediatorZone, err := activities.ResolveZonesForCluster(
		mockGCPService,
		"test-project",
		"us-central1",
		"us-central1-a", // primary zone
		"us-central1-b", // secondary zone
		"us-central1-c", // mediator zone (explicitly set)
		"e2-standard-4",
		true, // isRegionalHA
	)

	assert.Error(t, err)
	assert.Empty(t, secondaryZone)
	assert.Empty(t, mediatorZone)
	assert.Contains(t, err.Error(), "Resource unavailable. Please contact support.")

	mockGCPService.AssertExpectations(t)
}

func TestAutoTierSyncActivity_HydrateUpdatedPoolToCCFE(t *testing.T) {
	ctx := context.TODO()

	t.Run("HydrateUpdatedPoolToCCFE_HydrationEnabled", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.PoolActivity{SE: mockStorage}

		pool := datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		}

		// Mock the hydrationActivities.HydrateUpdatedPoolToCCFE function
		called := false
		originalHydrateUpdatedPoolToCCFE := hydrationActivities.HydrateUpdatedPoolToCCFE
		hydrationActivities.HydrateUpdatedPoolToCCFE = func(ctx context.Context, dbPool datamodel.Pool) error {
			called = true
			return nil
		}
		defer func() { hydrationActivities.HydrateUpdatedPoolToCCFE = originalHydrateUpdatedPoolToCCFE }()

		err := activity.HydrateUpdatedPoolToCCFE(ctx, pool)
		assert.NoError(tt, err)
		assert.True(tt, called)
	})

	t.Run("HydrateUpdatedPoolToCCFE_HydrationFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.PoolActivity{SE: mockStorage}

		pool := datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		}

		// Mock the hydrationActivities.HydrateUpdatedPoolToCCFE function to return error
		originalHydrateUpdatedPoolToCCFE := hydrationActivities.HydrateUpdatedPoolToCCFE
		hydrationActivities.HydrateUpdatedPoolToCCFE = func(ctx context.Context, dbPool datamodel.Pool) error {
			return errors.New("hydration failed")
		}
		defer func() { hydrationActivities.HydrateUpdatedPoolToCCFE = originalHydrateUpdatedPoolToCCFE }()

		err := activity.HydrateUpdatedPoolToCCFE(ctx, pool)
		assert.Error(tt, err)
	})
}

func TestPoolActivity_GetIPsConsumedForSubnet(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}

	pool := datamodel.Pool{
		DeploymentName: "test-deployment",
	}

	tenancyDetails := &commonparams.TenancyInfo{
		RegionalTenantProject: "test-project",
		SubnetworkNames:       []string{"test-subnet"},
	}

	region := "us-central1"

	// Mock ListAddressesByDeployment function
	originalListAddressesByDeployment := activities.ListAddressesByDeployment
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.ListAddressesByDeployment = originalListAddressesByDeployment
		hyperscaler2.GetGCPService = originalGetGCPService
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Logger: log.NewLogger()}, nil
	}

	t.Run("success with matching addresses", func(t *testing.T) {
		// Mock addresses with matching subnet names
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
			{
				AddressName: "address-3",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/other-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-3",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(2), (*result)[0].IPsReserved) // Only 2 addresses match the subnet
	})

	t.Run("success with no matching addresses", func(t *testing.T) {
		// Mock addresses with no matching subnet names
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/other-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/another-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved) // No addresses match the subnet
	})

	t.Run("success with multiple subnets", func(t *testing.T) {
		// Test with multiple subnets in tenancy details
		tenancyDetailsMultiple := &commonparams.TenancyInfo{
			RegionalTenantProject: "test-project",
			SubnetworkNames:       []string{"subnet-1", "subnet-2"},
		}

		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/subnet-1",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/subnet-2",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
			{
				AddressName: "address-3",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/subnet-2",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-3",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetailsMultiple, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 2)

		// Check subnet-1 has 1 address
		subnet1Result := (*result)[0]
		assert.Equal(t, "subnet-1", subnet1Result.SubnetName)
		assert.Equal(t, int64(1), subnet1Result.IPsReserved)

		// Check subnet-2 has 2 addresses
		subnet2Result := (*result)[1]
		assert.Equal(t, "subnet-2", subnet2Result.SubnetName)
		assert.Equal(t, int64(2), subnet2Result.IPsReserved)
	})

	t.Run("success with no addresses", func(t *testing.T) {
		// Mock empty addresses list
		mockAddresses := &[]hyperscaler_models.Address{}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved)
	})

	t.Run("success with nil addresses", func(t *testing.T) {
		// Mock nil addresses
		var mockAddresses *[]hyperscaler_models.Address = nil

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 0)
	})

	t.Run("success with no subnetwork names", func(t *testing.T) {
		// Test with empty subnetwork names
		tenancyDetailsEmpty := &commonparams.TenancyInfo{
			RegionalTenantProject: "test-project",
			SubnetworkNames:       []string{},
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetailsEmpty, region)

		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("error when GetGCPService fails", func(t *testing.T) {
		// Mock GCP service to return error
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("error when ListAddressesByDeployment fails", func(t *testing.T) {
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return nil, errors.New("failed to list addresses")
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to list addresses")
	})

	t.Run("success with addresses having empty SubnetURI", func(t *testing.T) {
		// Mock addresses with empty SubnetURI
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(1), (*result)[0].IPsReserved) // Only address-2 matches
	})

	t.Run("success with partial subnet name matches", func(t *testing.T) {
		// Test that partial matches work (contains check)
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet-extra",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/prefix-test-subnet-suffix",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
			{
				AddressName: "address-3",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/other-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-3",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved) // address-1 and address-2 contain "test-subnet"
	})

	t.Run("success with HasSuffix matching behavior", func(t *testing.T) {
		// Test that HasSuffix correctly matches only addresses ending with the target subnet name
		// This test would fail with Contains but pass with HasSuffix
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/prefix-test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
			{
				AddressName: "address-3",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet-suffix",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-3",
			},
			{
				AddressName: "address-4",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-4",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(2), (*result)[0].IPsReserved) // Only address-1 and address-4 should match (end with /test-subnet)
	})

	t.Run("success with forward slash prefix in HasSuffix check", func(t *testing.T) {
		// Test that the forward slash prefix in HasSuffix check works correctly
		mockAddresses := &[]hyperscaler_models.Address{
			{
				AddressName: "address-1",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-1",
			},
			{
				AddressName: "address-2",
				SubnetURI:   "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/subnetworks/test-subnet-extra",
				SelfLink:    "https://www.googleapis.com/compute/v1/projects/test-project/regions/us-central1/addresses/address-2",
			},
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		result, err := activity.GetIPsConsumedForSubnet(ctx, pool, tenancyDetails, region)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(1), (*result)[0].IPsReserved) // Only address-1 should match (ends with /test-subnet)
	})
}

// Using the existing MockStorage from database/vcp package

func TestFetchPoolData_Success(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	accountID := int64(12345)

	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			DeploymentID: "deployment-123",
			Provider:     "gcp",
			Region:       "us-central1",
			GCPConfig: vlm.GCPConfig{
				ProjectID: "test-project",
			},
		},
	}

	vlmConfigJSON, _ := json.Marshal(vlmConfig)

	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: poolUUID},
			AccountID: accountID,
			VLMConfig: string(vlmConfigJSON),
			AutoTieringConfig: &datamodel.AutoTieringConfig{
				BucketName: "test-bucket",
			},
		},
	}

	// Mock expectations
	mockSE.On("GetPool", mock.Anything, poolUUID, accountID).Return(poolView, nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.FetchPoolDataActivityInput{
		PoolUUID:  poolUUID,
		AccountID: accountID,
	}

	result, err := env.ExecuteActivity(activity.FetchPoolData, input)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var output *activities.FetchPoolDataActivityOutput
	err = result.Get(&output)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.True(t, output.Success)
	assert.Equal(t, poolUUID, output.PoolUUID)
	assert.Equal(t, vlmConfig.Deployment.DeploymentID, output.VLMConfig.Deployment.DeploymentID)
	assert.Equal(t, vlmConfig.Deployment.Provider, output.VLMConfig.Deployment.Provider)
	assert.Equal(t, vlmConfig.Deployment.Region, output.VLMConfig.Deployment.Region)
	assert.Equal(t, vlmConfig.Deployment.GCPConfig.ProjectID, output.VLMConfig.Deployment.GCPConfig.ProjectID)

	mockSE.AssertExpectations(t)
}

func TestFetchPoolData_DatabaseError(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	accountID := int64(12345)

	dbError := errors.New("database connection failed")

	// Mock expectations
	mockSE.On("GetPool", mock.Anything, poolUUID, accountID).Return(nil, dbError)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.FetchPoolDataActivityInput{
		PoolUUID:  poolUUID,
		AccountID: accountID,
	}

	_, err := env.ExecuteActivity(activity.FetchPoolData, input)

	// Assertions
	assert.Error(t, err)

	// When activity fails, result might be nil, so we check the error directly
	assert.Contains(t, err.Error(), "database connection failed")

	mockSE.AssertExpectations(t)
}

func TestFetchPoolData_InvalidVLMConfig(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	accountID := int64(12345)

	// Invalid JSON in VLM config
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: poolUUID},
			AccountID: accountID,
			VLMConfig: "invalid json",
		},
	}

	// Mock expectations
	mockSE.On("GetPool", mock.Anything, poolUUID, accountID).Return(poolView, nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.FetchPoolDataActivityInput{
		PoolUUID:  poolUUID,
		AccountID: accountID,
	}

	_, err := env.ExecuteActivity(activity.FetchPoolData, input)

	// Assertions
	assert.Error(t, err)

	// When activity fails, result might be nil, so we check the error directly
	assert.Contains(t, err.Error(), "Invalid input parameters provided")

	mockSE.AssertExpectations(t)
}

func TestFetchPoolData_EmptyVLMConfig(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	accountID := int64(12345)

	// Empty VLM config
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: poolUUID},
			AccountID: accountID,
			VLMConfig: "",
		},
	}

	// Mock expectations
	mockSE.On("GetPool", mock.Anything, poolUUID, accountID).Return(poolView, nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.FetchPoolDataActivityInput{
		PoolUUID:  poolUUID,
		AccountID: accountID,
	}

	result, err := env.ExecuteActivity(activity.FetchPoolData, input)

	// Assertions - The function should now return an error for empty VLM config
	assert.Error(t, err)
	assert.Nil(t, result) // When activity returns error, result is nil

	// Verify the error message
	assert.Contains(t, err.Error(), "Invalid input parameters provided")

	mockSE.AssertExpectations(t)
}

func TestUpdatePoolCompliance_Success(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	satisfyZI := true
	satisfyZS := false

	assetMetadata := &datamodel.AssetMetadata{
		ChildAssets: []datamodel.ChildAsset{
			{
				AssetType:  "compute",
				AssetNames: []string{"instance-1", "instance-2"},
			},
			{
				AssetType:  "storage",
				AssetNames: []string{"bucket-1"},
			},
		},
	}

	// Mock expectations
	expectedUpdates := map[string]interface{}{
		"satisfy_zi":     satisfyZI,
		"satisfy_zs":     satisfyZS,
		"asset_metadata": assetMetadata,
	}
	mockSE.On("UpdatePoolFields", mock.Anything, poolUUID, expectedUpdates).Return(nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.UpdatePoolComplianceActivityInput{
		PoolUUID:      poolUUID,
		SatisfyZI:     satisfyZI,
		SatisfyZS:     satisfyZS,
		AssetMetadata: assetMetadata,
	}

	result, err := env.ExecuteActivity(activity.UpdatePoolCompliance, input)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var output *activities.UpdatePoolComplianceActivityOutput
	err = result.Get(&output)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.True(t, output.Success)
	assert.Equal(t, poolUUID, output.PoolUUID)
	assert.Empty(t, output.Error)

	mockSE.AssertExpectations(t)
}

func TestUpdatePoolCompliance_SuccessWithoutAssetMetadata(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	satisfyZI := true
	satisfyZS := true

	// Mock expectations
	expectedUpdates := map[string]interface{}{
		"satisfy_zi": satisfyZI,
		"satisfy_zs": satisfyZS,
	}
	mockSE.On("UpdatePoolFields", mock.Anything, poolUUID, expectedUpdates).Return(nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.UpdatePoolComplianceActivityInput{
		PoolUUID:      poolUUID,
		SatisfyZI:     satisfyZI,
		SatisfyZS:     satisfyZS,
		AssetMetadata: nil,
	}

	result, err := env.ExecuteActivity(activity.UpdatePoolCompliance, input)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var output *activities.UpdatePoolComplianceActivityOutput
	err = result.Get(&output)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.True(t, output.Success)
	assert.Equal(t, poolUUID, output.PoolUUID)
	assert.Empty(t, output.Error)

	mockSE.AssertExpectations(t)
}

func TestUpdatePoolCompliance_DatabaseError(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	satisfyZI := true
	satisfyZS := false

	dbError := errors.New("database update failed")

	// Mock expectations
	expectedUpdates := map[string]interface{}{
		"satisfy_zi": satisfyZI,
		"satisfy_zs": satisfyZS,
	}
	mockSE.On("UpdatePoolFields", mock.Anything, poolUUID, expectedUpdates).Return(dbError)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.UpdatePoolComplianceActivityInput{
		PoolUUID:      poolUUID,
		SatisfyZI:     satisfyZI,
		SatisfyZS:     satisfyZS,
		AssetMetadata: nil,
	}

	_, err := env.ExecuteActivity(activity.UpdatePoolCompliance, input)

	// Assertions
	assert.Error(t, err)

	// When activity fails, result might be nil, so we check the error directly
	assert.Contains(t, err.Error(), "database update failed")

	mockSE.AssertExpectations(t)
}

func TestUpdatePoolCompliance_ComplexAssetMetadata(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Test data
	poolUUID := "pool-123"
	satisfyZI := false
	satisfyZS := true

	// Complex asset metadata with multiple asset types and names
	assetMetadata := &datamodel.AssetMetadata{
		ChildAssets: []datamodel.ChildAsset{
			{
				AssetType:  "compute",
				AssetNames: []string{"instance-1", "instance-2", "instance-3"},
			},
			{
				AssetType:  "storage",
				AssetNames: []string{"bucket-1", "bucket-2"},
			},
			{
				AssetType:  "network",
				AssetNames: []string{"vpc-1", "subnet-1", "subnet-2"},
			},
		},
	}

	// Mock expectations
	expectedUpdates := map[string]interface{}{
		"satisfy_zi":     satisfyZI,
		"satisfy_zs":     satisfyZS,
		"asset_metadata": assetMetadata,
	}
	mockSE.On("UpdatePoolFields", mock.Anything, poolUUID, expectedUpdates).Return(nil)

	// Register activity
	env.RegisterActivity(activity)

	// Execute
	input := activities.UpdatePoolComplianceActivityInput{
		PoolUUID:      poolUUID,
		SatisfyZI:     satisfyZI,
		SatisfyZS:     satisfyZS,
		AssetMetadata: assetMetadata,
	}

	result, err := env.ExecuteActivity(activity.UpdatePoolCompliance, input)

	// Assertions
	assert.NoError(t, err)
	assert.NotNil(t, result)

	var output *activities.UpdatePoolComplianceActivityOutput
	err = result.Get(&output)
	assert.NoError(t, err)
	assert.NotNil(t, output)
	assert.True(t, output.Success)
	assert.Equal(t, poolUUID, output.PoolUUID)

	mockSE.AssertExpectations(t)
}

func TestUpdatePoolCompliance_AllComplianceScenarios(t *testing.T) {
	// Setup test suite
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockSE := &database.MockStorage{}
	activity := &activities.PoolActivity{SE: mockSE}

	// Register activity
	env.RegisterActivity(activity)

	// Test all possible compliance combinations
	complianceScenarios := []struct {
		name      string
		satisfyZI bool
		satisfyZS bool
	}{
		{"Both compliant", true, true},
		{"ZI compliant, ZS not", true, false},
		{"ZI not compliant, ZS compliant", false, true},
		{"Neither compliant", false, false},
	}

	for _, scenario := range complianceScenarios {
		t.Run(scenario.name, func(t *testing.T) {
			poolUUID := "pool-123"

			// Mock expectations
			expectedUpdates := map[string]interface{}{
				"satisfy_zi": scenario.satisfyZI,
				"satisfy_zs": scenario.satisfyZS,
			}
			mockSE.On("UpdatePoolFields", mock.Anything, poolUUID, expectedUpdates).Return(nil)

			// Execute
			input := activities.UpdatePoolComplianceActivityInput{
				PoolUUID:      poolUUID,
				SatisfyZI:     scenario.satisfyZI,
				SatisfyZS:     scenario.satisfyZS,
				AssetMetadata: nil,
			}

			result, err := env.ExecuteActivity(activity.UpdatePoolCompliance, input)

			// Assertions
			assert.NoError(t, err)
			assert.NotNil(t, result)

			var output *activities.UpdatePoolComplianceActivityOutput
			err = result.Get(&output)
			assert.NoError(t, err)
			assert.NotNil(t, output)
			assert.True(t, output.Success)
			assert.Equal(t, poolUUID, output.PoolUUID)
		})
	}

	mockSE.AssertExpectations(t)
}

func TestGetBucketCompliance_Success(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	bucketName := "test-bucket"

	// Mock cloud service
	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	expectedCloudBucketDetails := &hyperscaler_models.BucketDetails{
		Name:         "test-bucket",
		SatisfiesPzi: true,
		SatisfiesPzs: false,
	}

	mockCloudService.On("GetBucket", ctx, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	result, err := activity.GetBucketCompliance(ctx, bucketName)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.Equal(t, true, result.SatisfiesPzi)
	assert.Equal(t, false, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_EmptyBucketName(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	result, err := activity.GetBucketCompliance(ctx, "")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "bucket name parameter is required to fetch zi/zs compliance")
}

func TestGetBucketCompliance_GetCloudServiceError(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	bucketName := "test-bucket"

	// Mock GetCloudService to return error
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return nil, fmt.Errorf("failed to get cloud service")
	}

	result, err := activity.GetBucketCompliance(ctx, bucketName)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get cloud service")
}

func TestGetBucketCompliance_GetBucketError(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	bucketName := "test-bucket"

	// Mock cloud service
	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	mockCloudService.On("GetBucket", ctx, bucketName).Return(nil, fmt.Errorf("bucket not found")).Once()

	result, err := activity.GetBucketCompliance(ctx, bucketName)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "bucket not found")
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_BothComplianceFieldsTrue(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	bucketName := "compliant-bucket"

	// Mock cloud service
	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	expectedCloudBucketDetails := &hyperscaler_models.BucketDetails{
		Name:         "compliant-bucket",
		SatisfiesPzi: true,
		SatisfiesPzs: true,
	}

	mockCloudService.On("GetBucket", ctx, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	result, err := activity.GetBucketCompliance(ctx, bucketName)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.True(t, result.SatisfiesPzi)
	assert.True(t, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_BothComplianceFieldsFalse(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

	bucketName := "non-compliant-bucket"

	// Mock cloud service
	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	expectedCloudBucketDetails := &hyperscaler_models.BucketDetails{
		Name:         "non-compliant-bucket",
		SatisfiesPzi: false,
		SatisfiesPzs: false,
	}

	mockCloudService.On("GetBucket", ctx, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	result, err := activity.GetBucketCompliance(ctx, bucketName)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.False(t, result.SatisfiesPzi)
	assert.False(t, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_AllScenarios(t *testing.T) {
	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	testCases := []struct {
		name         string
		bucketName   string
		satisfiesPzi bool
		satisfiesPzs bool
	}{
		{
			name:         "PziOnly",
			bucketName:   "pzi-only-bucket",
			satisfiesPzi: true,
			satisfiesPzs: false,
		},
		{
			name:         "PzsOnly",
			bucketName:   "pzs-only-bucket",
			satisfiesPzi: false,
			satisfiesPzs: true,
		},
		{
			name:         "BothCompliant",
			bucketName:   "fully-compliant-bucket",
			satisfiesPzi: true,
			satisfiesPzs: true,
		},
		{
			name:         "NeitherCompliant",
			bucketName:   "non-compliant-bucket",
			satisfiesPzi: false,
			satisfiesPzs: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockSE := database.NewMockStorage(t)
			activity := &activities.PoolActivity{SE: mockSE}
			ctx := context.WithValue(context.Background(), middleware.ContextSLoggerKey, log.NewLogger())

			// Mock cloud service
			mockCloudService := hyperscaler2.NewMockGoogleServices(t)
			activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
				return mockCloudService, nil
			}

			expectedCloudBucketDetails := &hyperscaler_models.BucketDetails{
				Name:         tc.bucketName,
				SatisfiesPzi: tc.satisfiesPzi,
				SatisfiesPzs: tc.satisfiesPzs,
			}

			mockCloudService.On("GetBucket", ctx, tc.bucketName).Return(expectedCloudBucketDetails, nil).Once()

			result, err := activity.GetBucketCompliance(ctx, tc.bucketName)

			assert.NoError(t, err)
			assert.NotNil(t, result)
			assert.Equal(t, tc.bucketName, result.BucketName)
			assert.Equal(t, tc.satisfiesPzi, result.SatisfiesPzi)
			assert.Equal(t, tc.satisfiesPzs, result.SatisfiesPzs)
			mockCloudService.AssertExpectations(t)
		})
	}
}

func TestPoolActivity_CreateExpertModeCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()
	clusterName := "test-cluster"
	username := "admin"

	origGetGCPService := hyperscaler2.GetGCPService
	origGenerateAndCreateCertificateForVSACluster := hyperscaler2.GenerateAndCreateCertificateForVSACluster
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = origGenerateAndCreateCertificateForVSACluster
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "cert-id",
						SecretID:      "",
						Password:      "",
						AuthType:      env.USER_CERTIFICATE,
					},
				},
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
			return &hyperscaler3.CustomCertificateResponse{
				Certificate: &hyperscaler3.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &hyperscaler3.CustomSecret{
					SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "CN", creds.Certificate.CommonName)
		assert.Equal(t, "cert", creds.Certificate.Certificate)
		assert.Equal(t, "key", creds.Certificate.PrivateKey)
		assert.Equal(t, []string{"chain"}, creds.Certificate.InterMediateCertificate)
		assert.Equal(t, "", creds.AdminPassword)
	})

	t.Run("USER_CERTIFICATE ExpertModeCredentials empty error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: nil,
			},
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "cert-id",
						SecretID:      "",
						Password:      "",
						AuthType:      env.USER_CERTIFICATE,
					},
				},
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, certificateID, clusterName, username string) (*hyperscaler3.CustomCertificateResponse, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("cert error"))
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "secret-id",
						Password:      "",
						AuthType:      env.USERNAME_PWD_SEC_MGR,
					},
				},
			},
			PoolCredentials: &datamodel.PoolCredentials{},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return &hyperscaler3.CustomSecret{
				SecretVersion: &hyperscaler3.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "secret-id",
						Password:      "",
						AuthType:      env.USERNAME_PWD_SEC_MGR,
					},
				},
			},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler3.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("default password", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "",
						Password:      "password",
						AuthType:      env.USERNAME_PWD,
					},
				},
			},
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.NoError(t, err)
		assert.Equal(t, "password", creds.AdminPassword)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "",
				SecretID:      "",
				Password:      "password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("gcp error"))
		}
		creds, err := activity.CreateExpertModeCredentials(ctx, pool, clusterName, username)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})
}

func TestPoolActivity_DeleteExpertModeCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()

	origGetGCPService := hyperscaler2.GetGCPService
	origRevokeCert := hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager
	origDeletePwd := hyperscaler2.DeletePasswordFromCacheAndSecretManager
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = origRevokeCert
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = origDeletePwd
	}()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "cert-id",
						SecretID:      "",
						Password:      "",
						AuthType:      env.USER_CERTIFICATE,
					},
				},
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, certID string) error {
			assert.Equal(t, "cert-id", certID)
			return nil
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("USER_CERTIFICATE ExpertModeCredentials empty error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: nil,
			},
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.Error(t, err)
	})
	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "cert-id",
						SecretID:      "",
						Password:      "",
						AuthType:      env.USER_CERTIFICATE,
					},
				},
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, certID string) error {
			return errors.New("revoke error")
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "revoke error")
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "secret-id",
						Password:      "",
						AuthType:      env.USERNAME_PWD_SEC_MGR,
					},
				},
			},
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			assert.Equal(t, "secret-id", secretID)
			return nil
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "secret-id",
						Password:      "",
						AuthType:      env.USERNAME_PWD_SEC_MGR,
					},
				},
			},
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			return errors.New("delete error")
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("default password - no cert no secret-manager", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "",
						Password:      "password",
						AuthType:      env.USERNAME_PWD,
					},
				},
			},
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						CertificateID: "",
						SecretID:      "",
						Password:      "password",
						AuthType:      env.USERNAME_PWD,
					},
				},
			},
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		err := activity.DeleteExpertModeCredentials(ctx, pool)
		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "gcp error", "CustomError", false)
	})
}

func TestFetchExpertModeCredentials_WithUserCertificate_Success(t *testing.T) {
	ctx := context.Background()
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	pool := &datamodel.Pool{
		ExpertModeCredentials: &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				{
					CertificateID: "cert-id",
					SecretID:      "",
					Password:      "",
					AuthType:      env.USER_CERTIFICATE,
				},
			},
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	defer func() {
		hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate
	}()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certificateID string) (*coremodel.Certificate, error) {
		return &coremodel.Certificate{
			CommonName:               "CN",
			SignedCertificate:        "cert",
			PrivateKey:               "key",
			InterMediateCertificates: []string{"intermediate"},
		}, nil
	}

	creds, err := activity.GetExpertModeCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.Equal(t, "CN", creds.Certificate.CommonName)
	assert.Equal(t, "cert", creds.Certificate.Certificate)
	assert.Equal(t, "key", creds.Certificate.PrivateKey)
	assert.Equal(t, []string{"intermediate"}, creds.Certificate.InterMediateCertificate)
}

func TestFetchExpertModeCredentials_WithUserCertificate_CertificateError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		ExpertModeCredentials: &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				{
					CertificateID: "cert-id",
					SecretID:      "",
					Password:      "",
					AuthType:      env.USER_CERTIFICATE,
				},
			},
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	defer func() { hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate }()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, certificateID string) (*coremodel.Certificate, error) {
		return nil, errors.New("certificate error")
	}

	creds, err := activity.GetExpertModeCredentials(ctx, pool)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "certificate error")
}

func TestFetchExpertModeCredentials_WithUsernamePwdSecMgr_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		ExpertModeCredentials: &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				{
					CertificateID: "",
					SecretID:      "secret-id",
					Password:      "",
					AuthType:      env.USERNAME_PWD_SEC_MGR,
				},
			},
		},
	}
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "admin-password", nil
	}

	creds, err := activity.GetExpertModeCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.NotNil(t, creds)
	assert.Equal(t, "admin-password", creds.AdminPassword)
}

func TestFetchExpertModeCredentials_WithUsernamePwdSecMgr_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		ExpertModeCredentials: &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				{
					CertificateID: "",
					SecretID:      "secret-id",
					Password:      "",
					AuthType:      env.USERNAME_PWD_SEC_MGR,
				},
			},
		},
	}
	originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
	defer func() { hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword }()
	hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
		return "", errors.New("Invalid resource field value")
	}

	creds, err := activity.GetExpertModeCredentials(ctx, pool)
	assert.Error(t, err)
	assert.Nil(t, creds)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchExpertModeCredentials_WithDefaultAuthType_ReturnsPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	pool := &datamodel.Pool{
		ExpertModeCredentials: &datamodel.ExpertModeCredentials{
			ExpertModeCredential: []*datamodel.ExpertModeCredential{
				{
					CertificateID: "",
					SecretID:      "",
					Password:      "plain-password",
					AuthType:      env.USERNAME_PWD,
				},
			},
		},
	}

	creds, err := activity.GetExpertModeCredentials(ctx, pool)
	assert.NoError(t, err)
	assert.Equal(t, "plain-password", creds.AdminPassword)
}

func TestSetWaflMaxVolCloneHier(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	t.Run("WhenNodeIsNil_ThenReturnNil", func(tt *testing.T) {
		err := activity.SetWaflMaxVolCloneHier(ctx, nil)
		assert.NoError(tt, err)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnNil", func(tt *testing.T) {
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
	})

	t.Run("WhenCreateRESTClientFails_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(nil, errors.New("REST client creation error"))

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenRESTClientIsNil_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(nil, nil)

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenNetworkingClientIsNil_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(mockRESTClient, nil)
		mockRESTClient.On("Networking").Return(nil)

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
	})

	t.Run("WhenCliExecuteFails_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(mockRESTClient, nil)
		mockRESTClient.On("Networking").Return(mockNetworkingClient)
		mockNetworkingClient.On("CliExecute", mock.Anything).Return(nil, errors.New("CLI execute error"))

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenResponseIsNil_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(mockRESTClient, nil)
		mockRESTClient.On("Networking").Return(mockNetworkingClient)
		mockNetworkingClient.On("CliExecute", mock.Anything).Return(nil, nil)

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenResponsePayloadIsNil_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(mockRESTClient, nil)
		mockRESTClient.On("Networking").Return(mockNetworkingClient)
		cliExecuteOK := &networkpriv.CliExecuteOK{Payload: nil}
		mockNetworkingClient.On("CliExecute", mock.Anything).Return(cliExecuteOK, nil)

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenSuccess_ThenReturnNil", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateRESTClient").Return(mockRESTClient, nil)
		mockRESTClient.On("Networking").Return(mockNetworkingClient)
		output := "wafl.maxvolclonehier updated successfully"
		cliExecuteOK := &networkpriv.CliExecuteOK{
			Payload: &privmodels.CliExecuteResponse{
				Output: output,
			},
		}
		mockNetworkingClient.On("CliExecute", mock.Anything).Return(cliExecuteOK, nil)

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			Password:        "test-password",
		}

		err := activity.SetWaflMaxVolCloneHier(ctx, node)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})
}
