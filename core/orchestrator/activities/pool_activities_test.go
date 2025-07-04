package activities_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	digitalCert "crypto/x509"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/repository"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/api/iam/v1"
	"gorm.io/gorm"
	"netapp.com/vsa/lifecycle-manager/pkg/vlmconfig"
)

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
	pool := repository.ConvertPoolViewToPool(poolView)

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

func TestSavePoolWithClusterDetails_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{Name: "test-pool"}
	cluster := &datamodel.ClusterDetails{}

	mockStorage.On("SavePoolWithVsaClusterDetails", ctx, pool, cluster).Return(nil)

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

	mockStorage.On("SavePoolWithVsaClusterDetails", ctx, pool, cluster).Return(gorm.ErrInvalidData)

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

	mockStorage.On("CreatedPool", ctx, pool).Return(pool, nil)

	// Act
	result, err := activity.CreatedPool(ctx, pool)

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

	mockStorage.On("CreatedPool", ctx, pool).Return(nil, gorm.ErrInvalidData)

	// Act
	result, err := activity.CreatedPool(ctx, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateTenancy_Success(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	createTenancy := activities.FindTenancyAndGetSubnetwork
	GetGCPService := activities.GetGCPService
	defer func() {
		activities.FindTenancyAndGetSubnetwork = createTenancy
		activities.GetGCPService = GetGCPService
	}()

	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := commonparams.CreatePoolParams{Name: "test-pool"}

	tenancyInfo := &commonparams.TenancyInfo{}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Logger: log.NewLogger()}, nil
	}
	activities.FindTenancyAndGetSubnetwork = func(ctx context.Context, gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*commonparams.TenancyInfo, error) {
		return tenancyInfo, nil
	}

	// Act
	result, err := activity.CreateTenancy(ctx, pool)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, tenancyInfo, result)
	mockStorage.AssertExpectations(t)
}

func TestCreateTenancy_Failure(t *testing.T) {
	// Arrange
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := commonparams.CreatePoolParams{Name: "test-pool"}
	mockStorage := database.NewMockStorage(t)
	createTenancy := activities.FindTenancyAndGetSubnetwork
	GetGCPService := activities.GetGCPService

	t.Run("WhenGetGCPServiceFails", func(tt *testing.T) {
		activity := activities.PoolActivity{SE: mockStorage}

		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}

		// Act
		result, err := activity.CreateTenancy(ctx, pool)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
	t.Run("WhenFindTenancyAndGetSubnetworkFails", func(tt *testing.T) {
		activity := activities.PoolActivity{SE: mockStorage}

		defer func() {
			activities.FindTenancyAndGetSubnetwork = createTenancy
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.FindTenancyAndGetSubnetwork = func(ctx context.Context, gcpService hyperscaler.GoogleServices, consumerVPC string, customerProjectNumber string, tenantProjectRegion *string) (*commonparams.TenancyInfo, error) {
			return nil, errors.New("Error finding tenancy unit")
		}

		// Act
		result, err := activity.CreateTenancy(ctx, pool)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
}

func Test_FindTenancyAndGetSubnetwork(t *testing.T) {
	ctx := context.TODO()
	consumerVPC := "test-vpc"
	customerProjectNumber := "123456"
	tenantProjectNumber := "654321"
	snHostProject := "1234321"
	tenantProjectRegion := "us-central1"
	logger := util.GetLogger(ctx)
	snhostSubnetName := "vsa-us-central1"

	t.Run("WhenRegionNil", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, "").Return("", errors.New("Error finding tenancy unit"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, nil)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetTenantProjectFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("", errors.New("Error finding tenancy unit"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetSnHostFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetSnHost", tenantProjectNumber).Return("", errors.New("Error getting sn host"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetSnHostNotFoundAndSuccess", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetSnHost", tenantProjectNumber).Return("", errors.New("Error getting sn host not found"))

		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Name\": \"vsa-us-central1\", \"Network\": \"projects/1234321/global/networks/host-network\"}"), nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(&models.Subnet{Name: snhostSubnetName, Network: "projects/1234321/global/networks/host-network", GatewayAddress: "10.0.0.3"}, nil).Once()

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Nil(tt, err)
		assert.NotNil(tt, tenancyInfo)
	})
	t.Run("WhenGetSubnetworkFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Error getting subnetwork"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenSubnetworkAlreadyExists", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(&models.Subnet{Name: snhostSubnetName}, nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.NoError(tt, err)
		assert.NotEqual(tt, tenancyInfo.SnHostProject, snhostSubnetName)
	})
	t.Run("WhenCreateSubnetworkForTenantProjectFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return(nil, errors.New("Error creating subnetwork"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenSubnetResponseConversionFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("Invalid Response"), nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenParseProjectIdFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Network\": \"host-network\"}"), nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetSubnetworkFailsAfterCreatingTheSubnetwork", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Subnetwork not found")).Once()
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Name\": \"vsa-us-central1\", \"Network\": \"projects/1234321/global/networks/host-network\"}"), nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Error getting subnetwork")).Once()

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenFindTenancyAndGetSubnetworkSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSnHost", tenantProjectNumber).Return(snHostProject, nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(nil, errors.New("Subnetwork not found")).Once()
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Name\": \"vsa-us-central1\", \"Network\": \"projects/1234321/global/networks/host-network\"}"), nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, snhostSubnetName).Return(&models.Subnet{Name: snhostSubnetName, Network: "projects/1234321/global/networks/host-network", GatewayAddress: "10.0.0.3"}, nil).Once()

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.NoError(tt, err)
		assert.NotNil(tt, tenancyInfo)
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

	mockStorage.On("SavePoolWithVsaClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Act
	err := activity.SavePoolWithClusterDetails(ctx, pool, &datamodel.ClusterDetails{})

	// Assert
	assert.Nil(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetONTAPProvider_Success(t *testing.T) {
	// Arrange
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
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
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
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
	cfg := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{
			NetConfig:        map[vlmconfig.VSALIFType]vlmconfig.NetworkConfig{},
			GCPConfig:        vlmconfig.GCPConfig{},
			OntapCredentials: vlmconfig.OntapCredentials{},
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
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "password", "test-tenant-project@xyz.com", "test-tenant-project")
	assert.NoError(t, err)
	assert.Equal(t, "test-deployment", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
	assert.Equal(t, "test-zone1", cfg.Deployment.Zone.Zone1)
	assert.Equal(t, "test-zone2", cfg.Deployment.Zone.Zone2)
	assert.Equal(t, "test-network", cfg.Deployment.NetConfig[vlmconfig.LIFTypeInterCluster].VPC)
	assert.Equal(t, "test-sn-host-project", cfg.Deployment.NetConfig[vlmconfig.LIFTypeInterCluster].GCPNetworkConfig.SubnetProjectID)
	assert.Equal(t, int64(64), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(1024), cfg.Deployment.SPConfig.IOps)
	assert.Equal(t, "1024Gi", cfg.Deployment.SPConfig.Size)
}

func Test_prepareVlmConfig_FileNotFound(t *testing.T) {
	cfg := &vlmconfig.VLMConfig{}
	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "password", "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no such file or directory")
}

func Test_prepareVlmConfig_InvalidJSON(t *testing.T) {
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte("invalid-json"), nil
	}

	cfg := &vlmconfig.VLMConfig{}
	dsc := &vmrs.Decision{
		ChosenVMs: []string{"c4-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1024,
			DesiredThroughputInMiBs: 64,
			DesiredCapacityInGiB:    1024,
		},
	}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone1", "test=zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "password", "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid character")
}

func Test_prepareVlmConfig_EmptyDeploymentName(t *testing.T) {
	cfg := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{
			NetConfig:        map[vlmconfig.VSALIFType]vlmconfig.NetworkConfig{},
			GCPConfig:        vlmconfig.GCPConfig{},
			OntapCredentials: vlmconfig.OntapCredentials{},
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
	err := activities.PrepareVlmConfig(cfg, "", "test-region", "test-zone", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project", dsc, "password", "test-tenant-project@xyz.com", "test-tenant-project")
	assert.NoError(t, err)
	assert.Equal(t, "", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
}

func Test_CreateVSASVM_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlmconfig.VSALIFType][]vlmconfig.LIFConfig{
					vlmconfig.LIFTypeIscsi: {
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}}, {BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)
	mockStorage.On("CreateLif", mock.Anything, mock.Anything).Return(&datamodel.Lif{}, nil)
	mockVlmClient.On("VSASVMCreate", mock.Anything, mock.Anything).Return(nil)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	_, err := env.ExecuteActivity(activity.CreateVSASVM, pool, vlmConfig)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSASVM_DBCreationError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlmconfig.VSALIFType][]vlmconfig.LIFConfig{
					vlmconfig.LIFTypeIscsi: {
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, errors.New("connection error"))
	mockVlmClient.On("VSASVMCreate", ctx, mock.Anything).Return(nil)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	svm, err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Nil(t, svm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection error")
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSASVM_FailsToCreateSVM(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockVlmClient.On("VSASVMCreate", ctx, mock.Anything).Return(errors.New("failed to create SVM"))

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	svm, err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Nil(t, svm)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create SVM")
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSASVM_CouldNotFetchNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockVlmClient.On("VSASVMCreate", ctx, mock.Anything).Return(nil)
	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	svm, err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Nil(t, svm)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_CreateVSASVM_NotEnoughNodes(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockVlmClient.On("VSASVMCreate", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	_, err := env.ExecuteActivity(activity.CreateVSASVM, pool, vlmConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not enough nodes in the cluster")
	mockStorage.AssertExpectations(t)
}

func Test_CreateVSASVM_FailsToCreateLif(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Deployment: vlmconfig.DeploymentConfig{DeploymentID: "test-deployment"},
		Svm: map[string]vlmconfig.SvmConfig{
			"test-deployment-datasvm-gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlmconfig.VSALIFType][]vlmconfig.LIFConfig{
					vlmconfig.LIFTypeIscsi: {
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
				},
			},
		},
	}

	mockVlmClient.On("VSASVMCreate", mock.Anything, mock.Anything).Return(nil)
	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}}, {BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)
	mockStorage.On("CreateLif", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create LIF"))

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	_, err := env.ExecuteActivity(activity.CreateVSASVM, pool, vlmConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create LIF")
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSACluster_Success(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	getPasswordForVSACluster := activities.GetPasswordForVSACluster
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient

	defer func() {
		activities.GetPasswordForVSACluster = getPasswordForVSACluster
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}
	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeployCreate", ctx, cfg).Return(nil)
	assert.NotNil(t, cfg)

	_, err := activity.CreateVSACluster(ctx, cfg)

	assert.NoError(t, err)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSACluster_FailsToDeployCluster(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	getPasswordForVSACluster := activities.GetPasswordForVSACluster
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetPasswordForVSACluster = getPasswordForVSACluster
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}
	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeployCreate", ctx, cfg).Return(errors.New("failed to deploy VSA cluster"))
	assert.NotNil(t, cfg)

	_, err := activity.CreateVSACluster(ctx, cfg)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to deploy VSA cluster")
	mockVlmClient.AssertExpectations(t)
}

func Test_SaveNodeDetails_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vmConfig := vlmconfig.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
			vlmconfig.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlmconfig.DeploymentConfig{
		OntapCredentials: vlmconfig.OntapCredentials{
			Username: "admin",
			Password: "password",
		},
		VSAInstanceType: "n1-standard-4",
	}
	vasNode := &vsa.Node{}

	mockProvider.On("GetNodeByName", mock.Anything).Return(vasNode, nil)
	mockStorage.On("CreateNode", ctx, mock.Anything).Return(&datamodel.Node{}, nil)

	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool)

	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "test-node", node.Name)
	mockStorage.AssertExpectations(t)
}

func Test_SaveNodeDetails_FailsToCreateNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vmConfig := vlmconfig.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
			vlmconfig.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlmconfig.DeploymentConfig{
		OntapCredentials: vlmconfig.OntapCredentials{
			Username: "admin",
			Password: "password",
		},
		VSAInstanceType: "n1-standard-4",
	}
	vasNode := &vsa.Node{}

	mockProvider.On("GetNodeByName", mock.Anything).Return(vasNode, nil)
	mockStorage.On("CreateNode", ctx, mock.Anything).Return(nil, errors.New("failed to create node"))

	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "failed to create node")
	mockStorage.AssertExpectations(t)
}

func Test_SaveNodeDetails_FailsToFetchNodeByName(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vmConfig := vlmconfig.VMConfig{
		HostName: "test-node",
		SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
			vlmconfig.LIFTypeNodeMgmt: {IP: "192.168.1.1"},
		},
		Zone: "test-zone",
	}
	deploymentConfig := vlmconfig.DeploymentConfig{
		OntapCredentials: vlmconfig.OntapCredentials{
			Username: "admin",
			Password: "password",
		},
		VSAInstanceType: "n1-standard-4",
	}

	mockProvider.On("GetNodeByName", mock.Anything).Return(nil, errors.New("failed to fetch node"))
	node, err := activities.SaveNodeDetails(ctx, mockStorage, vmConfig, deploymentConfig, pool)

	assert.Error(t, err)
	assert.Nil(t, node)
	mockStorage.AssertExpectations(t)
}

func Test_SaveVSANodeDetails_NoClusterDetailsProvided(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Cloud: vlmconfig.CloudConfig{HAPairs: []vlmconfig.HAPair{}},
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "no cluster details provided")
}

func Test_SaveVSANodeDetails_NoHAPairsProvided(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Cloud: vlmconfig.CloudConfig{HAPairs: []vlmconfig.HAPair{}},
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "no cluster details provided")
}

func Test_SaveVSANodeDetails_FailsToSaveFirstNode(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Cloud: vlmconfig.CloudConfig{
			HAPairs: []vlmconfig.HAPair{
				{
					VM1: vlmconfig.VMConfig{HostName: "node1"},
					VM2: vlmconfig.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlmconfig.VMConfig, deploymentConfig vlmconfig.DeploymentConfig, pool *datamodel.Pool) (*datamodel.Node, error) {
		if vmConfig.HostName == "node1" {
			return nil, errors.New("failed to save node1")
		}
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig)

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
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Cloud: vlmconfig.CloudConfig{
			HAPairs: []vlmconfig.HAPair{
				{
					VM1: vlmconfig.VMConfig{HostName: "node1"},
					VM2: vlmconfig.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlmconfig.VMConfig, deploymentConfig vlmconfig.DeploymentConfig, pool *datamodel.Pool) (*datamodel.Node, error) {
		if vmConfig.HostName == "node2" {
			return nil, errors.New("failed to save node2")
		}
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig)

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
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlmconfig.VLMConfig{
		Cloud: vlmconfig.CloudConfig{
			HAPairs: []vlmconfig.HAPair{
				{
					VM1: vlmconfig.VMConfig{HostName: "node1"},
					VM2: vlmconfig.VMConfig{HostName: "node2"},
				},
			},
		},
	}

	activities.SaveNodeDetails = func(ctx context.Context, se database.Storage, vmConfig vlmconfig.VMConfig, deploymentConfig vlmconfig.DeploymentConfig, pool *datamodel.Pool) (*datamodel.Node, error) {
		return &datamodel.Node{Name: vmConfig.HostName}, nil
	}

	node, err := activity.SaveVSANodeDetails(ctx, pool, vlmConfig)

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
	assert.Contains(t, err.Error(), "failed to delete SVM record")
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

func Test_DeleteLIFsDeletesAllLIFsSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}
	lifs := []*datamodel.Lif{
		{Name: "lif1"},
		{Name: "lif2"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("GetLifByNodeID", ctx, nodes[0].ID, nodes[0].AccountID).Return(lifs[0], nil)
	mockStorage.On("GetLifByNodeID", ctx, nodes[1].ID, nodes[1].AccountID).Return(lifs[1], nil)
	mockStorage.On("DeleteLif", ctx, lifs[0]).Return(nil)
	mockStorage.On("DeleteLif", ctx, lifs[1]).Return(nil)

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteLIFsReturnsErrorWhenNodesNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("nodes not found"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to retrieve nodes for pool")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteLIFsSkipsDeletedLIFs(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}
	lif := &datamodel.Lif{BaseModel: datamodel.BaseModel{DeletedAt: &gorm.DeletedAt{Valid: true}}, Name: "lif1"}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("GetLifByNodeID", ctx, nodes[0].ID, nodes[0].AccountID).Return(lif, nil)

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteLIFsReturnsErrorWhenLIFRetrievalFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("GetLifByNodeID", ctx, nodes[0].ID, nodes[0].AccountID).Return(nil, errors.New("failed to retrieve LIF"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to retrieve LIFs for Node")
	mockStorage.AssertExpectations(t)
}

func Test_DeleteLIFsReturnsErrorWhenLIFDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}
	lif := &datamodel.Lif{Name: "lif1"}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("GetLifByNodeID", ctx, nodes[0].ID, nodes[0].AccountID).Return(lif, nil)
	mockStorage.On("DeleteLif", ctx, lif).Return(errors.New("failed to delete LIF"))

	err := activities.DeleteLIFs(ctx, mockStorage, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete LIF record")
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
	assert.Contains(t, err.Error(), "SVM not found")
	mockStorage.AssertExpectations(t)
}

func ReturnsErrorWhenErroredSVMFails(t *testing.T) {
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
	assert.Contains(t, err.Error(), "failed to retrieve nodes for pool")
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
	assert.Contains(t, err.Error(), "failed to retrieve nodes for pool")
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
	assert.Contains(t, err.Error(), "failed to update node record to deleting")
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
	assert.Contains(t, err.Error(), "SVM not found")
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
	assert.Contains(t, err.Error(), "failed to update SVM record to deleting svm1")
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
	assert.Contains(t, err.Error(), "failed to retrieve nodes for pool")
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
	assert.Contains(t, err.Error(), "failed to delete node record")
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

func Test_ReturnsErrorWhenClusterDetailsAreMissing(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: ""}}

	result, err := activity.DeleteVSADeployment(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "pool cannot be deleted with active clusters")
}

func Test_ReturnsErrorWhenVLMConfigPreparationFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	getPasswordForVSACluster := activities.GetPasswordForVSACluster
	prepareVLMConfig := activities.PrepareVlmConfig
	defer func() {
		activities.GetPasswordForVSACluster = getPasswordForVSACluster
		activities.PrepareVlmConfig = prepareVLMConfig
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"},
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 64, Iops: 1000, PrimaryZone: "zone1"}}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config")
	}

	result, err := activity.DeleteVSADeployment(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenVSAClusterDeletionFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}
	getPasswordForVSACluster := activities.GetPasswordForVSACluster
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetPasswordForVSACluster = getPasswordForVSACluster
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"},
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 64, Iops: 1000, PrimaryZone: "zone1"}}

	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}
	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeploymentDelete", ctx, mock.Anything).Return(errors.New("failed to delete VSA cluster"))

	result, err := activity.DeleteVSADeployment(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to delete VSA cluster")
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_DeletesVSADeploymentSuccessfully(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{SE: mockStorage}
	getPasswordForVSACluster := activities.GetPasswordForVSACluster
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetPasswordForVSACluster = getPasswordForVSACluster
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"},
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 64, Iops: 1000, PrimaryZone: "zone1"}}

	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}
	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeploymentDelete", ctx, mock.Anything).Return(nil)

	result, err := activity.DeleteVSADeployment(ctx, pool)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_ReturnsErrorWhenListPoolsFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{AccountID: 1, Network: "test-network"}

	mockStorage.On("ListPools", ctx, mock.Anything).Return(nil, errors.New("failed to list pools"))

	err := activity.ReleaseSubnet(ctx, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list pools")
	mockStorage.AssertExpectations(t)
}

// Unit tests for ReleaseSubnet in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_ReleaseSubnetN(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := datamodel.Pool{
		AccountID: 1,
		Network:   "test-network",
		Account:   &datamodel.Account{Name: "test-account"},
	}
	poolView := &datamodel.PoolView{Pool: pool}

	pool2 := datamodel.Pool{
		AccountID: 1,
		Network:   "test-network-2",
		Account:   &datamodel.Account{Name: "test-account"},
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
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = GetGCPService
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
		GetGCPService := activities.GetGCPService
		GetSubnetForConsumerProjectAndRelease := activities.GetSubnetForConsumerProjectAndRelease
		defer func() {
			activities.GetGCPService = GetGCPService
			activities.GetSubnetForConsumerProjectAndRelease = GetSubnetForConsumerProjectAndRelease
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activities.GetSubnetForConsumerProjectAndRelease = func(gcpService hyperscaler.GoogleServices, consumerVpc, accountName, region, subnetworkName string, clusterDetails datamodel.ClusterDetails) error {
			return errors.New("release subnet error")
		}
		defer func() {}()
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "release subnet error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("releasesSubnet", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(t)
		GetGCPService := activities.GetGCPService
		GetSubnetForConsumerProjectAndRelease := activities.GetSubnetForConsumerProjectAndRelease
		defer func() {
			activities.GetGCPService = GetGCPService
			activities.GetSubnetForConsumerProjectAndRelease = GetSubnetForConsumerProjectAndRelease
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activities.GetSubnetForConsumerProjectAndRelease = func(gcpService hyperscaler.GoogleServices, consumerVpc, accountName, region, subnetworkName string, clusterDetails datamodel.ClusterDetails) error {
			return nil
		}
		activity := activities.PoolActivity{SE: mockStorage}
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

// Unit tests for _getSubnetForConsumerProjectAndRelease
func Test_getSubnetForConsumerProjectAndRelease(t *testing.T) {
	consumerVpc := "test-vpc"
	accountName := "test-account"
	localRegion := "us-central1"
	subnetworkName := "vsa-us-central1"
	tenantProjectNumber := "tenant-project"
	snHost := "sn-host"
	clusterDetails := datamodel.ClusterDetails{
		ExternalName:          "test-cluster",
		SnHostProject:         snHost,
		RegionalTenantProject: tenantProjectNumber,
	}
	t.Run("getTenantProjectFails", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)

		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetTenantProject", consumerVpc, accountName, localRegion).Return("", errors.New("tenant error"))
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, datamodel.ClusterDetails{})
		assert.Error(tt, err)
		mockService.AssertExpectations(tt)
	})

	t.Run("getSnHostFails", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)

		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetTenantProject", consumerVpc, accountName, localRegion).Return(tenantProjectNumber, nil)
		mockService.On("GetSnHost", tenantProjectNumber).Return("", errors.New("sn host error"))
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, datamodel.ClusterDetails{})
		assert.Error(tt, err, errors.New("sn host error"))
		mockService.AssertExpectations(tt)
	})

	t.Run("GetSubnetworkFails", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)

		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetSubnetwork", snHost, localRegion, subnetworkName).Return(nil, errors.New("subnet not found"))
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, clusterDetails)
		assert.Error(tt, err, errors.New("subnet not found"))
		mockService.AssertExpectations(tt)
	})
	t.Run("GetSubnetworkFails", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)

		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetTenantProject", consumerVpc, accountName, localRegion).Return(tenantProjectNumber, nil)
		mockService.On("GetSnHost", tenantProjectNumber).Return(snHost, nil)
		mockService.On("GetSubnetwork", snHost, localRegion, subnetworkName).Return(nil, errors.New("subnet not found"))
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, datamodel.ClusterDetails{})
		assert.Error(tt, err, errors.New("subnet not found"))
		mockService.AssertExpectations(tt)
	})

	t.Run("releaseSubnetworkFails", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetTenantProject", consumerVpc, accountName, localRegion).Return(tenantProjectNumber, nil)
		mockService.On("GetSnHost", tenantProjectNumber).Return(snHost, nil)
		mockService.On("GetSubnetwork", snHost, localRegion, subnetworkName).Return(&models.Subnet{}, nil)
		mockService.On("ReleaseSubnetwork", localRegion, snHost, subnetworkName).Return(errors.New("release error"))
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, datamodel.ClusterDetails{})
		assert.Error(tt, err)
		mockService.AssertExpectations(tt)
	})

	t.Run("releasesSubnet successfully", func(tt *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		mockService.On("GetTenantProject", consumerVpc, accountName, localRegion).Return(tenantProjectNumber, nil)
		mockService.On("GetSnHost", tenantProjectNumber).Return(snHost, nil)
		mockService.On("GetSubnetwork", snHost, localRegion, subnetworkName).Return(&models.Subnet{}, nil)
		mockService.On("ReleaseSubnetwork", localRegion, snHost, subnetworkName).Return(nil)
		err := activities.GetSubnetForConsumerProjectAndRelease(mockService, consumerVpc, accountName, localRegion, subnetworkName, datamodel.ClusterDetails{})
		assert.NoError(tt, err)
		mockService.AssertExpectations(tt)
	})
}

func Test_SkipsSubnetReleaseWhenMultiplePoolsExist(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{AccountID: 1, Network: "test-network"}
	pools := []*datamodel.PoolView{{}, {}}

	mockStorage.On("ListPools", ctx, mock.Anything).Return(pools, nil)

	err := activity.ReleaseSubnet(ctx, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
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
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(&models.Firewall{}, nil)

		err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetFirewallFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, errors.New(errString))

		err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), fmt.Sprintf("Error getting subnet for project: %s, vpc name: %s, firewall name: %s. Error : %s", projectName, vpcName, firewallName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenInsertFirewallFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		errString := "failed to insert firewall"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, nil)
		mgs.On("InsertFirewall", &models.Firewall{
			ProjectName:      projectName,
			Name:             firewallName,
			VPCNetworkName:   vpcName,
			Priority:         priority,
			Direction:        direction,
			SourceRanges:     firewallSourceRanges,
			AllowedPortRules: firewallAllowedPortRules,
		}).Return(errors.New(errString))

		err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.EqualError(tt, err, errString)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenInsertFirewallSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, nil)
		mgs.On("InsertFirewall", mock.Anything).Return(nil)

		err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
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
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(&models.VPCNetwork{}, nil)

		err := activities.CreateVPC(mgs, projectName, vpcName)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))

		err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), fmt.Sprintf("Error getting vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkFailsWithNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "not found"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))
		mgs.On("CreateVPC", &models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(nil)

		err := activities.CreateVPC(mgs, projectName, vpcName)
		assert.Nil(tt, err)
		mgs.AssertExpectations(tt)
	})
	t.Run("WhenCreateVPCFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "failed to create VPC"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, nil)
		mgs.On("CreateVPC", &models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(errors.New(errString))

		err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), fmt.Sprintf("Error creating vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetVPCNetworkAfterCreationFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		errString := "failed to get VPC after creation"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, nil).Once()
		mgs.On("CreateVPC", &models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(errors.New(errString))

		err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), fmt.Sprintf("Error creating vpc for project: %s and vpc name: %s. Error : %s", projectName, vpcName, errString))
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateVPCSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		CreateVPC := activities.CreateVPC
		defer func() {
			activities.CreateVPC = CreateVPC
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, nil).Once()
		mgs.On("CreateVPC", &models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(nil)

		err := activities.CreateVPC(mgs, projectName, vpcName)
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
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(&models.Subnet{}, nil)

		err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetSubnetworkFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		errString := "unexpected error"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, errors.New(errString))

		err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.EqualError(tt, customErr.Unwrap(), "Error getting subnet for project: test-project, vpc name: test-vpc, subnet name: test-subnet. Error : "+errString)
		} else {
			tt.Fatalf("Expected a CustomError, got: %T", err)
		}
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateSubnetworkFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		errString := "failed to create subnetwork"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, nil)
		mgs.On("CreateSubnetwork", mock.Anything).Return(errors.New(errString))

		err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.EqualError(tt, err, errString)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenCreateSubnetworkSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)

		InsertSubnet := activities.InsertSubnet
		defer func() {
			activities.InsertSubnet = InsertSubnet
		}()
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(nil, nil)
		mgs.On("CreateSubnetwork", mock.Anything).Return(nil)

		err := activities.InsertSubnet(mgs, projectName, &region, subnetName, vpcName, ipCidrRange)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})
}

// Unit test for _newGcpServices in core/orchestrator/activities/pool_activities_test.go
func Test_newGcpServices_ReturnsInitializedGcpServices(t *testing.T) {
	ctx := context.TODO()
	services := activities.NewGcpServices(ctx)

	assert.NotNil(t, services)
	assert.Equal(t, ctx, services.Ctx)
	assert.NotNil(t, services.Logger)
	assert.NotNil(t, services.Retry)
}

// Unit test for setupNetworkFirewallsForIscsi in core/orchestrator/activities/pool_activities_test.go
func Test_setupNetworkFirewallsForIscsi(t *testing.T) {
	mockService := new(hyperscaler.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	network := "test-network"

	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	// Success case
	t.Run("WhenSetupNetworkFirewallsForIscsiSucceeds", func(tt *testing.T) {
		mockService.On("GetLogger").Return(util.GetLogger(context.TODO()))
		activities.InsertFirewall = func(service hyperscaler.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) error {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, "data-iscsi-ingress", name)
			assert.Equal(t, network, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "3260"}, allowedPorts)
			return nil
		}
		err := activities.SetupNetworkFirewallsForIscsi(mockService, snHostProject, network)
		assert.NoError(t, err)
	})
	t.Run("WhenSetupNetworkFirewallsForIscsiFails", func(tt *testing.T) {
		activities.InsertFirewall = func(service hyperscaler.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) error {
			return errors.New("firewall error")
		}
		err := activities.SetupNetworkFirewallsForIscsi(mockService, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "firewall error")
	})
}

// Unit test for SetupNetwork in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_SetupNetwork(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(activity.SetupNetwork)

	region := "us-central1"
	project := "test-project"
	snHostProject := "test-sn-host-project"
	network := "test-network"
	t.Run("WhenSetupNetworkSucceeds", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
		vpcCreate := activities.CreateVPC
		subnetCreate := activities.InsertSubnet
		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
			activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
			activities.CreateVPC = vpcCreate
			activities.InsertSubnet = subnetCreate
			activities.InsertFirewall = InsertFirewall
		}()

		mockService := new(google.GcpServices)
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return nil
		}
		activities.InsertSubnet = func(service hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
			return nil
		}
		activities.InsertFirewall = func(service hyperscaler.GoogleServices, projectName, firewallName, vpcName string, priority int64, trafficDirection string, firewallSourceRanges, firewallAllowedPortRules []string) error {
			return nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler.GoogleServices, snHostProject, network string) error {
			return nil
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.NoError(t, err)
	})
	t.Run("WhenSetupNetwork_CreateVPCFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
		vpcCreate := activities.CreateVPC
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
			activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
			activities.CreateVPC = vpcCreate
		}()

		mockService := new(google.GcpServices)
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return errors.New("failed to create VPC")
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
	})
	t.Run("WhenSetupNetwork_InsertSubnetFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
		vpcCreate := activities.CreateVPC
		subnetCreate := activities.InsertSubnet
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
			activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
			activities.CreateVPC = vpcCreate
			activities.InsertSubnet = subnetCreate
		}()

		mockService := new(google.GcpServices)
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return nil
		}
		activities.InsertSubnet = func(service hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
			return errors.New("failed to insert subnet")
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
	})
	t.Run("WhenSetupNetwork_InsertFirewallFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
		vpcCreate := activities.CreateVPC
		subnetCreate := activities.InsertSubnet
		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
			activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
			activities.CreateVPC = vpcCreate
			activities.InsertSubnet = subnetCreate
			activities.InsertFirewall = InsertFirewall
		}()

		mockService := new(google.GcpServices)
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}

		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return nil
		}
		activities.InsertSubnet = func(service hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
			return nil
		}
		activities.InsertFirewall = func(service hyperscaler.GoogleServices, projectName, firewallName, vpcName string, priority int64, trafficDirection string, firewallSourceRanges, firewallAllowedPortRules []string) error {
			return errors.New("failed to insert firewall")
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
	})
	t.Run("WhenGetGCPServiceFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = originalGetGCPService
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get GCP service")
	})
	t.Run("WhenSetupNetworkWithFirewallFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
		}()

		mockService := new(google.GcpServices)
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}
		callCount := 0
		activities.SetupNetworkWithFirewall = func(ctx context.Context, project, vpcName string, region *string, subnetName, ipCidrRange string, firewallPriority int64, direction string, sourceRanges, allowedPortRules []string) error {
			callCount++
			if callCount == 2 {
				return errors.New("failed to setup network with firewall")
			}
			return nil
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup network with firewall")
	})
	t.Run("WhenSetupNetworkFirewallsForIscsiFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalSetupNetworkWithFirewall := activities.SetupNetworkWithFirewall
		originalSetupNetworkFirewallsForIscsi := activities.SetupNetworkFirewallsForIscsi
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.SetupNetworkWithFirewall = originalSetupNetworkWithFirewall
			activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
		}()
		mockService := new(google.GcpServices)

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockService, nil
		}
		activities.SetupNetworkWithFirewall = func(ctx context.Context, project, vpcName string, region *string, subnetName, ipCidrRange string, firewallPriority int64, direction string, sourceRanges, allowedPortRules []string) error {
			return nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler.GoogleServices, snHostProject, network string) error {
			return errors.New("failed to setup iscsi firewall")
		}
		_, err := env.ExecuteActivity(activity.SetupNetwork, region, project, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup iscsi firewall")
	})
}

func Test_generateAndCreateCertificateForVSACluster(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 3072)
	secretValue := commonparams.ConvertPrivateKeyToString(key, activities.RsaKeyType)
	param := &models.CustomCertificate{
		CertificateID:    "certid",
		CaName:           "ca",
		CertOwningEntity: "pid",
		Region:           "us",
		CaGroupName:      "pool",
		PemCsr:           "-----BEGIN CERTIFICATE REQUEST-----\nY3Ny\n-----END CERTIFICATE REQUEST-----\n",
	}
	reqParam := &models.CustomCertificateParam{
		CertificateID:    "certid",
		CaName:           "ca",
		CertOwningEntity: "pid",
		Region:           "us",
		CaPoolName:       "pool",
		CommonName:       "test-cn",
		Domains:          []string{"*.test.com"},
	}

	t.Run("success", func(tt *testing.T) {
		gService := hyperscaler.NewMockGoogleServices(tt)
		generateCSR := activities.GenerateCSR
		defer func() {
			activities.GenerateCSR = generateCSR
		}()

		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}

		gService.On("GetLogger").Return(log.NewLogger())
		gService.On("CreateCertificate", param).Return(param, nil)
		gService.On("CreateSecret", param.CertOwningEntity, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomSecret{Name: "pid"}, nil)

		certificate, secret, err := activities.GenerateAndCreateCertificateForVSACluster(gService, reqParam)

		assert.NoError(t, err)
		assert.NotNil(t, certificate)
		assert.NotNil(t, secret)
		gService.AssertNumberOfCalls(t, "CreateCertificate", 1)
		gService.AssertNumberOfCalls(t, "CreateSecret", 1)
		gService.AssertExpectations(tt)
	})
	t.Run("CreateCertificate fails", func(tt *testing.T) {
		gService := hyperscaler.NewMockGoogleServices(tt)
		generateCSR := activities.GenerateCSR
		defer func() {
			activities.GenerateCSR = generateCSR
		}()

		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}

		gService.On("GetLogger").Return(log.NewLogger())
		gService.On("CreateCertificate", param).Return(nil, fmt.Errorf("cert error"))
		certificate, secret, err := activities.GenerateAndCreateCertificateForVSACluster(gService, reqParam)
		assert.Nil(t, certificate)
		assert.Nil(t, secret)
		assert.EqualError(t, err, "cert error")
		gService.AssertExpectations(tt)
	})
	t.Run("CreateSecret fails and revoke fails", func(tt *testing.T) {
		gService := hyperscaler.NewMockGoogleServices(tt)
		generateCSR := activities.GenerateCSR
		defer func() {
			activities.GenerateCSR = generateCSR
		}()

		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}

		gService.On("GetLogger").Return(log.NewLogger())
		gService.On("CreateCertificate", param).Return(param, nil)
		gService.On("CreateSecret", param.CertOwningEntity, mock.Anything, mock.Anything, secretValue).Return(nil, errors.New("secret error"))
		gService.On("RevokeCertificate", param).Return("", errors.New("revoke error"))
		certificate, secret, err := activities.GenerateAndCreateCertificateForVSACluster(gService, reqParam)
		assert.EqualError(t, err, "revoke error")
		assert.Nil(t, certificate)
		assert.Nil(t, secret)
		gService.AssertExpectations(tt)
	})

	t.Run("CreateSecret fails and revoke succeeds", func(tt *testing.T) {
		gService := hyperscaler.NewMockGoogleServices(tt)
		generateCSR := activities.GenerateCSR
		defer func() {
			activities.GenerateCSR = generateCSR
		}()

		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), key, nil
		}
		gService.On("GetLogger").Return(log.NewLogger())
		gService.On("CreateCertificate", param).Return(param, nil)
		gService.On("CreateSecret", param.CertOwningEntity, mock.Anything, mock.Anything, secretValue).Return(nil, errors.New("secret error"))
		gService.On("RevokeCertificate", param).Return(param.CertificateID, nil)
		certificate, secret, err := activities.GenerateAndCreateCertificateForVSACluster(gService, reqParam)
		assert.EqualError(t, err, "secret error")
		assert.Nil(t, certificate)
		assert.Nil(t, secret)
		gService.AssertExpectations(tt)
	})
}

func TestPoolActivity_CreateCertificate(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	region := "us-central1"
	clusterName := "test-cluster"
	mockStorage := database.NewMockStorage(t)
	pa := &activities.PoolActivity{SE: mockStorage}
	t.Run("Success", func(tt *testing.T) {
		getGCPService := activities.GetGCPService
		generateAndCreateCertificateForVSACluster := activities.GenerateAndCreateCertificateForVSACluster

		defer func() {
			activities.GetGCPService = getGCPService
			activities.GenerateAndCreateCertificateForVSACluster = generateAndCreateCertificateForVSACluster
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, param *models.CustomCertificateParam) (*models.CustomCertificate, *models.CustomSecret, error) {
			return &models.CustomCertificate{}, &models.CustomSecret{}, nil
		}
		err := pa.CreateCertificate(ctx, region, clusterName)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetGCPService fails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = originalGetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		err := pa.CreateCertificate(ctx, region, clusterName)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})
	t.Run("GenerateAndCreateCertificateForVSACluster Fails", func(tt *testing.T) {
		getGCPService := activities.GetGCPService
		generateAndCreateCertificateForVSACluster := activities.GenerateAndCreateCertificateForVSACluster

		defer func() {
			activities.GetGCPService = getGCPService
			activities.GenerateAndCreateCertificateForVSACluster = generateAndCreateCertificateForVSACluster
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, param *models.CustomCertificateParam) (*models.CustomCertificate, *models.CustomSecret, error) {
			return nil, nil, errors.New("certificate error")
		}
		err := pa.CreateCertificate(ctx, region, clusterName)
		assert.EqualError(t, err, "certificate error")
		mockStorage.AssertExpectations(t)
	})
}

func Test_GeneratePasswordForVSACluster(t *testing.T) {
	projectId := "test-project"
	userName := "test-user"
	region := "test-region"

	t.Run("PasswordGenerationFails", func(tt *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "", errors.New("password generation failed")
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()

		mockGCPService.On("GetLogger").Return(log.NewLogger())
		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.Error(tt, err)
		assert.Nil(tt, secret)
		assert.Contains(tt, err.Error(), "password generation failed")
	})

	t.Run("SecretCreationFails", func(tt *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		originalGeneratePassword := utils.GenerateStrongPassword
		utils.GenerateStrongPassword = func(length int) (string, error) {
			return "xyzpassword", nil
		}
		defer func() { utils.GenerateStrongPassword = originalGeneratePassword }()

		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret get error"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret creation failed"))

		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.Error(tt, err)
		assert.Nil(tt, secret)
		assert.Contains(tt, err.Error(), "secret creation failed")
		mockGCPService.AssertExpectations(tt)
	})
	t.Run("GetSecretWithLatestVersion success", func(tt *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&models.CustomSecret{}, nil)

		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})
	t.Run("Success", func(tt *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret get error"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "secretID"}}, nil)
		defer func() {
			commonparams.RemoveFromUserAuthCache("secretID")
		}()

		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})
}

func TestPoolActivity_CreateSecret(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	secretID := "test-secret"
	region := "test-region"
	mockStorage := database.NewMockStorage(t)
	pa := &activities.PoolActivity{SE: mockStorage}

	t.Run("Success", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalGeneratePassword := activities.GeneratePasswordForVSACluster
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.GeneratePasswordForVSACluster = originalGeneratePassword
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectId, region, secretId string) (*models.CustomSecret, error) {
			return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: secretID}}, nil
		}
		_, err := pa.CreateSecret(ctx, region, secretID)
		assert.NoError(tt, err)
	})

	t.Run("GetGCPService fails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = originalGetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := pa.CreateSecret(ctx, region, secretID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})

	t.Run("GeneratePasswordForVSACluster fails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalGeneratePassword := activities.GeneratePasswordForVSACluster
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.GeneratePasswordForVSACluster = originalGeneratePassword
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectId, region, secretId string) (*models.CustomSecret, error) {
			return nil, errors.New("password error")
		}
		_, err := pa.CreateSecret(ctx, region, secretID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "password error")
	})
}

func Test_CreateGCPBucket_Success(t *testing.T) {
	mockGcp := hyperscaler.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.EXPECT().InitializeClients().Return(nil)

	// Create a bucket in the project if it doesn't exist
	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(nil)
	err := activities.CreateGCPBucket(ctx, projectId, bucketName, region, mockGcp)
	assert.NoError(t, err)
}

func Test_CreateGCPBucket_Failure(t *testing.T) {
	mockGcp := hyperscaler.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.EXPECT().InitializeClients().Return(nil)

	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(errors.New("failed to create bucket"))
	err := activities.CreateGCPBucket(ctx, projectId, bucketName, region, mockGcp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create bucket")
}

func Test_EnableAutoTiering_Failure(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	bucketName := "region-poolId"
	projectId := "test-project"

	// Save original and mock _createGCPBucket
	origCreateGCPBucket := activities.CreateGCPBucket
	defer func() { activities.CreateGCPBucket = origCreateGCPBucket }()
	activities.CreateGCPBucket = func(ctx context.Context, projectId, poolName, region string, gcpService hyperscaler.GoogleServices) error {
		return errors.New("Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
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
	defer func() { activities.CreateServiceAccountAndAttachRole = origCreateServiceAccountAndAttachRole }()

	t.Run("success", func(t *testing.T) {
		expectedSA := &iam.ServiceAccount{Name: "projects/test-project/serviceAccounts/test-sa"}
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
			return expectedSA, nil
		}

		sa, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("error", func(t *testing.T) {
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
			return nil, errors.New("Mock error: failed to create service account")
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
	expectedSA := &iam.ServiceAccount{Email: saEmail}
	roles := []string{"roles/storage.objectUser"}

	t.Run("success", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		createReq := &iam.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(nil)

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("create service account fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		createReq := &iam.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(nil, errors.New("create error"))

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "create error")
	})

	t.Run("attach roles fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		createReq := &iam.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &iam.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(errors.New("attach error"))

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.Error(t, err)
		assert.Nil(t, sa)
		assert.Contains(t, err.Error(), "attach error")
	})
}

func TestPoolActivity_DeleteAutoTierBucket(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	bucketName := "us-central1-test-pool"

	// Save and mock DeleteGCPBucket
	origDeleteGCPBucket := activities.DeleteGCPBucket
	defer func() { activities.DeleteGCPBucket = origDeleteGCPBucket }()

	t.Run("success", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
			return nil
		}
		err := activity.DeleteAutoTierBucket(ctx, bucketName)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
			return errors.New("delete failed")
		}
		err := activity.DeleteAutoTierBucket(ctx, bucketName)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})
}

func Test_deleteGCPBucket(t *testing.T) {
	ctx := context.Background()
	poolId := "test-pool"
	region := "us-central1"
	bucketName := fmt.Sprintf("%s-%s", region, poolId)

	t.Run("Success", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().DeleteBucket(ctx, bucketName).Return(nil)
		err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().DeleteBucket(ctx, bucketName).Return(errors.New("delete failed"))
		err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})

	t.Run("InitClients fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(errors.New("init error"))
		err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "init error")
	})
}

func Test_deleteServiceAccount(t *testing.T) {
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)

	t.Run("success", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().DeleteServiceAccount(saEmail).Return(nil)
		err := activities.DeleteSrvcAccount(ctx, projectID, saAccountID, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("delete fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(nil)
		mockGcp.EXPECT().DeleteServiceAccount(saEmail).Return(errors.New("delete failed"))
		err := activities.DeleteSrvcAccount(ctx, projectID, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})

	t.Run("init fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().InitializeClients().Return(errors.New("init error"))
		err := activities.DeleteSrvcAccount(ctx, projectID, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "init error")
	})
}

func TestPoolActivity_DeleteServiceAccount(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"

	origDeleteSrvcAccount := activities.DeleteSrvcAccount
	defer func() { activities.DeleteSrvcAccount = origDeleteSrvcAccount }()

	t.Run("success", func(t *testing.T) {
		activities.DeleteSrvcAccount = func(ctx context.Context, projectID, saAccountID string, gcpService hyperscaler.GoogleServices) error {
			return nil
		}
		err := activity.DeleteServiceAccount(ctx, projectID, saAccountID)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteSrvcAccount = func(ctx context.Context, projectID, saAccountID string, gcpService hyperscaler.GoogleServices) error {
			return errors.New("delete error")
		}
		err := activity.DeleteServiceAccount(ctx, projectID, saAccountID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})
}

func TestGenerateCSR(t *testing.T) {
	commonName := "test.example.com"
	domains := []string{"test.example.com", "www.test.example.com"}
	csrDER, key, err := activities.GenerateCSR(commonName, domains)
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

func Test_getPasswordFromCacheOrSecretManager(t *testing.T) {
	ctx := context.Background()
	secretID := "test-secret"

	t.Run("PasswordExistsInCache", func(tt *testing.T) {
		commonparams.AddToUserAuthCache(secretID, "cached-password")
		getPasswordForVSACluster := activities.GetPasswordForVSACluster
		defer func() {
			activities.GetPasswordForVSACluster = getPasswordForVSACluster
			commonparams.RemoveFromUserAuthCache(secretID)
		}()
		activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "cached-password"}}, nil
		}
		password := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "cached-password", password)
	})

	t.Run("PasswordNotInCacheAndSecretManagerSucceeds", func(tt *testing.T) {
		getPasswordForVSACluster := activities.GetPasswordForVSACluster
		defer func() {
			activities.GetPasswordForVSACluster = getPasswordForVSACluster
			commonparams.RemoveFromUserAuthCache(secretID)
		}()
		activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "secret-manager-password"}}, nil
		}
		password := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "secret-manager-password", password)
	})

	t.Run("PasswordNotInCacheAndSecretManagerFails", func(tt *testing.T) {
		getPasswordForVSACluster := activities.GetPasswordForVSACluster
		defer func() {
			activities.GetPasswordForVSACluster = getPasswordForVSACluster
			commonparams.RemoveFromUserAuthCache(secretID)
		}()
		activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, secretID string) (*models.CustomSecret, error) {
			return nil, assert.AnError
		}
		password := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "", password)
	})
}

func Test_IdentifyVMs_SuccessfullyPreparesConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
	}()
	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		// return errors.New("failed to prepare VLM config")
		return nil
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", "password", "test-tenant-project@xyz.com", "test-tenant-project")

	assert.NoError(t, err)
}

func Test_IdentifyVMs_FailsToPrepareConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
	}()
	activities.GetPasswordForVSACluster = func(ctx context.Context, projectId, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, password string, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config")
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", "password", "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", "password", "test-tenant-project@xyz.com", "test-tenant-project")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load VMRS config from file")
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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", "password", "test-tenant-project@xyz.com", "test-tenant-project")
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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", "password", "test-tenant-project@xyz.com", "test-tenant-project")
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
	assert.Contains(t, err.Error(), "failed to retrieve nodes")
	mockStorage.AssertExpectations(t)
}

func Test_deleteVSAClusterPassword(t *testing.T) {
	projectId := "test-project"
	secretID := "test-secret"

	t.Run("DeleteSecret called when GetSecretWithLatestVersion passes", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", projectId, secretID).Return(nil, nil)
		mockGCP.On("DeleteSecret", projectId, secretID).Return(nil)

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, projectId, secretID)
		assert.NoError(t, err)
		mockGCP.AssertCalled(t, "DeleteSecret", projectId, secretID)
	})

	t.Run("DeleteSecret returns error", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete error"))

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, projectId, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("Delete Secret fails if GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", projectId, secretID).Return(&models.CustomSecret{}, fmt.Errorf("get secret error"))

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, projectId, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "get secret error")
		mockGCP.AssertNotCalled(t, "DeleteSecret", projectId, secretID)
	})
}

// Unit test for DeleteSecret in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_DeleteSecret(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	secretID := "test-secret"
	mockStorage := database.NewMockStorage(t)
	pa := &activities.PoolActivity{SE: mockStorage}

	origGetGCPService := activities.GetGCPService
	origDeleteVSAClusterPassword := activities.DeletePasswordFromCacheAndSecretManager
	defer func() {
		activities.GetGCPService = origGetGCPService
		activities.DeletePasswordFromCacheAndSecretManager = origDeleteVSAClusterPassword
	}()

	t.Run("Success", func(tt *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, projectId, secretID string) error {
			return nil
		}
		err := pa.DeleteSecret(ctx, secretID)
		assert.NoError(tt, err)
	})

	t.Run("GetGCPService fails", func(tt *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		err := pa.DeleteSecret(ctx, secretID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})

	t.Run("DeletePasswordFromCacheAndSecretManager fails", func(tt *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, projectId, secretID string) error {
			return errors.New("delete secret error")
		}
		err := pa.DeleteSecret(ctx, secretID)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "delete secret error")
	})
}
