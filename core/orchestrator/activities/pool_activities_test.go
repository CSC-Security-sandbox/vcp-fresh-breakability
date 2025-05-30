package activities_test

import (
	"context"
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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
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
	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("GetPool", ctx, pool.UUID, int64(0)).Return(pool, nil)

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

	t.Run("WhenGetTenantProjectFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return("", errors.New("Error finding tenancy unit"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetSubnetworkFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Error getting subnetwork"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenSubnetworkAlreadyExists", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(&models.Subnet{Name: "vsa-us-central1"}, nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.NoError(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenCreateSubnetworkForTenantProjectFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return(nil, errors.New("Error creating subnetwork"))

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenSubnetResponseConversionFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("Invalid Response"), nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenParseProjectIdFails", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Subnetwork not found"))
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Network\": \"host-network\"}"), nil)

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenGetSubnetworkFailsAfterCreatingTheSubnetwork", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Subnetwork not found")).Once()
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Name\": \"test-subnet\", \"Network\": \"projects/1234321/global/networks/host-network\"}"), nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, "test-subnet").Return(nil, errors.New("Error getting subnetwork")).Once()

		tenancyInfo, err := activities.FindTenancyAndGetSubnetwork(ctx, mgs, consumerVPC, customerProjectNumber, &tenantProjectRegion)
		assert.Error(tt, err)
		assert.Nil(tt, tenancyInfo)
	})
	t.Run("WhenFindTenancyAndGetSubnetworkSucceeds", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		mgs.On("GetTenantProject", consumerVPC, customerProjectNumber, tenantProjectRegion).Return(tenantProjectNumber, nil)
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetSubnetwork", tenantProjectNumber, tenantProjectRegion, "vsa-us-central1").Return(nil, errors.New("Subnetwork not found")).Once()
		mgs.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, tenantProjectRegion).Return([]byte("{\"Name\": \"test-subnet\", \"Network\": \"projects/1234321/global/networks/host-network\"}"), nil)
		mgs.On("GetSubnetwork", snHostProject, tenantProjectRegion, "test-subnet").Return(&models.Subnet{Name: "test-subnet", Network: "projects/1234321/global/networks/host-network", GatewayAddress: "10.0.0.3"}, nil).Once()

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
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
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
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
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
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project")
	assert.NoError(t, err)
	assert.Equal(t, "test-deployment", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
	assert.Equal(t, "test-zone", cfg.Deployment.Zone.Zone1)
	assert.Equal(t, "test-network", cfg.Deployment.NetConfig[vlmconfig.LIFTypeInterCluster].VPC)
	assert.Equal(t, "test-sn-host-project", cfg.Deployment.NetConfig[vlmconfig.LIFTypeInterCluster].GCPNetworkConfig.SubnetProjectID)
}

func Test_prepareVlmConfig_FileNotFound(t *testing.T) {
	cfg := &vlmconfig.VLMConfig{}
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project")
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
	err := activities.PrepareVlmConfig(cfg, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project")
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
	err := activities.PrepareVlmConfig(cfg, "", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project")
	assert.NoError(t, err)
	assert.Equal(t, "", cfg.Deployment.DeploymentID)
	assert.Equal(t, "test-region", cfg.Deployment.Region)
}

func Test_CreateVSASVM_Success(t *testing.T) {
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

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}}, {BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)
	mockStorage.On("CreateLif", ctx, mock.Anything).Return(&datamodel.Lif{}, nil)
	mockVlmClient.On("VSASVMCreate", ctx, mock.Anything).Return(nil)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

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

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

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

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

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

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_CreateVSASVM_NotEnoughNodes(t *testing.T) {
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
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not enough nodes in the cluster")
	mockStorage.AssertExpectations(t)
}

func Test_CreateVSASVM_FailsToCreateLif(t *testing.T) {
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

	mockVlmClient.On("VSASVMCreate", ctx, mock.Anything).Return(nil)
	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}}, {BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)
	mockStorage.On("CreateLif", ctx, mock.Anything).Return(nil, errors.New("failed to create LIF"))

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	err := activity.CreateVSASVM(ctx, pool, vlmConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create LIF")
	mockStorage.AssertExpectations(t)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSACluster_Success(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeployCreate", ctx, cfg).Return(nil)

	result, err := activity.CreateVSACluster(ctx, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project", 1024)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	mockVlmClient.AssertExpectations(t)
}

func Test_CreateVSACluster_FailsToPrepareConfig(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	cfg := &vlmconfig.VLMConfig{}
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
	}()

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
		return errors.New("failed to prepare VLM config")
	}

	mockVlmClient.On("VSAClusterDeployGet", ctx, cfg).Return(prepareVLMConfig, nil)

	result, err := activity.CreateVSACluster(ctx, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project", 1024)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_CreateVSACluster_FailsToDeployCluster(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}

	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
		return nil
	}
	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeployCreate", ctx, cfg).Return(errors.New("failed to deploy VSA cluster"))

	result, err := activity.CreateVSACluster(ctx, "test-deployment", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-sn-host-project", 1024)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to deploy VSA cluster")
	mockVlmClient.AssertExpectations(t)
}

func Test_SaveNodeDetails_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider) // Use the mock provider
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
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
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
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
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
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

func Test_CreateLifForSvm_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	activity := activities.PoolActivity{SE: mockStorage}
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	svm := &datamodel.Svm{Name: "test-svm"}
	cluster := []map[string]string{
		{"dataLif": "192.168.1.1/24"},
		{"dataLif": "192.168.1.2/24"},
	}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockProvider.On("CreateDataLIF", mock.Anything).Return(&vsa.Lif{
		Name:         "san_lif_node1",
		ExternalUUID: "uuid1",
		IPAddress:    "192.168.1.1",
		SubnetMask:   "255.255.255.0",
	}, nil).Once()
	mockProvider.On("CreateDataLIF", mock.Anything).Return(&vsa.Lif{
		Name:         "san_lif_node2",
		ExternalUUID: "uuid2",
		IPAddress:    "192.168.1.2",
		SubnetMask:   "255.255.255.0",
	}, nil).Once()
	mockStorage.On("CreateLif", ctx, mock.Anything).Return(&datamodel.Lif{}, nil)

	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
	}

	err := activity.CreateLifForSvm(ctx, &coremodel.Node{}, cluster, pool, svm)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func Test_CreateLifForSvm_NotEnoughNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	svm := &datamodel.Svm{Name: "test-svm"}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)

	err := activity.CreateLifForSvm(ctx, &coremodel.Node{}, nil, pool, svm)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not enough nodes in the cluster")
	mockStorage.AssertExpectations(t)
}

func Test_CreateLifForSvm_MissingDataLif(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	activity := activities.PoolActivity{SE: mockStorage}
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	svm := &datamodel.Svm{Name: "test-svm"}
	cluster := []map[string]string{
		{"dataLif": "192.168.1.1/24"},
		{},
	}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}
	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
	}

	mockProvider.On("CreateDataLIF", mock.Anything).Return(&vsa.Lif{
		Name:         "san_lif_node1",
		ExternalUUID: "uuid1",
		IPAddress:    "192.168.1.1",
		SubnetMask:   "255.255.255.0",
	}, nil).Once()
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockStorage.On("CreateLif", ctx, mock.Anything).Return(&datamodel.Lif{}, nil)

	err := activity.CreateLifForSvm(ctx, &coremodel.Node{}, cluster, pool, svm)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "missing dataLif in cluster details for node index")
	mockStorage.AssertExpectations(t)
}

func Test_CreateLifForSvm_FailsToCreateLif(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)
	activity := activities.PoolActivity{SE: mockStorage}
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	svm := &datamodel.Svm{Name: "test-svm"}
	cluster := []map[string]string{
		{"dataLif": "192.168.1.1/24"},
		{"dataLif": "192.168.1.2/24"},
	}
	nodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node1"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node2"},
	}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	mockProvider.On("CreateDataLIF", mock.Anything).Return(nil, errors.New("failed to create LIF")).Once()

	activities.GetProviderByNode = func(node *coremodel.Node) vsa.Provider {
		return mockProvider
	}

	err := activity.CreateLifForSvm(ctx, &coremodel.Node{}, cluster, pool, svm)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create LIF")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
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

func Test_ReturnsErrorWhenNodesRetrievalFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, errors.New("failed to retrieve nodes"))

	result, err := activity.DeleteVSADeployment(ctx, pool)

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to retrieve nodes")
	mockStorage.AssertExpectations(t)
}

func Test_ReturnsErrorWhenVLMConfigPreparationFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	prepareVLMConfig := activities.PrepareVlmConfig
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"}}
	nodes := []*datamodel.Node{{ZoneName: "zone1"}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
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
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"}}
	nodes := []*datamodel.Node{{ZoneName: "zone1"}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
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
	prepareVLMConfig := activities.PrepareVlmConfig
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{ClusterDetails: datamodel.ClusterDetails{ExternalName: "test-deployment"}}
	nodes := []*datamodel.Node{{ZoneName: "zone1"}}

	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nodes, nil)
	activities.PrepareVlmConfig = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone, network, subnet, projectId, snHostProject string) error {
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

func Test_SkipsSubnetReleaseWhenMultiplePoolsExist(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	pool := &datamodel.Pool{AccountID: 1, Network: "test-network"}
	pools := []*datamodel.Pool{{}, {}}

	mockStorage.On("ListPools", ctx, mock.Anything).Return(pools, nil)

	err := activity.ReleaseSubnet(ctx, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_setupNetworkWithFirewall(t *testing.T) {
	ctx := context.TODO()
	projectName := "test-project"
	vpcName := "test-vpc"
	region := "us-central1"
	subnetIpCidrRange := "10.0.0.0/16"
	firewallSourceRanges := []string{"0.0.0.0/0"}
	firewallAllowedPortRules := []string{"tcp", "udp"}
	subnetName := "test-subnet"
	firewallPriority := int64(1000)
	t.Run("WhenCreateVPCFails", func(tt *testing.T) {
		errString := "Failed to create VPC"
		err := errors.New(errString)

		vpcCreate := activities.CreateVPC
		setupNetwork := activities.SetupNetworkWithFirewall
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.CreateVPC = vpcCreate
			activities.SetupNetworkWithFirewall = setupNetwork
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return err
		}

		errResp := activities.SetupNetworkWithFirewall(ctx, projectName, vpcName, &region, subnetName, subnetIpCidrRange, firewallPriority, "INGRESS", firewallSourceRanges, firewallAllowedPortRules)
		assert.Equal(tt, errString, errResp.Error())
	})
	t.Run("WhenCreateSubnetFails", func(tt *testing.T) {
		errString := "Failed to create subnet"
		err := errors.New(errString)
		vpcCreate := activities.CreateVPC
		setupNetwork := activities.SetupNetworkWithFirewall
		subnetCreate := activities.InsertSubnet
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.CreateVPC = vpcCreate
			activities.SetupNetworkWithFirewall = setupNetwork
			activities.InsertSubnet = subnetCreate
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return nil
		}
		activities.InsertSubnet = func(service hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
			return err
		}
		errResp := activities.SetupNetworkWithFirewall(ctx, projectName, vpcName, &region, subnetName, subnetIpCidrRange, firewallPriority, "INGRESS", firewallSourceRanges, firewallAllowedPortRules)
		assert.Equal(tt, errString, errResp.Error())
	})
	t.Run("WhenInsertFirewallFails", func(tt *testing.T) {
		errString := "Failed to create firewall"
		err := errors.New(errString)

		vpcCreate := activities.CreateVPC
		setupNetwork := activities.SetupNetworkWithFirewall
		subnetCreate := activities.InsertSubnet
		InsertFirewall := activities.InsertFirewall
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.CreateVPC = vpcCreate
			activities.SetupNetworkWithFirewall = setupNetwork
			activities.InsertSubnet = subnetCreate
			activities.InsertFirewall = InsertFirewall
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.CreateVPC = func(service hyperscaler.GoogleServices, projectName, vpcName string) error {
			return nil
		}
		activities.InsertSubnet = func(service hyperscaler.GoogleServices, projectName string, region *string, subnetName string, vpcName string, ipCidrRange string) error {
			return nil
		}
		activities.InsertFirewall = func(service hyperscaler.GoogleServices, projectName, firewallName, vpcName string, priority int64, trafficDirection string, firewallSourceRanges, firewallAllowedPortRules []string) error {
			return err
		}
		errResp := activities.SetupNetworkWithFirewall(ctx, projectName, vpcName, &region, subnetName, subnetIpCidrRange, firewallPriority, "INGRESS", firewallSourceRanges, firewallAllowedPortRules)
		assert.Equal(tt, errString, errResp.Error())
	})
	t.Run("WhensetupNetworkWithFirewallSucceeds", func(tt *testing.T) {
		vpcCreate := activities.CreateVPC
		setupNetwork := activities.SetupNetworkWithFirewall
		subnetCreate := activities.InsertSubnet
		InsertFirewall := activities.InsertFirewall
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.CreateVPC = vpcCreate
			activities.SetupNetworkWithFirewall = setupNetwork
			activities.InsertSubnet = subnetCreate
			activities.InsertFirewall = InsertFirewall
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
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
		errResp := activities.SetupNetworkWithFirewall(ctx, projectName, vpcName, &region, subnetName, subnetIpCidrRange, firewallPriority, "INGRESS", firewallSourceRanges, firewallAllowedPortRules)
		assert.NoError(tt, errResp)
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

	ctx := context.TODO()
	region := "us-central1"
	project := "test-project"
	snHostProject := "test-sn-host-project"
	network := "test-network"
	t.Run("WhenSetupNetworkSucceeds", func(tt *testing.T) {
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

		// Success case
		activities.SetupNetworkWithFirewall = func(ctx context.Context, project, vpcName string, region *string, subnetName, ipCidrRange string, firewallPriority int64, direction string, sourceRanges, allowedPortRules []string) error {
			return nil
		}
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler.GoogleServices, snHostProject, network string) error {
			return nil
		}
		err := activity.SetupNetwork(ctx, region, project, snHostProject, network)
		assert.NoError(t, err)
	})
	t.Run("WhenGetGCPServiceFails", func(tt *testing.T) {
		originalGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = originalGetGCPService
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}
		err := activity.SetupNetwork(ctx, region, project, snHostProject, network)
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
		err := activity.SetupNetwork(ctx, region, project, snHostProject, network)
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
		err := activity.SetupNetwork(ctx, region, project, snHostProject, network)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup iscsi firewall")
	})
}

func Test_CreateGCPBucket_Success(t *testing.T) {
	mockGcp := hyperscaler.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	poolId := "f2d9e18b-6716-9170-de70-827f5f769907"

	mockGcp.EXPECT().InitializeClients().Return(nil)

	bucketName := fmt.Sprintf("%s-%s", region, poolId)

	// Create a bucket in the project if it doesn't exist
	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(nil)
	err := activities.CreateGCPBucket(ctx, projectId, poolId, region, mockGcp)
	assert.NoError(t, err)
}

func Test_CreateGCPBucket_Failure(t *testing.T) {
	mockGcp := hyperscaler.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	poolId := "f2d9e18b-6716-9170-de70-827f5f769907"

	mockGcp.EXPECT().InitializeClients().Return(nil)
	bucketName := fmt.Sprintf("%s-%s", region, poolId)

	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region).Return(errors.New("failed to create bucket"))
	err := activities.CreateGCPBucket(ctx, projectId, poolId, region, mockGcp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create bucket")
}

func Test_EnableAutoTiering_Failure(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	params := commonparams.CreatePoolParams{Name: "test-pool"}
	projectId := "test-project"

	// Save original and mock _createGCPBucket
	origCreateGCPBucket := activities.CreateGCPBucket
	defer func() { activities.CreateGCPBucket = origCreateGCPBucket }()
	activities.CreateGCPBucket = func(ctx context.Context, projectId, poolName, region string, gcpService hyperscaler.GoogleServices) error {
		return errors.New("Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
	}

	err := activity.EnableAutoTiering(ctx, params, "f2d9e18b-6716-9170-de70-827f5f769907", projectId)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
}
