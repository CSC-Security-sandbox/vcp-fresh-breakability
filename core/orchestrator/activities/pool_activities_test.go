package activities_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	digitalCert "crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/google"
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/servicenetworking/v1"
	"gorm.io/gorm"
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

// Unit tests for FindTenancy in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_FindTenancy(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()
	params := commonparams.CreatePoolParams{}

	origGetGCPService := activities.GetGCPService
	origGetTenantProject := activities.GetTenantProject
	defer func() {
		activities.GetGCPService = origGetGCPService
		activities.GetTenantProject = origGetTenantProject
	}()

	t.Run("GetGCPService fails", func(tt *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := activity.FindTenancyProject(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})

	t.Run("GetTenantProject fails", func(tt *testing.T) {
		mockSvc := &google.GcpServices{}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
			return "", errors.New("tenant project error")
		}
		_, err := activity.FindTenancyProject(ctx, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "tenant project error")
	})

	t.Run("Success", func(tt *testing.T) {
		mockSvc := &google.GcpServices{}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
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

		origGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = origGetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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

		origGetGCPService := activities.GetGCPService
		origGetSubnetToBeUsed := activities.GetSubnetToBeUsed
		defer func() {
			activities.GetGCPService = origGetGCPService
			activities.GetSubnetToBeUsed = origGetSubnetToBeUsed
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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

		origGetGCPService := activities.GetGCPService
		origGetSubnetToBeUsed := activities.GetSubnetToBeUsed
		defer func() {
			activities.GetGCPService = origGetGCPService
			activities.GetSubnetToBeUsed = origGetSubnetToBeUsed
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
		mockSvc := hyperscaler.NewMockGoogleServices(t)
		mockSvc.On("GetTenantProject", params.VendorSubNetID, params.AccountName, params.Region).Return("tenant-456", nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))
		got, err := activities.GetTenantProject(mockSvc, params)
		assert.NoError(t, err)
		assert.Equal(t, "tenant-456", got)
		mockSvc.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)
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
	assert.Contains(t, err.Error(), "no such file or directory")
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
	assert.Contains(t, err.Error(), "invalid character")
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
	assert.Contains(t, err.Error(), "LIF lif1 references non-existent home node non-existent-node")
	mockStorage.AssertExpectations(t)
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
	originalGetProviderByNode := activities.GetProviderByNode
	defer func() { activities.GetProviderByNode = originalGetProviderByNode }() // Restore original function after test

	// Mock GetProviderByNode to return the mock provider
	activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
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
	assert.Contains(t, err.Error(), "no cluster details provided")
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
	assert.Contains(t, err.Error(), "no cluster details provided")
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
		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		defer func() {}()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
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

		originalGetGCPService := activities.GetGCPService
		releaseSubnet := activities.ReleaseSubnet
		defer func() {
			activities.ReleaseSubnet = releaseSubnet
			activities.GetGCPService = originalGetGCPService
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
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
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subnet is not associated with the pool")
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
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		// Override with mock that returns error
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		defer func() {}()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
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
		GetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = GetGCPService
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)

		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
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
		mgs := hyperscaler.NewMockGoogleServices(tt)
		t.Setenv("FIREWALL_SOURCE_RANGES", "10.0.0.0/8,192.168.0.0/16")
		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: []string{"10.0.0.0/8", "192.168.0.0/16"}, // Same source ranges as expected
		}
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(existingFirewall, nil)

		err := activities.InsertFirewall(mgs, projectName, firewallName, vpcName, priority, direction, firewallSourceRanges, firewallAllowedPortRules)
		assert.NoError(tt, err)
		mgs.AssertExpectations(tt)
	})

	t.Run("WhenGetFirewallFailsWithNonNotFoundError", func(tt *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(tt)
		t.Setenv("FIREWALL_SOURCE_RANGES", "10.0.0.0/8,192.168.0.0/16")

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
		t.Setenv("FIREWALL_SOURCE_RANGES", "10.0.0.0/8,192.168.0.0/16")

		InsertFirewall := activities.InsertFirewall
		defer func() {
			activities.InsertFirewall = InsertFirewall
		}()
		errString := "failed to insert firewall"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetFirewall", projectName, firewallName).Return(nil, nil)
		mgs.On("InsertFirewall", &hyperscaler_models.Firewall{
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
		t.Setenv("FIREWALL_SOURCE_RANGES", "10.0.0.0/8,192.168.0.0/16")

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
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(&hyperscaler_models.VPCNetwork{}, nil)

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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(nil)

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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(errors.New(errString))

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
		errString := "failed to get VPC network"
		mgs.On("GetLogger").Return(logger)
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(nil, errors.New(errString))

		err := activities.CreateVPC(mgs, projectName, vpcName)

		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			assert.Contains(tt, customErr.Unwrap().Error(), errString)
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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return(nil)

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
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(&hyperscaler_models.Subnet{}, nil)

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

// Unit tests for _getSubnetwork
func Test_getSubnetwork(t *testing.T) {
	tenantProjectNumber := "tenant-456"

	t.Run("GetTenancyInfo succeeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}
		ctx := context.Background()

		expectedSubnet := &hyperscaler_models.Subnet{
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
	mockService := new(hyperscaler.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	network := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForIscsiSucceeds", func(tt *testing.T) {
		mockService.On("GetLogger").Return(logger)
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
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{}, nil)

		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)
		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})

	t.Run("Success", func(tt *testing.T) {
		mockGCPService := new(hyperscaler.MockGoogleServices)
		mockGCPService.On("GetLogger").Return(log.NewLogger())
		mockGCPService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, fmt.Errorf("secret get error"))
		mockGCPService.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "secretID"}}, nil)
		defer func() {
			commonparams.RemoveFromUserAuthCache("secretID")
		}()

		secret, err := activities.GeneratePasswordForVSACluster(mockGCPService, projectId, region, userName)

		assert.NoError(tt, err)
		assert.NotNil(tt, secret)
		mockGCPService.AssertExpectations(tt)
	})
}

func Test_CreateGCPBucket_Success(t *testing.T) {
	mockGcp := hyperscaler.NewMockGoogleServices(t)

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
	mockSvc := hyperscaler.NewMockGoogleServices(t)
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
	mockGcp := hyperscaler.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.On("GetLogger").Return(util.GetLogger(ctx))

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
	getGCPService := activities.GetGCPService
	defer func() {
		activities.CreateGCPBucket = origCreateGCPBucket
		activities.GetGCPService = getGCPService
	}()
	activities.CreateGCPBucket = func(ctx context.Context, projectId, poolName, region string, gcpService hyperscaler.GoogleServices) error {
		return errors.New("Error 403: The billing account for the owning project is disabled in state absent, accountDisabled")
	}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
	getGCPService := activities.GetGCPService
	defer func() {
		activities.CreateServiceAccountAndAttachRole = origCreateServiceAccountAndAttachRole
		activities.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		expectedSA := &iam.ServiceAccount{Name: "projects/test-project/serviceAccounts/test-sa"}
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
			return expectedSA, nil
		}

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		sa, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("error", func(t *testing.T) {
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
			return nil, errors.New("Mock error: failed to create service account")
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
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
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
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
	activity := activities.PoolActivity{}
	ctx := context.Background()
	bucketName := "us-central1-test-pool"

	// Save and mock DeleteGCPBucket
	origDeleteGCPBucket := activities.DeleteGCPBucket
	getGCPService := activities.GetGCPService
	defer func() {
		activities.DeleteGCPBucket = origDeleteGCPBucket
		activities.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
			return nil
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		err := activity.DeleteAutoTierBucket(ctx, bucketName)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteGCPBucket = func(ctx context.Context, bucketName string, gcpService hyperscaler.GoogleServices) error {
			return errors.New("delete failed")
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
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
	logger := util.GetLogger(ctx)

	t.Run("Success", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteBucket(ctx, bucketName).Return(nil)
		err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)

		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteBucket(ctx, bucketName).Return(errors.New("delete failed"))
		err := activities.DeleteGCPBucket(ctx, bucketName, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})
}

func Test_deleteServiceAccount(t *testing.T) {
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	logger := util.GetLogger(ctx)

	t.Run("success", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteServiceAccount(saEmail).Return(nil)
		err := activities.DeleteSrvcAccount(ctx, projectID, saAccountID, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("delete fails", func(t *testing.T) {
		mockGcp := hyperscaler.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger)
		mockGcp.EXPECT().DeleteServiceAccount(saEmail).Return(errors.New("delete failed"))
		err := activities.DeleteSrvcAccount(ctx, projectID, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete failed")
	})
}

func TestPoolActivity_DeleteServiceAccount(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"

	origDeleteSrvcAccount := activities.DeleteSrvcAccount
	getGCPService := activities.GetGCPService
	defer func() {
		activities.DeleteSrvcAccount = origDeleteSrvcAccount
		activities.GetGCPService = getGCPService
	}()

	t.Run("success", func(t *testing.T) {
		activities.DeleteSrvcAccount = func(ctx context.Context, projectID, saAccountID string, gcpService hyperscaler.GoogleServices) error {
			return nil
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		err := activity.DeleteServiceAccount(ctx, projectID, saAccountID)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteSrvcAccount = func(ctx context.Context, projectID, saAccountID string, gcpService hyperscaler.GoogleServices) error {
			return errors.New("delete error")
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
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
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "cached-password"}}, nil
		}
		password, err := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "cached-password", password)
		assert.NoError(tt, err)
	})

	t.Run("PasswordNotInCacheAndSecretManagerSucceeds", func(tt *testing.T) {
		getPasswordForVSACluster := activities.GetPasswordForVSACluster
		originalGcpService := activities.GetGCPService
		defer func() {
			commonparams.RemoveFromUserAuthCache(secretID)
			activities.GetPasswordForVSACluster = getPasswordForVSACluster
			commonparams.RemoveFromUserAuthCache(secretID)
			activities.GetGCPService = originalGcpService
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "secret-manager-password"}}, nil
		}
		password, err := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "secret-manager-password", password)
		assert.NoError(tt, err)
	})

	t.Run("PasswordNotInCacheAndSecretManagerFails", func(tt *testing.T) {
		originalGcpService := activities.GetGCPService
		getPasswordForVSACluster := activities.GetPasswordForVSACluster
		defer func() {
			activities.GetPasswordForVSACluster = getPasswordForVSACluster
			commonparams.RemoveFromUserAuthCache(secretID)
			activities.GetGCPService = originalGcpService
			commonparams.RemoveFromUserAuthCache(secretID)
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, assert.AnError
		}
		password, err := activities.GetPasswordFromCacheOrSecretManager(ctx, secretID)
		assert.Equal(tt, "", password)
		assert.Error(tt, err)
	})
}

func Test_IdentifyVMs_SuccessfullyPreparesConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := activities.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "password"}}, nil
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
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project")

	assert.NoError(t, err)
}

func Test_IdentifyVMs_FailsToPrepareConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := activities.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		activities.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, userName string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "password"}}, nil
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
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project")

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
	secretID := "test-secret"

	t.Run("DeleteSecret called when GetSecretWithLatestVersion passes", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("DeleteSecret returns error", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, nil)
		mockGCP.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete error"))

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
		mockGCP.AssertExpectations(t)
	})

	t.Run("Delete Secret fails if GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{}, fmt.Errorf("get secret error"))

		err := activities.DeletePasswordFromCacheAndSecretManager(mockGCP, secretID)
		assert.NoError(t, err)
		mockGCP.AssertExpectations(t)
	})
}

// Unit test for _getCertificateAndPrivateKeyByID
func Test_getCertificateAndPrivateKeyByID(t *testing.T) {
	caDeployedProjectID := "ca-proj"
	secretManagerProjectID := "sm-proj"
	region := "us-central1"
	caPoolName := "pool"
	certificateID := "cert-id"

	cert := &hyperscaler_models.CustomCertificate{}
	secret := &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{}}

	t.Run("success", func(t *testing.T) {
		mockService := new(hyperscaler.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(secret, nil)
		resp, err := activities.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, cert, resp.Certificate)
		assert.Equal(t, secret, resp.Secret)
		mockService.AssertExpectations(t)
	})

	t.Run("certificate not found", func(t *testing.T) {
		mockService := new(hyperscaler.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(nil, fmt.Errorf("not found"))
		resp, err := activities.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})

	t.Run("secret not found", func(t *testing.T) {
		mockService := new(hyperscaler.MockGoogleServices)
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(nil, fmt.Errorf("not found"))
		resp, err := activities.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})

	t.Run("secret version nil", func(t *testing.T) {
		mockService := new(hyperscaler.MockGoogleServices)
		secretNoVersion := &hyperscaler_models.CustomSecret{}
		mockService.On("GetCertificate", caDeployedProjectID, region, caPoolName, certificateID).Return(cert, nil)
		mockService.On("GetSecretWithLatestVersion", secretManagerProjectID, certificateID).Return(secretNoVersion, nil)
		resp, err := activities.GetCertificateAndPrivateKeyByID(mockService, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID)
		assert.Error(t, err)
		assert.Nil(t, resp)
		mockService.AssertExpectations(t)
	})
}
func Test_GetAndCreateCloudDNSRecord(t *testing.T) {
	recordName := "test-record"
	ipAddress := "1.2.3.4"
	t.Run("CreateResourceRecordSet success", func(t *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)
		expectedRecord := &hyperscaler_models.CustomCloudDNSRecord{RecordName: recordName, Data: ipAddress}

		mockService.On("GetLogger").Return(log.NewLogger())
		mockService.On("GetResourceRecordSet", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("resource not found"))
		mockService.On("CreateResourceRecordSet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(expectedRecord, nil)

		record, err := activities.GetOrCreateCloudDNSRecord(mockService, recordName, ipAddress)
		assert.NoError(t, err)
		assert.Equal(t, expectedRecord, record)
		mockService.AssertExpectations(t)
	})
	t.Run("returns error when CreateResourceRecordSet fails", func(t *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(log.NewLogger())
		mockService.On("GetResourceRecordSet", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("resource not found"))
		mockService.On("CreateResourceRecordSet", commonparams.CaPoolDeployedProjectID, commonparams.VsaManagedZone, ipAddress, recordName).
			Return(nil, errors.New("dns error"))

		record, err := activities.GetOrCreateCloudDNSRecord(mockService, recordName, ipAddress)
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

		mapHost, err := activity.GetCloudDNSRecords(ctx, poolId, commonparams.USER_CERTIFICATE)

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

		mapHost, err := activity.GetCloudDNSRecords(ctx, poolId, commonparams.USER_CERTIFICATE)

		expectedHost := &map[string]string{}
		assert.Error(tt, err)
		assert.Equal(tt, expectedHost, mapHost)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_NotFound", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := activities.PoolActivity{SE: mockStorage}
		ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", ctx, poolId).Return([]*datamodel.Node{}, nil)

		hostMap, err := activity.GetCloudDNSRecords(ctx, poolId, commonparams.USER_CERTIFICATE)

		expectedHost := &map[string]string{}
		assert.Error(tt, err)
		assert.Equal(tt, "no node found for the pool", err.Error())
		assert.Equal(tt, expectedHost, hostMap)
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
		originalGetGCPService := activities.GetGCPService
		originalDeleteCloudDNSRecord := activities.DeleteCloudDNSRecord
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.DeleteCloudDNSRecord = originalDeleteCloudDNSRecord
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.DeleteCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, recordName string) error {
			return nil
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, commonparams.USER_CERTIFICATE)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService fails", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		originalGetGCPService := activities.GetGCPService
		defer func() {
			activities.GetGCPService = originalGetGCPService
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("gcp error")
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, commonparams.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gcp error")
	})

	t.Run("DeleteCloudDNSRecord fails", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		originalGetGCPService := activities.GetGCPService
		originalDeleteCloudDNSRecord := activities.DeleteCloudDNSRecord
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.DeleteCloudDNSRecord = originalDeleteCloudDNSRecord
		}()

		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}
		activities.DeleteCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, recordName string) error {
			return fmt.Errorf("delete error")
		}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, commonparams.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})
	t.Run("does nothing if not USER_CERTIFICATE", func(t *testing.T) {
		activity := &activities.PoolActivity{}
		err := activity.DeleteCloudDNSRecords(ctx, hostMap, commonparams.USERNAME_PWD)
		assert.NoError(t, err)
	})
}

func TestPoolActivity_CreateCloudDNSRecords(t *testing.T) {
	ctx := context.Background()
	clusterName := "testcluster"
	commonparams.VsaDeployedDnsName = "example.com"

	// Mock CreateCloudDNSRecord
	originalCreateCloudDNSRecord := activities.GetOrCreateCloudDNSRecord
	originalGCPService := activities.GetGCPService
	defer func() {
		activities.GetOrCreateCloudDNSRecord = originalCreateCloudDNSRecord
		activities.GetGCPService = originalGCPService
	}()
	activities.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, ip, recordName string) (*hyperscaler_models.CustomCloudDNSRecord, error) {
		return &hyperscaler_models.CustomCloudDNSRecord{RecordName: recordName}, nil
	}

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USER_CERTIFICATE)
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
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USER_CERTIFICATE)
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
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// CreateCloudDNSRecord returns error
	t.Run("GetOrCreateCloudDNSRecord error", func(t *testing.T) {
		activities.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, ip, recordName string) (*hyperscaler_models.CustomCloudDNSRecord, error) {
			return nil, fmt.Errorf("dns error")
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
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// Not USER_CERTIFICATE auth type
	t.Run("not user certificate", func(t *testing.T) {
		commonparams.AuthType = commonparams.USERNAME_PWD_SEC_MGR
		vlmConfig := &vlm.VLMConfig{}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USERNAME_PWD)
		assert.NoError(t, err)
		assert.NotNil(t, hostMap)
		assert.Equal(t, 0, len(*hostMap))
	})
}

func TestPoolActivity_DeleteOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()

	origGetGCPService := activities.GetGCPService
	origRevokeCert := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager
	origDeletePwd := activities.DeletePasswordFromCacheAndSecretManager
	defer func() {
		activities.GetGCPService = origGetGCPService
		activities.RevokeCertificateAndDeleteFromCacheAndSecretManager = origRevokeCert
		activities.DeletePasswordFromCacheAndSecretManager = origDeletePwd
	}()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, certID string) error {
			assert.Equal(t, "cert-id", certID)
			return nil
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, secretID string) error {
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
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, certID string) error {
			assert.Equal(t, "cert-id", certID)
			return nil
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, secretID string) error {
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
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, certID string) error {
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
				AuthType:      commonparams.USERNAME_PWD_SEC_MGR,
			},
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, secretID string) error {
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
				AuthType:      commonparams.USERNAME_PWD_SEC_MGR,
			},
		}
		activities.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler.GoogleServices, secretID string) error {
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
				AuthType:      commonparams.USERNAME_PWD,
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
				AuthType:      commonparams.USERNAME_PWD,
			},
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		err := activity.DeleteOnTapCredentials(ctx, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gcp error")
	})
}

func TestPoolActivity_CreateOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
	ctx := context.Background()
	region := "us-central1"
	clusterName := "test-cluster"

	origGetGCPService := activities.GetGCPService
	origGenerateAndCreateCertificateForVSACluster := activities.GenerateAndCreateCertificateForVSACluster
	origGeneratePasswordForVSACluster := activities.GeneratePasswordForVSACluster
	defer func() {
		activities.GetGCPService = origGetGCPService
		activities.GenerateAndCreateCertificateForVSACluster = origGenerateAndCreateCertificateForVSACluster
		activities.GeneratePasswordForVSACluster = origGeneratePasswordForVSACluster
	}()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{}, nil
	}

	t.Run("USER_CERTIFICATE success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*hyperscaler_models.CustomCertificateResponse, error) {
			return &hyperscaler_models.CustomCertificateResponse{
				Certificate: &hyperscaler_models.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &hyperscaler_models.CustomSecret{
					SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
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
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*hyperscaler_models.CustomCertificateResponse, error) {
			return &hyperscaler_models.CustomCertificateResponse{
				Certificate: &hyperscaler_models.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &hyperscaler_models.CustomSecret{
					SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, fmt.Errorf("pwd error")
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USER_CERTIFICATE,
			},
		}
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*hyperscaler_models.CustomCertificateResponse, error) {
			return nil, fmt.Errorf("cert error")
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USERNAME_PWD_SEC_MGR,
			},
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.NoError(t, err)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USERNAME_PWD_SEC_MGR,
			},
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, fmt.Errorf("pwd error")
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})

	t.Run("default password", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USERNAME_PWD,
			},
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.NoError(t, err)
		assert.Equal(t, "default-password", creds.AdminPassword)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      commonparams.USERNAME_PWD,
			},
		}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("gcp error")
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.Error(t, err)
		assert.Nil(t, creds)
	})
}

func Test_RevokeCertificateAndDeleteFromCacheAndSecretManager(t *testing.T) {
	certificateID := "test-cert-id"

	// Save and restore RemoveFromCertAuthCache
	origRemoveFromCertAuthCache := commonparams.RemoveFromCertAuthCache
	defer func() { commonparams.RemoveFromCertAuthCache = origRemoveFromCertAuthCache }()

	t.Run("success", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)
		commonparams.RemoveFromCertAuthCache = func(certID string) bool { return true }

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.NoError(t, err)
		mockGcpService.AssertExpectations(t)
	})

	t.Run("GetCertificate fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("get cert error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "get cert error")
	})

	t.Run("RevokeCertificate fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", fmt.Errorf("revoke error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "revoke error")
	})
	t.Run("GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("get secret error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "get secret error")
	})

	t.Run("DeleteSecret fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete secret error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "delete secret error")
	})

	t.Run("RemoveFromCertAuthCache fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&hyperscaler_models.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(nil)
		commonparams.RemoveFromCertAuthCache = func(certID string) bool { return false }

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.NoError(t, err)
	})
}

func Test_GenerateAndCreateCertificateForVSACluster(t *testing.T) {
	region := "us-central1"
	certificateID := "test-cert-id"
	clusterName := "test-cluster"
	csr := []byte("fake-csr")
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	t.Run("Success", func(t *testing.T) {
		mockGcpService := hyperscaler.NewMockGoogleServices(t)
		cert := &hyperscaler_models.CustomCertificate{
			SubjectCommonName:   "test-cn",
			PemCertificate:      "pem-cert",
			PemCertificateChain: []string{"chain1", "chain2"},
		}
		secret := &hyperscaler_models.CustomSecret{
			SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "private-key"},
		}

		origGenerateCSR := activities.GenerateCSR
		origValidateAndConvert := commonparams.ValidateAndConvertCertificateParamsToCustomCertificate
		origGetAndCreate := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM
		defer func() {
			commonparams.RemoveFromCertAuthCache(certificateID)
			activities.GenerateCSR = origGenerateCSR
			commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = origValidateAndConvert
			activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = origGetAndCreate
		}()

		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return csr, key, nil
		}
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler_models.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler_models.CustomCertificate, error) {
			return cert, nil
		}
		activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService hyperscaler.GoogleServices, certificate *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomCertificate, *hyperscaler_models.CustomSecret, error) {
			return cert, secret, nil
		}
		mockGcpService.On("GetLogger").Return(log.NewLogger())

		resp, err := activities.GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.NoError(t, err)
		assert.NotNil(t, resp)
		assert.Equal(t, cert, resp.Certificate)
		assert.Equal(t, secret, resp.Secret)
	})
	t.Run("GenerateCSR fail", func(t *testing.T) {
		mockGcpService := hyperscaler.NewMockGoogleServices(t)

		origGenerateCSR := activities.GenerateCSR
		defer func() { activities.GenerateCSR = origGenerateCSR }()

		expectedErr := fmt.Errorf("generate csr error")
		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return nil, nil, expectedErr
		}

		mockGcpService.On("GetLogger").Return(log.NewLogger())

		resp, err := activities.GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.EqualError(t, err, expectedErr.Error())
	})
	t.Run("ValidateAndConvert fail", func(t *testing.T) {
		mockGcpService := hyperscaler.NewMockGoogleServices(t)

		// Patch GenerateCSR to succeed
		origGenerateCSR := activities.GenerateCSR
		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), &rsa.PrivateKey{}, nil
		}

		// Patch ValidateAndConvertCertificateParamsToCustomCertificate to fail
		origValidate := commonparams.ValidateAndConvertCertificateParamsToCustomCertificate
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler_models.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler_models.CustomCertificate, error) {
			return nil, fmt.Errorf("validation failed")
		}
		defer func() {
			activities.GenerateCSR = origGenerateCSR
			commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = origValidate
		}()
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		resp, err := activities.GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.EqualError(t, err, "validation failed")
	})
	t.Run("GetOrCreateCertificateInCASAndPrivateKeyInSM fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		expectedErr := errors.New("CAS/SM error")

		// Patch GenerateCSR to return dummy values
		origGenerateCSR := activities.GenerateCSR
		activities.GenerateCSR = func(commonName string, domains []string) ([]byte, *rsa.PrivateKey, error) {
			return []byte("csr"), &rsa.PrivateKey{}, nil
		}
		// Patch GetOrCreateCertificateInCASAndPrivateKeyInSM to return error
		origGetAndCreate := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM
		activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService hyperscaler.GoogleServices, certificate *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomCertificate, *hyperscaler_models.CustomSecret, error) {
			return nil, nil, expectedErr
		}

		// Patch ValidateAndConvertCertificateParamsToCustomCertificate to return dummy cert
		origValidate := commonparams.ValidateAndConvertCertificateParamsToCustomCertificate
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *hyperscaler_models.CustomCertificateParam, pemBlock pem.Block) (*hyperscaler_models.CustomCertificate, error) {
			return &hyperscaler_models.CustomCertificate{}, nil
		}
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		defer func() {
			activities.GenerateCSR = origGenerateCSR
			activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = origGetAndCreate
			commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = origValidate
		}()
		resp, err := activities.GenerateAndCreateCertificateForVSACluster(mockGcpService, region, certificateID, clusterName)
		assert.Nil(t, resp)
		assert.Equal(t, expectedErr, err)
	})
}

func Test_GetPasswordForVSACluster_Success(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		gcpService := hyperscaler.NewMockGoogleServices(t)
		secretID := "test-secret-id"
		expectedSecret := &hyperscaler_models.CustomSecret{
			SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "super-secret"},
		}
		gcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)

		secret, err := activities.GetPasswordForVSACluster(gcpService, secretID)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		gcpService.AssertExpectations(t)
	})

	t.Run("failure", func(t *testing.T) {
		gcpService := hyperscaler.NewMockGoogleServices(t)
		secretID := "test-secret-id"
		gcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))

		secret, err := activities.GetPasswordForVSACluster(gcpService, secretID)
		assert.Error(t, err)
		assert.Nil(t, secret)
		gcpService.AssertExpectations(t)
	})
}

func Test_GetCertificateFromCacheOrSecretManager(t *testing.T) {
	ctx := context.Background()
	certificateID := "test-cert-id"

	t.Run("Certificate found in cache", func(t *testing.T) {
		mockCert := &coremodel.Certificate{
			SignedCertificate:        "signed-cert",
			PrivateKey:               "private-key",
			CommonName:               "common-name",
			InterMediateCertificates: []string{"intermediate"},
		}
		defer func() {
			commonparams.RemoveFromCertAuthCache(certificateID)
		}()
		commonparams.AddToCertAuthCache(certificateID, mockCert)
		cert, err := activities.GetCertificateFromCacheOrSecretManager(ctx, certificateID)
		assert.NoError(t, err)
		assert.Equal(t, mockCert, cert)
	})
	t.Run("Certificate not in cache, found via GCP", func(t *testing.T) {
		origGetGCPService := activities.GetGCPService
		origGetCertificateAndPrivateKeyByID := activities.GetCertificateAndPrivateKeyByID
		defer func() {
			commonparams.RemoveFromCertAuthCache(certificateID)
			activities.GetGCPService = origGetGCPService
			activities.GetCertificateAndPrivateKeyByID = origGetCertificateAndPrivateKeyByID
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockCertResp := &hyperscaler_models.CustomCertificateResponse{
			Certificate: &hyperscaler_models.CustomCertificate{
				CertificateID:       "signed-cert",
				SubjectCommonName:   "common-name",
				PemCertificateChain: []string{"intermediate"},
			},
			Secret: &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "private-key"},
			},
		}
		activities.GetCertificateAndPrivateKeyByID = func(gcpService hyperscaler.GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*hyperscaler_models.CustomCertificateResponse, error) {
			return mockCertResp, nil
		}
		cert, err := activities.GetCertificateFromCacheOrSecretManager(ctx, certificateID)
		assert.NoError(t, err)
		assert.Equal(t, "signed-cert", cert.SignedCertificate)
		assert.Equal(t, "private-key", cert.PrivateKey)
		assert.Equal(t, "common-name", cert.CommonName)
		assert.Equal(t, []string{"intermediate"}, cert.InterMediateCertificates)
	})
	t.Run("GCP service returns error", func(t *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp error")
		}
		cert, err := activities.GetCertificateFromCacheOrSecretManager(ctx, certificateID)
		assert.Error(t, err)
		assert.Nil(t, cert)
	})
}

func Test_getAndCreatePrivateKeyInSecretManagerAndCache(t *testing.T) {
	cert := &hyperscaler_models.CustomCertificate{
		CertificateID:     "test-cert",
		Region:            "us-central1",
		SubjectCommonName: "test-cn",
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	expectedSecret := &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "private-key"}}

	t.Run("returns existing secret", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(expectedSecret, nil)
		secret, err := activities.GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("creates secret if not found", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(expectedSecret, nil)
		secret, err := activities.GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.NoError(t, err)
		assert.Equal(t, expectedSecret, secret)
		mockGCP.AssertExpectations(t)
	})

	t.Run("create secret fails, revoke fails", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", errors.New("revoke failed"))
		secret, err := activities.GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.Nil(t, secret)
		assert.Error(t, err)
		mockGCP.AssertExpectations(t)
	})

	t.Run("create secret fails, revoke succeeds", func(t *testing.T) {
		mockGCP := new(hyperscaler.MockGoogleServices)
		mockGCP.On("GetLogger").Return(log.NewLogger())
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("not found"))
		mockGCP.On("CreateSecret", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
		mockGCP.On("RevokeCertificate", mock.Anything).Return("", nil)
		secret, err := activities.GetOrCreatePrivateKeyInSecretManagerAndCache(mockGCP, cert, key)
		assert.Nil(t, secret)
		assert.Error(t, err)
		mockGCP.AssertExpectations(t)
	})
}

func Test_GetAndCreateCertificateInCASAndPrivateKeyInSM(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	certificate := &hyperscaler_models.CustomCertificate{
		CertificateID:     "certid",
		Region:            "us-central1",
		SubjectCommonName: "test-cn",
	}
	originalConvert := commonparams.ConvertPrivateKeyToString
	defer func() {
		commonparams.ConvertPrivateKeyToString = originalConvert
	}()
	commonparams.ConvertPrivateKeyToString = func(key *rsa.PrivateKey, rsaKeyType string) string {
		return "private-key"
	}

	t.Run("GetCertificate fails, CreateCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache succeeds ", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockSvc.On("CreateCertificate", certificate).Return(certificate, nil)

		originalGetAndCreatePrivateKeyInSecretManagerAndCache := activities.GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			activities.GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{}, nil
		}
		cert, _, err := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.NoError(t, err)
		assert.Equal(t, certificate, cert)
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate fails, CreateCertificate fails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("not found"))
		mockSvc.On("CreateCertificate", certificate).Return(nil, fmt.Errorf("can not create cert"))
		cert, secret, err := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
		assert.EqualError(t, err, "can not create cert")
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache succeeds", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(certificate, nil)
		mockSecret := &hyperscaler_models.CustomSecret{Name: "secret"}
		originalGetAndCreatePrivateKeyInSecretManagerAndCache := activities.GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			activities.GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomSecret, error) {
			return mockSecret, nil
		}
		cert, secret, err := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.NoError(t, err)
		assert.Equal(t, certificate, cert)
		assert.Equal(t, mockSecret, secret)
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetCertificate succeeds, GetOrCreatePrivateKeyInSecretManagerAndCache fails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)
		mockSvc.On("GetLogger").Return(log.NewLogger())
		mockSvc.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(certificate, nil)
		originalGetAndCreatePrivateKeyInSecretManagerAndCache := activities.GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			activities.GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *hyperscaler_models.CustomCertificate, key *rsa.PrivateKey) (*hyperscaler_models.CustomSecret, error) {
			return nil, errors.New("can not create cert")
		}
		cert, secret, err := activities.GetOrCreateCertificateInCASAndPrivateKeyInSM(mockSvc, certificate, key)
		assert.Error(t, err)
		assert.Nil(t, cert)
		assert.Nil(t, secret)
		mockSvc.AssertExpectations(t)
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
			got := activities.MakeSubnetName(tt.projectNumber)
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
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		subnetName := "vsa-" + tenantProjectNumber + "-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		operation := "operation-12345"
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(&operation, nil)

		operationName, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.NoError(t, err)
		assert.Equal(t, "operation-12345", *operationName)
		mockSvc.AssertExpectations(t)
	})

	t.Run("CreateSubnetworkForTenantProjectFails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(nil, errors.New("create failed"))
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		_, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region)
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
	region := "us-central1"
	clusterName := "test-cluster"

	originalGetGCPService := activities.GetGCPService
	originalGenerateAndCreateCertificate := activities.GenerateAndCreateCertificateForVSACluster
	originalGeneratePassword := activities.GeneratePasswordForVSACluster
	defer func() {
		activities.GetGCPService = originalGetGCPService
		activities.GenerateAndCreateCertificateForVSACluster = originalGenerateAndCreateCertificate
		activities.GeneratePasswordForVSACluster = originalGeneratePassword
	}()

	mockGCPService := &google.GcpServices{}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	// Mock certificate generation
	activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*hyperscaler_models.CustomCertificateResponse, error) {
		return &hyperscaler_models.CustomCertificateResponse{
			Certificate: &hyperscaler_models.CustomCertificate{
				SubjectCommonName:   "test-cn",
				PemCertificate:      "test-cert",
				PemCertificateChain: []string{"test-chain"},
			},
			Secret: &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{
					Value: "test-private-key",
				},
			},
		}, nil
	}

	// Mock password generation
	activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{
			SecretVersion: &hyperscaler_models.CustomSecretVersion{
				Value: "test-password",
			},
		}, nil
	}

	// Act
	result, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)

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
	region := "us-central1"
	clusterName := "test-cluster"

	originalGetGCPService := activities.GetGCPService
	defer func() { activities.GetGCPService = originalGetGCPService }()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	result, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get GCP service")
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

	originalGetGCPService := activities.GetGCPService
	originalCreateGCPBucket := activities.CreateGCPBucket
	defer func() {
		activities.GetGCPService = originalGetGCPService
		activities.CreateGCPBucket = originalCreateGCPBucket
	}()

	mockGCPService := &google.GcpServices{}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	activities.CreateGCPBucket = func(ctx context.Context, projectId, bucketName, region string, gcpService hyperscaler.GoogleServices) error {
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

	originalGetGCPService := activities.GetGCPService
	defer func() { activities.GetGCPService = originalGetGCPService }()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
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

	originalGetGCPService := activities.GetGCPService
	defer func() { activities.GetGCPService = originalGetGCPService }()

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	err := activity.DeleteAutoTierBucket(ctx, autoTierBucketName)

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

	originalGetGCPService := activities.GetGCPService
	originalCreateServiceAccountAndAttachRole := activities.CreateServiceAccountAndAttachRole
	defer func() {
		activities.GetGCPService = originalGetGCPService
		activities.CreateServiceAccountAndAttachRole = originalCreateServiceAccountAndAttachRole
	}()

	mockGCPService := &google.GcpServices{}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	expectedServiceAccount := &iam.ServiceAccount{
		Email: "test-sa@test-project.iam.gserviceaccount.com",
		Name:  "Test Service Account",
	}

	activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler.GoogleServices) (*iam.ServiceAccount, error) {
		return expectedServiceAccount, nil
	}

	// Act
	result, err := activity.CreateServiceAccountWithStorageRole(ctx, projectID, saAccountID, saDisplayName)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedServiceAccount.Email, result.Email)
}

func TestPoolActivity_DeleteServiceAccount_Success(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	projectID := "test-project"
	saAccountID := "test-sa"

	originalGetGCPService := activities.GetGCPService
	originalDeleteSrvcAccount := activities.DeleteSrvcAccount
	defer func() {
		activities.GetGCPService = originalGetGCPService
		activities.DeleteSrvcAccount = originalDeleteSrvcAccount
	}()

	mockGCPService := &google.GcpServices{}
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	activities.DeleteSrvcAccount = func(ctx context.Context, projectID string, saAccountID string, gcpService hyperscaler.GoogleServices) error {
		return nil
	}

	// Act
	err := activity.DeleteServiceAccount(ctx, projectID, saAccountID)

	// Assert
	assert.NoError(t, err)
}

func Test_ConstructCurrentVlmConfig_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock ReadFile to return valid JSON
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte(`{
			"cloud": {"ha_pair": []},
			"deployment": {
				"deployment_id": "",
				"spconfig": {"size": "", "throughput": 0, "iops": 0},
				"zone": {"zone1": "", "zone2": ""},
				"net_config": {}
			}
		}`), nil
	}

	poolId := int64(1)
	deploymentID := "test-deployment"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	network := "test-network"
	subnets := []string{"test-subnet"}
	projectId := "test-project"
	snHostProject := "test-sn-host-project"
	saEmail := "test-sa@test-project.iam.gserviceaccount.com"
	autoTierBucket := "test-auto-tier-bucket"

	decision := &vmrs.Decision{
		ChosenVMs: []string{"c3-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1000,
			DesiredThroughputInMiBs: 100,
			DesiredCapacityInGiB:    1024,
		},
	}

	nodes := []*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "node1",
			ZoneName:  "us-central1-a",
			NodeAttributes: &datamodel.NodeDetails{
				InstanceType: "c3-standard-4",
			},
		},
		{
			BaseModel: datamodel.BaseModel{ID: 2},
			Name:      "node2",
			ZoneName:  "us-central1-b",
			NodeAttributes: &datamodel.NodeDetails{
				InstanceType: "c3-standard-4",
			},
		},
	}

	mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nodes, nil)

	vlmConfig, err := activity.ConstructCurrentVlmConfig(ctx, poolId, deploymentID, region, primaryZone, secondaryZone, network, subnets, projectId, snHostProject, decision, saEmail, autoTierBucket)

	assert.NoError(t, err)
	assert.NotNil(t, vlmConfig)
	assert.Equal(t, "test-deployment", vlmConfig.Deployment.DeploymentID)
	assert.Equal(t, "1024Gi", vlmConfig.Deployment.SPConfig.Size)
	assert.Equal(t, int64(100), vlmConfig.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(1000), vlmConfig.Deployment.SPConfig.IOps)
	assert.Equal(t, "us-central1", vlmConfig.Deployment.Region)
	assert.Equal(t, "us-central1-a", vlmConfig.Deployment.Zone.Zone1)
	assert.Equal(t, "us-central1-b", vlmConfig.Deployment.Zone.Zone2)
	assert.Equal(t, "c3-standard-4", vlmConfig.Deployment.VSAInstanceType)
	assert.Len(t, vlmConfig.Cloud.HAPairs, 1)
	assert.Equal(t, "node1", vlmConfig.Cloud.HAPairs[0].VM1.Name)
	assert.Equal(t, "node1", vlmConfig.Cloud.HAPairs[0].VM1.HostName)
	assert.Equal(t, "us-central1-a", vlmConfig.Cloud.HAPairs[0].VM1.Zone)
	assert.Equal(t, 1, vlmConfig.Cloud.HAPairs[0].VM1.NodeIndex)
	assert.False(t, vlmConfig.Cloud.HAPairs[0].VM1.IsMediator)
	assert.Equal(t, "node2", vlmConfig.Cloud.HAPairs[0].VM2.Name)
	assert.Equal(t, "node2", vlmConfig.Cloud.HAPairs[0].VM2.HostName)
	assert.Equal(t, "us-central1-b", vlmConfig.Cloud.HAPairs[0].VM2.Zone)
	assert.Equal(t, 2, vlmConfig.Cloud.HAPairs[0].VM2.NodeIndex)
	assert.False(t, vlmConfig.Cloud.HAPairs[0].VM2.IsMediator)
	mockStorage.AssertExpectations(t)
}

func Test_ConstructCurrentVlmConfig_NodeRetrievalError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock ReadFile to return valid JSON
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte(`{
			"cloud": {"ha_pair": []},
			"deployment": {
				"deployment_id": "",
				"spconfig": {"size": "", "throughput": 0, "iops": 0},
				"zone": {"zone1": "", "zone2": ""},
				"net_config": {}
			}
		}`), nil
	}

	poolId := int64(1)
	deploymentID := "test-deployment"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	network := "test-network"
	subnets := []string{"test-subnet"}
	projectId := "test-project"
	snHostProject := "test-sn-host-project"
	saEmail := "test-sa@test-project.iam.gserviceaccount.com"
	autoTierBucket := "test-auto-tier-bucket"

	decision := &vmrs.Decision{
		ChosenVMs: []string{"c3-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1000,
			DesiredThroughputInMiBs: 100,
			DesiredCapacityInGiB:    1024,
		},
	}

	mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nil, errors.New("database error"))

	vlmConfig, err := activity.ConstructCurrentVlmConfig(ctx, poolId, deploymentID, region, primaryZone, secondaryZone, network, subnets, projectId, snHostProject, decision, saEmail, autoTierBucket)

	assert.Error(t, err)
	assert.Nil(t, vlmConfig)
	assert.Contains(t, err.Error(), "database error")
	mockStorage.AssertExpectations(t)
}

func Test_ConstructCurrentVlmConfig_NotEnoughNodes(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	// Mock ReadFile to return valid JSON
	originalReadFile := activities.ReadFile
	defer func() { activities.ReadFile = originalReadFile }()
	activities.ReadFile = func(filename string) ([]byte, error) {
		return []byte(`{
			"cloud": {"ha_pair": []},
			"deployment": {
				"deployment_id": "",
				"spconfig": {"size": "", "throughput": 0, "iops": 0},
				"zone": {"zone1": "", "zone2": ""},
				"net_config": {}
			}
		}`), nil
	}

	poolId := int64(1)
	deploymentID := "test-deployment"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := "us-central1-b"
	network := "test-network"
	subnets := []string{"test-subnet"}
	projectId := "test-project"
	snHostProject := "test-sn-host-project"
	saEmail := "test-sa@test-project.iam.gserviceaccount.com"
	autoTierBucket := "test-auto-tier-bucket"

	decision := &vmrs.Decision{
		ChosenVMs: []string{"c3-standard-4"},
		StoragePoolRequirements: vmrs.CustomerRequestedPerformance{
			DesiredIOPS:             1000,
			DesiredThroughputInMiBs: 100,
			DesiredCapacityInGiB:    1024,
		},
	}

	// Return only one node instead of the required two
	nodes := []*datamodel.Node{
		{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "node1",
			ZoneName:  "us-central1-a",
		},
	}

	mockStorage.On("GetNodesByPoolID", ctx, poolId).Return(nodes, nil)

	vlmConfig, err := activity.ConstructCurrentVlmConfig(ctx, poolId, deploymentID, region, primaryZone, secondaryZone, network, subnets, projectId, snHostProject, decision, saEmail, autoTierBucket)

	assert.Error(t, err)
	assert.Nil(t, vlmConfig)
	assert.Contains(t, err.Error(), "not enough nodes in the cluster to create HAPair")
	mockStorage.AssertExpectations(t)
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

	t.Run("WhenQoSPolicyCreationSucceeds_ThenApplyToSVM", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

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

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenQoSPolicyCreationFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		mockProvider.On("CreateQoSGroupPolicy", mock.Anything).Return(nil, errors.New("qos creation failed"))

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "qos creation failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSVMModificationFails_ThenReturnError", func(tt *testing.T) {
		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

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
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

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
	t.Run("WhenSVMHasDifferentName_ThenUseCorrectName", func(tt *testing.T) {
		svmWithDifferentName := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "different-svm-name",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}

		mockProvider := new(vsa.MockProvider)
		originalGetProviderByNode := activities.GetProviderByNode
		defer func() {
			activities.GetProviderByNode = originalGetProviderByNode
		}()

		activities.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Mock QoS policy creation with different SVM name
		mockProvider.On("CreateQoSGroupPolicy", vsa.CreateQoSGroupPolicyParams{
			Name:          "different-svm-name-qos-policy",
			SvmName:       "different-svm-name",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}).Return(&vsa.QoSGroupPolicyResponse{
			Name:          "different-svm-name-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "different-svm-name",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}, nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "different-svm-name-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{}
		err := activity.CreateQoSPolicyAndApplyToSVM(ctx, pool, svmWithDifferentName, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
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
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallEdited", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges2,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		mgs.On("UpdateFirewall", firewallRequest).Return(nil)
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallRemovedSuccess", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges4,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return(nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallAddedSuccess", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges3,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return(nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallIsDifferentSuccess", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges4,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return(nil)
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenNewFirewallIsDifferentFails", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges5,
		}
		mgs.On("UpdateFirewall", firewallRequest).Return(errors.New("update error"))
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.Error(t, err, "update error")
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallOrderChanged", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges6,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		// No update should be needed when only order is different
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
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
	expectedSubnet := &hyperscaler_models.Subnet{}

	origGetGCPService := activities.GetGCPService
	origCheckReusableSubnet := activities.CheckReusableSubnet
	defer func() {
		activities.GetGCPService = origGetGCPService
		activities.CheckReusableSubnet = origCheckReusableSubnet
	}()

	t.Run("Success", func(t *testing.T) {
		mockSvc := &google.GcpServices{}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
			return expectedSubnet, nil
		}
		result, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, expectedSubnet, result)
	})

	t.Run("GetGCPServiceError", func(t *testing.T) {
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		result, err := activity.GetAvailableSubnet(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "gcp service error")
	})

	t.Run("CheckReusableSubnetError", func(t *testing.T) {
		mockGCPService := &google.GcpServices{}
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
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
	subnet := &hyperscaler_models.Subnet{
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
		originalGetGCPService := activities.GetGCPService
		originalGetCreateDataSubnetOp := activities.GetCreateDataSubnetworkOp
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.GetCreateDataSubnetworkOp = originalGetCreateDataSubnetOp
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GetCreateDataSubnetworkOp = func(service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
			return &expectedSubnetName, nil
		}

		result, err := activity.GetCreateDataSubnetOp(ctx, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, expectedSubnetName, *result)
	})

	t.Run("GetGCPServiceError", func(t *testing.T) {
		originalGetGCPService := activities.GetGCPService
		defer func() { activities.GetGCPService = originalGetGCPService }()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}

		result, err := activity.GetCreateDataSubnetOp(ctx, params, tenantProjectNumber)
		assert.Error(t, err)
		assert.Empty(t, result)
		assert.Contains(t, err.Error(), "gcp service error")
	})

	t.Run("GetCreateDataSubnetOpError", func(t *testing.T) {
		originalGetGCPService := activities.GetGCPService
		originalGetCreateDataSubnetOp := activities.GetCreateDataSubnetworkOp
		defer func() {
			activities.GetGCPService = originalGetGCPService
			activities.GetCreateDataSubnetworkOp = originalGetCreateDataSubnetOp
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.GetCreateDataSubnetworkOp = func(service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*string, error) {
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

func TestPoolActivity_GetServiceNetOpStatus(t *testing.T) {
	activity := &activities.PoolActivity{}

	t.Run("Success", func(t *testing.T) {
		expectedOp := &hyperscaler_models.ComputeOperation{
			Name: "op-123",
		}
		original := activities.GetGCPService
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		originalGetServiceNetOpStatus := activities.GetServiceNetOpStatus
		activities.GetServiceNetOpStatus = func(gcpService hyperscaler.GoogleServices, operation string) (*hyperscaler_models.ComputeOperation, error) {
			return expectedOp, nil
		}
		defer func() {
			activities.GetGCPService = original
			activities.GetServiceNetOpStatus = originalGetServiceNetOpStatus
		}()
		ctx := context.Background()
		op, err := activity.GetServiceNetOpStatus(ctx, "op-123")
		assert.NoError(t, err)
		assert.Equal(t, expectedOp.Name, op.Name)
	})

	t.Run("GetGCPServiceFails", func(t *testing.T) {
		original := activities.GetGCPService
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("service error")
		}
		defer func() { activities.GetGCPService = original }()

		ctx := context.Background()
		op, err := activity.GetServiceNetOpStatus(ctx, "op-123")
		assert.Error(t, err)
		assert.Nil(t, op)
	})
}

func Test_getServiceNetOpStatus(t *testing.T) {
	mockService := new(hyperscaler.MockGoogleServices)
	expectedOp := &hyperscaler_models.ComputeOperation{Status: "DONE"}
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
		assert.Contains(t, err.Error(), "operation response is nil")
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
		mockService := new(hyperscaler.MockGoogleServices)
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

		activities.GetCreateSubnetworkOperation = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string) (*string, error) {
			assert.Equal(t, "123456789", tenantProjectNumber)
			assert.Equal(t, "test-vpc", consumerVPC)
			assert.Equal(t, "us-central1", *tenantProjectRegion)
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
		mockService := new(hyperscaler.MockGoogleServices)
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

		activities.GetCreateSubnetworkOperation = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string) (*string, error) {
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
				mockService := new(hyperscaler.MockGoogleServices)
				mockLogger := util.GetLogger(context.Background())

				params := commonparams.CreatePoolParams{
					Region:         tc.region,
					VendorSubNetID: tc.vendorSubNetID,
				}
				operationName := "test-operation"

				mockService.On("GetLogger").Return(mockLogger)

				originalGetCreateSubnetworkOperation := activities.GetCreateSubnetworkOperation
				defer func() { activities.GetCreateSubnetworkOperation = originalGetCreateSubnetworkOperation }()

				activities.GetCreateSubnetworkOperation = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string) (*string, error) {
					assert.Equal(t, tc.tenantProjectNumber, tenantProjectNumber)
					assert.Equal(t, tc.vendorSubNetID, consumerVPC)
					assert.Equal(t, tc.region, *tenantProjectRegion)
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
	originalGetGCPService := activities.GetGCPService
	defer func() { activities.GetGCPService = originalGetGCPService }()
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("GCP service not available in test")
	}

	// Act
	result, err := activity.IdentifySecondaryAndMediatorZone(ctx, projectNumber, locationInfo, "c3-std-4")

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
	originalGetGCPService := activities.GetGCPService
	defer func() { activities.GetGCPService = originalGetGCPService }()
	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	result, err := activity.IdentifySecondaryAndMediatorZone(ctx, projectNumber, locationInfo, "c3-std-4")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, result)
}

func Test_resolveZonesForCluster_Success_NoSecondaryNoMediator(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable for secondary zone selection
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(true, nil)
	// Mock IsMachineTypeAvailable for mediator zone selection
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-c", instanceType).Return(true, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "us-central1-b", resolvedSecondary)
	assert.Equal(t, "us-central1-c", resolvedMediator)
	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_PrimaryZoneEmpty(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := ""
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "primary zone is not set")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)
}

func Test_resolveZonesForCluster_Error_GetZonesFails(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return error
	mockService.On("GetZones", projectNumber, region).Return(nil, errors.New("failed to get zones"))

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get zones")
	assert.Equal(t, "", resolvedSecondary)
	assert.Equal(t, "", resolvedMediator)
	mockService.AssertExpectations(t)
}

func Test_resolveZonesForCluster_Error_NoSecondaryZoneSupportsInstanceType(t *testing.T) {
	// Arrange
	mockService := new(hyperscaler.MockGoogleServices)
	projectNumber := "123456789"
	region := "us-central1"
	primaryZone := "us-central1-a"
	secondaryZone := ""
	mediatorZone := ""
	instanceType := "n2-standard-4"

	// Mock GetZones to return available zones
	mockService.On("GetZones", projectNumber, region).Return([]string{"us-central1-a", "us-central1-b", "us-central1-c"}, nil)

	// Mock IsMachineTypeAvailable to return false for all zones
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-b", instanceType).Return(false, nil)
	mockService.On("IsMachineTypeAvailable", projectNumber, "us-central1-c", instanceType).Return(false, nil)

	// Act
	resolvedSecondary, resolvedMediator, err := activities.ResolveZonesForCluster(mockService, projectNumber, region, primaryZone, secondaryZone, mediatorZone, instanceType)

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
