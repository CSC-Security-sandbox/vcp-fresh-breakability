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
	models "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	coremodel "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs"
	vmrs_decision "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vmrs/decision"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/api/iam/v1"
	"google.golang.org/api/servicenetworking/v1"
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
		origGetSubnetwork := activities.GetOrCreateSubnetwork
		defer func() {
			activities.GetGCPService = origGetGCPService
			activities.GetOrCreateSubnetwork = origGetSubnetwork
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := activity.CreateOrGetSubnetwork(ctx, params, tenantProjectNumber)
		if err == nil || !strings.Contains(err.Error(), "gcp service error") {
			t.Errorf("expected error from GetGCPService, got %v", err)
		}
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSubnetwork succeeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}

		mockSvc := &google.GcpServices{}
		origGetGCPService := activities.GetGCPService
		origGetSubnetwork := activities.GetOrCreateSubnetwork
		defer func() {
			activities.GetGCPService = origGetGCPService
			activities.GetOrCreateSubnetwork = origGetSubnetwork
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		expected := &commonparams.TenancyInfo{}
		activities.GetOrCreateSubnetwork = func(se database.Storage, service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*commonparams.TenancyInfo, error) {
			return expected, nil
		}
		result, err := activity.CreateOrGetSubnetwork(ctx, params, tenantProjectNumber)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != expected {
			t.Errorf("expected %v, got %v", expected, result)
		}
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSubnetwork fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}

		mockSvc := &google.GcpServices{}
		origGetGCPService := activities.GetGCPService
		origGetSubnetwork := activities.GetOrCreateSubnetwork
		defer func() {
			activities.GetGCPService = origGetGCPService
			activities.GetOrCreateSubnetwork = origGetSubnetwork
		}()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetOrCreateSubnetwork = func(se database.Storage, service hyperscaler.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*commonparams.TenancyInfo, error) {
			return nil, errors.New("subnetwork error")
		}
		_, err := activity.CreateOrGetSubnetwork(ctx, params, tenantProjectNumber)
		if err == nil || !strings.Contains(err.Error(), "subnetwork error") {
			t.Errorf("expected error from GetSubnetwork, got %v", err)
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
			"test-deployment-datasvm-gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.DefaultLIFTypeIscsi: {
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

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig)

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
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeIscsi: {
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, errors.New("connection error"))

	svm, err := activity.SaveSVMAndLifData(ctx, pool, vlmConfig)

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
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockStorage.On("CreateSVM", ctx, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", ctx, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	svm, err := activity.SaveSVMAndLifData(ctx, pool, vlmConfig)

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
			"test-deployment-datasvm-gcnv-default-svm": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}},
	}, nil)

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig)

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
			"test-deployment-datasvm-gcnv": {
				Svmname: "test-svm",
				Svmuuid: "test-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.DefaultLIFTypeIscsi: {
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
	mockStorage.On("CreateLif", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create LIF"))

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create LIF")
	mockStorage.AssertExpectations(t)
}

func Test_CreateVlmConfig_Success(t *testing.T) {
	activity := activities.PoolActivity{}
	prepareVLMConfigForVLMClient := activities.PrepareVlmConfigForVLMClient
	defer func() {
		activities.PrepareVlmConfigForVLMClient = prepareVLMConfigForVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}

	activities.PrepareVlmConfigForVLMClient = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return nil
	}
	assert.NotNil(t, cfg)

	_, err := activity.CreateVlmConfig(ctx, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", &vmrs.Decision{}, "test-sa-email", "test-auto-tier-bucket")

	assert.NoError(t, err)
}

func Test_CreateVlmConfig_FailsToPrepareConfig(t *testing.T) {
	activity := activities.PoolActivity{}
	prepareVLMConfigForVLMClient := activities.PrepareVlmConfigForVLMClient
	defer func() {
		activities.PrepareVlmConfigForVLMClient = prepareVLMConfigForVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	cfg := &vlmconfig.VLMConfig{}

	activities.PrepareVlmConfigForVLMClient = func(cfg *vlmconfig.VLMConfig, deploymentName, region, zone1, zone2, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config")
	}
	assert.NotNil(t, cfg)

	_, err := activity.CreateVlmConfig(ctx, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", "test-subnet", "test-project", "test-sn-host-project", &vmrs.Decision{}, "test-sa-email", "test-auto-tier-bucket")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_UpdateVSACluster_Success(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	getVLMClient := activities.GetVLMClient

	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}

	cred := vlmconfig.OntapCredentials{}
	curr := &vlmconfig.VLMConfig{}
	newConfig := &vlmconfig.VLMConfig{}

	mockVlmClient.On("VSAClusterDeployUpdate", ctx, cred, curr, newConfig).Return(nil, nil)

	_, err := activity.UpdateVSACluster(ctx, curr, newConfig, cred)

	assert.NoError(t, err)
	mockVlmClient.AssertExpectations(t)
}

func Test_UpdateVSACluster_Failure(t *testing.T) {
	mockVlmClient := new(vlm.MockClientFactory)
	activity := activities.PoolActivity{}
	getVLMClient := activities.GetVLMClient
	defer func() {
		activities.GetVLMClient = getVLMClient
	}()
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	cred := vlmconfig.OntapCredentials{}
	curr := &vlmconfig.VLMConfig{}
	newConfig := &vlmconfig.VLMConfig{}

	activities.GetVLMClient = func(ctx context.Context, logger log.Logger, vlmConfig *vlmconfig.VLMConfig) vlm.ClientFactory {
		return mockVlmClient
	}
	mockVlmClient.On("VSAClusterDeployUpdate", ctx, cred, curr, newConfig).Return(nil, errors.New("failed to update VSA cluster"))

	_, err := activity.UpdateVSACluster(ctx, curr, newConfig, cred)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update VSA cluster")
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
		err := activity.ReleaseSubnet(ctx, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestPoolActivity_ReleaseSubnet(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
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

	t.Run("GetGCPServiceFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		originalGetGCPService := activities.GetGCPService
		defer func() { activities.GetGCPService = originalGetGCPService }()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool}, nil)

		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "gcp service error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("ReleaseSubnetworkFails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		mgs := hyperscaler.NewMockGoogleServices(t)
		originalGetGCPService := activities.GetGCPService
		defer func() { activities.GetGCPService = originalGetGCPService }()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool}, nil)
		mgs.On("GetLogger").Return(logger).Maybe()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
			return errors.New("release error")
		}
		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "release error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		mgs := hyperscaler.NewMockGoogleServices(t)
		originalGetGCPService := activities.GetGCPService
		defer func() { activities.GetGCPService = originalGetGCPService }()
		activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		mockStorage.On("ListPools", ctx, mock.Anything).Return([]*datamodel.PoolView{pool}, nil)
		mgs.On("GetLogger").Return(logger).Maybe()
		releaseSubnet := activities.ReleaseSubnet
		defer func() { activities.ReleaseSubnet = releaseSubnet }()
		activities.ReleaseSubnet = func(service hyperscaler.GoogleServices, snHost, subnetName string) error {
			return nil
		}

		err := activity.ReleaseSubnet(ctx, &rawPool)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
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
		existingFirewall := &models.Firewall{
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

// Unit tests for _getSubnetwork
func Test_getSubnetwork(t *testing.T) {
	params := commonparams.CreatePoolParams{
		AccountName:    "customer-project",
		Region:         "us-central1",
		VendorSubNetID: "vpc-123",
	}
	tenantProjectNumber := "tenant-456"
	logger := log.NewLogger()

	t.Run("snHostProject found and getSubnetToBeUsed succeeds", func(t *testing.T) {
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockStorage := database.NewMockStorage(t)

		mockService.On("GetLogger").Return(logger)
		mockService.On("GetSnHost", tenantProjectNumber).Return("sn-host", nil)
		expectedSubnet := &models.Subnet{
			Name:           "subnet-1",
			IpCidrRange:    "10.0.0.0/24",
			Network:        "projects/sn-host/global/networks/test-network",
			GatewayAddress: "10.0.0.1",
		}
		originalGetSubnetToBeUsed := activities.GetSubnetToBeUsed
		activities.GetSubnetToBeUsed = func(service hyperscaler.GoogleServices, se database.Storage, customerProjectNumber, tenantProjectNumber, snHostProject, tenantProjectRegion string) (*models.Subnet, error) {
			return expectedSubnet, nil
		}
		defer func() {
			activities.GetSubnetToBeUsed = originalGetSubnetToBeUsed
		}()
		info, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, tenantProjectNumber, info.RegionalTenantProject)
		assert.Equal(t, "test-network", info.Network)
		assert.Equal(t, []string{"subnet-1"}, info.SubnetworkNames)
		assert.Equal(t, "sn-host", info.SnHostProject)
		assert.Equal(t, "10.0.0.1", info.Gateway)
		mockService.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("snHostProject not found, CreateSubnetwork succeeds", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(logger)
		mockService.On("GetSnHost", tenantProjectNumber).Return("", nil)
		expectedSubnet := &models.Subnet{
			Name:           "subnet-2",
			IpCidrRange:    "10.0.1.0/24",
			Network:        "projects/sn-host/global/networks/test-network2",
			GatewayAddress: "10.0.1.1",
		}
		originalCreateSubnetwork := activities.CreateSubnetwork
		activities.CreateSubnetwork = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, region *string) (*models.Subnet, error) {
			return expectedSubnet, nil
		}
		defer func() {
			activities.CreateSubnetwork = originalCreateSubnetwork
		}()
		info, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.NoError(t, err)
		assert.Equal(t, "test-network2", info.Network)
		assert.Equal(t, []string{"subnet-2"}, info.SubnetworkNames)
		mockService.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetSnHost returns error not containing 'not found'", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(logger)
		mockService.On("GetSnHost", tenantProjectNumber).Return("", errors.New("some error"))
		_, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.Error(t, err)
		mockService.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("getSubnetToBeUsed returns error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(logger)
		mockService.On("GetSnHost", tenantProjectNumber).Return("sn-host", nil)
		activities.GetSubnetToBeUsed = func(service hyperscaler.GoogleServices, se database.Storage, customerProjectNumber, tenantProjectNumber, snHostProject, tenantProjectRegion string) (*models.Subnet, error) {
			return nil, errors.New("subnet error")
		}
		_, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.Error(t, err)
		mockService.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("CreateSubnetwork returns error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetSnHost", tenantProjectNumber).Return("", nil)
		mockService.On("GetLogger").Return(logger)

		originalCreateSubnetwork := activities.CreateSubnetwork
		activities.CreateSubnetwork = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, region *string) (*models.Subnet, error) {
			return nil, errors.New("create subnet error")
		}
		defer func() {
			activities.CreateSubnetwork = originalCreateSubnetwork
		}()
		_, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.Error(t, err)
		mockService.AssertExpectations(t)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ParseProjectId returns error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		mockService := hyperscaler.NewMockGoogleServices(t)
		mockService.On("GetLogger").Return(logger)
		mockService.On("GetSnHost", tenantProjectNumber).Return("", nil)
		expectedSubnet := &models.Subnet{
			Name:           "subnet-3",
			IpCidrRange:    "10.0.2.0/24",
			Network:        "invalid-network",
			GatewayAddress: "10.0.2.1",
		}
		originalCreateSubnetwork := activities.CreateSubnetwork
		activities.CreateSubnetwork = func(service hyperscaler.GoogleServices, tenantProjectNumber, consumerVPC string, region *string) (*models.Subnet, error) {
			return expectedSubnet, nil
		}
		defer func() {
			activities.CreateSubnetwork = originalCreateSubnetwork
		}()
		_, err := activities.GetOrCreateSubnetwork(mockStorage, mockService, params, tenantProjectNumber)
		assert.Error(t, err)
		mockService.AssertExpectations(t)
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
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "cached-password"}}, nil
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
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "secret-manager-password"}}, nil
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
		activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*models.CustomSecret, error) {
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
	activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, secretID string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return nil
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", []string{"test-subnet"}, "test-project", "test-sn-host-project", "test-tenant-project@xyz.com", "test-tenant-project")

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
	activities.GetPasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, userName string) (*models.CustomSecret, error) {
		return &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "password"}}, nil
	}

	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config")
	}

	customerRequestedPerformance := &vmrs.CustomerRequestedPerformance{}
	_, err := activity.IdentifyVMs(ctx, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", []string{"test-subnet"}, "test-project", "test-sn-host-project", "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", []string{"test-subnet"}, "test-project", "test-sn-host-project", "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", []string{"test-subnet"}, "test-project", "test-sn-host-project", "test-tenant-project@xyz.com", "test-tenant-project")

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
	_, err := activity.IdentifyVMs(ctx, "test-path", *customerRequestedPerformance, "test-deployment", "test-region", "test-zone1", "test-zone2", "test-network", []string{"test-subnet"}, "test-project", "test-sn-host-project", "test-tenant-project@xyz.com", "test-tenant-project")

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
		mockGCP.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&models.CustomSecret{}, fmt.Errorf("get secret error"))

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

	cert := &models.CustomCertificate{}
	secret := &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{}}

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
		secretNoVersion := &models.CustomSecret{}
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
		expectedRecord := &models.CustomCloudDNSRecord{RecordName: recordName, Data: ipAddress}

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
	activities.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, ip, recordName string) (*models.CustomCloudDNSRecord, error) {
		return &models.CustomCloudDNSRecord{RecordName: recordName}, nil
	}

	activities.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Logger: log.NewLogger()}, nil
	}
	// Success case
	t.Run("success", func(t *testing.T) {
		vlmConfig := &vlmconfig.VLMConfig{
			Cloud: vlmconfig.CloudConfig{
				HAPairs: []vlmconfig.HAPair{
					{
						VM1: vlmconfig.VMConfig{
							SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
								vlmconfig.LIFTypeNodeMgmt: {IP: "1.1.1.1"},
							},
						},
						VM2: vlmconfig.VMConfig{
							SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
								vlmconfig.LIFTypeNodeMgmt: {IP: "2.2.2.2"},
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
		vlmConfig := &vlmconfig.VLMConfig{
			Cloud: vlmconfig.CloudConfig{
				HAPairs: []vlmconfig.HAPair{},
			},
		}
		pa := &activities.PoolActivity{}
		hostMap, err := pa.CreateCloudDNSRecords(ctx, vlmConfig, clusterName, commonparams.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Nil(t, hostMap)
	})

	// No SystemLIFs
	t.Run("no SystemLIFs", func(t *testing.T) {
		vlmConfig := &vlmconfig.VLMConfig{
			Cloud: vlmconfig.CloudConfig{
				HAPairs: []vlmconfig.HAPair{
					{
						VM1: vlmconfig.VMConfig{SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{}},
						VM2: vlmconfig.VMConfig{SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{}},
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
		activities.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler.GoogleServices, ip, recordName string) (*models.CustomCloudDNSRecord, error) {
			return nil, fmt.Errorf("dns error")
		}
		vlmConfig := &vlmconfig.VLMConfig{
			Cloud: vlmconfig.CloudConfig{
				HAPairs: []vlmconfig.HAPair{
					{
						VM1: vlmconfig.VMConfig{
							SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
								vlmconfig.LIFTypeNodeMgmt: {IP: "1.1.1.1"},
							},
						},
						VM2: vlmconfig.VMConfig{
							SystemLIFs: map[vlmconfig.VSALIFType]vlmconfig.LIFConfig{
								vlmconfig.LIFTypeNodeMgmt: {IP: "2.2.2.2"},
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
		vlmConfig := &vlmconfig.VLMConfig{}
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
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*models.CustomCertificateResponse, error) {
			return &models.CustomCertificateResponse{
				Certificate: &models.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
					RootCACertificate:   "root",
				},
				Secret: &models.CustomSecret{
					SecretVersion: &models.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{
				SecretVersion: &models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		creds, err := activity.CreateOnTapCredentials(ctx, pool, region, clusterName)
		assert.NoError(t, err)
		assert.Equal(t, "CN", creds.Certificate.CommonName)
		assert.Equal(t, "cert", creds.Certificate.Certificate)
		assert.Equal(t, "key", creds.Certificate.PrivateKey)
		assert.Equal(t, []string{"chain"}, creds.Certificate.InterMediateCertificate)
		assert.Equal(t, "root", creds.Certificate.CaCertificate)
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
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*models.CustomCertificateResponse, error) {
			return &models.CustomCertificateResponse{
				Certificate: &models.CustomCertificate{
					SubjectCommonName:   "CN",
					PemCertificate:      "cert",
					PemCertificateChain: []string{"chain"},
				},
				Secret: &models.CustomSecret{
					SecretVersion: &models.CustomSecretVersion{Value: "key"},
				},
			}, nil
		}
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*models.CustomSecret, error) {
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
		activities.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler.GoogleServices, region, certificateID, clusterName string) (*models.CustomCertificateResponse, error) {
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
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*models.CustomSecret, error) {
			return &models.CustomSecret{
				SecretVersion: &models.CustomSecretVersion{Value: "pwd"},
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
		activities.GeneratePasswordForVSACluster = func(gcpService hyperscaler.GoogleServices, projectID, region, secretID string) (*models.CustomSecret, error) {
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
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&models.CustomSecret{}, nil)
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
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", fmt.Errorf("revoke error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "revoke error")
	})
	t.Run("GetSecretWithLatestVersion fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(nil, errors.New("get secret error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "get secret error")
	})

	t.Run("DeleteSecret fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&models.CustomSecret{}, nil)
		mockGcpService.On("DeleteSecret", mock.Anything, mock.Anything).Return(fmt.Errorf("delete secret error"))

		err := activities.RevokeCertificateAndDeleteFromCacheAndSecretManager(mockGcpService, certificateID)
		assert.EqualError(t, err, "delete secret error")
	})

	t.Run("RemoveFromCertAuthCache fails", func(t *testing.T) {
		mockGcpService := new(hyperscaler.MockGoogleServices)
		mockGcpService.On("GetLogger").Return(log.NewLogger())
		mockGcpService.On("GetCertificate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&models.CustomCertificate{}, nil)
		mockGcpService.On("RevokeCertificate", mock.Anything).Return("", nil)
		mockGcpService.On("GetSecretWithLatestVersion", mock.Anything, mock.Anything).Return(&models.CustomSecret{}, nil)
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
		cert := &models.CustomCertificate{
			SubjectCommonName:   "test-cn",
			PemCertificate:      "pem-cert",
			PemCertificateChain: []string{"chain1", "chain2"},
		}
		secret := &models.CustomSecret{
			SecretVersion: &models.CustomSecretVersion{Value: "private-key"},
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
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *models.CustomCertificateParam, pemBlock pem.Block) (*models.CustomCertificate, error) {
			return cert, nil
		}
		activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService hyperscaler.GoogleServices, certificate *models.CustomCertificate, key *rsa.PrivateKey) (*models.CustomCertificate, *models.CustomSecret, error) {
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
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *models.CustomCertificateParam, pemBlock pem.Block) (*models.CustomCertificate, error) {
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
		activities.GetOrCreateCertificateInCASAndPrivateKeyInSM = func(gcpService hyperscaler.GoogleServices, certificate *models.CustomCertificate, key *rsa.PrivateKey) (*models.CustomCertificate, *models.CustomSecret, error) {
			return nil, nil, expectedErr
		}

		// Patch ValidateAndConvertCertificateParamsToCustomCertificate to return dummy cert
		origValidate := commonparams.ValidateAndConvertCertificateParamsToCustomCertificate
		commonparams.ValidateAndConvertCertificateParamsToCustomCertificate = func(param *models.CustomCertificateParam, pemBlock pem.Block) (*models.CustomCertificate, error) {
			return &models.CustomCertificate{}, nil
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
		expectedSecret := &models.CustomSecret{
			SecretVersion: &models.CustomSecretVersion{Value: "super-secret"},
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
		mockCertResp := &models.CustomCertificateResponse{
			Certificate: &models.CustomCertificate{
				CertificateID:       "signed-cert",
				SubjectCommonName:   "common-name",
				PemCertificateChain: []string{"intermediate"},
			},
			Secret: &models.CustomSecret{
				SecretVersion: &models.CustomSecretVersion{Value: "private-key"},
			},
		}
		activities.GetCertificateAndPrivateKeyByID = func(gcpService hyperscaler.GoogleServices, caDeployedProjectID, secretManagerProjectID, region, caPoolName, certificateID string) (*models.CustomCertificateResponse, error) {
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
	cert := &models.CustomCertificate{
		CertificateID:     "test-cert",
		Region:            "us-central1",
		SubjectCommonName: "test-cn",
	}
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	expectedSecret := &models.CustomSecret{SecretVersion: &models.CustomSecretVersion{Value: "private-key"}}

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
	certificate := &models.CustomCertificate{
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

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *models.CustomCertificate, key *rsa.PrivateKey) (*models.CustomSecret, error) {
			return &models.CustomSecret{}, nil
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
		mockSecret := &models.CustomSecret{Name: "secret"}
		originalGetAndCreatePrivateKeyInSecretManagerAndCache := activities.GetOrCreatePrivateKeyInSecretManagerAndCache
		defer func() {
			activities.GetOrCreatePrivateKeyInSecretManagerAndCache = originalGetAndCreatePrivateKeyInSecretManagerAndCache
		}()

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *models.CustomCertificate, key *rsa.PrivateKey) (*models.CustomSecret, error) {
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

		activities.GetOrCreatePrivateKeyInSecretManagerAndCache = func(gcpService hyperscaler.GoogleServices, cert *models.CustomCertificate, key *rsa.PrivateKey) (*models.CustomSecret, error) {
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
	subnetCreated := &servicenetworking.Subnetwork{
		Name:    "subnet-foo",
		Network: "projects/sn-host-project/global/networks/test-network",
	}
	subnetBytes, _ := json.Marshal(subnetCreated)
	expectedSubnet := &models.Subnet{
		Name:           "subnet-foo",
		Network:        "projects/sn-host-project/global/networks/test-network",
		IpCidrRange:    "10.0.0.0/24",
		GatewayAddress: "10.0.0.1",
	}

	t.Run("success", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		mockSvc.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(subnetBytes, nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))
		mockSvc.On("GetSubnetwork", "sn-host-project", region, "subnet-foo").
			Return(expectedSubnet, nil)

		subnet, err := activities.CreateSubnetwork(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.NoError(t, err)
		assert.Equal(t, expectedSubnet, subnet)
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
		mockSvc.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(nil, errors.New("create failed"))
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		subnet, err := activities.CreateSubnetwork(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.Error(t, err)
		assert.Nil(t, subnet)
		mockSvc.AssertExpectations(t)
	})

	t.Run("jsonUnmarshalFails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		mockSvc.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, region, subnetName).
			Return([]byte("invalid-json"), nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		subnet, err := activities.CreateSubnetwork(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.Error(t, err)
		assert.Nil(t, subnet)
		mockSvc.AssertExpectations(t)
	})

	t.Run("ParseProjectId fails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		badNetwork := &servicenetworking.Subnetwork{
			Name:    "subnet-foo",
			Network: "bad-format",
		}
		badBytes, _ := json.Marshal(badNetwork)
		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		mockSvc.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(badBytes, nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		subnet, err := activities.CreateSubnetwork(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.Error(t, err)
		assert.Nil(t, subnet)
		mockSvc.AssertExpectations(t)
	})

	t.Run("GetSubnetworkFails", func(t *testing.T) {
		mockSvc := hyperscaler.NewMockGoogleServices(t)

		subnetName := "vsa-654321-" + strconv.Itoa(int(time.Now().Unix()))
		makeSubnetName := activities.MakeSubnetName
		defer func() { activities.MakeSubnetName = makeSubnetName }()
		activities.MakeSubnetName = func(projectNumber string) string {
			return subnetName
		}
		mockSvc.On("CreateSubnetworkForTenantProject", tenantProjectNumber, consumerVPC, region, subnetName).
			Return(subnetBytes, nil)
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))
		mockSvc.On("GetSubnetwork", "sn-host-project", region, "subnet-foo").
			Return(nil, errors.New("get subnet failed"))

		subnet, err := activities.CreateSubnetwork(mockSvc, tenantProjectNumber, consumerVPC, &region)
		assert.Error(t, err)
		assert.Nil(t, subnet)
		mockSvc.AssertExpectations(t)
	})
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallEdited", func(t *testing.T) {
		mgs := hyperscaler.NewMockGoogleServices(t)
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
		existingFirewall := &models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &models.Firewall{
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
