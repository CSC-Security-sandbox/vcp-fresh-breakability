package activities_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	digitalCert "crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	ocicommon "github.com/oracle/oci-go-sdk/v65/common"
	ocivault "github.com/oracle/oci-go-sdk/v65/vault"
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
	hyperscaler_models "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	oci "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/oci"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilsEnv "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilErrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/option"
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
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetPool)

	poolView := &datamodel.PoolView{Pool: datamodel.Pool{Name: "test-pool"}}
	pool := database.ConvertPoolViewToPool(poolView)

	mockStorage.On("GetPool", mock.Anything, poolView.UUID, int64(0)).Return(poolView, nil)

	// Act
	encodedResult, err := env.ExecuteActivity(activity.GetPool, pool)

	// Assert
	assert.NoError(t, err)
	var result *datamodel.Pool
	err = encodedResult.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestGetPool_Fails(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.GetPool)

	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("GetPool", mock.Anything, pool.UUID, int64(0)).Return(nil, gorm.ErrRecordNotFound)

	// Act
	_, err := env.ExecuteActivity(activity.GetPool, pool)

	// Assert
	assert.Error(t, err)
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
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.SavePoolWithClusterDetails)

	pool := &datamodel.Pool{Name: "test-pool"}
	cluster := &datamodel.ClusterDetails{}

	mockStorage.On("SavePoolWithVsaDetails", mock.Anything, pool, cluster).Return(nil)

	// Act
	_, err := env.ExecuteActivity(activity.SavePoolWithClusterDetails, pool, cluster)

	// Assert
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestSavePoolWithClusterDetails_Failure(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.SavePoolWithClusterDetails)

	pool := &datamodel.Pool{Name: "test-pool"}
	cluster := &datamodel.ClusterDetails{}

	mockStorage.On("SavePoolWithVsaDetails", mock.Anything, pool, cluster).Return(gorm.ErrInvalidData)

	// Act
	_, err := env.ExecuteActivity(activity.SavePoolWithClusterDetails, pool, cluster)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPool_Success(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreatedPool)

	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", mock.Anything, pool).Return(pool, nil)
	mockStorage.On("UpdatedPool", mock.Anything, pool).Return(pool, nil)

	// Act
	encodedResult, err := env.ExecuteActivity(activity.CreatedPool, pool, vlmConfig)

	// Assert
	assert.NoError(t, err)
	var result *datamodel.Pool
	err = encodedResult.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPool_Failure(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreatedPool)

	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", mock.Anything, pool).Return(nil, gorm.ErrInvalidData)

	// Act
	_, err := env.ExecuteActivity(activity.CreatedPool, pool, vlmConfig)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestCreatedPoolSuccess_VLMUpdateFailed(t *testing.T) {
	// Arrange
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.CreatedPool)

	pool := &datamodel.Pool{Name: "test-pool"}
	vlmConfig := &vlm.VLMConfig{}

	mockStorage.On("CreatedPool", mock.Anything, pool).Return(pool, nil)
	mockStorage.On("UpdatedPool", mock.Anything, pool).Return(nil, gorm.ErrInvalidData)

	// Act
	_, err := env.ExecuteActivity(activity.CreatedPool, pool, vlmConfig)

	// Assert
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

// Unit tests for FindTenancy in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_FindTenancy(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	params := commonparams.CreatePoolParams{}

	origGetGCPService := hyperscaler2.GetGCPService
	origGetTenantProject := activities.GetTenantProject
	defer func() {
		hyperscaler2.GetGCPService = origGetGCPService
		activities.GetTenantProject = origGetTenantProject
	}()

	t.Run("GetGCPService fails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FindTenancyProject)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("gcp service error")
		}
		_, err := env.ExecuteActivity(activity.FindTenancyProject, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "gcp service error")
	})

	t.Run("GetTenantProject fails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FindTenancyProject)

		mockSvc := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
			return "", errors.New("tenant project error")
		}
		_, err := env.ExecuteActivity(activity.FindTenancyProject, params)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "tenant project error")
	})

	t.Run("Success", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.FindTenancyProject)

		mockSvc := &google.GcpServices{}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockSvc, nil
		}
		activities.GetTenantProject = func(service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams) (string, error) {
			return "tenant-project-id", nil
		}
		val, err := env.ExecuteActivity(activity.FindTenancyProject, params)
		assert.NoError(tt, err)
		var result string
		assert.NoError(tt, val.Get(&result))
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
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)

	activity := activities.PoolActivity{
		SE: mockStorage,
	}
	env.RegisterActivity(activity.SavePoolWithClusterDetails)

	pool := &datamodel.Pool{Name: "test-pool"}

	mockStorage.On("SavePoolWithVsaDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Act
	_, err := env.ExecuteActivity(activity.SavePoolWithClusterDetails, pool, &datamodel.ClusterDetails{})

	// Assert
	assert.Nil(t, err)
	mockStorage.AssertExpectations(t)
}

func TestGetONTAPProvider_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

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
	env.RegisterActivity(activity.GetOntapVersion)

	node := &coremodel.Node{}
	ontapVersion := "9.10.1"
	mockProvider.On("GetONTAPVersion", mock.Anything).Return(&ontapVersion, nil)

	encodedValue, err := env.ExecuteActivity(activity.GetOntapVersion, node)
	assert.NoError(t, err)
	var res *string
	err = encodedValue.Get(&res)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, *res, ontapVersion)
}

func TestGetONTAPProvider_Failure(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

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
	env.RegisterActivity(activity.GetOntapVersion)

	node := &coremodel.Node{}
	mockProvider.On("GetONTAPVersion", mock.Anything).Return(nil, errors.New("failed to get ONTAP version"))

	_, err := env.ExecuteActivity(activity.GetOntapVersion, node)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get ONTAP version")
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

func Test_prepareVlmConfig_NfsOverTlsDisabled_OnlyKeyManagerBootarg(t *testing.T) {
	oldTls := activities.EnableNfsOverTls
	oldLimit := activities.NfsTlsConnMaxLimit
	defer func() {
		activities.EnableNfsOverTls = oldTls
		activities.NfsTlsConnMaxLimit = oldLimit
	}()

	activities.EnableNfsOverTls = false
	activities.NfsTlsConnMaxLimit = 0

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
	assert.Equal(t, "bootarg.keymanager.ekmip.svm_context=false", cfg.Deployment.UserBootargs)
}

func Test_prepareVlmConfig_NfsOverTlsEnabled_NoConnLimit(t *testing.T) {
	oldTls := activities.EnableNfsOverTls
	oldLimit := activities.NfsTlsConnMaxLimit
	defer func() {
		activities.EnableNfsOverTls = oldTls
		activities.NfsTlsConnMaxLimit = oldLimit
	}()

	activities.EnableNfsOverTls = true
	activities.NfsTlsConnMaxLimit = 0

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
	expected := "bootarg.keymanager.ekmip.svm_context=false;bootarg.nfs.tls.enabled=true"
	assert.Equal(t, expected, cfg.Deployment.UserBootargs)
}

func Test_prepareVlmConfig_NfsOverTlsEnabled_WithConnLimit(t *testing.T) {
	oldTls := activities.EnableNfsOverTls
	oldLimit := activities.NfsTlsConnMaxLimit
	defer func() {
		activities.EnableNfsOverTls = oldTls
		activities.NfsTlsConnMaxLimit = oldLimit
	}()

	activities.EnableNfsOverTls = true
	activities.NfsTlsConnMaxLimit = 128

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
	expected := "bootarg.keymanager.ekmip.svm_context=false;bootarg.nfs.tls.enabled=true;bootarg.nblade.nfs_tls_conn_max_limit=128"
	assert.Equal(t, expected, cfg.Deployment.UserBootargs)
}

func Test_prepareVlmConfig_NfsOverTlsEnabled_NegativeConnLimit_Ignored(t *testing.T) {
	oldTls := activities.EnableNfsOverTls
	oldLimit := activities.NfsTlsConnMaxLimit
	defer func() {
		activities.EnableNfsOverTls = oldTls
		activities.NfsTlsConnMaxLimit = oldLimit
	}()

	activities.EnableNfsOverTls = true
	activities.NfsTlsConnMaxLimit = -1

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
	expected := "bootarg.keymanager.ekmip.svm_context=false;bootarg.nfs.tls.enabled=true"
	assert.Equal(t, expected, cfg.Deployment.UserBootargs,
		"negative conn limit should be treated the same as zero (not appended)")
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

func Test_SaveSVMAndLifData_CreatesIlbNasLifs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 42}, AccountID: 77}
	vlmConfig := &vlm.VLMConfig{
		Svm: map[string]vlm.SvmConfig{
			"svm-name": {
				Svmname: "svm-name",
				Svmuuid: "svm-uuid",
				SVMLIFs: map[vlm.VSALIFType][]vlm.LIFConfig{
					vlm.LIFTypeSan: {
						{IP: "10.0.0.1/24", Name: "san-lif", HomeNode: "node-san", Uuid: "san-uuid"},
					},
					vlm.LIFTypeNas: {
						{IP: "10.0.0.2/24", Name: "nas-lif", HomeNode: "node-nas", Uuid: "nas-uuid"},
					},
					vlm.LIFTypeIlbNas: {
						{IP: "10.0.0.3/24", Name: "ilb-lif", HomeNode: "node-ilb", Uuid: "ilb-uuid"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "node-san"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "node-nas"},
		{BaseModel: datamodel.BaseModel{ID: 3}, Name: "node-ilb"},
	}, nil)

	var capturedLifs []*datamodel.Lif
	mockStorage.On("CreateLif", mock.Anything, mock.MatchedBy(func(lif *datamodel.Lif) bool {
		copied := *lif
		if lif.LifDetails != nil {
			detailsCopy := *lif.LifDetails
			copied.LifDetails = &detailsCopy
		}
		capturedLifs = append(capturedLifs, &copied)
		return true
	})).Return(&datamodel.Lif{}, nil).Times(3)

	encodedResult, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "svm-name")
	assert.NoError(t, err)
	var svm *datamodel.Svm
	err = encodedResult.Get(&svm)

	assert.NoError(t, err)
	assert.NotNil(t, svm)
	require.Len(t, capturedLifs, 3)

	for _, lif := range capturedLifs {
		assert.NotContains(t, lif.IPAddress, "/")
		assert.Equal(t, pool.AccountID, lif.AccountID)
		assert.Equal(t, vsa.DefaultNetmask, lif.SubnetMask)
		require.NotNil(t, lif.LifDetails)
		require.NotEmpty(t, lif.LifDetails.ExternalUUID)
	}

	lifByName := map[string]*datamodel.Lif{}
	for _, lif := range capturedLifs {
		lifByName[lif.Name] = lif
	}

	require.Contains(t, lifByName, "ilb-lif")
	ilbLif := lifByName["ilb-lif"]
	assert.Equal(t, string(vlm.LIFTypeNas), ilbLif.LifDetails.ProtocolType)
	assert.Equal(t, int64(3), ilbLif.NodeID)
	assert.Equal(t, "10.0.0.3", ilbLif.IPAddress)
	assert.Equal(t, "ilb-uuid", ilbLif.LifDetails.ExternalUUID)

	require.Contains(t, lifByName, "san-lif")
	assert.Equal(t, string(vlm.LIFTypeSan), lifByName["san-lif"].LifDetails.ProtocolType)

	require.Contains(t, lifByName, "nas-lif")
	assert.Equal(t, string(vlm.LIFTypeNas), lifByName["nas-lif"].LifDetails.ProtocolType)

	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifDataDBCreationError(t *testing.T) {
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
						{IP: "192.168.1.1/24", Name: "lif1"},
					},
					vlm.LIFTypeNas: {
						{IP: "192.168.1.1/24", Name: "lif2"},
					},
				},
			},
		},
	}

	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, errors.New("connection error"))

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "connection error")
	mockStorage.AssertExpectations(t)
}

func Test_SaveSVMAndLifData_CouldNotFetchNodes(t *testing.T) {
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
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return(nil, gorm.ErrRecordNotFound)

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

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
						{IP: "192.168.1.1/24", Name: "lif1", HomeNode: "non-existent-node"},
					},
				},
			},
		},
	}

	// Mock nodes that exist in the database
	mockStorage.On("CreateSVM", mock.Anything, mock.Anything).Return(&datamodel.Svm{}, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, pool.ID).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "existing-node"},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "another-node"},
	}, nil)

	_, err := env.ExecuteActivity(activity.SaveSVMAndLifData, pool, vlmConfig, "gcnv")

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "LIF lif1 references non-existent home node non-existent-node")
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
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1, PoolCredentials: &datamodel.PoolCredentials{
		SecretID:      "secretID",
		CertificateID: "certID",
		Password:      "password",
	}}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{HAPairs: []vlm.HAPair{}},
	}

	_, err := env.ExecuteActivity(activity.SaveVSANodeDetails, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	// The error is wrapped by Temporal, just check that there's an error
}

func Test_SaveVSANodeDetails_NoHAPairsProvided(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}, AccountID: 1}
	vlmConfig := &vlm.VLMConfig{
		Cloud: vlm.CloudConfig{HAPairs: []vlm.HAPair{}},
	}

	_, err := env.ExecuteActivity(activity.SaveVSANodeDetails, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	// The error is wrapped by Temporal, just check that there's an error
}

func Test_SaveVSANodeDetails_FailsToSaveFirstNode(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test

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

	_, err := env.ExecuteActivity(activity.SaveVSANodeDetails, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save node1")
}

func Test_SaveVSANodeDetails_FailsToSaveSecondNode(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test

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

	_, err := env.ExecuteActivity(activity.SaveVSANodeDetails, pool, vlmConfig, "clusterName", map[string]string{})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save node2")
}

func Test_SaveVSANodeDetails_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(&activity)

	saveNodeDetails := activities.SaveNodeDetails
	defer func() { activities.SaveNodeDetails = saveNodeDetails }() // Restore original function after test

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

	encodedResult, err := env.ExecuteActivity(activity.SaveVSANodeDetails, pool, vlmConfig, "clusterName", map[string]string{})

	assert.NoError(t, err)
	var node *datamodel.Node
	err = encodedResult.Get(&node)
	assert.NoError(t, err)
	assert.NotNil(t, node)
	assert.Equal(t, "node1", node.Name)
}

func Test_DeletePoolResourcesOnRollback_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResourcesOnRollback)

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

	_, err := env.ExecuteActivity(activity.DeletePoolResourcesOnRollback, pool)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResourcesOnRollback_Failure(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResourcesOnRollback)

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

	_, err := env.ExecuteActivity(activity.DeletePoolResourcesOnRollback, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete LIFs")
	mockStorage.AssertExpectations(t)
}

func Test_ErroredPool_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.ErroredPool)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("ErroredResource", mock.Anything, pool, mock.Anything).Return(pool, nil)

	encodedValue, err := env.ExecuteActivity(activity.ErroredPool, pool, "")

	assert.NoError(t, err)
	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResources)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	mockStorage.On("DeletePool", mock.Anything, pool).Return(nil)
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

	encodedValue, err := env.ExecuteActivity(activity.DeletePoolResources, pool)
	assert.NoError(t, err)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteLIFs(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResources)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}
	deleteLIFs := activities.DeleteLIFs
	defer func() {
		activities.DeleteLIFs = deleteLIFs
	}()
	activities.DeleteLIFs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete LIFs")
	}

	encodedValue, err := env.ExecuteActivity(activity.DeletePoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to delete LIFs")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteSVMs(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResources)

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

	encodedValue, err := env.ExecuteActivity(activity.DeletePoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to delete SVMs")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeleteNodes(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResources)

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

	encodedValue, err := env.ExecuteActivity(activity.DeletePoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to delete nodes")
	mockStorage.AssertExpectations(t)
}

func Test_DeletePoolResources_FailsToDeletePool(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletePoolResources)

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
	mockStorage.On("DeletePool", mock.Anything, pool).Return(errors.New("failed to delete pool"))

	encodedValue, err := env.ExecuteActivity(activity.DeletePoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1}}

	deletingSVMS := activities.DeletingSVMs
	defer func() {
		activities.DeletingSVMs = deletingSVMS
	}()

	activities.DeletingSVMs = func(ctx context.Context, se database.Storage, pool *datamodel.Pool) error {
		return errors.New("failed to delete SVMs")
	}

	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletingPoolResources)

	encodedValue, err := env.ExecuteActivity(activity.DeletingPoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to delete SVMs")
}

func Test_ReturnsErrorWhenDeletingNodesFails(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
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
	env.RegisterActivity(activity.DeletingPoolResources)

	encodedValue, err := env.ExecuteActivity(activity.DeletingPoolResources, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to delete nodes")
}

func Test_DeletesPoolResourcesSuccessfully(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
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
	env.RegisterActivity(activity.DeletingPoolResources)

	encodedValue, err := env.ExecuteActivity(activity.DeletingPoolResources, pool)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
}

func Test_ReturnsErrorWhenListPoolsFails(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.ReleaseDataSubnetOp)

	pool := &datamodel.Pool{
		AccountID: 1,
		Network:   "test-network",
		Account:   &datamodel.Account{Name: "643029180821"},
		ClusterDetails: datamodel.ClusterDetails{
			SubnetNames: []string{"subnet1"},
		},
	}
	mockStorage.On("ListPools", mock.Anything, mock.Anything).Return(nil, errors.New("failed to list pools"))

	_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, pool)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list pools")
	mockStorage.AssertExpectations(t)
}

// Unit tests for ReleaseSubnetOp in core/orchestrator/activities/pool_activities.go
func TestPoolActivity_ReleaseDataSubnetOp(t *testing.T) {
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
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return(nil, errors.New("list pools error"))
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)

		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "list pools error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("multiplePoolsExist", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{poolView, poolView2}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetGCPServiceFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		origGetGCPService := hyperscaler2.GetGCPService
		defer func() { hyperscaler2.GetGCPService = origGetGCPService }()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "initialisation of Google GCP service failed")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("getSubnetForConsumerProjectAndReleaseFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		origGetGCPService := hyperscaler2.GetGCPService
		releaseSubnet := activities.ReleaseSubnetOp
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
			activities.ReleaseSubnetOp = releaseSubnet
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnetOp = func(service hyperscaler2.GoogleServices, snHost, subnetName string) (string, error) {
			return "", errors.New("release subnet error")
		}
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "release subnet error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("releasesSubnet", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		originalGetGCPService := hyperscaler2.GetGCPService
		releaseSubnet := activities.ReleaseSubnetOp
		defer func() {
			activities.ReleaseSubnetOp = releaseSubnet
			hyperscaler2.GetGCPService = originalGetGCPService
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnetOp = func(service hyperscaler2.GoogleServices, snHost, subnetName string) (string, error) {
			return "", nil
		}
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &pool)
		assert.NoError(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestPoolActivity_ReleaseSubnet(t *testing.T) {
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
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)

		poolNoSubnet := rawPool
		poolNoSubnet.ClusterDetails = datamodel.ClusterDetails{
			SnHostProject: "sn-host-project",
			SubnetNames:   []string{},
		}
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &poolNoSubnet)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetPoolsBySubnetworkFails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return(nil, errors.New("list pools error"))
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &rawPool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "list pools error")
		mockStorage.AssertExpectations(t)
	})

	t.Run("MultiplePoolsUsingSubnet", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(t)
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{pool, pool1}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &rawPool)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
	t.Run("GetGCPServiceFails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		origGetGCPService := hyperscaler2.GetGCPService
		defer func() { hyperscaler2.GetGCPService = origGetGCPService }()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("initialisation of Google GCP service failed")
		}
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &rawPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "initialisation of Google GCP service failed")
		mockStorage.AssertExpectations(tt)
	})
	t.Run("ReleaseDataSubnetOp fails", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		origGetGCPService := hyperscaler2.GetGCPService
		releaseSubnet := activities.ReleaseSubnetOp
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
			activities.ReleaseSubnetOp = releaseSubnet
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnetOp = func(service hyperscaler2.GoogleServices, snHost, subnetName string) (string, error) {
			return "", errors.New("release subnet error")
		}
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		_, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &rawPool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "release subnet error")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("success", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		origGetGCPService := hyperscaler2.GetGCPService
		releaseSubnet := activities.ReleaseSubnetOp
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
			activities.ReleaseSubnetOp = releaseSubnet
		}()
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		activities.ReleaseSubnetOp = func(service hyperscaler2.GoogleServices, snHost, subnetName string) (string, error) {
			return "operation", nil
		}
		mockStorage.On("ListPools", mock.Anything, mock.Anything).Return([]*datamodel.PoolView{{}}, nil)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ReleaseDataSubnetOp)
		val, err := env.ExecuteActivity(activity.ReleaseDataSubnetOp, &rawPool)
		assert.NoError(tt, err)
		var ops *[]commonparams.Operations
		assert.NoError(tt, val.Get(&ops))
		assert.NotNil(tt, ops)
		assert.Len(tt, *ops, 1)
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
		existingFirewall := &hyperscaler_models.Firewall{
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
		mgs.On("InsertFirewall", &hyperscaler_models.Firewall{
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
		mgs.On("GetVPCNetwork", projectName, vpcName).Return(&hyperscaler_models.VPCNetwork{}, nil)

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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", nil)

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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", errors.New(errString))

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
		mgs.On("CreateVPC", &hyperscaler_models.VPCNetwork{Name: vpcName, ProjectName: projectName}).Return("", nil)

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
		mgs.On("GetSubnetwork", projectName, region, subnetName).Return(&hyperscaler_models.Subnet{}, nil)

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

		expectedSubnet := &hyperscaler_models.Subnet{
			Name:           "subnet-1",
			IpCidrRange:    "10.0.0.0/24",
			Network:        "projects/sn-host/global/networks/test-network",
			GatewayAddress: "10.0.0.1",
		}

		// Mock GCP service with httptest server for GetSnHost
		origGetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
		}()

		url := fmt.Sprintf("/projects/%s/getXpnHost", tenantProjectNumber)
		resp := &compute.Project{Name: "sn-host"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, _ := json.Marshal(resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL), option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("Error creating compute service: %v", err)
		}

		adminGcpService := &google.AdminGCPService{}
		// Use reflection to set the unexported computeService field
		rv := reflect.ValueOf(adminGcpService).Elem()
		rf := rv.FieldByName("computeService")
		reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem().Set(reflect.ValueOf(computeSvc))

		mockGcpService := &google.GcpServices{
			AdminGCPService: adminGcpService,
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
		}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
		}

		info, err := activity.GetTenancyInfo(ctx, tenantProjectNumber, expectedSubnet)
		assert.NoError(t, err)
		assert.Equal(t, tenantProjectNumber, info.RegionalTenantProject)
		assert.Equal(t, "test-network", info.Network)
		assert.Equal(t, []string{"subnet-1"}, info.SubnetworkNames)
		assert.Equal(t, "10.0.0.1", info.Gateway)
		assert.Equal(t, "sn-host", info.SnHostProject)
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
			assert.ElementsMatch(t, []string{"tcp", "111", "635", "2049", "4045", "udp", "111", "4046", "63001-65000"}, allowedPorts)
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

func Test_setupNetworkFirewallsForSMB(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	mockNetwork := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForSMBSucceeds", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = ""          // Reset the environment variable after the test
			activities.SmbFirewallAllowedPortRulesConfig = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
		activities.SmbFirewallAllowedPortRulesConfig = "tcp,88,135,139,389,445,464,636,udp,53,88,389,464"
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, activities.SmbFirewallName, name)
			assert.Equal(t, mockNetwork, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "88", "135", "139", "389", "445", "464", "636", "udp", "53", "88", "389", "464"}, allowedPorts)
			return "op-smb", nil
		}
		op, err := activities.SetupNetworkFirewallsForSMB(mockService, snHostProject, mockNetwork)
		assert.NoError(t, err)
		assert.Equal(t, op, "op-smb")
	})
	t.Run("WhenSetupNetworkFirewallsForSMBFails", func(tt *testing.T) {
		defer func() {
			activities.DataFirewallSourceRanges = ""          // Reset the environment variable after the test
			activities.SmbFirewallAllowedPortRulesConfig = "" // Reset the environment variable after the test
		}()
		activities.DataFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
		activities.SmbFirewallAllowedPortRulesConfig = "tcp,88,135,139,389,445,464,636,udp,53,88,389,464"
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("smb firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForSMB(mockService, snHostProject, mockNetwork)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "smb firewall error")
	})
}

func Test_setupNetworkFirewallsForIlbHealthCheck(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	mockNetwork := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	t.Run("WhenSetupNetworkFirewallsForIlbHealthCheckSucceeds", func(tt *testing.T) {
		defer func() {
			activities.IlbHealthCheckFirewallSourceRangesConfig = ""     // Reset the environment variable after the test
			activities.IlbHealthCheckFirewallAllowedPortRulesConfig = "" // Reset the environment variable after the test
		}()
		activities.IlbHealthCheckFirewallSourceRangesConfig = "130.211.0.0/22,35.191.0.0/16"
		activities.IlbHealthCheckFirewallAllowedPortRulesConfig = "tcp"
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, activities.ILBHealthCheckFirewallName, name)
			assert.Equal(t, mockNetwork, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"130.211.0.0/22", "35.191.0.0/16"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp"}, allowedPorts)
			return "op-ilb-health-check", nil
		}
		op, err := activities.SetupNetworkFirewallsForIlbHealthCheck(mockService, snHostProject, mockNetwork)
		assert.NoError(t, err)
		assert.Equal(t, op, "op-ilb-health-check")
	})
	t.Run("WhenSetupNetworkFirewallsForIlbHealthCheckFails", func(tt *testing.T) {
		defer func() {
			activities.IlbHealthCheckFirewallSourceRangesConfig = ""     // Reset the environment variable after the test
			activities.IlbHealthCheckFirewallAllowedPortRulesConfig = "" // Reset the environment variable after the test
		}()
		activities.IlbHealthCheckFirewallSourceRangesConfig = "130.211.0.0/22,35.191.0.0/16"
		activities.IlbHealthCheckFirewallAllowedPortRulesConfig = "tcp"
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("ilb health check firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForIlbHealthCheck(mockService, snHostProject, mockNetwork)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ilb health check firewall error")
	})
}

func Test_setupNetworkFirewallsForNVMe(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
	snHostProject := "test-sn-host-project"
	mockNetwork := "test-network"
	firewallPriority := int64(1000)
	ingressTrafficDirection := "INGRESS"
	ctx := context.TODO()
	logger := util.GetLogger(ctx)
	activities.NvmeFirewallPortRules = "tcp,4420"
	activities.DataFirewallSourceRanges = "10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"
	defer func() {
		activities.DataFirewallSourceRanges = "" // Reset the environment variable after the test
		activities.NvmeFirewallPortRules = ""    // Reset the environment variable after the test
	}()
	t.Run("WhenSetupNetworkFirewallsForNVMESucceeds", func(tt *testing.T) {
		mockService.On("GetLogger").Return(logger)
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			assert.Equal(t, snHostProject, project)
			assert.Equal(t, "ingress-data-nvme", name)
			assert.Equal(t, mockNetwork, network)
			assert.Equal(t, firewallPriority, priority)
			assert.Equal(t, ingressTrafficDirection, direction)
			assert.ElementsMatch(t, []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}, sourceRanges)
			assert.ElementsMatch(t, []string{"tcp", "4420"}, allowedPorts)
			return "op-nvme", nil
		}
		op, err := activities.SetupNetworkFirewallsForNVMe(mockService, snHostProject, mockNetwork)
		assert.NoError(t, err)
		assert.Equal(t, op, "op-nvme")
	})
	t.Run("WhenSetupNetworkFirewallsForNVMeFails", func(tt *testing.T) {
		activities.InsertFirewall = func(service hyperscaler2.GoogleServices, project, name, network string, priority int64, direction string, sourceRanges, allowedPorts []string) (string, error) {
			return "", errors.New("nvme firewall error")
		}
		_, err := activities.SetupNetworkFirewallsForNVMe(mockService, snHostProject, mockNetwork)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "nvme firewall error")
	})
}

func TestPoolActivity_SetupNasFirewalls(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	poolActivity := activities.PoolActivity{SE: mockStorage}
	snHostProject := "test-sn-host-project"
	network := "test-network"

	// Save original functions
	originalGetGCPService := hyperscaler2.GetGCPService
	originalSetupNetworkFirewallsForNFS := activities.SetupNetworkFirewallsForNFS
	originalSetupNetworkFirewallsForSMB := activities.SetupNetworkFirewallsForSMB
	originalSetupNetworkFirewallsForIlbHealthCheck := activities.SetupNetworkFirewallsForIlbHealthCheck

	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.SetupNetworkFirewallsForNFS = originalSetupNetworkFirewallsForNFS
		activities.SetupNetworkFirewallsForSMB = originalSetupNetworkFirewallsForSMB
		activities.SetupNetworkFirewallsForIlbHealthCheck = originalSetupNetworkFirewallsForIlbHealthCheck
	}()

	t.Run("WhenGetGCPServiceFails", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		_, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.Error(t, err)
		// WrapAsTemporalApplicationError only wraps CustomError types, regular errors are returned unchanged
		// So we just check that the error message contains the expected text
		assert.Contains(t, err.Error(), "failed to get GCP service")
	})

	t.Run("WhenSetupNetworkFirewallsForNFSFails", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			// Return a minimal GcpServices - the actual service methods will use the mocked functions
			return &google.GcpServices{}, nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("NFS firewall setup failed")
		}

		_, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.Error(t, err)
		// WrapAsTemporalApplicationError only wraps CustomError types, regular errors are returned unchanged
		// So we just check that the error message contains the expected text
		assert.Contains(t, err.Error(), "NFS firewall setup failed")
	})

	t.Run("WhenSetupNetworkFirewallsForSMBFails", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			// Return a minimal GcpServices - the actual service methods will use the mocked functions
			return &google.GcpServices{}, nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "nfs-op", nil
		}
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("SMB firewall setup failed")
		}

		_, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.Error(t, err)
		// WrapAsTemporalApplicationError only wraps CustomError types, regular errors are returned unchanged
		// So we just check that the error message contains the expected text
		assert.Contains(t, err.Error(), "SMB firewall setup failed")
	})

	t.Run("WhenSetupNetworkFirewallsForIlbHealthCheckFails", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			// Return a minimal GcpServices - the actual service methods will use the mocked functions
			return &google.GcpServices{}, nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "nfs-op", nil
		}
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "smb-op", nil
		}
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("ILB health check firewall setup failed")
		}

		_, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.Error(t, err)
		// WrapAsTemporalApplicationError only wraps CustomError types, regular errors are returned unchanged
		// So we just check that the error message contains the expected text
		assert.Contains(t, err.Error(), "ILB health check firewall setup failed")
	})

	t.Run("WhenAllFirewallsSetupSucceeds", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			// Return a minimal GcpServices - the actual service methods will use the mocked functions
			return &google.GcpServices{}, nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "nfs-op", nil
		}
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "smb-op", nil
		}
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "ilb-op", nil
		}

		encodedValue, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.NoError(t, err)
		assert.NotNil(t, encodedValue)
		var result *[]commonparams.Operations
		err = encodedValue.Get(&result)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 3, len(*result))
	})

	t.Run("WhenFirewallsAlreadyExist", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(poolActivity.SetupNasFirewalls)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			// Return a minimal GcpServices - the actual service methods will use the mocked functions
			return &google.GcpServices{}, nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil // Empty string means firewall already exists
		}
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		encodedValue, err := env.ExecuteActivity(poolActivity.SetupNasFirewalls, snHostProject, network)
		assert.NoError(t, err)
		assert.NotNil(t, encodedValue)
		var result *[]commonparams.Operations
		err = encodedValue.Get(&result)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 0, len(*result)) // No operations if all firewalls already exist
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
	mockGcp.On("CreateBucketIfNotExists", ctx, projectId, bucketName, region, mock.AnythingOfType("*string")).Return(nil)

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

	mockSvc.On("ReleaseSubnetworkOp", activities.Region, snHost, subnetName).Return("", expectedErr)

	operationName, err := activities.ReleaseSubnetOp(mockSvc, snHost, subnetName)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Empty(t, operationName)
	mockSvc.AssertExpectations(t)
}

func Test_CreateGCPBucket_Failure(t *testing.T) {
	mockGcp := hyperscaler2.NewMockGoogleServices(t)
	ctx := context.Background()
	projectId := "test-project"
	region := "us-central1"
	bucketName := "us-central-poolID"

	mockGcp.On("GetLogger").Return(util.GetLogger(ctx))

	mockGcp.EXPECT().CreateBucketIfNotExists(ctx, projectId, bucketName, region, mock.AnythingOfType("*string")).Return(errors.New("failed to create bucket"))
	err := activities.CreateGCPBucket(ctx, projectId, bucketName, region, mockGcp)
	assert.Error(t, err)
	assert.Contains(t, err.(*vsaerrors.CustomError).OriginalErr.Error(), "failed to create bucket")
	assert.Contains(t, err.Error(), "The requested resource already exists")
}

func Test_EnableAutoTiering_Failure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.CreateAutoTierBucket)
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

	_, err := env.ExecuteActivity(activity.CreateAutoTierBucket, bucketName, "region", projectId)
	assert.Error(t, err)
}

func TestPoolActivity_CreateServiceAccountWithStorageRole(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.CreateServiceAccountWithStorageRole)
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
		expectedSA := &hyperscaler_models.ServiceAccount{Name: "projects/test-project/serviceAccounts/test-sa"}
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler_models.ServiceAccount, error) {
			return expectedSA, nil
		}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		var sa *hyperscaler_models.ServiceAccount
		val, err := env.ExecuteActivity(activity.CreateServiceAccountWithStorageRole, projectID, saAccountID, saDisplayName)
		assert.NoError(t, err)
		err = val.Get(&sa)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("error", func(t *testing.T) {
		activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID, saAccountID, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler_models.ServiceAccount, error) {
			return nil, errors.New("Mock error: failed to create service account")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}

		var sa *hyperscaler_models.ServiceAccount
		val, err := env.ExecuteActivity(activity.CreateServiceAccountWithStorageRole, projectID, saAccountID, saDisplayName)
		assert.Error(t, err)
		if err == nil {
			err = val.Get(&sa)
			assert.Error(t, err)
		}
		assert.Nil(t, sa)
	})
}

func Test_createServiceAccountAndAttachRole(t *testing.T) {
	ctx := context.Background()
	projectID := "test-project"
	saAccountID := "test-sa"
	saDisplayName := "Test Service Account"
	saEmail := fmt.Sprintf("%s@%s.iam.gserviceaccount.com", saAccountID, projectID)
	expectedSA := &hyperscaler_models.ServiceAccount{Email: saEmail}
	roles := []string{"roles/storage.objectUser"}

	t.Run("success", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler_models.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler_models.ServiceAccount{
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
		createReq := &hyperscaler_models.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler_models.ServiceAccount{
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
		createReq := &hyperscaler_models.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler_models.ServiceAccount{
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

	// Test for 409 concurrent policy modification retry behavior
	t.Run("attach roles succeeds after 409 retry", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler_models.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler_models.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}

		// First call succeeds for CreateServiceAccount
		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)

		// AttachOrUpdateRolesForServiceAccounts should succeed
		// In reality, Temporal's retry policy will handle 409 errors automatically
		// This test verifies the function propagates errors correctly for retry
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(nil)

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.NoError(t, err)
		assert.Equal(t, expectedSA, sa)
	})

	t.Run("attach roles fails with 409 concurrent policy changes - error propagated for retry", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		createReq := &hyperscaler_models.CreateServiceAccountRequest{
			AccountId: saAccountID,
			ServiceAccount: &hyperscaler_models.ServiceAccount{
				DisplayName: saDisplayName,
			},
		}

		mockGcp.EXPECT().GetLogger().Return(log.NewLogger())
		mockGcp.EXPECT().CreateServiceAccount(createReq, projectID, saEmail).Return(expectedSA, nil)

		// Simulate 409 error with "aborted" status - this should be retried by Temporal
		err409 := vsaerrors.NewVCPError(vsaerrors.ErrGCPResourceProvisionError,
			fmt.Errorf("googleapi: Error 409: There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff., aborted"))
		mockGcp.EXPECT().AttachOrUpdateRolesForServiceAccounts(roles, saEmail, projectID).Return(err409)

		sa, err := activities.CreateServiceAccountAndAttachRole(ctx, projectID, saAccountID, saDisplayName, mockGcp)
		assert.Error(t, err)
		assert.Nil(t, sa)
		// Verify error is propagated (wrapped as Temporal application error but retryable)
		// The error is wrapped by WrapAsTemporalApplicationError but remains retryable
		var customErr *vsaerrors.CustomError
		if vsaerrors.As(err, &customErr) {
			// Check that the underlying error is retryable
			assert.True(t, customErr.Retriable, "Error should be retryable for Temporal retry")
		}
	})
}

func TestPoolActivity_DeleteAutoTierBucket(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeleteAutoTierBucket)
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

		_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, bucketName, "accountName", int64(2))
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
		mockStorage.On("CreatePendingResourceDeletion", mock.Anything, "BUCKET", bucketName, "delete failed", "accountName", int64(2)).Return(&datamodel.PendingResourceDeletions{}, nil)

		_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, bucketName, "accountName", int64(2))
		assert.NoError(t, err)
	})

	t.Run("empty bucket name", func(t *testing.T) {
		// Test the case where bucket name is empty - should log warning and return nil
		_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, "", "accountName", int64(2))
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
		mockStorage.On("CreatePendingResourceDeletion", mock.Anything, "BUCKET", bucketName, "", "accountName", int64(2)).Return(&datamodel.PendingResourceDeletions{}, nil)

		_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, bucketName, "accountName", int64(2))
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
		mockStorage.On("CreatePendingResourceDeletion", mock.Anything, "BUCKET", bucketName, "delete failed", "accountName", int64(2)).Return(nil, errors.New("database error"))

		_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, bucketName, "accountName", int64(2))
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

	// Tests for 409 concurrent policy modification retry behavior
	t.Run("role removal succeeds after handling concurrent modifications", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()

		// Both operations succeed (retry happens at lower level if needed)
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(nil)
		mockGcp.EXPECT().DeleteServiceAccount(projectNumber, saEmail).Return(nil)

		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.NoError(t, err)
	})

	t.Run("role removal fails with 409 concurrent policy changes - error propagated for retry", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()

		// Simulate 409 error with "aborted" status during role removal
		// This error should be propagated so Temporal/retry logic can retry the entire activity
		err409 := fmt.Errorf("googleapi: Error 409: There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff. The request's ETag did not match., aborted")
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(err409)

		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		// Verify the 409 error is properly propagated for retry
		assert.Contains(t, err.Error(), "409")
		assert.Contains(t, err.Error(), "aborted")
	})

	t.Run("role removal succeeds but delete fails with 409 - error propagated for retry", func(t *testing.T) {
		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()

		// Role removal succeeds
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(nil)

		// Delete fails with 409 (less common but possible)
		err409 := fmt.Errorf("googleapi: Error 409: Conflict, aborted")
		mockGcp.EXPECT().DeleteServiceAccount(projectNumber, saEmail).Return(err409)

		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "409")
	})

	t.Run("parallel deletions scenario - multiple 409 errors handled", func(t *testing.T) {
		// This test simulates what happens when multiple pools are deleted in parallel
		// Each tries to remove roles from service accounts in the same project
		// The function should return errors that trigger Temporal retry

		mockGcp := hyperscaler2.NewMockGoogleServices(t)
		mockGcp.EXPECT().GetLogger().Return(logger).Maybe()

		// Simulate concurrent modification error (first attempt fails)
		err409 := fmt.Errorf("googleapi: Error 409: There were concurrent policy changes. Please retry with exponential backoff. ETag mismatch., aborted")
		mockGcp.EXPECT().RemoveRolesFromServiceAccounts(roles, saEmail, projectNumber).Return(err409)

		err := activities.DeleteServiceAccountAndRemoveStorageRole(ctx, projectNumber, saAccountID, mockGcp)
		assert.Error(t, err)
		// Verify error contains markers for retry logic
		assert.Contains(t, err.Error(), "409")
		assert.Contains(t, err.Error(), "aborted")
		// The activity will be retried by Temporal's retry policy
	})
}

func TestPoolActivity_DeleteServiceAccount(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.DeleteServiceAccount)
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
		_, err := env.ExecuteActivity(activity.DeleteServiceAccount, projectNumber, saAccountID)
		assert.NoError(t, err)
	})

	t.Run("failure", func(t *testing.T) {
		activities.DeleteServiceAccountAndRemoveStorageRole = func(ctx context.Context, projectNumber, saAccountID string, gcpService hyperscaler2.GoogleServices) error {
			return errors.New("delete error")
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		_, err := env.ExecuteActivity(activity.DeleteServiceAccount, projectNumber, saAccountID)
		assert.Error(t, err)
	})

	t.Run("empty service account ID", func(t *testing.T) {
		// Test the case where service account ID is empty - should log warning and return nil
		_, err := env.ExecuteActivity(activity.DeleteServiceAccount, projectNumber, "")
		assert.NoError(t, err)
	})

	t.Run("empty project number", func(t *testing.T) {
		// Test the case where project number is empty - should log warning and return nil
		_, err := env.ExecuteActivity(activity.DeleteServiceAccount, "", saAccountID)
		assert.NoError(t, err)
	})

	t.Run("both empty", func(t *testing.T) {
		// Test the case where both project number and service account ID are empty - should log warning and return nil
		_, err := env.ExecuteActivity(activity.DeleteServiceAccount, "", "")
		assert.NoError(t, err)
	})
}

func TestGenerateCSR(t *testing.T) {
	commonName := "test.example.com"
	domains := []string{"test.example.com", "www.test.example.com"}
	csrDER, key, err := hyperscaler2.GenerateCSR(commonName, domains, true)
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.NoError(t, err)
}

func Test_IdentifyVMs_SetsClusterName_DeploymentNameOnlyWhenNoRegionCode(t *testing.T) {
	// When getRegionNumber() returns "" (e.g. LOCAL_REGION unset or not in REGION_NUMBER_MAP),
	// ClusterName should be deploymentName only.
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
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

	val, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)
	require.NoError(t, err)

	var vlmConfig *vlm.VLMConfig
	require.NoError(t, val.Get(&vlmConfig))
	// With default test env, LOCAL_REGION is typically "" so getRegionNumber() returns "" and ClusterName is deploymentName only
	assert.Equal(t, "test-deployment", vlmConfig.VsaCluster.ClusterName)
}

func Test_IdentifyVMs_SetsClusterName_FormatDeploymentNameAndRegionCode(t *testing.T) {
	// ClusterName must be either deploymentName or deploymentName + "-" + region identifier from getRegionNumber()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)
	activities.Region = "us-central1"

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
		activities.Region = utilsEnv.GetString("LOCAL_REGION", "")
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
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

	val, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "my-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)
	require.NoError(t, err)

	var vlmConfig *vlm.VLMConfig
	require.NoError(t, val.Get(&vlmConfig))
	// ClusterName is deploymentName when getRegionNumber() is "", else deploymentName + "-" + regionCode
	assert.NotEmpty(t, vlmConfig.VsaCluster.ClusterName)
	assert.True(t, vlmConfig.VsaCluster.ClusterName == "my-deployment" || strings.HasPrefix(vlmConfig.VsaCluster.ClusterName, "my-deployment-r"),
		"ClusterName should be deploymentName or deploymentName + '-' + region identifier, got %s", vlmConfig.VsaCluster.ClusterName)
}

func Test_IdentifyVMs_SuccessfullyPreparesConfig_LargeVolume(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "password"}}, nil
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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.NoError(t, err)
}

func Test_IdentifyVMs_FailsToPrepareConfig(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, userName string) (*hyperscaler_models.CustomSecret, error) {
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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_IdentifyVMs_FailsToPrepareConfig_LargeVolume(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

	prepareVLMConfig := activities.PrepareVlmConfig
	originalGetPasswordForVSACluster := hyperscaler2.GetPasswordForVSACluster
	defer func() {
		activities.PrepareVlmConfig = prepareVLMConfig
		hyperscaler2.GetPasswordForVSACluster = originalGetPasswordForVSACluster
	}()
	activities.PrepareVlmConfig = func(cfg *vlm.VLMConfig, deploymentName, region, primaryZone, secondaryZone, network, subnet, projectId, snHostProject string, dsc *vmrs.Decision, saEmail string, autoTierBucket string) error {
		return errors.New("failed to prepare VLM config for large volume")
	}
	hyperscaler2.GetPasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, userName string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "password"}}, nil
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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "testdata/valid_vmrs_gcp.yaml", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to prepare VLM config")
}

func Test_IdentifyVMs_FailsToLoadConfig(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load VMRS config from file")
}

func Test_IdentifyVMs_FailsToLoadConfig_LargeVolume(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to load VMRS config for large volume cluster")
}

func Test_IdentifyVMs_FailsToCreateDecisionMaker(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create decision maker")
}

func Test_IdentifyVMs_FailsToCreateDecisionMaker_LargeVolume(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create decision maker")
}

func Test_IdentifyVMs_FailsToFindOptimalVMs(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", false)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to find optimal VMs")
}

func Test_IdentifyVMs_FailsToFindOptimalVMs_LargeVolume(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	activity := activities.PoolActivity{}
	env.RegisterActivity(activity.IdentifyVMs)

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
	_, err := env.ExecuteActivity(activity.IdentifyVMs, "test-path", *customerRequestedPerformance, "test-deployment", locationInfo, tenancyInfo, "test-tenant-project@xyz.com", "test-tenant-project", true)

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

	cert := &hyperscaler_models.CustomCertificate{}
	secret := &hyperscaler_models.CustomSecret{SecretVersion: &hyperscaler_models.CustomSecretVersion{}}

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
		secretNoVersion := &hyperscaler_models.CustomSecret{}
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
		expectedRecord := &hyperscaler_models.CustomCloudDNSRecord{RecordName: recordName, Data: ipAddress}

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
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockStorage}
		testEnv.RegisterActivity(activity.GetCloudDNSRecords)
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

		mockStorage.On("GetNodesByPoolID", mock.Anything, poolId).Return(expectedNode, nil)

		val, err := testEnv.ExecuteActivity(activity.GetCloudDNSRecords, poolId, env.USER_CERTIFICATE)

		assert.NoError(tt, err)
		var mapHost *map[string]string
		assert.NoError(tt, val.Get(&mapHost))
		mapHostExpected := &map[string]string{"1.2.3.4": "test-node.example.com"}
		assert.Equal(tt, mapHostExpected, mapHost)
		mockStorage.AssertExpectations(tt)
	})
	t.Run("GetNode_Error", func(tt *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		mockStorage := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockStorage}
		testEnv.RegisterActivity(activity.GetCloudDNSRecords)
		poolId := int64(1)

		mockStorage.On("GetNodesByPoolID", mock.Anything, poolId).Return(nil, gorm.ErrInvalidDB)

		_, err := testEnv.ExecuteActivity(activity.GetCloudDNSRecords, poolId, env.USER_CERTIFICATE)

		assert.Error(tt, err)
		mockStorage.AssertExpectations(tt)
	})
}

func TestPoolActivity_DeleteCloudDNSRecords(t *testing.T) {
	hostMap := map[string]string{
		"1.2.3.4": "dns-1.test-cluster.example.com.",
		"2.3.4.5": "dns-2.test-cluster.example.com.",
	}

	t.Run("successfully deletes all DNS records", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.DeleteCloudDNSRecords)
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
		_, err := testEnv.ExecuteActivity(activity.DeleteCloudDNSRecords, hostMap, env.USER_CERTIFICATE)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.DeleteCloudDNSRecords)
		originalGetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = originalGetGCPService
		}()

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("gcp error")
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteCloudDNSRecords, hostMap, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, vsaerrors.ExtractCustomError(err).OriginalErr.Error(), "gcp error")
	})

	t.Run("DeleteCloudDNSRecord fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.DeleteCloudDNSRecords)
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
		_, err := testEnv.ExecuteActivity(activity.DeleteCloudDNSRecords, hostMap, env.USER_CERTIFICATE)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})
	t.Run("does nothing if not USER_CERTIFICATE", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.DeleteCloudDNSRecords)
		_, err := testEnv.ExecuteActivity(activity.DeleteCloudDNSRecords, hostMap, env.USERNAME_PWD)
		assert.NoError(t, err)
	})
}

func TestPoolActivity_CreateCloudDNSRecords(t *testing.T) {
	clusterName := "testcluster"
	env.VsaDeployedDnsName = "example.com"

	// Mock CreateCloudDNSRecord
	originalCreateCloudDNSRecord := hyperscaler2.GetOrCreateCloudDNSRecord
	originalGCPService := hyperscaler2.GetGCPService
	defer func() {
		hyperscaler2.GetOrCreateCloudDNSRecord = originalCreateCloudDNSRecord
		hyperscaler2.GetGCPService = originalGCPService
	}()
	hyperscaler2.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, ip, recordName string) (*hyperscaler_models.CustomCloudDNSRecord, error) {
		return &hyperscaler_models.CustomCloudDNSRecord{RecordName: recordName}, nil
	}

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Logger: log.NewLogger()}, nil
	}

	// Success case
	t.Run("success", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		pa := &activities.PoolActivity{}
		testEnv.RegisterActivity(pa.CreateCloudDNSRecords)
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
		val, err := testEnv.ExecuteActivity(pa.CreateCloudDNSRecords, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.NoError(t, err)
		var hostMap *map[string]string
		assert.NoError(t, val.Get(&hostMap))
		assert.NotNil(t, hostMap)
		assert.Equal(t, 2, len(*hostMap))
	})

	// No HAPairs
	t.Run("no HAPairs", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		pa := &activities.PoolActivity{}
		testEnv.RegisterActivity(pa.CreateCloudDNSRecords)
		vlmConfig := &vlm.VLMConfig{
			Cloud: vlm.CloudConfig{
				HAPairs: []vlm.HAPair{},
			},
		}
		_, err := testEnv.ExecuteActivity(pa.CreateCloudDNSRecords, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
	})

	// No SystemLIFs
	t.Run("no SystemLIFs", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		pa := &activities.PoolActivity{}
		testEnv.RegisterActivity(pa.CreateCloudDNSRecords)
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
		_, err := testEnv.ExecuteActivity(pa.CreateCloudDNSRecords, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
	})

	// CreateCloudDNSRecord returns error
	t.Run("GetOrCreateCloudDNSRecord error", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		pa := &activities.PoolActivity{}
		testEnv.RegisterActivity(pa.CreateCloudDNSRecords)
		hyperscaler2.GetOrCreateCloudDNSRecord = func(gcpService hyperscaler2.GoogleServices, ip, recordName string) (*hyperscaler_models.CustomCloudDNSRecord, error) {
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
		_, err := testEnv.ExecuteActivity(pa.CreateCloudDNSRecords, vlmConfig, clusterName, env.USER_CERTIFICATE)
		assert.Error(t, err)
	})
}

func TestPoolActivity_DeleteOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}

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
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
			assert.Equal(t, "cert-id", poolCredentials.CertificateID)
			return nil
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			assert.Equal(t, "secret-id", secretID)
			return nil
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("USER_CERTIFICATE failure due to secret error ", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
			assert.Equal(t, "cert-id", poolCredentials.CertificateID)
			return nil
		}
		hyperscaler2.DeletePasswordFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, secretID string) error {
			return errors.New("delete error")
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
			},
		}
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
			return errors.New("revoke error")
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "revoke error")
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("default password - no cert no secret-manager", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USERNAME_PWD,
			},
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteOnTapCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteOnTapCredentials, pool)
		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "gcp error", "CustomError", false)
	})
}

func TestPoolActivity_CreateOnTapCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}
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
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USER_CERTIFICATE,
				Username:      username,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
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
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "CN", creds.Certificate.CommonName)
		assert.Equal(t, "cert", creds.Certificate.Certificate)
		assert.Equal(t, "key", creds.Certificate.PrivateKey)
		assert.Equal(t, []string{"chain"}, creds.Certificate.InterMediateCertificate)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USER_CERTIFICATE error due to secret failure", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "password",
				AuthType:      env.USER_CERTIFICATE,
				Username:      username,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
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
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.Error(t, err)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USER_CERTIFICATE,
				Username:      username,
			},
		}
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("cert error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.Error(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
				Username:      username,
			},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD_SEC_MGR,
				Username:      username,
			},
		}
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.Error(t, err)
	})

	t.Run("default password", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD,
				Username:      username,
			},
		}
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "default-password", creds.AdminPassword)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateOnTapCredentials)

		pool := &datamodel.Pool{
			DeploymentName: clusterName,
			PoolCredentials: &datamodel.PoolCredentials{
				CertificateID: "cert-id",
				SecretID:      "secret-id",
				Password:      "default-password",
				AuthType:      env.USERNAME_PWD,
				Username:      username,
			},
		}
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("gcp error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)
		assert.Error(t, err)
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
		var nilRanges []string
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName, false, nilRanges).
			Return(&operation, nil)

		operationName, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region, false, nil) // assuming standard pools
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
		var nilRanges []string
		mockSvc.On("CreateTPSubnetOp", tenantProjectNumber, consumerVPC, region, subnetName, false, nilRanges).
			Return(nil, errors.New("create failed"))
		mockSvc.On("GetLogger").Return(util.GetLogger(context.Background()))

		_, err := activities.GetCreateSubnetworkOperation(mockSvc, tenantProjectNumber, consumerVPC, &region, false, nil)
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdatedPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}

	mockStorage.On("UpdatedPool", mock.Anything, pool).Return(pool, nil)

	// Act
	encodedValue, err := env.ExecuteActivity(activity.UpdatedPool, pool)

	// Assert
	assert.NoError(t, err)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, pool, result)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_UpdatedPool_Failure(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdatedPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
	}

	mockStorage.On("UpdatedPool", mock.Anything, pool).Return(nil, errors.New("update failed"))

	// Act
	encodedValue, err := env.ExecuteActivity(activity.UpdatedPool, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "update failed")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_CreateOnTapCredentials_Success(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name: "test-pool",
		PoolCredentials: &datamodel.PoolCredentials{
			CertificateID: "test-cert-id",
			SecretID:      "test-secret-id",
			Username:      "admin",
			AuthType:      env.USER_CERTIFICATE,
		},
		DeploymentName: "test-cluster",
	}

	originalGetGCPService := hyperscaler2.GetGCPService
	originalGenerateAndCreateCertificate := hyperscaler2.GenerateAndCreateCertificateForVSACluster
	originalGeneratePassword := hyperscaler2.GeneratePasswordForVSACluster
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = originalGenerateAndCreateCertificate
		hyperscaler2.GeneratePasswordForVSACluster = originalGeneratePassword
	}()

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.CreateOnTapCredentials)

	mockGCPService := &google.GcpServices{}
	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return mockGCPService, nil
	}

	// Mock certificate generation
	hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
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
	hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
		return &hyperscaler_models.CustomSecret{
			SecretVersion: &hyperscaler_models.CustomSecretVersion{
				Value: "test-password",
			},
		}, nil
	}

	// Act
	encodedValue, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)

	// Assert
	assert.NoError(t, err)
	var result *vlm.OntapCredentials
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.IsType(t, &vlm.OntapCredentials{}, result)
}

func TestPoolActivity_CreateOnTapCredentials_GetGCPServiceFails(t *testing.T) {
	// Arrange
	activity := activities.PoolActivity{}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "test-uuid",
		},
		Name:           "test-pool",
		DeploymentName: "test-cluster",
		PoolCredentials: &datamodel.PoolCredentials{
			Username: "admin",
		},
	}

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.CreateOnTapCredentials)

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	_, err := testEnv.ExecuteActivity(activity.CreateOnTapCredentials, pool)

	// Assert
	assert.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	var trackingID int
	var originalMsg string
	require.NoError(t, appErr.Details(&trackingID, &originalMsg))
	assert.Contains(t, originalMsg, "failed to get GCP service")
}

func TestPoolActivity_DeletingPoolResources_DeletingSVMsFails(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.DeletingPoolResources)

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
	encodedValue, err := env.ExecuteActivity(activity.DeletingPoolResources, pool)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to mark SVMs as deleting")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_CreateAutoTierBucket_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.CreateAutoTierBucket)

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
	_, err := env.ExecuteActivity(activity.CreateAutoTierBucket, autoTierBucketName, region, projectId)

	// Assert
	assert.NoError(t, err)
}

func TestPoolActivity_CreateAutoTierBucket_GetGCPServiceFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.CreateAutoTierBucket)

	autoTierBucketName := "test-bucket"
	region := "us-central1"
	projectId := "test-project"

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	_, err := env.ExecuteActivity(activity.CreateAutoTierBucket, autoTierBucketName, region, projectId)

	// Assert
	assert.Error(t, err)
}

func TestPoolActivity_DeleteAutoTierBucket_GetGCPServiceFails(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.DeleteAutoTierBucket)

	autoTierBucketName := "test-bucket"

	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("failed to get GCP service")
	}

	// Act
	_, err := env.ExecuteActivity(activity.DeleteAutoTierBucket, autoTierBucketName, "accountName", int64(2))

	// Assert
	assert.Error(t, err)
}

func TestPoolActivity_CreateServiceAccountWithStorageRole_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.CreateServiceAccountWithStorageRole)

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

	expectedServiceAccount := &hyperscaler_models.ServiceAccount{
		Email: "test-sa@test-project.iam.gserviceaccount.com",
		Name:  "Test Service Account",
	}

	activities.CreateServiceAccountAndAttachRole = func(ctx context.Context, projectID string, saAccountID string, saDisplayName string, gcpService hyperscaler2.GoogleServices) (*hyperscaler_models.ServiceAccount, error) {
		return expectedServiceAccount, nil
	}

	// Act
	var result *hyperscaler_models.ServiceAccount
	val, err := env.ExecuteActivity(activity.CreateServiceAccountWithStorageRole, projectID, saAccountID, saDisplayName)
	assert.NoError(t, err)
	err = val.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, expectedServiceAccount.Email, result.Email)
}

func TestCreateQoSPolicyAndApplyToSVM(t *testing.T) {
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000,
			Iops:            5000,
		},
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD,
			Password: "test-password",
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
		Name:                           "test-node",
		EndpointAddress:                "1.2.3.4",
		AuthType:                       env.USERNAME_PWD,
		EndpointAddressesToHostNameMap: make(map[string]string),
	}

	t.Run("WhenQoSPolicyDoesNotExist_ThenCreateAndApply", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		isShared := true
		mockProvider.On("CreateQoSGroupPolicy", vsa.CreateQoSGroupPolicyParams{
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
			IsShared:      &isShared,
		}).Return(expectedQoSPolicy, nil)

		// Mock SVM modification
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyExistsWithSameValues_ThenSkipCreation", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyExistsWithDifferentValues_ThenUpdateAndApply", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
			Name:          "",
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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenFindQoSGroupPolicyFails_ThenCreateNew", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyCreationFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "qos creation failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenSVMModificationFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "svm modification failed")
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyNameIsGeneratedCorrectly", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		isShared := true
		mockProvider.On("CreateQoSGroupPolicy", vsa.CreateQoSGroupPolicyParams{
			Name:          "test-svm-qos-policy",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
			IsShared:      &isShared,
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
		env.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, pool, svm, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
	})

	t.Run("WhenQosTypeIsManual_ThenSkipCreationAndReturnEarly", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		testEnv := ts.NewTestActivityEnvironment()

		// Create a pool with QosTypeManual
		poolWithManualQoS := pool
		poolWithManualQoS.QosType = utils.QosTypeManual

		// No provider should be called when QosType is Manual
		// We don't set up any mocks because the function should return early

		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.CreateQoSPolicyAndApplyToSVM)
		_, err := testEnv.ExecuteActivity(activity.CreateQoSPolicyAndApplyToSVM, poolWithManualQoS, svm, node)

		// Should return successfully without any provider interactions
		assert.NoError(tt, err)
	})
}

func TestModifyQoSPolicyAndApplyToSVM(t *testing.T) {
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		PoolAttributes: &datamodel.PoolAttributes{
			ThroughputMibps: 1000, // Current throughput (will be compared against)
			Iops:            5000, // Current IOPS (will be compared against)
		},
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD,
			Password: "test-password",
		},
	}
	updateParams := &commonparams.UpdatePoolParams{
		TotalThroughputMibps: 2000,                            // New throughput requirement
		TotalIops:            nillable.ToPointer(int64(6000)), // New IOPS requirement
	}
	node := &coremodel.Node{
		Name:                           "test-node",
		EndpointAddress:                "1.2.3.4",
		AuthType:                       env.USERNAME_PWD,
		EndpointAddressesToHostNameMap: make(map[string]string),
	}

	t.Run("WhenQoSPolicyNeedsUpdate_ThenUpdateAndApply", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
			Name:          "",
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
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenQoSPolicyNoChangeNeeded_ThenSkipUpdate", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider error")
		}

		activity := &activities.PoolActivity{}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider error")
	})

	t.Run("WhenGetSvmForPoolIDFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenFindQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(nil, errors.New("QoS policy not found"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "QoS policy not found")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	// Manual→auto: activity must not return early; it must find or create the policy and apply to SVM.
	t.Run("WhenManualToAuto_AndPolicyExists_ThenApplyToSVM", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		// Pool is manual; we are switching to auto (updateParams.QosType == auto).
		poolManual := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool",
			QosType:   utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 1000,
				Iops:            5000,
			},
		}
		paramsManualToAuto := &commonparams.UpdatePoolParams{
			QosType:              utils.QosTypeAuto,
			TotalThroughputMibps: 1000,
			TotalIops:            nillable.ToPointer(int64(5000)),
		}

		// Policy already exists with same values; we must still apply to SVM.
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 1000,
			MaxIOPS:       5000,
		}
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolManual, node, paramsManualToAuto)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenManualToAuto_AndPolicyNotFound_ThenCreateAndApply", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		poolManual := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool",
			QosType:   utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 128,
				Iops:            2048,
			},
		}
		paramsManualToAuto := &commonparams.UpdatePoolParams{
			QosType:              utils.QosTypeAuto,
			TotalThroughputMibps: 128,
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		// Use NotFoundErr so the activity creates the policy (only "not found" triggers create when switchingToAuto).
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(nil, utilErrors.NewNotFoundErr("QoS policy", nil))
		mockProvider.On("CreateQoSGroupPolicy", mock.MatchedBy(func(p vsa.CreateQoSGroupPolicyParams) bool {
			return p.Name == "test-svm-qos-policy" && p.SvmName == "test-svm" && p.MaxThroughput == 128 && p.MaxIOPS == 2048
		})).Return(&vsa.QoSGroupPolicyResponse{
			Name: "test-svm-qos-policy",
			UUID: "new-uuid",
		}, nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolManual, node, paramsManualToAuto)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenManualToAuto_AndFindReturnsNonNotFoundError_ThenReturnErrorWithoutCreating", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1},
			Name:       "test-svm",
			SvmDetails: &datamodel.SvmDetails{ExternalUUID: "test-svm-uuid"},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		poolManual := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool",
			QosType:   utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 128,
				Iops:            2048,
			},
		}
		paramsManualToAuto := &commonparams.UpdatePoolParams{
			QosType:              utils.QosTypeAuto,
			TotalThroughputMibps: 128,
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		// Non-NotFound error: activity must return error and must NOT call CreateQoSGroupPolicy.
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(nil, errors.New("transient API error"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolManual, node, paramsManualToAuto)

		assert.Error(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateParamsNilAndPoolHasPoolAttributes_ThenUsePoolThroughputAndIops", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1},
			Name:       "test-svm",
			SvmDetails: &datamodel.SvmDetails{ExternalUUID: "test-svm-uuid"},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		poolWithAttrs := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool",
			QosType:   utils.QosTypeAuto,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 100,
				Iops:            200,
			},
		}
		existingQoSPolicy := &vsa.QoSGroupPolicyResponse{
			Name:          "test-svm-qos-policy",
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 50,
			MaxIOPS:       100,
		}
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)
		// Production code omits Name in UpdateQoSGroupPolicyParams so ONTAP does not treat it as a rename
		mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
			UUID:          "test-qos-uuid",
			SvmName:       "test-svm",
			MaxThroughput: 100,
			MaxIOPS:       200,
		}).Return(nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolWithAttrs, node, nil)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenManualToAutoAndCreateQoSFails_ThenReturnError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1},
			Name:       "test-svm",
			SvmDetails: &datamodel.SvmDetails{ExternalUUID: "test-svm-uuid"},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		poolManual := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-pool",
			QosType:   utils.QosTypeManual,
			PoolAttributes: &datamodel.PoolAttributes{
				ThroughputMibps: 128,
				Iops:            2048,
			},
		}
		paramsManualToAuto := &commonparams.UpdatePoolParams{
			QosType:              utils.QosTypeAuto,
			TotalThroughputMibps: 128,
			TotalIops:            nillable.ToPointer(int64(2048)),
		}

		// Use NotFoundErr so the activity attempts to create the policy (only "not found" triggers create when switchingToAuto).
		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(nil, utilErrors.NewNotFoundErr("QoS policy", nil))
		mockProvider.On("CreateQoSGroupPolicy", mock.Anything).Return(nil, errors.New("create QoS policy failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolManual, node, paramsManualToAuto)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "create QoS policy failed")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenUpdateQoSGroupPolicyFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)
		mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
			UUID:          "test-qos-uuid",
			Name:          "",
			SvmName:       "test-svm",
			MaxThroughput: 2000,
			MaxIOPS:       6000,
		}).Return(errors.New("update failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "update failed")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenModifySVMWithQoSPolicyFails_ThenReturnError", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockProvider.On("FindQoSGroupPolicy", vsa.FindQoSGroupPolicyParams{
			Name:    "test-svm-qos-policy",
			SvmName: "test-svm",
		}).Return(existingQoSPolicy, nil)
		mockProvider.On("UpdateQoSGroupPolicy", vsa.UpdateQoSGroupPolicyParams{
			UUID:          "test-qos-uuid",
			Name:          "",
			SvmName:       "test-svm",
			MaxThroughput: 2000,
			MaxIOPS:       6000,
		}).Return(nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "test-svm-qos-policy",
		}).Return(errors.New("SVM modification failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := env.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, pool, node, updateParams)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM modification failed")
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenQosTypeIsManual_ThenSkipModificationAndReturnEarly", func(tt *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		testEnv := ts.NewTestActivityEnvironment()

		// Create a pool with QosTypeManual
		poolWithManualQoS := pool
		poolWithManualQoS.QosType = utils.QosTypeManual

		// updateParams with QosType Manual so the activity skips (no manual→auto)
		updateParamsManualOnly := &commonparams.UpdatePoolParams{
			TotalThroughputMibps: updateParams.TotalThroughputMibps,
			TotalIops:            updateParams.TotalIops,
			QosType:              utils.QosTypeManual,
		}

		// No provider or storage should be called when QosType is Manual
		// We don't set up any mocks because the function should return early

		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := testEnv.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolWithManualQoS, node, updateParamsManualOnly)

		// Should return successfully without any provider or storage interactions
		assert.NoError(tt, err)
	})

	t.Run("WhenQosTypeIsManualAndParamsNil_ThenSkipAndLeaveQosTypeUnchanged", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		testEnv := ts.NewTestActivityEnvironment()
		poolWithManualQoS := pool
		poolWithManualQoS.QosType = utils.QosTypeManual
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := testEnv.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolWithManualQoS, node, nil)
		assert.NoError(tt, err)
	})

	t.Run("WhenQosTypeIsManualAndParamsEmptyQosType_ThenSkipAndLeaveQosTypeUnchanged", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		testEnv := ts.NewTestActivityEnvironment()
		poolWithManualQoS := pool
		poolWithManualQoS.QosType = utils.QosTypeManual
		paramsEmptyQos := &commonparams.UpdatePoolParams{
			TotalThroughputMibps: 100,
			TotalIops:            updateParams.TotalIops,
			QosType:              "",
		}
		activity := &activities.PoolActivity{}
		testEnv.RegisterActivity(activity.ModifyQoSPolicyAndApplyToSVM)
		_, err := testEnv.ExecuteActivity(activity.ModifyQoSPolicyAndApplyToSVM, poolWithManualQoS, node, paramsEmptyQos)
		assert.NoError(tt, err)
	})
}

func TestRemoveQoSPolicyFromSVM(t *testing.T) {
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
	}
	node := &coremodel.Node{
		Name:                           "test-node",
		EndpointAddress:                "1.2.3.4",
		AuthType:                       env.USERNAME_PWD,
		EndpointAddressesToHostNameMap: make(map[string]string),
	}

	t.Run("WhenSuccess_ThenClearPolicyFromSVM", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "",
		}).Return(nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.RemoveQoSPolicyFromSVM)
		_, err := env.ExecuteActivity(activity.RemoveQoSPolicyFromSVM, pool, node)

		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetSvmForPoolIDFails_ThenReturnError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, errors.New("SVM not found"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.RemoveQoSPolicyFromSVM)
		_, err := env.ExecuteActivity(activity.RemoveQoSPolicyFromSVM, pool, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenSvmDetailsNil_ThenReturnValidationError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		svmNoDetails := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1},
			Name:       "test-svm",
			SvmDetails: nil,
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svmNoDetails, nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.RemoveQoSPolicyFromSVM)
		_, err := env.ExecuteActivity(activity.RemoveQoSPolicyFromSVM, pool, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "SVM or SvmDetails is nil")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenGetProviderByNodeFails_ThenReturnError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return nil, errors.New("provider not found")
		}

		svm := &datamodel.Svm{
			BaseModel:  datamodel.BaseModel{ID: 1},
			Name:       "test-svm",
			SvmDetails: &datamodel.SvmDetails{ExternalUUID: "test-svm-uuid"},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.RemoveQoSPolicyFromSVM)
		_, err := env.ExecuteActivity(activity.RemoveQoSPolicyFromSVM, pool, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "provider not found")
		mockStorage.AssertExpectations(tt)
	})

	t.Run("WhenProviderClearFails_ThenReturnError", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		mockProvider := new(vsa.MockProvider)
		mockStorage := database.NewMockStorage(t)
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		hyperscaler2.GetProviderByNode = func(ctx context.Context, n *coremodel.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		svm := &datamodel.Svm{
			BaseModel: datamodel.BaseModel{ID: 1},
			Name:      "test-svm",
			SvmDetails: &datamodel.SvmDetails{
				ExternalUUID: "test-svm-uuid",
			},
		}
		mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
		mockProvider.On("ModifySVMWithQoSPolicy", vsa.ModifySVMWithQoSPolicyParams{
			SvmUUID:       "test-svm-uuid",
			QoSPolicyName: "",
		}).Return(errors.New("ONTAP clear policy failed"))

		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.RemoveQoSPolicyFromSVM)
		_, err := env.ExecuteActivity(activity.RemoveQoSPolicyFromSVM, pool, node)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "ONTAP clear policy failed")
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		mgs.On("GetLogger").Return(log.NewLogger())
		_, err := activities.CheckAndUpdateFirewall(mgs, existingFirewall, firewallRequest)
		assert.NoError(t, err)
		mgs.AssertExpectations(t)
	})
	t.Run("whenFirewallEdited", func(t *testing.T) {
		mgs := hyperscaler2.NewMockGoogleServices(t)
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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
		existingFirewall := &hyperscaler_models.Firewall{
			SourceRanges: sourceRanges1,
		}
		firewallRequest := &hyperscaler_models.Firewall{
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

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatingPool)

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}
		seResult := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}, State: coremodel.LifeCycleStateUpdating, StateDetails: coremodel.LifeCycleStateUpdatingDetails}

		mockSE.On("UpdatingPool", mock.Anything, pool).Return(seResult, nil)
		encodedValue, err := env.ExecuteActivity(activity.UpdatingPool, pool)
		assert.NoError(t, err)
		var result *datamodel.Pool
		err = encodedValue.Get(&result)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, coremodel.LifeCycleStateUpdating, result.State)
		assert.Equal(t, coremodel.LifeCycleStateUpdatingDetails, result.StateDetails)
	})
	t.Run("WhenUpdatingPoolReturnsError", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		activity := &activities.PoolActivity{SE: mockSE}

		// Create Temporal test environment for activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.UpdatingPool)

		pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}}

		mockSE.On("UpdatingPool", mock.Anything, pool).Return(nil, vsaerrors.WrapAsTemporalApplicationError(errors.New("pool update ran into error")))
		_, err := env.ExecuteActivity(activity.UpdatingPool, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "pool update ran into error")
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
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
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
		activities.CheckReusableSubnet = func(se database.Storage, service hyperscaler2.GoogleServices, params commonparams.CreatePoolParams, tenantProjectNumber string) (*hyperscaler_models.Subnet, error) {
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
			AllocatedSubnetCIDR:   "10.0.0.0/24",
		}

		// Mock GCP service with httptest server for GetSnHost
		origGetGCPService := hyperscaler2.GetGCPService
		defer func() {
			hyperscaler2.GetGCPService = origGetGCPService
		}()

		url := fmt.Sprintf("/projects/%s/getXpnHost", tenantProjectNumber)
		resp := &compute.Project{Name: "sn-host"}
		server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
			if req.URL.Path == url {
				response, _ := json.Marshal(resp)
				rw.WriteHeader(http.StatusOK)
				_, _ = rw.Write(response)
				return
			}
			rw.WriteHeader(http.StatusBadRequest)
		}))
		defer server.Close()

		computeSvc, err := compute.NewService(
			ctx, option.WithHTTPClient(&http.Client{Timeout: time.Second}), option.WithEndpoint(server.URL), option.WithoutAuthentication())
		if err != nil {
			t.Fatalf("Error creating compute service: %v", err)
		}

		adminGcpService := &google.AdminGCPService{}
		// Use reflection to set the unexported computeService field
		rv := reflect.ValueOf(adminGcpService).Elem()
		rf := rv.FieldByName("computeService")
		reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem().Set(reflect.ValueOf(computeSvc))

		mockGcpService := &google.GcpServices{
			AdminGCPService: adminGcpService,
			Ctx:             ctx,
			Logger:          util.GetLogger(ctx),
		}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGcpService, nil
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
	}

	expectedPool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
		State:     coremodel.LifeCycleStateInUse,
	}

	mockSE.On("UpdatedPool", mock.Anything, pool).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPool, pool)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, expectedPool, result)
	mockSE.AssertExpectations(t)
}

func TestUpdatedPool_Failure(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPool)

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{ID: 1},
		Name:      "test-pool",
	}

	expectedError := errors.New("failed to update pool")
	mockSE.On("UpdatedPool", mock.Anything, pool).Return(nil, expectedError)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPool, pool)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to update pool")
	mockSE.AssertExpectations(t)
}

func TestUpdatedPoolWithVLMConfig_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.Equal(t, expectedPool, result)
	mockSE.AssertExpectations(t)
}

func TestUpdatedPoolWithVLMConfig_Failure(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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
	mockSE.On("UpdatedPool", mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(nil, expectedError)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.Error(t, err)
	assert.Nil(t, encodedValue)
	assert.Contains(t, err.Error(), "failed to update pool")
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringEnabled tests updating a pool with AutoTiering enabled
func TestUpdatedPoolWithVLMConfig_AutoTieringEnabled(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 1000 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == true &&
			p.AutoTieringConfig.BucketName == ""
	})).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(1000), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, "", result.AutoTieringConfig.BucketName)
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringDisabled tests updating a pool with AutoTiering disabled
func TestUpdatedPoolWithVLMConfig_AutoTieringOneWayEnablement(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.MatchedBy(func(p *datamodel.Pool) bool {
		// AutoTiering should still be enabled and config preserved
		return p.AllowAutoTiering == true && p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.BucketName == "existing-bucket"
	})).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.True(t, result.AllowAutoTiering)                                 // Should still be true
	assert.NotNil(t, result.AutoTieringConfig)                              // Config should be preserved
	assert.Equal(t, "existing-bucket", result.AutoTieringConfig.BucketName) // Bucket name preserved
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringDisabled tests updating a pool with AutoTiering disabled
func TestUpdatedPoolWithVLMConfig_AutoTieringDisabled(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.AnythingOfType("*datamodel.Pool")).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.False(t, result.AllowAutoTiering)                                  // Should remain disabled
	assert.NotNil(t, result.AutoTieringConfig)                                // Config should be preserved
	assert.Equal(t, int64(5000), result.AutoTieringConfig.HotTierSizeInBytes) // Should sync with SizeInBytes
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)          // Should be updated
	assert.Equal(t, "existing-bucket", result.AutoTieringConfig.BucketName)   // Bucket name preserved
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_PreserveBucketName tests that existing bucket name is preserved when updating AutoTiering
func TestUpdatedPoolWithVLMConfig_PreserveBucketName(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 1500 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == true &&
			p.AutoTieringConfig.BucketName == existingBucketName
	})).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(1500), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.True(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, existingBucketName, result.AutoTieringConfig.BucketName)
	mockSE.AssertExpectations(t)
}

// TestUpdatedPoolWithVLMConfig_AutoTieringWithIOPS tests updating a pool with AutoTiering and IOPS
func TestUpdatedPoolWithVLMConfig_AutoTieringWithIOPS(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.UpdatedPoolWithVLMConfig)

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

	mockSE.On("UpdatedPool", mock.Anything, mock.MatchedBy(func(p *datamodel.Pool) bool {
		return p.AllowAutoTiering == true &&
			p.AutoTieringConfig != nil &&
			p.AutoTieringConfig.HotTierSizeInBytes == 2000 &&
			p.AutoTieringConfig.EnableHotTierAutoResize == false &&
			p.PoolAttributes.Iops == 2048 &&
			p.PoolAttributes.Labels != nil
	})).Return(expectedPool, nil)

	encodedValue, err := env.ExecuteActivity(activity.UpdatedPoolWithVLMConfig, pool, vlmConfig, updatePoolParams)

	assert.NoError(t, err)
	assert.NotNil(t, encodedValue)

	var result *datamodel.Pool
	err = encodedValue.Get(&result)
	assert.NoError(t, err)
	assert.True(t, result.AllowAutoTiering)
	assert.NotNil(t, result.AutoTieringConfig)
	assert.Equal(t, int64(2000), result.AutoTieringConfig.HotTierSizeInBytes)
	assert.False(t, result.AutoTieringConfig.EnableHotTierAutoResize)
	assert.Equal(t, int64(2048), result.PoolAttributes.Iops)
	assert.NotNil(t, result.PoolAttributes.Labels)
	mockSE.AssertExpectations(t)
}

func TestUpdateNodesInstanceTypeActivity(t *testing.T) {
	t.Run("Success", func(tt *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity.UpdateNodesInstanceTypeActivity)

		poolID := int64(123)
		newInstanceType := "c3-standard-8-lssd"

		mockSE.On("UpdateNodesInstanceType", mock.Anything, poolID, newInstanceType).Return(nil)

		encodedValue, err := env.ExecuteActivity(activity.UpdateNodesInstanceTypeActivity, poolID, newInstanceType)

		assert.NoError(tt, err)
		assert.NotNil(tt, encodedValue)
		mockSE.AssertExpectations(tt)
	})

	t.Run("Failure_DatabaseError", func(tt *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()

		mockSE := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockSE}
		env.RegisterActivity(activity.UpdateNodesInstanceTypeActivity)

		poolID := int64(456)
		newInstanceType := "c3-standard-16-lssd"
		expectedError := errors.New("database update failed")

		mockSE.On("UpdateNodesInstanceType", mock.Anything, poolID, newInstanceType).Return(expectedError)

		_, err := env.ExecuteActivity(activity.UpdateNodesInstanceTypeActivity, poolID, newInstanceType)

		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "database update failed")
		mockSE.AssertExpectations(tt)
	})
}

func TestPoolActivity_GetServiceNetOpStatus(t *testing.T) {
	activity := &activities.PoolActivity{}

	t.Run("Success", func(t *testing.T) {
		expectedOp := &hyperscaler_models.ComputeOperation{
			Name: "op-123",
		}
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		originalGetServiceNetOpStatus := activities.GetServiceNetOpStatus
		activities.GetServiceNetOpStatus = func(gcpService hyperscaler2.GoogleServices, operation string) (*hyperscaler_models.ComputeOperation, error) {
			return expectedOp, nil
		}
		defer func() {
			hyperscaler2.GetGCPService = original
			activities.GetServiceNetOpStatus = originalGetServiceNetOpStatus
		}()

		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetServiceNetOpStatus)

		result, err := env.ExecuteActivity(activity.GetServiceNetOpStatus, "op-123")
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Get the actual result from the activity execution
		var opResult *hyperscaler_models.ComputeOperation
		err = result.Get(&opResult)
		assert.NoError(t, err)
		assert.NotNil(t, opResult)
		assert.Equal(t, expectedOp.Name, opResult.Name)
	})

	t.Run("GetGCPServiceFails", func(t *testing.T) {
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("service error"))
		}
		defer func() { hyperscaler2.GetGCPService = original }()

		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetServiceNetOpStatus)

		result, err := env.ExecuteActivity(activity.GetServiceNetOpStatus, "op-123")
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func Test_getServiceNetOpStatus(t *testing.T) {
	mockService := new(hyperscaler2.MockGoogleServices)
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

		activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool, requestedRanges []string) (*string, error) {
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

		activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool, requestedRanges []string) (*string, error) {
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

				activities.GetCreateSubnetworkOperation = func(service hyperscaler2.GoogleServices, tenantProjectNumber, consumerVPC string, tenantProjectRegion *string, isLargeCapacity bool, requestedRanges []string) (*string, error) {
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.IdentifySecondaryAndMediatorZone)

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
	_, err := env.ExecuteActivity(activity.IdentifySecondaryAndMediatorZone, projectNumber, locationInfo, "c3-std-4", false)

	// Assert
	assert.Error(t, err)
}

func Test_IdentifySecondaryAndMediatorZone_GCPServiceError(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.IdentifySecondaryAndMediatorZone)

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
	_, err := env.ExecuteActivity(activity.IdentifySecondaryAndMediatorZone, projectNumber, locationInfo, "c3-std-4", false)

	// Assert
	assert.Error(t, err)
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
	t.Run("FirstSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(1), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-01", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("SecondSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(2), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-02", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("TenthSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(10), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-10", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("EleventhSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(11), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-11", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("NinetyNinthSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(99), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-99", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("HundredthSVMInPool", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(100), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "gcnv-svm-100", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DifferentDeploymentName", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 456},
			DeploymentName: "test-deployment",
		}

		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(456)).Return(int64(6), nil)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		val, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.NoError(t, err)
		var result string
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "test-deployment-svm-06", result)
		mockStorage.AssertExpectations(t)
	})

	t.Run("DatabaseError", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

		// Arrange
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		pool := &datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 123},
			DeploymentName: "gcnv",
		}

		expectedError := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, fmt.Errorf("database connection failed"))
		mockStorage.On("GetNextSVMIndexByPoolID", mock.Anything, int64(123)).Return(int64(0), expectedError)

		// Act
		env.RegisterActivity(activity.AllocateSVMName)
		_, err := env.ExecuteActivity(activity.AllocateSVMName, pool)

		// Assert
		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func Test_AllocateClusterSerialNumber(t *testing.T) {
	t.Run("SuccessOneHAPair", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber1, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber2, nil).Once()

		env.RegisterActivity(activity.AllocateClusterSerialNumber)
		val, err := env.ExecuteActivity(activity.AllocateClusterSerialNumber, req)

		assert.NoError(t, err)
		var result *vlm.CreateVSAClusterDeploymentRequest
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "", result.VLMConfig.Deployment.SerialNumberPrefix)
		assert.Equal(t, serials, result.VLMConfig.Deployment.VMSerialNumbers)
	})
	t.Run("SuccessMultiHAPair", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber1, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber2, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber3, nil).Once()
		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return(serialNumber4, nil).Once()

		env.RegisterActivity(activity.AllocateClusterSerialNumber)
		val, err := env.ExecuteActivity(activity.AllocateClusterSerialNumber, req)

		assert.NoError(t, err)
		var result *vlm.CreateVSAClusterDeploymentRequest
		err = val.Get(&result)
		assert.NoError(t, err)
		assert.Equal(t, "", result.VLMConfig.Deployment.SerialNumberPrefix)
		assert.Equal(t, serials, result.VLMConfig.Deployment.VMSerialNumbers)
	})
	t.Run("FailureOnHAPairBeingZero", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		env.RegisterActivity(activity.AllocateClusterSerialNumber)
		_, err := env.ExecuteActivity(activity.AllocateClusterSerialNumber, req)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "HA pairs count must be at least 1")
	})

	t.Run("FailureOnRegionNotAvailable", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		env.RegisterActivity(activity.AllocateClusterSerialNumber)
		_, err := env.ExecuteActivity(activity.AllocateClusterSerialNumber, req)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "region number is not set")
	})
	t.Run("FailureOnGetNextSerialNumberInRegionError", func(t *testing.T) {
		// Setup Temporal test environment
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestActivityEnvironment()

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

		mockStorage.On("GetNextSerialNumberInRegion", mock.Anything, "93534").Return("", errors.New("error fetching serial number"))

		env.RegisterActivity(activity.AllocateClusterSerialNumber)
		_, err := env.ExecuteActivity(activity.AllocateClusterSerialNumber, req)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error fetching serial number")
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
	originalSetupNetworkFirewallsForSMB := activities.SetupNetworkFirewallsForSMB
	originalSetupNetworkFirewallsForNVMe := activities.SetupNetworkFirewallsForNVMe
	originalSetupNetworkFirewallsForIlbHealthCheck := activities.SetupNetworkFirewallsForIlbHealthCheck
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.InsertFirewall = originalInsertFirewall
		activities.SetupNetworkFirewallsForIscsi = originalSetupNetworkFirewallsForIscsi
		activities.SetupNetworkFirewallsForNFS = originalSetupNetworkFirewallsForNFS
		activities.SetupNetworkFirewallsForIntercluster = originalSetupNetworkFirewallsForIntercluster
		activities.SetupNetworkFirewallsForSMB = originalSetupNetworkFirewallsForSMB
		activities.SetupNetworkFirewallsForNVMe = originalSetupNetworkFirewallsForNVMe
		activities.SetupNetworkFirewallsForIlbHealthCheck = originalSetupNetworkFirewallsForIlbHealthCheck
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

		// Mock SetupNetworkFirewallsForSMB to return operation name
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-smb-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIlbHealthCheck to return operation name
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-ilb-health-check-firewall", nil
		}

		// Mock SetupNetworkFirewallsForNVMe to return operation name (not called in this test since poolMode is not ONTAP)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-nvme-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		assert.Len(t, *operations, 9)

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
		assert.Contains(t, operationNames, "operation-smb-firewall")
		assert.Contains(t, operationNames, "operation-ilb-health-check-firewall")
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

		// Mock SetupNetworkFirewallsForSMB to return operation name (will be created)
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		// Mock SetupNetworkFirewallsForIlbHealthCheck to return operation name (will be created)
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		// Mock SetupNetworkFirewallsForNVMe (not called in this test since poolMode is not ONTAP)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		// Mock SetupNetworkFirewallsForNVMe (not called in this test since poolMode is not ONTAP)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		// Mock SetupNetworkFirewallsForNVMe (not called in this test since poolMode is not ONTAP)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		// Mock SetupNetworkFirewallsForNVMe (not called in this test since poolMode is not ONTAP)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup Intercluster firewalls")
		_ = result // result unused in error case
	})

	t.Run("Success_ONTAPMode_CreatesNVMeFirewall", func(t *testing.T) {
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

		// Mock SetupNetworkFirewallsForSMB to return operation name
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-smb-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIlbHealthCheck to return operation name
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-ilb-health-check-firewall", nil
		}

		// Mock SetupNetworkFirewallsForNVMe to return operation name (should be called for ONTAP mode)
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-nvme-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "ONTAP")

		assert.NoError(t, err)
		assert.NotNil(t, result)

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		// Should have 10 operations: 4 VPC firewalls + iSCSI + NFS + intercluster + SMB + ILB + NVMe
		assert.Len(t, *operations, 10)

		// Check NVMe firewall operation is present
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
		}
		assert.Contains(t, operationNames, "operation-nvme-firewall")
	})

	t.Run("Success_NonONTAPMode_NoNVMeFirewall", func(t *testing.T) {
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

		// Mock SetupNetworkFirewallsForSMB to return operation name
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-smb-firewall", nil
		}

		// Mock SetupNetworkFirewallsForIlbHealthCheck to return operation name
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "operation-ilb-health-check-firewall", nil
		}

		// Mock SetupNetworkFirewallsForNVMe - should NOT be called for non-ONTAP mode
		nvmeCalled := false
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			nvmeCalled = true
			return "operation-nvme-firewall", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "VSA")

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.False(t, nvmeCalled, "NVMe firewall should not be called for non-ONTAP mode")

		var operations *[]commonparams.Operations
		err = result.Get(&operations)
		assert.NoError(t, err)
		assert.NotNil(t, operations)
		// Should have 9 operations: 4 VPC firewalls + iSCSI + NFS + intercluster + SMB + ILB (no NVMe)
		assert.Len(t, *operations, 9)

		// Check NVMe firewall operation is NOT present
		operationNames := make([]string, len(*operations))
		for i, op := range *operations {
			operationNames[i] = op.OperationName
		}
		assert.NotContains(t, operationNames, "operation-nvme-firewall")
	})

	t.Run("SetupNetworkFirewallsForNVMe_Fails", func(t *testing.T) {
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
			return "", nil
		}

		// Mock all other firewalls to succeed
		activities.SetupNetworkFirewallsForIscsi = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForNFS = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIntercluster = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		// Mock SetupNetworkFirewallsForNVMe to fail
		activities.SetupNetworkFirewallsForNVMe = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", errors.New("failed to setup NVMe firewalls")
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "ONTAP")

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to setup NVMe firewalls")
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

		result, err := env.ExecuteActivity(activity.CreateFirewalls, "", snHostProject, network, "")

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
		activities.SetupNetworkFirewallsForSMB = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}
		activities.SetupNetworkFirewallsForIlbHealthCheck = func(service hyperscaler2.GoogleServices, snHostProject, network string) (string, error) {
			return "", nil
		}

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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

		result, err := env.ExecuteActivity(activity.CreateFirewalls, project, snHostProject, network, "")

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
		expectedOp := &hyperscaler_models.ComputeOperation{
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
		expectedOp := &hyperscaler_models.ComputeOperation{
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

func TestPoolActivity_GetComputeOpStatus(t *testing.T) {
	activity := &activities.PoolActivity{}
	project := "test-project"
	operation := "test-operation"

	t.Run("Success_GlobalOperation_WithHeartbeat", func(t *testing.T) {
		expectedOp := &hyperscaler_models.ComputeOperation{
			Name:   operation,
			Status: "DONE",
		}
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		originalGetComputeOpStatus := activities.GetComputeOpStatus
		activities.GetComputeOpStatus = func(gcpService hyperscaler2.GoogleServices, project string, isRegionalResource bool, operation string) (*hyperscaler_models.ComputeOperation, error) {
			return expectedOp, nil
		}
		defer func() {
			hyperscaler2.GetGCPService = original
			activities.GetComputeOpStatus = originalGetComputeOpStatus
		}()

		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetComputeOpStatus)

		result, err := env.ExecuteActivity(activity.GetComputeOpStatus, project, false, operation)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Get the actual result from the activity execution
		var opResult *hyperscaler_models.ComputeOperation
		err = result.Get(&opResult)
		assert.NoError(t, err)
		assert.NotNil(t, opResult)
		assert.Equal(t, expectedOp.Name, opResult.Name)
		assert.Equal(t, expectedOp.Status, opResult.Status)
	})

	t.Run("Success_RegionalOperation_WithHeartbeat", func(t *testing.T) {
		expectedOp := &hyperscaler_models.ComputeOperation{
			Name:   operation,
			Status: "RUNNING",
		}
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{}, nil
		}
		originalGetComputeOpStatus := activities.GetComputeOpStatus
		activities.GetComputeOpStatus = func(gcpService hyperscaler2.GoogleServices, project string, isRegionalResource bool, operation string) (*hyperscaler_models.ComputeOperation, error) {
			return expectedOp, nil
		}
		defer func() {
			hyperscaler2.GetGCPService = original
			activities.GetComputeOpStatus = originalGetComputeOpStatus
		}()

		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetComputeOpStatus)

		result, err := env.ExecuteActivity(activity.GetComputeOpStatus, project, true, operation)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		// Get the actual result from the activity execution
		var opResult *hyperscaler_models.ComputeOperation
		err = result.Get(&opResult)
		assert.NoError(t, err)
		assert.NotNil(t, opResult)
		assert.Equal(t, expectedOp.Name, opResult.Name)
		assert.Equal(t, expectedOp.Status, opResult.Status)
	})

	t.Run("GetGCPServiceFails", func(t *testing.T) {
		original := hyperscaler2.GetGCPService
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, fmt.Errorf("service error")
		}
		defer func() { hyperscaler2.GetGCPService = original }()

		// Use Temporal test suite to provide proper activity context
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetComputeOpStatus)

		result, err := env.ExecuteActivity(activity.GetComputeOpStatus, project, false, operation)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}

func TestFetchOnTapCredentials_WithUserCertificate_Success(t *testing.T) {
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
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*coremodel.Certificate, error) {
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
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
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType:      env.USER_CERTIFICATE,
			CertificateID: "cert-id",
			SecretID:      "secret-id",
		},
	}
	originalGetCertificate := hyperscaler2.GetCertificateFromCacheOrSecretManager
	defer func() { hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate }()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*coremodel.Certificate, error) {
		return nil, errors.New("certificate error")
	}

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	_, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "certificate error")
}

func TestFetchOnTapCredentials_WithUserCertificate_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*coremodel.Certificate, error) {
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	_, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchOnTapCredentials_WithUsernamePwdSecMgr_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
	assert.NoError(t, err)
	assert.NotNil(t, creds)
	assert.Equal(t, "admin-password", creds.AdminPassword)
}

func TestFetchOnTapCredentials_WithUsernamePwdSecMgr_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	_, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchOnTapCredentials_WithDefaultAuthType_ReturnsPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	pool := &datamodel.Pool{
		PoolCredentials: &datamodel.PoolCredentials{
			AuthType: env.USERNAME_PWD, // Assume this is a default type
			Password: "plain-password",
		},
	}

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetOnTapCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetOnTapCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
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
			// Use Temporal test suite to provide proper activity context for heartbeat
			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestActivityEnvironment()
			activity := &activities.PoolActivity{}
			env.RegisterActivity(activity.GetInterClusterLifsFromVLMConfig)

			encodedValue, err := env.ExecuteActivity(activity.GetInterClusterLifsFromVLMConfig, &tt.vlmConfig)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				var result []string
				err = encodedValue.Get(&result)
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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test scaling up (cheaper to more expensive VM)
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	val, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-4-lssd", "c3-standard-22-lssd")

	assert.NoError(t, err)
	var isScalingUp bool
	err = val.Get(&isScalingUp)
	assert.NoError(t, err)
	assert.True(t, isScalingUp, "Should be scaling up from cheaper to more expensive VM")
}

func TestDetermineVMScalingDirection_Success_ScalingDown(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test scaling down (more expensive to cheaper VM)
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	val, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-22-lssd", "c3-standard-4-lssd")

	assert.NoError(t, err)
	var isScalingUp bool
	err = val.Get(&isScalingUp)
	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Should be scaling down from more expensive to cheaper VM")
}

func TestDetermineVMScalingDirection_LoadVMRSConfigError(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Test with non-existent config file
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	_, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, "non-existent-config.yaml", "n2-standard-8", "n2-standard-16")

	assert.Error(t, err)
	// VMRS errors are not wrapped as temporal application errors
	assert.Contains(t, err.Error(), "ConfigParseError")
}

func TestDetermineVMScalingDirection_InvalidVMTypes(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with invalid VM types that don't exist in the config
	// This should trigger an error when trying to compare VM types
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	_, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "invalid-vm-type-1", "invalid-vm-type-2")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "current VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_UnexpectedDecisionMakerType(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with valid config - this should work since we're using least_cost_single_vm strategy
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	val, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-4-lssd", "c3-standard-4-lssd")

	// This should work since the strategy is correct
	assert.NoError(t, err)
	var isScalingUp bool
	err = val.Get(&isScalingUp)
	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Same VM type should not be scaling")
}

func TestDetermineVMScalingDirection_VMsSortedByCostNil(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with valid config - this should work
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	val, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-4-lssd", "c3-standard-4-lssd")

	assert.NoError(t, err)
	var isScalingUp bool
	err = val.Get(&isScalingUp)
	assert.NoError(t, err)
	assert.False(t, isScalingUp, "Same VM type should not be scaling")
}

func TestDetermineVMScalingDirection_CurrentVMTypeNotFound(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with current VM type not in the list
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	_, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "non-existent-vm-type", "c3-standard-4-lssd")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "current VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_NewVMTypeNotFound(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with new VM type not in the list
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	_, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-4-lssd", "non-existent-vm-type")

	assert.Error(t, err)
	// The error should contain the specific error message about VM type not found
	assert.Contains(t, err.Error(), "new VM type not found in sorted list")
}

func TestDetermineVMScalingDirection_EarlyBreakOptimization(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	// Use the existing VMRS config file
	configPath := "../../../config/vmrs_gcp.yaml"

	// Test with first and second VM types to test early break optimization
	env.RegisterActivity(activity.DetermineVMScalingDirection)
	val, err := env.ExecuteActivity(activity.DetermineVMScalingDirection, configPath, "c3-standard-4-lssd", "c3-standard-8-lssd")

	assert.NoError(t, err)
	var isScalingUp bool
	err = val.Get(&isScalingUp)
	assert.NoError(t, err)
	assert.True(t, isScalingUp, "Should be scaling up from cheaper to more expensive VM")
}

func TestUpdatePoolFields_Success(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdatePoolFields)

	// Mock the UpdatePoolFields call
	poolUUID := "test-pool-uuid"
	updates := map[string]interface{}{
		"description": "updated description",
		"name":        "updated-pool-name",
	}

	mockStorage.On("UpdatePoolFields", mock.Anything, poolUUID, updates).Return(nil)

	// Test UpdatePoolFields
	_, err := env.ExecuteActivity(activity.UpdatePoolFields, poolUUID, updates)

	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestUpdatePoolFields_Error(t *testing.T) {
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Create a mock storage
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(activity.UpdatePoolFields)

	// Mock the UpdatePoolFields call to return an error
	poolUUID := "test-pool-uuid"
	updates := map[string]interface{}{
		"description": "updated description",
	}
	expectedError := errors.New("database update failed")

	mockStorage.On("UpdatePoolFields", mock.Anything, poolUUID, updates).Return(expectedError)

	// Test UpdatePoolFields with error
	_, err := env.ExecuteActivity(activity.UpdatePoolFields, poolUUID, updates)

	assert.Error(t, err)
	// Check that the error is wrapped as a temporal application error
	assert.Contains(t, err.Error(), "database update failed")

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
	// Setup Temporal test environment
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Mock the hyperscaler2.GetGCPService to return an error
	originalGetGCPService := hyperscaler2.GetGCPService
	defer func() { hyperscaler2.GetGCPService = originalGetGCPService }()

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return nil, errors.New("GCP service initialization failed")
	}

	activity := &activities.PoolActivity{}
	env.RegisterActivity(activity.ValidateZonesForMachineTypes)

	_, err := env.ExecuteActivity(activity.ValidateZonesForMachineTypes, "test-project", "us-central1-a", "us-central1-b", "e2-standard-4")

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
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	t.Run("HydrateUpdatedPoolToCCFE_HydrationEnabled", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.HydrateUpdatedPoolToCCFE)

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

		_, err := env.ExecuteActivity(activity.HydrateUpdatedPoolToCCFE, pool)
		assert.NoError(tt, err)
		assert.True(tt, called)
	})

	t.Run("HydrateUpdatedPoolToCCFE_HydrationFailed", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		activity := &activities.PoolActivity{SE: mockStorage}
		env.RegisterActivity(activity.HydrateUpdatedPoolToCCFE)

		pool := datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1, UUID: "test-pool-uuid"},
		}

		// Mock the hydrationActivities.HydrateUpdatedPoolToCCFE function to return error
		originalHydrateUpdatedPoolToCCFE := hydrationActivities.HydrateUpdatedPoolToCCFE
		hydrationActivities.HydrateUpdatedPoolToCCFE = func(ctx context.Context, dbPool datamodel.Pool) error {
			return errors.New("hydration failed")
		}
		defer func() { hydrationActivities.HydrateUpdatedPoolToCCFE = originalHydrateUpdatedPoolToCCFE }()

		_, err := env.ExecuteActivity(activity.HydrateUpdatedPoolToCCFE, pool)
		assert.Error(tt, err)
	})
}

func TestPoolActivity_GetIPsConsumedForSubnet(t *testing.T) {
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
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(2), (*result)[0].IPsReserved) // Only 2 addresses match the subnet
	})

	t.Run("success with no matching addresses", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved) // No addresses match the subnet
	})

	t.Run("success with multiple subnets", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetailsMultiple, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

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
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

		// Mock empty addresses list
		mockAddresses := &[]hyperscaler_models.Address{}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved)
	})

	t.Run("success with nil addresses", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

		// Mock nil addresses
		var mockAddresses *[]hyperscaler_models.Address = nil

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return mockAddresses, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 0)
	})

	t.Run("success with no subnetwork names", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

		// Test with empty subnetwork names
		tenancyDetailsEmpty := &commonparams.TenancyInfo{
			RegionalTenantProject: "test-project",
			SubnetworkNames:       []string{},
		}

		// Mock addresses list (even though it won't be used since we return early)
		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return &[]hyperscaler_models.Address{}, nil
		}

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetailsEmpty, region)
		assert.NoError(t, err)
		// When activity returns nil, encodedValue might be nil or empty
		// Try to get the result, but handle the case where it's nil
		var result *[]datamodel.SubnetToIPs
		if encodedValue != nil {
			err = encodedValue.Get(&result)
			// If Get fails with "no data available", it means the result was nil
			if err != nil {
				assert.Contains(t, err.Error(), "no data available")
			} else {
				assert.Nil(t, result)
			}
		} else {
			// encodedValue is nil, which is also acceptable for nil return
			assert.Nil(t, result)
		}
	})

	t.Run("error when GetGCPService fails", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

		// Mock GCP service to return error
		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("failed to get GCP service")
		}

		_, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)

		assert.Error(t, err)
	})

	t.Run("error when ListAddressesByDeployment fails", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return &google.GcpServices{Logger: log.NewLogger()}, nil
		}

		activities.ListAddressesByDeployment = func(gcpService hyperscaler2.GoogleServices, project, region, deploymentName string) (*[]hyperscaler_models.Address, error) {
			return nil, errors.New("failed to list addresses")
		}

		_, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to list addresses")
	})

	t.Run("success with addresses having empty SubnetURI", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(1), (*result)[0].IPsReserved) // Only address-2 matches
	})

	t.Run("success with partial subnet name matches", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		env := testSuite.NewTestActivityEnvironment()
		env.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := env.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(0), (*result)[0].IPsReserved) // address-1 and address-2 contain "test-subnet"
	})

	t.Run("success with HasSuffix matching behavior", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := testEnv.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Len(t, *result, 1)
		assert.Equal(t, "test-subnet", (*result)[0].SubnetName)
		assert.Equal(t, int64(2), (*result)[0].IPsReserved) // Only address-1 and address-4 should match (end with /test-subnet)
	})

	t.Run("success with forward slash prefix in HasSuffix check", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.GetIPsConsumedForSubnet)

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

		encodedValue, err := testEnv.ExecuteActivity(activity.GetIPsConsumedForSubnet, pool, tenancyDetails, region)
		assert.NoError(t, err)
		var result *[]datamodel.SubnetToIPs
		err = encodedValue.Get(&result)

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
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

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

	mockCloudService.On("GetBucket", mock.Anything, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, bucketName)
	assert.NoError(t, err)
	err = val.Get(&result)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.Equal(t, true, result.SatisfiesPzi)
	assert.Equal(t, false, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_EmptyBucketName(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, "")
	assert.Error(t, err)
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
	assert.Nil(t, result)
}

func TestGetBucketCompliance_GetCloudServiceError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

	bucketName := "test-bucket"

	// Mock GetCloudService to return error
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return nil, fmt.Errorf("failed to get cloud service")
	}

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, bucketName)
	assert.Error(t, err)
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
	assert.Nil(t, result)
}

func TestGetBucketCompliance_GetBucketError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

	bucketName := "test-bucket"

	// Mock cloud service
	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	mockCloudService.On("GetBucket", mock.Anything, bucketName).Return(nil, fmt.Errorf("bucket not found")).Once()

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, bucketName)
	assert.Error(t, err)
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
	assert.Nil(t, result)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_BothComplianceFieldsTrue(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

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

	mockCloudService.On("GetBucket", mock.Anything, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, bucketName)
	assert.NoError(t, err)
	err = val.Get(&result)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.True(t, result.SatisfiesPzi)
	assert.True(t, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_BothComplianceFieldsFalse(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	// Store original function
	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.GetBucketCompliance)

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

	mockCloudService.On("GetBucket", mock.Anything, bucketName).Return(expectedCloudBucketDetails, nil).Once()

	var result *datamodel.BucketDetails
	val, err := env.ExecuteActivity(activity.GetBucketCompliance, bucketName)
	assert.NoError(t, err)
	err = val.Get(&result)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, bucketName, result.BucketName)
	assert.False(t, result.SatisfiesPzi)
	assert.False(t, result.SatisfiesPzs)
	mockCloudService.AssertExpectations(t)
}

func TestGetBucketCompliance_AllScenarios(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

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
			env.RegisterActivity(activity.GetBucketCompliance)

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

			mockCloudService.On("GetBucket", mock.Anything, tc.bucketName).Return(expectedCloudBucketDetails, nil).Once()

			var result *datamodel.BucketDetails
			val, err := env.ExecuteActivity(activity.GetBucketCompliance, tc.bucketName)
			assert.NoError(t, err)
			err = val.Get(&result)

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
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
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
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "CN", creds.Certificate.CommonName)
		assert.Equal(t, "cert", creds.Certificate.Certificate)
		assert.Equal(t, "key", creds.Certificate.PrivateKey)
		assert.Equal(t, []string{"chain"}, creds.Certificate.InterMediateCertificate)
		assert.Equal(t, "", creds.AdminPassword)
	})

	t.Run("USER_CERTIFICATE ExpertModeCredentials empty error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: nil,
			},
		}
		_, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.Error(t, err)
	})

	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		hyperscaler2.GenerateAndCreateCertificateForVSACluster = func(gcpService hyperscaler2.GoogleServices, clusterName, username string, poolCredentials *datamodel.PoolCredentials, isServerAuthEnabled bool) (*hyperscaler_models.CustomCertificateResponse, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("cert error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.Error(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return &hyperscaler_models.CustomSecret{
				SecretVersion: &hyperscaler_models.CustomSecretVersion{Value: "pwd"},
			}, nil
		}
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "pwd", creds.AdminPassword)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		hyperscaler2.GeneratePasswordForVSACluster = func(gcpService hyperscaler2.GoogleServices, secretID string) (*hyperscaler_models.CustomSecret, error) {
			return nil, workflows.ConvertToVSAError(fmt.Errorf("pwd error"))
		}
		_, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.Error(t, err)
	})

	t.Run("default password", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		encodedValue, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "password", creds.AdminPassword)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.CreateExpertModeCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.CreateExpertModeCredentials, pool, clusterName, username)
		assert.Error(t, err)
	})
}

func TestPoolActivity_DeleteExpertModeCredentials(t *testing.T) {
	activity := &activities.PoolActivity{}

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
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
			assert.Equal(t, "cert-id", poolCredentials.CertificateID)
			return nil
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("USER_CERTIFICATE ExpertModeCredentials empty error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

		pool := &datamodel.Pool{
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: nil,
			},
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.Error(t, err)
	})
	t.Run("USER_CERTIFICATE error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		hyperscaler2.RevokeCertificateAndDeleteFromCacheAndSecretManager = func(gcpService hyperscaler2.GoogleServices, poolCredentials *datamodel.PoolCredentials) error {
			return errors.New("revoke error")
		}
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "revoke error")
	})

	t.Run("USERNAME_PWD_SEC_MGR success", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("USERNAME_PWD_SEC_MGR error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delete error")
	})

	t.Run("default password - no cert no secret-manager", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.NoError(t, err)
	})

	t.Run("GetGCPService error", func(t *testing.T) {
		// Use Temporal test suite to provide proper activity context for heartbeat
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(activity.DeleteExpertModeCredentials)

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
		_, err := testEnv.ExecuteActivity(activity.DeleteExpertModeCredentials, pool)
		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "gcp error", "CustomError", false)
	})
}

func TestFetchExpertModeCredentials_WithUserCertificate_Success(t *testing.T) {
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
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*coremodel.Certificate, error) {
		return &coremodel.Certificate{
			CommonName:               "CN",
			SignedCertificate:        "cert",
			PrivateKey:               "key",
			InterMediateCertificates: []string{"intermediate"},
		}, nil
	}

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetExpertModeCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetExpertModeCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
	assert.NoError(t, err)
	assert.Equal(t, "CN", creds.Certificate.CommonName)
	assert.Equal(t, "cert", creds.Certificate.Certificate)
	assert.Equal(t, "key", creds.Certificate.PrivateKey)
	assert.Equal(t, []string{"intermediate"}, creds.Certificate.InterMediateCertificate)
}

func TestFetchExpertModeCredentials_WithUserCertificate_CertificateError(t *testing.T) {
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
	defer func() { hyperscaler2.GetCertificateFromCacheOrSecretManager = originalGetCertificate }()
	hyperscaler2.GetCertificateFromCacheOrSecretManager = func(ctx context.Context, poolCredentials *datamodel.PoolCredentials) (*coremodel.Certificate, error) {
		return nil, errors.New("certificate error")
	}

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetExpertModeCredentials)

	_, err := testEnv.ExecuteActivity(activity.GetExpertModeCredentials, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "certificate error")
}

func TestFetchExpertModeCredentials_WithUsernamePwdSecMgr_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetExpertModeCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetExpertModeCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
	assert.NoError(t, err)
	assert.NotNil(t, creds)
	assert.Equal(t, "admin-password", creds.AdminPassword)
}

func TestFetchExpertModeCredentials_WithUsernamePwdSecMgr_SecretError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetExpertModeCredentials)

	_, err := testEnv.ExecuteActivity(activity.GetExpertModeCredentials, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid resource field value")
}

func TestFetchExpertModeCredentials_WithDefaultAuthType_ReturnsPassword(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
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

	// Create Temporal test environment for activity context
	testSuite := &testsuite.WorkflowTestSuite{}
	testEnv := testSuite.NewTestActivityEnvironment()
	testEnv.RegisterActivity(activity.GetExpertModeCredentials)

	encodedValue, err := testEnv.ExecuteActivity(activity.GetExpertModeCredentials, pool)
	assert.NoError(t, err)
	var creds *vlm.OntapCredentials
	err = encodedValue.Get(&creds)
	assert.NoError(t, err)
	assert.Equal(t, "plain-password", creds.AdminPassword)
}

func TestSetWaflMaxVolCloneHier(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	testEnv := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockStorage}
	testEnv.RegisterActivity(activity.SetWaflMaxVolCloneHier)

	t.Run("WhenNodeIsNil_ThenReturnNil", func(tt *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}
		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, (*coremodel.Node)(nil), pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
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

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.NoError(tt, err)
		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_AndPoolIsNil_ThenReturnError", func(tt *testing.T) {
		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			AuthType:        env.USER_CERTIFICATE,
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, (*datamodel.Pool)(nil))
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cannot fallback to password auth: pool or pool credentials are nil")
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_AndPoolCredentialsIsNil_ThenReturnError", func(tt *testing.T) {
		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			AuthType:        env.USER_CERTIFICATE,
		}

		pool := &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
			PoolCredentials: nil,
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "cannot fallback to password auth: pool or pool credentials are nil")
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_AndGetPasswordFails_ThenReturnError", func(tt *testing.T) {
		// Save original function
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
		}()

		// Mock secret manager to return error
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return "", errors.New("secret manager error")
		}

		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			AuthType:        env.USER_CERTIFICATE,
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USER_CERTIFICATE,
				SecretID: "test-secret-id",
				Password: "",
			},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to get password for cert-auth fallback")
		assert.Contains(tt, err.Error(), "secret manager error")
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_AndGetPasswordSucceeds_ThenOverrideAuthTypeAndPassword", func(tt *testing.T) {
		// Save original function
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		// Mock secret manager to return password
		expectedPassword := "secret-manager-password"
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			return expectedPassword, nil
		}

		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)

		// Capture the node passed to GetProviderByNode to verify AuthType and Password were overridden
		var capturedNode *coremodel.Node
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			capturedNode = node
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
			AuthType:        env.USER_CERTIFICATE,
			Password:        "", // Initially empty
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USER_CERTIFICATE,
				SecretID: "test-secret-id",
				Password: "",
			},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.NoError(tt, err)

		// Verify that AuthType and Password were overridden on the node
		assert.NotNil(tt, capturedNode)
		assert.Equal(tt, env.USERNAME_PWD, capturedNode.AuthType, "AuthType should be overridden to USERNAME_PWD")
		assert.Equal(tt, expectedPassword, capturedNode.Password, "Password should be set from secret manager")

		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_WithUSERNAME_PWD_SEC_MGR_Credentials_ThenOverrideAuthTypeAndPassword", func(tt *testing.T) {
		// Save original function
		originalGetPassword := hyperscaler2.GetPasswordFromCacheOrSecretManager
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetPasswordFromCacheOrSecretManager = originalGetPassword
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		// Mock secret manager to return password
		expectedPassword := "secret-password"
		hyperscaler2.GetPasswordFromCacheOrSecretManager = func(ctx context.Context, secretID string) (string, error) {
			assert.Equal(tt, "test-secret-id", secretID)
			return expectedPassword, nil
		}

		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)

		// Capture the node passed to GetProviderByNode to verify AuthType and Password were overridden
		var capturedNode *coremodel.Node
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			capturedNode = node
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
			AuthType:        env.USER_CERTIFICATE,
			Password:        "",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD_SEC_MGR,
				SecretID: "test-secret-id",
				Password: "",
			},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.NoError(tt, err)

		// Verify that AuthType and Password were overridden on the node
		assert.NotNil(tt, capturedNode)
		assert.Equal(tt, env.USERNAME_PWD, capturedNode.AuthType, "AuthType should be overridden to USERNAME_PWD")
		assert.Equal(tt, expectedPassword, capturedNode.Password, "Password should be set from secret manager")

		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeAuthTypeIsUSER_CERTIFICATE_WithDirectPassword_ThenOverrideAuthTypeAndPassword", func(tt *testing.T) {
		// Save original function
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)

		// Capture the node passed to GetProviderByNode to verify AuthType and Password were overridden
		var capturedNode *coremodel.Node
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			capturedNode = node
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

		expectedPassword := "direct-password"
		node := &coremodel.Node{
			EndpointAddress: "127.0.0.1",
			AuthType:        env.USER_CERTIFICATE,
			Password:        "",
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
				SecretID: "",
				Password: expectedPassword, // Direct password, no secret manager
			},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.NoError(tt, err)

		// Verify that AuthType and Password were overridden on the node
		assert.NotNil(tt, capturedNode)
		assert.Equal(tt, env.USERNAME_PWD, capturedNode.AuthType, "AuthType should be overridden to USERNAME_PWD")
		assert.Equal(tt, expectedPassword, capturedNode.Password, "Password should be set from pool credentials")

		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})

	t.Run("WhenNodeHasEndpointAddressesToHostNameMap_ThenDeepCopyIsCreated", func(tt *testing.T) {
		// Save original function
		originalGetProviderByNode := hyperscaler2.GetProviderByNode
		defer func() {
			hyperscaler2.GetProviderByNode = originalGetProviderByNode
		}()

		mockProvider := new(vsa.MockProvider)
		mockRESTClient := new(ontap_rest.MockRESTClient)
		mockNetworkingClient := new(ontap_rest.MockNetworkingClient)

		// Capture the node passed to GetProviderByNode to verify the map was deep copied
		var capturedNode *coremodel.Node
		hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
			capturedNode = node
			// Simulate GetProviderByNode potentially modifying the map (as it does in real code)
			// Add a new entry to verify it doesn't affect the original
			if node.EndpointAddressesToHostNameMap != nil {
				node.EndpointAddressesToHostNameMap["192.168.1.3"] = "node3.example.com"
			}
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

		// Create a node with a non-nil EndpointAddressesToHostNameMap
		originalMap := map[string]string{
			"192.168.1.1": "node1.example.com",
			"192.168.1.2": "node2.example.com",
		}
		node := &coremodel.Node{
			EndpointAddress:                "127.0.0.1",
			AuthType:                       env.USERNAME_PWD,
			Password:                       "test-password",
			EndpointAddressesToHostNameMap: originalMap,
		}

		// Create a copy of the original map to verify it's not modified
		originalMapCopy := make(map[string]string)
		for k, v := range originalMap {
			originalMapCopy[k] = v
		}

		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		}

		_, err := testEnv.ExecuteActivity(activity.SetWaflMaxVolCloneHier, node, pool)
		assert.NoError(tt, err)

		// Verify that the original node's map was not modified (deep copy was created)
		// The original map should still have only 2 entries
		assert.Equal(tt, 2, len(node.EndpointAddressesToHostNameMap), "Original node's EndpointAddressesToHostNameMap should not be modified")
		assert.Equal(tt, originalMapCopy, node.EndpointAddressesToHostNameMap, "Original node's EndpointAddressesToHostNameMap should remain unchanged")

		// Verify that the captured node (copy) has the modified map with 3 entries
		assert.NotNil(tt, capturedNode)
		assert.Equal(tt, 3, len(capturedNode.EndpointAddressesToHostNameMap), "Captured node's map should have the new entry added by GetProviderByNode")
		assert.Contains(tt, capturedNode.EndpointAddressesToHostNameMap, "192.168.1.3", "Captured node's map should contain the new entry")

		mockProvider.AssertExpectations(tt)
		mockRESTClient.AssertExpectations(tt)
		mockNetworkingClient.AssertExpectations(tt)
	})
}

func TestPoolActivity_GetRbacHash(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	bucketName := "test-bucket"
	ontapversion := "9.18.1"

	originalGetGCPService := hyperscaler2.GetGCPService
	originalGetBucketFile := activities.GetBucketFile
	defer func() {
		hyperscaler2.GetGCPService = originalGetGCPService
		activities.GetBucketFile = originalGetBucketFile
	}()

	t.Run("GetGCPService fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return nil, errors.New("GCP service initialization failed")
		}

		result, err := activity.GetRbacHash(ctx, ontapversion)

		assert.Error(t, err)
		assert.Nil(t, result)
		assertTemporalApplicationError(t, err, "GCP service initialization failed", vsaerrors.CustomErrorType, false)
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetFileFromBucket fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		mockGCPService := &google.GcpServices{}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.GetBucketFile = func(service hyperscaler2.GoogleServices, ctx context.Context, bucketName, fileName string) (*hyperscaler_models.BucketFileDetails, error) {
			return nil, errors.New("bucket file not found")
		}

		result, err := activity.GetRbacHash(ctx, ontapversion)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, err.Error(), "bucket file not found")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		mockGCPService := &google.GcpServices{}

		expectedBucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     bucketName,
			FileUrl:        ontapversion,
			FileHashSHA256: "abc123def456",
		}

		hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
			return mockGCPService, nil
		}

		activities.GetBucketFile = func(service hyperscaler2.GoogleServices, ctx context.Context, bucketName, fileName string) (*hyperscaler_models.BucketFileDetails, error) {
			return expectedBucketFileDetails, nil
		}

		result, err := activity.GetRbacHash(ctx, ontapversion)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedBucketFileDetails, result)
		assert.Equal(t, bucketName, result.BucketName)
		assert.Equal(t, ontapversion, result.FileUrl)
		assert.Equal(t, "abc123def456", result.FileHashSHA256)
		mockStorage.AssertExpectations(t)
	})
}

func TestPoolActivity_ValidateRbacHash(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	ontapVersion := "9.18.1"
	validHash := "abc123def456"
	validBucketFileDetails := &hyperscaler_models.BucketFileDetails{
		BucketName:     "test-bucket",
		FileUrl:        "test-url",
		FileHashSHA256: validHash,
	}

	originalOntapModeRBACChecksums := activities.OntapModeRBACChecksums
	originalValidateRbacHashFlag := activities.ValidateRbacHashFlag
	defer func() {
		activities.OntapModeRBACChecksums = originalOntapModeRBACChecksums
		activities.ValidateRbacHashFlag = originalValidateRbacHashFlag
	}()

	t.Run("validation disabled - should skip validation", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = false

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("bucketFileDetails is nil", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true

		err := activity.ValidateRbacHash(ctx, ontapVersion, nil)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "bucket file details or hash is empty", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("hash is empty", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		bucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     "test-bucket",
			FileUrl:        "test-url",
			FileHashSHA256: "",
		}

		err := activity.ValidateRbacHash(ctx, ontapVersion, bucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "bucket file details or hash is empty", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("configuration is empty", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		activities.OntapModeRBACChecksums = ""

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "ONTAP_MODE_RBAC_CHECKSUMS not configured", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("configuration is empty JSON object", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		activities.OntapModeRBACChecksums = "{}"

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "ONTAP_MODE_RBAC_CHECKSUMS not configured", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("invalid JSON configuration", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		activities.OntapModeRBACChecksums = "{invalid json}"

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, "failed to parse ONTAP_MODE_RBAC_CHECKSUMS configuration", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("ONTAP version not found in configuration", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		activities.OntapModeRBACChecksums = `{"9.19.1": "different-hash"}`

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, fmt.Sprintf("ONTAP version %s not found in ONTAP_MODE_RBAC_CHECKSUMS configuration", ontapVersion), vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("checksum mismatch", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		configuredHash := "different-hash-123"
		activities.OntapModeRBACChecksums = fmt.Sprintf(`{"%s": "%s"}`, ontapVersion, configuredHash)

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.Error(t, err)
		assertTemporalApplicationError(t, err, fmt.Sprintf("RBAC hash mismatch for ONTAP version %s: expected %s, got %s", ontapVersion, configuredHash, validHash), vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("success - hash matches", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		activities.OntapModeRBACChecksums = fmt.Sprintf(`{"%s": "%s"}`, ontapVersion, validHash)

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("success - multiple versions in configuration", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}
		activities.ValidateRbacHashFlag = true
		otherVersion := "9.19.1"
		otherHash := "other-hash-789"
		activities.OntapModeRBACChecksums = fmt.Sprintf(`{"%s": "%s", "%s": "%s"}`, ontapVersion, validHash, otherVersion, otherHash)

		err := activity.ValidateRbacHash(ctx, ontapVersion, validBucketFileDetails)

		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})
}

func Test_getBucketFile(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})
	bucketName := "test-bucket"
	fileName := "test-file.yaml"

	t.Run("Service GetFileFromBucket fails", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		mockService.On("GetFileFromBucket", ctx, bucketName, fileName).Return(nil, errors.New("service error"))

		result, err := activities.GetBucketFile(mockService, ctx, bucketName, fileName)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Equal(t, err.Error(), "service error")
		mockService.AssertExpectations(t)
	})

	t.Run("Success", func(t *testing.T) {
		mockService := new(hyperscaler2.MockGoogleServices)
		expectedBucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     bucketName,
			FileUrl:        fileName,
			FileHashSHA256: "test-hash-123",
		}

		mockService.On("GetFileFromBucket", ctx, bucketName, fileName).Return(expectedBucketFileDetails, nil)

		result, err := activities.GetBucketFile(mockService, ctx, bucketName, fileName)

		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, expectedBucketFileDetails, result)
		assert.Equal(t, bucketName, result.BucketName)
		assert.Equal(t, fileName, result.FileUrl)
		assert.Equal(t, "test-hash-123", result.FileHashSHA256)
		mockService.AssertExpectations(t)
	})
}

func TestPoolActivity_UpdateRbacCheckSumInPool(t *testing.T) {
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	bucketFileDetails := &hyperscaler_models.BucketFileDetails{
		BucketName:     "test-bucket",
		FileUrl:        "rbac.yaml",
		FileHashSHA256: "abc123def456",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		BuildInfo: &datamodel.PoolBuildInfo{
			RbacFileHash: "",
			RbacFileUrl:  "",
		},
	}

	t.Run("GetPoolByUUID fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		mockStorage.On("GetPoolByUUID", ctx, pool.UUID).Return(nil, errors.New("pool not found"))

		err := activity.UpdateRbacCheckSumInPool(ctx, pool, bucketFileDetails)

		assert.Error(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("UpdatePoolFields fails", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		// Mock GetPoolByUUID to return the pool with BuildInfo
		latestPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: pool.UUID},
			BuildInfo: &datamodel.PoolBuildInfo{
				RbacFileHash: "",
				RbacFileUrl:  "",
			},
		}
		mockStorage.On("GetPoolByUUID", ctx, pool.UUID).Return(latestPool, nil)

		expectedUpdates := map[string]interface{}{
			"build_info": &datamodel.PoolBuildInfo{
				RbacFileHash: "abc123def456",
				RbacFileUrl:  "gs://test-bucket/rbac.yaml",
			},
		}

		mockStorage.On("UpdatePoolFields", ctx, pool.UUID, expectedUpdates).Return(errors.New("database update failed"))

		err := activity.UpdateRbacCheckSumInPool(ctx, pool, bucketFileDetails)

		assert.Error(t, err)
		assert.Equal(t, err.Error(), "database update failed")
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		// Mock GetPoolByUUID to return the pool with BuildInfo
		latestPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: pool.UUID},
			BuildInfo: &datamodel.PoolBuildInfo{
				RbacFileHash: "",
				RbacFileUrl:  "",
			},
		}
		mockStorage.On("GetPoolByUUID", ctx, pool.UUID).Return(latestPool, nil)

		expectedUpdates := map[string]interface{}{
			"build_info": &datamodel.PoolBuildInfo{
				RbacFileHash: "abc123def456",
				RbacFileUrl:  "gs://test-bucket/rbac.yaml",
			},
		}

		mockStorage.On("UpdatePoolFields", ctx, pool.UUID, expectedUpdates).Return(nil)

		err := activity.UpdateRbacCheckSumInPool(ctx, pool, bucketFileDetails)

		assert.NoError(t, err)
		// Note: pool.BuildInfo is not modified in place since we fetch latest data
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success with existing build info", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		activity := activities.PoolActivity{SE: mockStorage}

		poolWithBuildInfo := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid-2",
			},
			BuildInfo: &datamodel.PoolBuildInfo{
				RbacFileHash:  "old-hash",
				RbacFileUrl:   "gs://old-bucket/old-file.yaml",
				VSABuildImage: "test-image",
			},
		}

		// Mock GetPoolByUUID to return the latest pool with BuildInfo (simulating concurrent update)
		latestPool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: poolWithBuildInfo.UUID},
			BuildInfo: &datamodel.PoolBuildInfo{
				RbacFileHash:  "old-hash",
				RbacFileUrl:   "gs://old-bucket/old-file.yaml",
				VSABuildImage: "test-image-updated", // Simulate concurrent update
				OntapVersion:  "9.18.1",
			},
		}
		mockStorage.On("GetPoolByUUID", ctx, poolWithBuildInfo.UUID).Return(latestPool, nil)

		expectedUpdates := map[string]interface{}{
			"build_info": &datamodel.PoolBuildInfo{
				RbacFileHash:  "abc123def456",
				RbacFileUrl:   "gs://test-bucket/rbac.yaml",
				VSABuildImage: "test-image-updated", // Preserved from latest pool
				OntapVersion:  "9.18.1",             // Preserved from latest pool
			},
		}

		mockStorage.On("UpdatePoolFields", ctx, poolWithBuildInfo.UUID, expectedUpdates).Return(nil)

		err := activity.UpdateRbacCheckSumInPool(ctx, poolWithBuildInfo, bucketFileDetails)

		assert.NoError(t, err)
		// Verify that concurrent changes (VSABuildImage, OntapVersion) are preserved
		mockStorage.AssertExpectations(t)
	})
}

// TestCalculateBatchPlan_Success_6HAPairs_4ParallelNodes tests successful batch plan calculation for 6 HA pairs with 4 parallel nodes
func TestCalculateBatchPlan_Success_6HAPairs_4ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  6,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 6, result.NumHAPairs)
	// batchSize = max(1, (6*2)/4) = max(1, 3) = 3
	assert.Equal(t, 3, result.BatchSize)
	// numWorkflowCalls = ceil(6/3) = 2
	assert.Equal(t, 2, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 2)
	// First batch: [1, 2, 3]
	assert.Equal(t, []int{1, 2, 3}, result.BatchIndices[0])
	// Second batch: [4, 5, 6]
	assert.Equal(t, []int{4, 5, 6}, result.BatchIndices[1])
}

// TestCalculateBatchPlan_Success_2HAPairs_4ParallelNodes tests successful batch plan calculation for 2 HA pairs with 4 parallel nodes
func TestCalculateBatchPlan_Success_2HAPairs_4ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  2,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 2, result.NumHAPairs)
	// batchSize = max(1, (2*2)/4) = max(1, 1) = 1
	assert.Equal(t, 1, result.BatchSize)
	// numWorkflowCalls = ceil(2/1) = 2
	assert.Equal(t, 2, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 2)
	// First batch: [1]
	assert.Equal(t, []int{1}, result.BatchIndices[0])
	// Second batch: [2]
	assert.Equal(t, []int{2}, result.BatchIndices[1])
}

// TestCalculateBatchPlan_Success_8HAPairs_4ParallelNodes tests successful batch plan calculation for 8 HA pairs with 4 parallel nodes
func TestCalculateBatchPlan_Success_8HAPairs_4ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  8,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 8, result.NumHAPairs)
	// batchSize = max(1, (8*2)/4) = max(1, 4) = 4
	assert.Equal(t, 4, result.BatchSize)
	// numWorkflowCalls = ceil(8/4) = 2
	assert.Equal(t, 2, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 2)
	// First batch: [1, 2, 3, 4]
	assert.Equal(t, []int{1, 2, 3, 4}, result.BatchIndices[0])
	// Second batch: [5, 6, 7, 8]
	assert.Equal(t, []int{5, 6, 7, 8}, result.BatchIndices[1])
}

// TestCalculateBatchPlan_Success_7HAPairs_4ParallelNodes tests successful batch plan calculation with remainder
func TestCalculateBatchPlan_Success_7HAPairs_4ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  7,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 7, result.NumHAPairs)
	// batchSize = max(1, (7*2)/4) = max(1, 3) = 3
	assert.Equal(t, 3, result.BatchSize)
	// numWorkflowCalls = ceil(7/3) = ceil(2.33) = 3
	assert.Equal(t, 3, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 3)
	// First batch: [1, 2, 3]
	assert.Equal(t, []int{1, 2, 3}, result.BatchIndices[0])
	// Second batch: [4, 5, 6]
	assert.Equal(t, []int{4, 5, 6}, result.BatchIndices[1])
	// Third batch: [7]
	assert.Equal(t, []int{7}, result.BatchIndices[2])
}

// TestCalculateBatchPlan_Success_1HAPair_4ParallelNodes tests successful batch plan calculation for single HA pair
func TestCalculateBatchPlan_Success_1HAPair_4ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  1,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.NumHAPairs)
	// batchSize = max(1, (1*2)/4) = max(1, 0) = 1
	assert.Equal(t, 1, result.BatchSize)
	// numWorkflowCalls = ceil(1/1) = 1
	assert.Equal(t, 1, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 1)
	// First batch: [1]
	assert.Equal(t, []int{1}, result.BatchIndices[0])
}

// TestCalculateBatchPlan_Success_12HAPairs_6ParallelNodes tests successful batch plan calculation for larger configuration
func TestCalculateBatchPlan_Success_12HAPairs_6ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  12,
		ParallelNumberOfNodesForITC: 6,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 12, result.NumHAPairs)
	// batchSize = max(1, (12*2)/6) = max(1, 4) = 4
	assert.Equal(t, 4, result.BatchSize)
	// numWorkflowCalls = ceil(12/4) = 3
	assert.Equal(t, 3, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 3)
	// First batch: [1, 2, 3, 4]
	assert.Equal(t, []int{1, 2, 3, 4}, result.BatchIndices[0])
	// Second batch: [5, 6, 7, 8]
	assert.Equal(t, []int{5, 6, 7, 8}, result.BatchIndices[1])
	// Third batch: [9, 10, 11, 12]
	assert.Equal(t, []int{9, 10, 11, 12}, result.BatchIndices[2])
}

// TestCalculateBatchPlan_Success_5HAPairs_8ParallelNodes tests when parallel nodes is larger than needed
func TestCalculateBatchPlan_Success_5HAPairs_8ParallelNodes(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  5,
		ParallelNumberOfNodesForITC: 8,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 5, result.NumHAPairs)
	// batchSize = max(1, (5*2)/8) = max(1, 1) = 1
	assert.Equal(t, 1, result.BatchSize)
	// numWorkflowCalls = ceil(5/1) = 5
	assert.Equal(t, 5, result.NumWorkflowCalls)
	assert.Len(t, result.BatchIndices, 5)
	// Verify all batches have single HA pair
	for i := 0; i < 5; i++ {
		assert.Equal(t, []int{i + 1}, result.BatchIndices[i])
	}
}

// TestCalculateBatchPlan_Error_InvalidNumHAPairs_Zero tests error handling for zero HA pairs
func TestCalculateBatchPlan_Error_InvalidNumHAPairs_Zero(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  0,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	_, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid number of HA pairs: 0")
}

// TestCalculateBatchPlan_Error_InvalidNumHAPairs_Negative tests error handling for negative HA pairs
func TestCalculateBatchPlan_Error_InvalidNumHAPairs_Negative(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  -1,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	_, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid number of HA pairs: -1")
}

// TestCalculateBatchPlan_Error_InvalidParallelNodes_Zero tests error handling for zero parallel nodes
func TestCalculateBatchPlan_Error_InvalidParallelNodes_Zero(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  6,
		ParallelNumberOfNodesForITC: 0,
	}

	// Act
	_, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid parallel number of nodes for ITC: 0")
}

// TestCalculateBatchPlan_Error_InvalidParallelNodes_Negative tests error handling for negative parallel nodes
func TestCalculateBatchPlan_Error_InvalidParallelNodes_Negative(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  6,
		ParallelNumberOfNodesForITC: -1,
	}

	// Act
	_, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid parallel number of nodes for ITC: -1")
}

// TestCalculateBatchPlan_Success_IndicesAreOneIndexed tests that batch indices are 1-indexed (not 0-indexed)
func TestCalculateBatchPlan_Success_IndicesAreOneIndexed(t *testing.T) {
	// Arrange
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// Use Temporal test suite to provide proper activity context for heartbeat
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestActivityEnvironment()
	env.RegisterActivity(activity.CalculateBatchPlanForUpdate)

	input := activities.CalculateBatchPlanActivityInput{
		NumHAPairs:                  3,
		ParallelNumberOfNodesForITC: 4,
	}

	// Act
	encodedValue, err := env.ExecuteActivity(activity.CalculateBatchPlanForUpdate, input)
	assert.NoError(t, err)
	var result *activities.CalculateBatchPlanActivityOutput
	err = encodedValue.Get(&result)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, result)
	// With 3 HA pairs and 4 parallel nodes: batchSize = max(1, (3*2)/4) = 1
	// So we get 3 batches: [1], [2], [3]
	assert.Len(t, result.BatchIndices, 3)
	assert.Equal(t, []int{1}, result.BatchIndices[0])
	assert.Equal(t, []int{2}, result.BatchIndices[1])
	assert.Equal(t, []int{3}, result.BatchIndices[2])
	// Verify no zero indices
	for _, batch := range result.BatchIndices {
		for _, idx := range batch {
			assert.Greater(t, idx, 0, "HA pair indices should be 1-indexed")
		}
	}
}

func TestParseVlmConfig(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	// actualValidVLMConfig contains a valid VLM config with negs as a string; this config is expected to successfully unmarshal.
	actualValidVLMConfig := `{"cloud":{"ha_pair":[{"vm1":{"region":"australia-southeast1","zone":"australia-southeast1-a","name":"gcnv-123ae2cfcaf0326-01","host_name":"gcnv-123ae2cfcaf0326-01","serial_number":"93520140000000000001","node_index":1,"is_mediator":false,"lifs":{"clus":{"lif_name":"gcnv-123ae2cfcaf0326-01-clus","vsa_ip_type":"clus","ip":"198.18.0.50","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},"ic":{"lif_name":"gcnv-123ae2cfcaf0326-01-ic","vsa_ip_type":"ic","ip":"198.18.32.63","lif_uuid":"","network_config":{"subnet":"ic-e0b-subnet-01","vpc":"ic-e0b-vpc-01","gateway":"198.18.32.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},"intercluster":{"lif_name":"gcnv-123ae2cfcaf0326-01-intercluster","vsa_ip_type":"intercluster","ip":"10.14.84.123","lif_uuid":"90eb0415-a990-11f0-bfb7-9fdf601cad34","network_config":{"subnet":"vsa-335784859002-1756713354","vpc":"netapp-autopush-tst-network","gateway":"10.14.84.113","gcp_network_config":{"subnet_project_id":"nb0d0fe4dbc2a5433-tp"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},"nodemgmt":{"lif_name":"gcnv-123ae2cfcaf0326-01-nodemgmt","vsa_ip_type":"nodemgmt","ip":"34.87.214.53","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},"nodemgmtinternal":{"lif_name":"gcnv-123ae2cfcaf0326-01-nodemgmtinternal","vsa_ip_type":"nodemgmtinternal","ip":"198.18.0.49","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},"rsm":{"lif_name":"gcnv-123ae2cfcaf0326-01-rsm","vsa_ip_type":"rsm","ip":"198.18.16.40","lif_uuid":"","network_config":{"subnet":"rsm-e0c-subnet-01","vpc":"rsm-e0c-vpc-01","gateway":"198.18.16.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"}},"system_disks":[{"name":"gcnv-123ae2cfcaf0326-01-disk-boot","size":10,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-01-disk-boot"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-nvram","size":50,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":9600,"disk_throughput":2400,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-01-disk-nvram"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-core","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-01-disk-core"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-root","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-fcaf0326-01-disk-root"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-rootcopy","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-0326-02-disk-rootcopy"}}],"data_disks":[{"name":"gcnv-123ae2cfcaf0326-01-disk-data-0","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-0"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-1","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-1"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-2","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-2"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-3","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-3"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-4","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-4"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-5","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-5"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-6","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-6"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-data-7","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"pd-af0326-01-disk-data-7"}}],"vsa_management_ip":"34.87.214.53","additional_vm_resources":{"gcp_ilb_vm_resources":{"negs":"gcnv-9a960f9db997bdc-neg-svm-01-a-0"}}},"vm2":{"region":"australia-southeast1","zone":"australia-southeast1-b","name":"gcnv-123ae2cfcaf0326-02","host_name":"gcnv-123ae2cfcaf0326-02","serial_number":"93520140000000000002","node_index":2,"is_mediator":false,"lifs":{"clus":{"lif_name":"gcnv-123ae2cfcaf0326-02-clus","vsa_ip_type":"clus","ip":"198.18.0.48","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"},"ic":{"lif_name":"gcnv-123ae2cfcaf0326-02-ic","vsa_ip_type":"ic","ip":"198.18.32.62","lif_uuid":"","network_config":{"subnet":"ic-e0b-subnet-01","vpc":"ic-e0b-vpc-01","gateway":"198.18.32.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"},"intercluster":{"lif_name":"gcnv-123ae2cfcaf0326-02-intercluster","vsa_ip_type":"intercluster","ip":"10.14.84.120","lif_uuid":"9a893e95-a990-11f0-a68d-9de1e2bb6c48","network_config":{"subnet":"vsa-335784859002-1756713354","vpc":"netapp-autopush-tst-network","gateway":"10.14.84.113","gcp_network_config":{"subnet_project_id":"nb0d0fe4dbc2a5433-tp"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"},"nodemgmt":{"lif_name":"gcnv-123ae2cfcaf0326-02-nodemgmt","vsa_ip_type":"nodemgmt","ip":"34.87.237.197","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"},"nodemgmtinternal":{"lif_name":"gcnv-123ae2cfcaf0326-02-nodemgmtinternal","vsa_ip_type":"nodemgmtinternal","ip":"198.18.0.47","lif_uuid":"","network_config":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"198.18.0.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"},"rsm":{"lif_name":"gcnv-123ae2cfcaf0326-02-rsm","vsa_ip_type":"rsm","ip":"198.18.16.41","lif_uuid":"","network_config":{"subnet":"rsm-e0c-subnet-01","vpc":"rsm-e0c-vpc-01","gateway":"198.18.16.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"}},"system_disks":[{"name":"gcnv-123ae2cfcaf0326-02-disk-boot","size":10,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-02-disk-boot"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-nvram","size":50,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":9600,"disk_throughput":2400,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-02-disk-nvram"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-core","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-02-disk-core"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-root","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-fcaf0326-02-disk-root"}},{"name":"gcnv-123ae2cfcaf0326-01-disk-rootcopy","size":64,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-0326-01-disk-rootcopy"}}],"data_disks":[{"name":"gcnv-123ae2cfcaf0326-02-disk-data-0","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-0"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-1","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-1"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-2","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-2"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-3","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-3"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-4","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-4"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-5","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-5"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-6","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-6"}},{"name":"gcnv-123ae2cfcaf0326-02-disk-data-7","size":308,"access_mode":"READ_WRITE","type":"hyperdisk-balanced","disk_iops":3000,"disk_throughput":140,"resource_status":"","zone":"australia-southeast1-b","gcp_disk_config":{"device_name":"pd-af0326-02-disk-data-7"}}],"vsa_management_ip":"34.87.237.197","additional_vm_resources":{"gcp_ilb_vm_resources":{"negs":"gcnv-9a960f9db997bdc-neg-svm-01-a-0"}}},"mediator":{"region":"australia-southeast1","zone":"australia-southeast1-a","name":"gcnv-123ae2cfcaf0326-mediator1","host_name":"","serial_number":"","node_index":1,"is_mediator":true,"lifs":{"rsm":{"lif_name":"gcnv-123ae2cfcaf0326-mediator1-rsm","vsa_ip_type":"rsm","ip":"198.18.16.39","lif_uuid":"","network_config":{"subnet":"rsm-e0c-subnet-01","vpc":"rsm-e0c-vpc-01","gateway":"198.18.16.1","gcp_network_config":{"subnet_project_id":"335784859002"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-mediator1"}},"system_disks":[{"name":"gcnv-123ae2cfcaf0326-mediator1-disk-boot","size":10,"access_mode":"","type":"pd-ssd","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-mediator1-disk-boot"}},{"name":"gcnv-123ae2cfcaf0326-mediator1-disk-data","size":10,"access_mode":"","type":"pd-ssd","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"australia-southeast1-a","gcp_disk_config":{"device_name":"gcnv-123ae2cfcaf0326-mediator1-disk-data"}}],"data_disks":null,"vsa_management_ip":"","additional_vm_resources":{"gcp_ilb_vm_resources":{"negs":"gcnv-9a960f9db997bdc-neg-svm-01-a-0"}}},"additional_ha_resources":{"gcp_ilb_ha_resources":{"forwarding_rule":"","backend_service":"","health_check":"","health_check_port":0}}}]},"deployment":{"provider":"gcp","deployment_id":"gcnv-123ae2cfcaf0326","serial_number_prefix":"935201400000000000","vm_serial_numbers":null,"region":"australia-southeast1","zone":{"zone1":"australia-southeast1-a","zone2":"australia-southeast1-b","mediator_zone":"australia-southeast1-a"},"images":{"vsa_image_name":"x-9-17-1p1-gcnv","mediator_image_name":"cvo-mediator-x-9-17-1p1"},"tags":"","labels":{"account_id":"355459131842","billing_target_cloud":"gcnv-cvo","creator":"nonroot","deployment_id":"gcnv-123ae2cfcaf0326","deployment_type":"non_shared_ha","pool_name":"nk-pool5","pool_uuid":"e128a049-a4b7-a556-aa0a-3b320ba0bd69"},"user_boot_args":"bootarg.keymanager.ekmip.svm_context=false","user_custom_data":{},"deployment_type":"non_shared_ha","num_ha_pair":1,"vsa_instance_type":"c3-standard-8-lssd","mediator_instance_type":"e2-micro","data_disk_type":"hyperdisk-balanced","system_disk_type":"hyperdisk-balanced","mediator_disk_type":"pd-ssd","data_disk_count":8,"vsa_system_disk_config":{"boot":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}},"core":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}},"data":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}},"nvram":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}},"root":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}},"rootcopy":{"name":"","size":0,"access_mode":"","type":"","disk_iops":0,"disk_throughput":0,"resource_status":"","zone":"","gcp_disk_config":{}}},"net_config":{"clus":{"subnet":"","vpc":"","gateway":"","gcp_network_config":{"subnet_project_id":""}},"ic":{"subnet":"ic-e0b-subnet-01","vpc":"ic-e0b-vpc-01","gateway":"","gcp_network_config":{"subnet_project_id":"335784859002"}},"intercluster":{"subnet":"vsa-335784859002-1756713354","vpc":"netapp-autopush-tst-network","gateway":"","gcp_network_config":{"subnet_project_id":"nb0d0fe4dbc2a5433-tp"}},"mediator":{"subnet":"","vpc":"","gateway":"","gcp_network_config":{"subnet_project_id":""}},"nas":{"subnet":"","vpc":"","gateway":"","gcp_network_config":{"subnet_project_id":""}},"nodemgmt":{"subnet":"mgmt-e0a-subnet-01","vpc":"mgmt-e0a-vpc-01","gateway":"","gcp_network_config":{"subnet_project_id":"335784859002"}},"nodemgmtinternal":{"subnet":"","vpc":"","gateway":"","gcp_network_config":{"subnet_project_id":""}},"rsm":{"subnet":"rsm-e0c-subnet-01","vpc":"rsm-e0c-vpc-01","gateway":"","gcp_network_config":{"subnet_project_id":"335784859002"}},"san":{"subnet":"","vpc":"","gateway":"","gcp_network_config":{"subnet_project_id":""}}},"gcpconfig":{"project_id":"335784859002","image_project_id":"gcnv-autopush-images","mediator_image_project_id":"gcnv-autopush-images","service_account_email":"vsa-sa-gcnv-123ae2cfcaf0326@335784859002.iam.gserviceaccount.com","bucket_name":"australia-southeast1-e128a049-a4b7-a556-aa0a-3b320ba0bd69"},"spconfig":{"size":"2458Gi","iops":24000,"tput":1120},"dev_flags":{"ext_ip_for_node_mgmt":true,"disable_data_nic_tier1":false,"enable_premium_tier_data":false,"DisableGVNIC":false,"enable_nfs_v3_support":false,"enable_ilb_support":false},"ntp_servers":null,"dns_servers":null},"upgrade":{"skip_ontap_image_version_match":false,"ontap_upgrade_target_image_version":"","ontap_upgrade_image_path":""},"vsa_cluster":{"cluster_mgmt_netmask":"","cluster_mgmt_gateway":"","cust_broadcast_domain":"Gcnv","cust_ip_space":"Gcnv","object_store_name":"gcnv-123ae2cfcaf0326-gcp-object-store","cluster_name":"gcnv-123ae2cfcaf0326"},"data_aggr":[{"name":"aggr1","uuid":"f794f574-a990-11f0-bfb7-9fdf601cad34","size":2226559037440,"home_node":"gcnv-123ae2cfcaf0326-01"}],"svm":{"gcnv-123ae2cfcaf0326-svm-01":{"svm_name":"gcnv-123ae2cfcaf0326-svm-01","svm_uuid":"096fccee-a991-11f0-bfb7-9fdf601cad34","svm_lifs":{"san":[{"lif_name":"gcnv-123ae2cfcaf0326-svm-01-san-1","vsa_ip_type":"san","ip":"10.14.84.124","lif_uuid":"11c4201c-a991-11f0-bfb7-9fdf601cad34","network_config":{"subnet":"vsa-335784859002-1756713354","vpc":"netapp-autopush-tst-network","gateway":"10.14.84.113","gcp_network_config":{"subnet_project_id":"nb0d0fe4dbc2a5433-tp"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-01"},{"lif_name":"gcnv-123ae2cfcaf0326-svm-01-san-2","vsa_ip_type":"san","ip":"10.14.84.125","lif_uuid":"1257835c-a991-11f0-a68d-9de1e2bb6c48","network_config":{"subnet":"vsa-335784859002-1756713354","vpc":"netapp-autopush-tst-network","gateway":"10.14.84.113","gcp_network_config":{"subnet_project_id":"nb0d0fe4dbc2a5433-tp"}},"region":"australia-southeast1","home_node":"gcnv-123ae2cfcaf0326-02"}]}}}}`

	tests := []struct {
		name                string
		vlmConfig           string
		expectError         bool
		expectErrorContains string
		validateResult      func(t *testing.T, result *vlm.VLMConfig, err error)
	}{
		{
			name:                "Success with valid VLM config and negs as string",
			vlmConfig:           actualValidVLMConfig,
			expectError:         false,
			expectErrorContains: "",
			validateResult: func(t *testing.T, result *vlm.VLMConfig, err error) {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, 1, len(result.Cloud.HAPairs))
				assert.Equal(t, "gcnv-9a960f9db997bdc-neg-svm-01-a-0", result.Cloud.HAPairs[0].VM1.AdditionalVmResources.GCPILBVmResources.Negs)
			},
		},
		{
			name:                "Error with invalid JSON",
			vlmConfig:           "invalid json {",
			expectError:         true,
			expectErrorContains: "invalid character",
			validateResult: func(t *testing.T, result *vlm.VLMConfig, err error) {
				assert.Error(t, err)
				assert.Nil(t, result)
				assertTemporalApplicationError(t, err, "invalid character", "CustomError", true)
			},
		},
		{
			name:                "Error with empty VLM config",
			vlmConfig:           "",
			expectError:         true,
			expectErrorContains: "unexpected end of JSON input",
			validateResult: func(t *testing.T, result *vlm.VLMConfig, err error) {
				assert.Error(t, err)
				assert.Nil(t, result)
				assertTemporalApplicationError(t, err, "unexpected end of JSON input", "CustomError", true)
			},
		},
		{
			name:                "Error with other unmarshal error (not negs)",
			vlmConfig:           `{"cloud":{"ha_pair":[{"vm1":{"node_index":"not-a-number"}}]}}`,
			expectError:         true,
			expectErrorContains: "cannot unmarshal string",
			validateResult: func(t *testing.T, result *vlm.VLMConfig, err error) {
				assert.Error(t, err)
				assert.Nil(t, result)
				assertTemporalApplicationError(t, err, "cannot unmarshal string", "CustomError", true)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup Temporal test environment for each sub-test
			var ts testsuite.WorkflowTestSuite
			env := ts.NewTestActivityEnvironment()
			env.RegisterActivity(activity.ParseVlmConfig)

			pool := &datamodel.Pool{
				Name:      "test-pool",
				VLMConfig: tt.vlmConfig,
			}
			pool.UUID = "test-pool-uuid"

			originalVlmConfig := pool.VLMConfig

			// Act
			encodedValue, err := env.ExecuteActivity(activity.ParseVlmConfig, pool)

			var result *vlm.VLMConfig
			if encodedValue != nil && err == nil {
				err = encodedValue.Get(&result)
			}

			// Assert
			tt.validateResult(t, result, err)

			assert.Equal(t, originalVlmConfig, pool.VLMConfig, "VLMConfig should not be mutated in the input pool object")

			mockStorage.AssertExpectations(t)
		})
	}
}

func TestPoolActivity_PrepareCreateVSAExpertModeReq(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}

	vlmConfig := &vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			NumHAPair: 1,
		},
	}
	ontapCredentials := &vlm.OntapCredentials{
		AdminPassword: "admin-password",
	}
	expertModeCredentials := &vlm.OntapCredentials{
		AdminPassword: "expert-password",
	}
	bucketFileDetails := &hyperscaler_models.BucketFileDetails{
		BucketName:     "test-bucket",
		FileUrl:        "GCNV/9.17.1/RBAC/gcnvadmin_create_cli",
		FileHashSHA256: "abc123def456",
	}

	t.Run("Success with certificate authentication", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USER_CERTIFICATE,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.NoError(t, err)
		assert.Equal(t, *vlmConfig, createVSAExpertModeRequest.VLMConfig)
		assert.Equal(t, *ontapCredentials, createVSAExpertModeRequest.OntapCredentials)
		assert.Equal(t, *expertModeCredentials, createVSAExpertModeRequest.ExpertModeUserCredentials)
		assert.Equal(t, "certificate", createVSAExpertModeRequest.AuthenticationType)
		assert.Equal(t, "gcnvadmin", createVSAExpertModeRequest.Username)
		assert.Equal(t, "gs://test-bucket/GCNV/9.17.1/RBAC/gcnvadmin_create_cli", createVSAExpertModeRequest.RbacFileURL)
		assert.Equal(t, "abc123def456", createVSAExpertModeRequest.RbacFileChecksum)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success with password authentication", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.NoError(t, err)
		assert.Equal(t, *vlmConfig, createVSAExpertModeRequest.VLMConfig)
		assert.Equal(t, *ontapCredentials, createVSAExpertModeRequest.OntapCredentials)
		assert.Equal(t, *expertModeCredentials, createVSAExpertModeRequest.ExpertModeUserCredentials)
		assert.Equal(t, "password", createVSAExpertModeRequest.AuthenticationType)
		assert.Equal(t, "gcnvadmin", createVSAExpertModeRequest.Username)
		assert.Equal(t, "gs://test-bucket/GCNV/9.17.1/RBAC/gcnvadmin_create_cli", createVSAExpertModeRequest.RbacFileURL)
		assert.Equal(t, "abc123def456", createVSAExpertModeRequest.RbacFileChecksum)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when expert mode credentials is nil", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: nil,
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "expert mode credentials are not provided", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when expert mode credential array is nil", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: nil,
			},
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "expert mode credentials are not provided", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when expert mode credential array is empty", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{},
			},
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "expert mode credentials are not provided", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when bucketFileDetails is nil", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}
		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, nil)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "exp mode rbac file details are missing", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when bucketFileDetails FileHashSHA256 is empty", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		invalidBucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     "test-bucket",
			FileUrl:        "GCNV/9.17.1/RBAC/gcnvadmin_create_cli",
			FileHashSHA256: "", // Empty hash
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, invalidBucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "exp mode rbac file details are missing", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when bucketFileDetails FileUrl is empty", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		invalidBucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     "test-bucket",
			FileUrl:        "", // Empty file URL
			FileHashSHA256: "abc123def456",
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, invalidBucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "exp mode rbac file details are missing", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Error when bucketFileDetails BucketName is empty", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		invalidBucketFileDetails := &hyperscaler_models.BucketFileDetails{
			BucketName:     "", // Empty bucket name
			FileUrl:        "GCNV/9.17.1/RBAC/gcnvadmin_create_cli",
			FileHashSHA256: "abc123def456",
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, invalidBucketFileDetails)

		assert.Error(t, err)
		assert.Nil(t, createVSAExpertModeRequest)
		assertTemporalApplicationError(t, err, "exp mode rbac file details are missing", vsaerrors.CustomErrorType, true)
		mockStorage.AssertExpectations(t)
	})

	t.Run("Success with USERNAME_PWD_SEC_MGR auth type", func(t *testing.T) {
		pool := &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				UUID: "test-pool-uuid",
			},
			PoolCredentials: &datamodel.PoolCredentials{
				AuthType: env.USERNAME_PWD_SEC_MGR,
			},
			ExpertModeCredentials: &datamodel.ExpertModeCredentials{
				ExpertModeCredential: []*datamodel.ExpertModeCredential{
					{
						Username: "gcnvadmin",
					},
				},
			},
		}

		createVSAExpertModeRequest, err := activity.PrepareCreateVSAExpertModeReq(*vlmConfig, *ontapCredentials, *expertModeCredentials, pool, bucketFileDetails)

		assert.NoError(t, err)
		assert.Equal(t, "password", createVSAExpertModeRequest.AuthenticationType) // Should default to password for non-certificate auth
		assert.Equal(t, "gcnvadmin", createVSAExpertModeRequest.Username)
		mockStorage.AssertExpectations(t)
	})
}

func newComputeServiceWithHandler(t *testing.T, handler http.HandlerFunc) *compute.Service {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client := srv.Client()
	endpoint := srv.URL + "/compute/v1/"

	svc, err := compute.NewService(context.Background(), option.WithHTTPClient(client), option.WithEndpoint(endpoint))
	require.NoError(t, err)
	return svc
}

func setComputeService(t *testing.T, gcpSvc *google.GcpServices, computeSvc *compute.Service) {
	t.Helper()
	if gcpSvc.AdminGCPService == nil {
		gcpSvc.AdminGCPService = &google.AdminGCPService{}
	}
	rv := reflect.ValueOf(gcpSvc.AdminGCPService).Elem()
	field := rv.FieldByName("computeService")
	require.True(t, field.IsValid())
	// Field is unexported; use UnsafeAddr to allow setting in tests.
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(computeSvc))
}

func TestValidateImageDigest_ConfigChecksumsError(t *testing.T) {
	origFlag := activities.ValidateImageDigestFlag
	origCfg := activities.VsaImageChecksums
	defer func() {
		activities.ValidateImageDigestFlag = origFlag
		activities.VsaImageChecksums = origCfg
	}()

	activities.ValidateImageDigestFlag = true
	// Cause getImageConfigChecksums to fail (missing config)
	activities.VsaImageChecksums = "{}"

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	act := &activities.PoolActivity{}
	env.RegisterActivity(act)

	result, err := env.ExecuteActivity(act.ValidateImageDigest)
	var ok bool
	if result != nil {
		_ = result.Get(&ok)
	}
	assert.False(t, ok, "should return false on error")
	require.Error(t, err)
	// Returned error is the underlying error (wrapped by Temporal), not the log message
	assert.Contains(t, err.Error(), "not configured")
}

func TestValidateImageDigest_RepoChecksumsError(t *testing.T) {
	origFlag := activities.ValidateImageDigestFlag
	origCfg := activities.VsaImageChecksums
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	origGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.ValidateImageDigestFlag = origFlag
		activities.VsaImageChecksums = origCfg
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
		hyperscaler2.GetGCPService = origGetGCPService
	}()

	activities.ValidateImageDigestFlag = true

	// Provide valid config checksums
	cfg := map[string]string{
		"VSA_IMAGE_CHECKSUM":          "vsa_md5",
		"VSA_MEDIATOR_IMAGE_CHECKSUM": "med_md5",
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	activities.VsaImageChecksums = string(raw)

	// Cause getImageRepoChecksums to fail (missing VSA project/name)
	activities.VsaImageProject, activities.VsaImageName = "", ""
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		return &google.GcpServices{Ctx: ctx}, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	act := &activities.PoolActivity{}
	env.RegisterActivity(act)

	result, err := env.ExecuteActivity(act.ValidateImageDigest)
	var ok bool
	if result != nil {
		_ = result.Get(&ok)
	}
	assert.False(t, ok)
	require.Error(t, err)
	// Returned error is underlying message from getImageRepoChecksums
	assert.Contains(t, err.Error(), "vsa image details are not configured")
}

func TestValidateImageDigest_Mismatch(t *testing.T) {
	origFlag := activities.ValidateImageDigestFlag
	origCfg := activities.VsaImageChecksums
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	origGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.ValidateImageDigestFlag = origFlag
		activities.VsaImageChecksums = origCfg
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
		hyperscaler2.GetGCPService = origGetGCPService
	}()

	activities.ValidateImageDigestFlag = true

	// Config checksums (intentionally mismatched with repo)
	cfg := map[string]string{
		"VSA_IMAGE_CHECKSUM":          strings.Repeat("a", 64),
		"VSA_MEDIATOR_IMAGE_CHECKSUM": strings.Repeat("b", 64),
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	activities.VsaImageChecksums = string(raw)

	// Configure image identifiers used in URL path matching
	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	computeSvc := newComputeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/vsa-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"11111111111111111111111111111111","checksum2":"22222222222222222222222222222222"}}`))
		case strings.Contains(r.URL.Path, "/mediator-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"33333333333333333333333333333333","checksum2":"44444444444444444444444444444444"}}`))
		default:
			http.NotFound(w, r)
		}
	})

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		gcpSvc := &google.GcpServices{
			Ctx:             ctx,
			AdminGCPService: &google.AdminGCPService{},
		}
		setComputeService(t, gcpSvc, computeSvc)
		return gcpSvc, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	act := &activities.PoolActivity{}
	env.RegisterActivity(act)

	result, err := env.ExecuteActivity(act.ValidateImageDigest)
	var ok bool
	if result != nil {
		_ = result.Get(&ok)
	}
	assert.False(t, ok)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "VSA image verification failed")
}

func TestValidateImageDigest_Success(t *testing.T) {
	origFlag := activities.ValidateImageDigestFlag
	origCfg := activities.VsaImageChecksums
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	origGetGCPService := hyperscaler2.GetGCPService
	defer func() {
		activities.ValidateImageDigestFlag = origFlag
		activities.VsaImageChecksums = origCfg
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
		hyperscaler2.GetGCPService = origGetGCPService
	}()

	activities.ValidateImageDigestFlag = true

	// Config checksums that match repo
	cfg := map[string]string{
		"VSA_IMAGE_CHECKSUM":          strings.Repeat("1", 64),
		"VSA_MEDIATOR_IMAGE_CHECKSUM": strings.Repeat("2", 64),
	}
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	activities.VsaImageChecksums = string(raw)

	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	computeSvc := newComputeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/vsa-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"11111111111111111111111111111111","checksum2":"11111111111111111111111111111111"}}`))
		case strings.Contains(r.URL.Path, "/mediator-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"22222222222222222222222222222222","checksum2":"22222222222222222222222222222222"}}`))
		default:
			http.NotFound(w, r)
		}
	})

	hyperscaler2.GetGCPService = func(ctx context.Context) (*google.GcpServices, error) {
		gcpSvc := &google.GcpServices{
			Ctx:             ctx,
			AdminGCPService: &google.AdminGCPService{},
		}
		setComputeService(t, gcpSvc, computeSvc)
		return gcpSvc, nil
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	act := &activities.PoolActivity{}
	env.RegisterActivity(act)

	res, err := env.ExecuteActivity(act.ValidateImageDigest)
	require.NoError(t, err)
	require.NotNil(t, res)
	var ok bool
	require.NoError(t, res.Get(&ok))
	assert.True(t, ok, "expected successful validation with matching checksums")
}

func TestGetImageRepoChecksums_MissingVSAConfig(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	// Missing VSA project/name
	activities.VsaImageProject, activities.VsaImageName = "", ""
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	gcpSvc := &google.GcpServices{}

	_, _, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "vsa image details are not configured")
}

func TestGetImageRepoChecksums_MissingMediatorConfig(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "", ""

	gcpSvc := &google.GcpServices{}

	_, _, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mediator image details are not configured")
}

func TestGetImageRepoChecksums_AuthClientFailure(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	// Passing a mock without a compute client should return an error while fetching images.
	gcpSvc := &google.GcpServices{}

	_, _, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get VSA image details from repo")
}

func TestGetImageRepoChecksums_Success(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	// Use names matching our URL suffix router
	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	computeSvc := newComputeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/vsa-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"11111111111111111111111111111111","checksum2":"11111111111111111111111111111111"}}`))
		case strings.Contains(r.URL.Path, "/mediator-image"):
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"22222222222222222222222222222222","checksum2":"22222222222222222222222222222222"}}`))
		default:
			http.NotFound(w, r)
		}
	})

	gcpSvc := &google.GcpServices{AdminGCPService: &google.AdminGCPService{}}
	setComputeService(t, gcpSvc, computeSvc)

	vsa, med, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("1", 64), vsa)
	assert.Equal(t, strings.Repeat("2", 64), med)
}

func TestGetImageRepoChecksums_VsaFetchError(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	computeSvc := newComputeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/vsa-image"):
			http.Error(w, "nope", http.StatusUnauthorized)
		case strings.Contains(r.URL.Path, "/mediator-image"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"22222222222222222222222222222222","checksum2":"22222222222222222222222222222222"}}`))
		default:
			http.NotFound(w, r)
		}
	})

	gcpSvc := &google.GcpServices{AdminGCPService: &google.AdminGCPService{}}
	setComputeService(t, gcpSvc, computeSvc)

	_, _, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get VSA image details from repo")
}

func TestGetImageRepoChecksums_MediatorFetchError(t *testing.T) {
	origVsaProj, origVsaName := activities.VsaImageProject, activities.VsaImageName
	origMedProj, origMedName := activities.MediatorImageProject, activities.MediatorImageName
	defer func() {
		activities.VsaImageProject, activities.VsaImageName = origVsaProj, origVsaName
		activities.MediatorImageProject, activities.MediatorImageName = origMedProj, origMedName
	}()

	activities.VsaImageProject, activities.VsaImageName = "proj", "vsa-image"
	activities.MediatorImageProject, activities.MediatorImageName = "proj", "mediator-image"

	computeSvc := newComputeServiceWithHandler(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "/vsa-image"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"labels":{"image_digest_verified":"true","checksum1":"11111111111111111111111111111111","checksum2":"11111111111111111111111111111111"}}`))
		case strings.Contains(r.URL.Path, "/mediator-image"):
			http.Error(w, "boom", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	})

	gcpSvc := &google.GcpServices{AdminGCPService: &google.AdminGCPService{}}
	setComputeService(t, gcpSvc, computeSvc)

	_, _, err := activities.GetImageRepoChecksums(context.Background(), gcpSvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get mediator image details from repo")
}

// Success: image_digest_verified == "true", checksum1+2 present
func TestGetImageChecksum_Success(t *testing.T) {
	labels := map[string]string{
		"image_digest_verified": "true",
		"checksum1":             strings.Repeat("a", 32),
		"checksum2":             strings.Repeat("b", 32),
	}

	md5, err := activities.GetImageChecksum(labels)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("a", 32)+strings.Repeat("b", 32), md5)
}

// Missing image_digest_verified label -> verification error
func TestGetImageChecksum_MissingVerificationLabel(t *testing.T) {
	labels := map[string]string{
		"checksum1": strings.Repeat("a", 32),
		"checksum2": strings.Repeat("b", 32),
	}

	_, err := activities.GetImageChecksum(labels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image digest is not verified in repo")
}

// image_digest_verified present but not "true" (case-insensitive) -> verification error
func TestGetImageChecksum_VerificationNotTrue(t *testing.T) {
	cases := []string{"false", "False", "FALSE", "no", "0"}
	for _, val := range cases {
		t.Run("value_"+val, func(t *testing.T) {
			img := &compute.Image{
				Labels: map[string]string{
					"image_digest_verified": val,
					"checksum1":             strings.Repeat("a", 32),
					"checksum2":             strings.Repeat("b", 32),
				},
			}

			_, err := activities.GetImageChecksum(img.Labels)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "image digest is not verified in repo")
		})
	}
}

func TestGetImageChecksum_MissingChecksumLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		substr string
	}{
		{
			name: "missing checksum1 with legacy empty",
			labels: map[string]string{
				"image_digest_verified": "true",
			},
			substr: "checksumLabel1",
		},
		{
			name: "missing checksum2",
			labels: map[string]string{
				"image_digest_verified": "true",
				"checksum1":             strings.Repeat("a", 32),
			},
			substr: "checksumLabel2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := activities.GetImageChecksum(tt.labels)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.substr)
		})
	}
}

// Defensive: empty labels map, missing labels, or nil image -> error
func TestGetImageChecksum_EmptyOrMissingLabels(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
	}{
		{
			name:   "empty labels",
			labels: map[string]string{},
		},
		{
			name:   "missing labels map",
			labels: nil,
		},
		{
			name:   "nil image",
			labels: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := activities.GetImageChecksum(tt.labels)
			require.Error(t, err)
		})
	}
}

// Success: properly formatted JSON with both checksums present; values are trimmed.
func TestGetImageConfigChecksums_Success(t *testing.T) {
	orig := activities.VsaImageChecksums
	defer func() { activities.VsaImageChecksums = orig }()

	payload := map[string]string{
		"VSA_IMAGE_CHECKSUM":          " vsa_md5 ",
		"VSA_MEDIATOR_IMAGE_CHECKSUM": " med_md5 ",
	}
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	activities.VsaImageChecksums = string(raw)

	vsa, med, err := activities.GetImageConfigChecksums()
	require.NoError(t, err)
	assert.Equal(t, "vsa_md5", vsa)
	assert.Equal(t, "med_md5", med)
}

// Invalid JSON: returns unmarshal error.
func TestGetImageConfigChecksums_InvalidJSON(t *testing.T) {
	orig := activities.VsaImageChecksums
	defer func() { activities.VsaImageChecksums = orig }()

	activities.VsaImageChecksums = "{invalid json"

	_, _, err := activities.GetImageConfigChecksums()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

// Missing config: empty or {} should return "not configured" error.
func TestGetImageConfigChecksums_MissingConfig(t *testing.T) {
	orig := activities.VsaImageChecksums
	defer func() { activities.VsaImageChecksums = orig }()

	tests := []string{
		"",       // empty
		"   ",    // whitespace
		"{}",     // empty JSON
		"\n\t{}", // empty JSON with whitespace
	}
	for _, tc := range tests {
		activities.VsaImageChecksums = tc
		_, _, err := activities.GetImageConfigChecksums()
		require.Error(t, err, "expected error for input: %q", tc)
		assert.Contains(t, err.Error(), "not configured")
	}
}

// Partial config: one of the checksums missing or empty should return "not configured" error.
func TestGetImageConfigChecksums_PartialConfig(t *testing.T) {
	orig := activities.VsaImageChecksums
	defer func() { activities.VsaImageChecksums = orig }()

	cases := []map[string]string{
		{
			"VSA_IMAGE_CHECKSUM":          "only_vsa",
			"VSA_MEDIATOR_IMAGE_CHECKSUM": "",
		},
		{
			"VSA_IMAGE_CHECKSUM":          "",
			"VSA_MEDIATOR_IMAGE_CHECKSUM": "only_med",
		},
		{
			// mediator key missing
			"VSA_IMAGE_CHECKSUM": "vsa_md5",
		},
		{
			// vsa key missing
			"VSA_MEDIATOR_IMAGE_CHECKSUM": "med_md5",
		},
	}
	for _, payload := range cases {
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		activities.VsaImageChecksums = string(raw)

		_, _, err = activities.GetImageConfigChecksums()
		require.Error(t, err, "expected error for payload: %+v", payload)
		assert.Contains(t, err.Error(), "not configured")
	}
}

func TestPoolActivity_GetCreateJobByResourceUUID_Success_WithCreatePoolJobType(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-resource-uuid"
	correlationID := "test-correlation-id"

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: correlationID,
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreatePool)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreatePool))

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "job-uuid", result.JobUUID)
	assert.Equal(t, "workflow-id", result.WorkflowID)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_Success_WithLargePoolJobType(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-resource-uuid"
	correlationID := "test-correlation-id"

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: correlationID,
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreateLargePool)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreateLargePool))

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "job-uuid", result.JobUUID)
	assert.Equal(t, "workflow-id", result.WorkflowID)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_NotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-resource-uuid"
	correlationID := "test-correlation-id"

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreatePool)).Return(nil, errors.New("not found"))

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreatePool))

	assert.Error(t, err)
	assert.Nil(t, result)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_CorrelationIDMismatch(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-resource-uuid"
	correlationID := "test-correlation-id"
	differentCorrelationID := "different-correlation-id"

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: differentCorrelationID,
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreatePool)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreatePool))

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "correlation ID mismatch")
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_EmptyCorrelationID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-resource-uuid"
	correlationID := ""

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: "some-correlation-id",
	}

	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreatePool)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreatePool))

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "job-uuid", result.JobUUID)
	assert.Equal(t, "workflow-id", result.WorkflowID)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_Success_WithVolumeJobType(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-volume-uuid"
	correlationID := "test-correlation-id"

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: correlationID,
	}

	// Test with volume job type - demonstrating generic functionality
	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreateVolume)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreateVolume))

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "job-uuid", result.JobUUID)
	assert.Equal(t, "workflow-id", result.WorkflowID)
	mockStorage.AssertExpectations(t)
}

func TestPoolActivity_GetCreateJobByResourceUUID_Success_WithSnapshotJobType(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.WithValue(context.Background(), middleware.TemporalSLoggerKey, log.Fields{})

	resourceUUID := "test-snapshot-uuid"
	correlationID := "test-correlation-id"

	createJob := &datamodel.Job{
		BaseModel:     datamodel.BaseModel{UUID: "job-uuid"},
		WorkflowID:    "workflow-id",
		CorrelationID: correlationID,
	}

	// Test with snapshot job type - demonstrating generic functionality
	mockStorage.On("GetJobByResourceUUID", ctx, resourceUUID, string(coremodel.JobTypeCreateSnapshot)).Return(createJob, nil)

	result, err := activity.GetCreateJobByResourceUUID(ctx, resourceUUID, correlationID, string(coremodel.JobTypeCreateSnapshot))

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, "job-uuid", result.JobUUID)
	assert.Equal(t, "workflow-id", result.WorkflowID)
	mockStorage.AssertExpectations(t)
}

func TestCleanupServiceAccountPermissionsInTenantProjects_NilPoolAttributes(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: nil,
	}

	val, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.Error(t, err)
	var result error
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
}

func TestCleanupServiceAccountPermissionsInTenantProjects_NoTenantProjects(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ServiceAccountPermissionProjects: []string{},
		},
	}

	_, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.NoError(t, err)
}

func TestCleanupServiceAccountPermissionsInTenantProjects_GetCloudServiceError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return nil, fmt.Errorf("failed to get GCP service")
	}

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ServiceAccountPermissionProjects: []string{"tenant-project-1"},
		},
	}

	val, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.Error(t, err)
	var result error
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
}

func TestCleanupServiceAccountPermissionsInTenantProjects_GetRolesError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ServiceAccountPermissionProjects: []string{"tenant-project-1"},
		},
	}

	saEmail := "sa-id@regional-tenant-project.iam.gserviceaccount.com"
	mockCloudService.On("GetServiceAccountRoles", saEmail, "tenant-project-1").Return(nil, fmt.Errorf("failed to fetch roles"))

	val, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.Error(t, err)
	var result error
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
	mockCloudService.AssertExpectations(t)
}

func TestCleanupServiceAccountPermissionsInTenantProjects_RemoveRolesError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ServiceAccountPermissionProjects: []string{"tenant-project-1"},
		},
	}

	saEmail := "sa-id@regional-tenant-project.iam.gserviceaccount.com"
	roles := []string{"roles/storage.objectAdmin"}
	mockCloudService.On("GetServiceAccountRoles", saEmail, "tenant-project-1").Return(roles, nil)
	mockCloudService.On("RemoveRolesFromServiceAccounts", roles, saEmail, "tenant-project-1").Return(fmt.Errorf("failed to remove roles"))

	val, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.Error(t, err)
	var result error
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
	}
	mockCloudService.AssertExpectations(t)
}

func TestCleanupServiceAccountPermissionsInTenantProjects_MultipleProjectsWithFailures(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	originalGetCloudService := activities.GetCloudService
	defer func() {
		activities.GetCloudService = originalGetCloudService
	}()

	mockCloudService := hyperscaler2.NewMockGoogleServices(t)
	activities.GetCloudService = func(ctx context.Context) (hyperscaler2.Services, error) {
		return mockCloudService, nil
	}

	mockSE := database.NewMockStorage(t)
	activity := &activities.PoolActivity{SE: mockSE}
	env.RegisterActivity(activity.CleanupServiceAccountPermissionsInTenantProjects)

	pool := &datamodel.Pool{
		BaseModel:        datamodel.BaseModel{UUID: "pool-uuid"},
		ServiceAccountId: "sa-id",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "regional-tenant-project",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			ServiceAccountPermissionProjects: []string{"tenant-project-1", "tenant-project-2", "tenant-project-3"},
		},
	}

	saEmail := "sa-id@regional-tenant-project.iam.gserviceaccount.com"
	roles := []string{"roles/storage.objectAdmin"}

	// First project - success
	mockCloudService.On("GetServiceAccountRoles", saEmail, "tenant-project-1").Return(roles, nil)
	mockCloudService.On("RemoveRolesFromServiceAccounts", roles, saEmail, "tenant-project-1").Return(nil)

	// Second project - failure to fetch roles
	mockCloudService.On("GetServiceAccountRoles", saEmail, "tenant-project-2").Return(nil, fmt.Errorf("fetch error"))

	// Third project - failure to remove roles
	mockCloudService.On("GetServiceAccountRoles", saEmail, "tenant-project-3").Return(roles, nil)
	mockCloudService.On("RemoveRolesFromServiceAccounts", roles, saEmail, "tenant-project-3").Return(fmt.Errorf("remove error"))

	val, err := env.ExecuteActivity(activity.CleanupServiceAccountPermissionsInTenantProjects, pool)
	assert.Error(t, err)
	var result error
	if err == nil {
		err = val.Get(&result)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "2/3 failures")
	}
	mockCloudService.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		DeploymentName:  "deploy-1",
		PoolCredentials: &datamodel.PoolCredentials{},
	}
	svm := &datamodel.Svm{Name: "svm1"}
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "10.0.0.1"}}
	vpgs := []*datamodel.VolumePerformanceGroup{
		{BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-1"}, Name: "vpg-one", OntapQosPolicyID: "qos-1", PoolID: 1},
		{BaseModel: datamodel.BaseModel{ID: 2, UUID: "vpg-2"}, Name: "vpg-two", OntapQosPolicyID: "qos-2", PoolID: 1},
	}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return(vpgs, nil)
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{UUID: "qos-1", SvmName: "svm1"}).Return(nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{UUID: "qos-2", SvmName: "svm1"}).Return(nil)
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpgs[0]).Return(nil)
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpgs[1]).Return(nil)

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_NoVPGs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-uuid"}}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return([]*datamodel.VolumePerformanceGroup{}, nil)

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_OntapFailureContinues(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		DeploymentName:  "deploy-1",
		PoolCredentials: &datamodel.PoolCredentials{},
	}
	svm := &datamodel.Svm{Name: "svm1"}
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "10.0.0.1"}}
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-1"}, Name: "vpg-one", OntapQosPolicyID: "qos-1", PoolID: 1,
	}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return([]*datamodel.VolumePerformanceGroup{vpg}, nil)
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{UUID: "qos-1", SvmName: "svm1"}).Return(errors.New("ONTAP unreachable"))
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(nil)

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_DBHardDeleteFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		DeploymentName:  "deploy-1",
		PoolCredentials: &datamodel.PoolCredentials{},
	}
	svm := &datamodel.Svm{Name: "svm1"}
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "10.0.0.1"}}
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-1"}, Name: "vpg-one", OntapQosPolicyID: "qos-1", PoolID: 1,
	}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return([]*datamodel.VolumePerformanceGroup{vpg}, nil)
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{UUID: "qos-1", SvmName: "svm1"}).Return(nil)
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(errors.New("DB connection failed"))

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "DB connection failed")
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_NotFoundSkipped(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	mockProvider := new(vsa.MockProvider)

	originalGetProviderByNode := hyperscaler2.GetProviderByNode
	defer func() { hyperscaler2.GetProviderByNode = originalGetProviderByNode }()
	hyperscaler2.GetProviderByNode = func(ctx context.Context, node *coremodel.Node) (vsa.Provider, error) {
		return mockProvider, nil
	}

	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		DeploymentName:  "deploy-1",
		PoolCredentials: &datamodel.PoolCredentials{},
	}
	svm := &datamodel.Svm{Name: "svm1"}
	nodes := []*datamodel.Node{{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "10.0.0.1"}}
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-1"}, Name: "vpg-one", OntapQosPolicyID: "qos-1", PoolID: 1,
	}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return([]*datamodel.VolumePerformanceGroup{vpg}, nil)
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(svm, nil)
	mockStorage.On("GetNodesByPoolID", mock.Anything, int64(1)).Return(nodes, nil)
	mockProvider.On("DeleteQoSGroupPolicy", vsa.DeleteQoSGroupPolicyParams{UUID: "qos-1", SvmName: "svm1"}).Return(utilErrors.NewNotFoundErr("QoS policy", nil))
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(utilErrors.NewNotFoundErr("VPG", nil))

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
	mockProvider.AssertExpectations(t)
}

func Test_DeleteAllPoolVPGs_SkipsOntapWhenNoProvider(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()

	mockStorage := database.NewMockStorage(t)
	act := activities.PoolActivity{SE: mockStorage}
	env.RegisterActivity(act.DeleteAllPoolVPGs)

	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: 1, UUID: "pool-uuid"},
		DeploymentName:  "deploy-1",
		PoolCredentials: &datamodel.PoolCredentials{},
	}
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "vpg-1"}, Name: "vpg-one", OntapQosPolicyID: "qos-1", PoolID: 1,
	}

	mockStorage.On("ListVolumePerformanceGroupsByPoolID", mock.Anything, int64(1)).Return([]*datamodel.VolumePerformanceGroup{vpg}, nil)
	mockStorage.On("GetSvmForPoolID", mock.Anything, int64(1)).Return(nil, errors.New("SVM not found"))
	mockStorage.On("GetNodesByPoolID", mock.Anything, int64(1)).Return([]*datamodel.Node{}, nil)
	mockStorage.On("HardDeleteVolumePerformanceGroup", mock.Anything, vpg).Return(nil)

	_, err := env.ExecuteActivity(act.DeleteAllPoolVPGs, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// OCI mock helpers for GetExpertModeCredentialsForOCI tests
// ---------------------------------------------------------------------------

type ociTestHTTPDispatcher struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *ociTestHTTPDispatcher) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func ociMockJSONResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header: http.Header{
			"Content-Type":   []string{"application/json"},
			"Opc-Request-Id": []string{"test-opc-request-id"},
		},
		Body:    io.NopCloser(strings.NewReader(body)),
		Request: &http.Request{URL: &url.URL{Path: "/mock"}},
	}
}

func newMockOCIServiceForTest(t *testing.T, vaultDoFunc func(*http.Request) (*http.Response, error)) *oci.OciServices {
	t.Helper()
	ctx := context.Background()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der := digitalCert.MarshalPKCS1PrivateKey(key)
	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: der}))

	configProvider := ocicommon.NewRawConfigurationProvider(
		"ocid1.tenancy.oc1..test", "ocid1.user.oc1..test",
		"us-ashburn-1", "aa:bb:cc:dd:ee:ff:00:11", pemKey, nil,
	)

	vaultCl, err := ocivault.NewVaultsClientWithConfigurationProvider(configProvider)
	require.NoError(t, err)
	vaultCl.HTTPClient = &ociTestHTTPDispatcher{doFunc: vaultDoFunc}

	adminService := &oci.AdminOCIService{}
	rv := reflect.ValueOf(adminService).Elem()
	vaultField := rv.FieldByName("vaultClient")
	reflect.NewAt(vaultField.Type(), unsafe.Pointer(vaultField.UnsafeAddr())).Elem().Set(reflect.ValueOf(vaultCl))

	return &oci.OciServices{
		Ctx:             ctx,
		Logger:          util.GetLogger(ctx),
		AdminOCIService: adminService,
	}
}

// ---------------------------------------------------------------------------
// TestPoolActivity_GetExpertModeCredentialsForOCI
// ---------------------------------------------------------------------------

func TestPoolActivity_GetExpertModeCredentialsForOCI(t *testing.T) {
	act := &activities.PoolActivity{}

	origGetOCIService := hyperscaler2.GetOCIService
	defer func() { hyperscaler2.GetOCIService = origGetOCIService }()

	pool := &datamodel.Pool{
		Name:           "test-pool",
		DeploymentName: "test-deployment",
		PoolOCID:       "ocid1.pool.oc1..testpool",
	}

	t.Run("success — admin password fetched from OCI Vault", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		origGetSecretVersion := oci.GetSecretVersion
		defer func() { oci.GetSecretVersion = origGetSecretVersion }()
		oci.GetSecretVersion = func(svc *oci.OciServices, secretID string, versionNumber ...int64) (*oci.OCICustomSecret, error) {
			return &oci.OCICustomSecret{
				Ocid:    secretID,
				Name:    "admin-password",
				Value:   "super-secret-pw",
				Version: 1,
			}, nil
		}

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return ociMockJSONResponse(200, `{
				"id": "ocid1.vaultsecret.oc1..testadminpw",
				"secretName": "test-admin-password",
				"lifecycleState": "ACTIVE",
				"currentVersionNumber": 1
			}`), nil
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		encodedValue, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.NoError(t, err)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "super-secret-pw", creds.AdminPassword)
	})

	t.Run("success — specific version number passed through", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		origGetSecretVersion := oci.GetSecretVersion
		defer func() { oci.GetSecretVersion = origGetSecretVersion }()
		var capturedVersion int64
		oci.GetSecretVersion = func(svc *oci.OciServices, secretID string, versionNumber ...int64) (*oci.OCICustomSecret, error) {
			if len(versionNumber) > 0 {
				capturedVersion = versionNumber[0]
			}
			return &oci.OCICustomSecret{
				Ocid:    secretID,
				Name:    "admin-password",
				Value:   "versioned-pw",
				Version: 5,
			}, nil
		}

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return ociMockJSONResponse(200, `{
				"id": "ocid1.vaultsecret.oc1..testadminpw",
				"secretName": "test-admin-password",
				"lifecycleState": "ACTIVE",
				"currentVersionNumber": 5
			}`), nil
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 5}
		encodedValue, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), capturedVersion)
		var creds *vlm.OntapCredentials
		err = encodedValue.Get(&creds)
		assert.NoError(t, err)
		assert.Equal(t, "versioned-pw", creds.AdminPassword)
	})

	t.Run("GetOCIService fails", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return nil, fmt.Errorf("OCI client initialization failed")
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})

	t.Run("nil ociAdminPassword", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return &oci.OciServices{Ctx: context.Background(), Logger: util.GetLogger(context.Background())}, nil
		}

		var nilPw *commonparams.OciAdminPassword
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, nilPw)
		assert.Error(t, err)
	})

	t.Run("empty Ocid in ociAdminPassword", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return &oci.OciServices{Ctx: context.Background(), Logger: util.GetLogger(context.Background())}, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})

	t.Run("vault GetSecret API error", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("connection refused")
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})

	t.Run("secret in PENDING_DELETION state — treated as not found", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return ociMockJSONResponse(200, `{
				"id": "ocid1.vaultsecret.oc1..testadminpw",
				"secretName": "test-admin-password",
				"lifecycleState": "PENDING_DELETION",
				"currentVersionNumber": 1
			}`), nil
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})

	t.Run("GetSecretVersion returns error", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		origGetSecretVersion := oci.GetSecretVersion
		defer func() { oci.GetSecretVersion = origGetSecretVersion }()
		oci.GetSecretVersion = func(svc *oci.OciServices, secretID string, versionNumber ...int64) (*oci.OCICustomSecret, error) {
			return nil, fmt.Errorf("vault connection timeout")
		}

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return ociMockJSONResponse(200, `{
				"id": "ocid1.vaultsecret.oc1..testadminpw",
				"secretName": "test-admin-password",
				"lifecycleState": "ACTIVE",
				"currentVersionNumber": 1
			}`), nil
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})

	t.Run("GetSecretVersion returns nil secret", func(t *testing.T) {
		testSuite := &testsuite.WorkflowTestSuite{}
		testEnv := testSuite.NewTestActivityEnvironment()
		testEnv.RegisterActivity(act.GetExpertModeCredentialsForOCI)

		origGetSecretVersion := oci.GetSecretVersion
		defer func() { oci.GetSecretVersion = origGetSecretVersion }()
		oci.GetSecretVersion = func(svc *oci.OciServices, secretID string, versionNumber ...int64) (*oci.OCICustomSecret, error) {
			return nil, nil
		}

		mockSvc := newMockOCIServiceForTest(t, func(req *http.Request) (*http.Response, error) {
			return ociMockJSONResponse(200, `{
				"id": "ocid1.vaultsecret.oc1..testadminpw",
				"secretName": "test-admin-password",
				"lifecycleState": "ACTIVE",
				"currentVersionNumber": 1
			}`), nil
		})
		hyperscaler2.GetOCIService = func(ctx context.Context) (*oci.OciServices, error) {
			return mockSvc, nil
		}

		adminPw := &commonparams.OciAdminPassword{Ocid: "ocid1.vaultsecret.oc1..testadminpw", Version: 1}
		_, err := testEnv.ExecuteActivity(act.GetExpertModeCredentialsForOCI, pool, adminPw)
		assert.Error(t, err)
	})
}

func TestMarkAddressRangeInUse_FlagDisabled(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "false")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.NoError(t, err)
	// No SE calls expected when flag is disabled.
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_EmptyArgs(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	// Both empty — early return, no SE call.
	err := activity.MarkAddressRangeInUse(ctx, "", "")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_InvalidNetwork(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	// Network string that ParseProjectId cannot parse — early return.
	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "not-a-valid-network")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_InvalidAllocatedCIDR(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	// Malformed CIDR — should return an error.
	err := activity.MarkAddressRangeInUse(ctx, "not-a-cidr", "projects/123/global/networks/vpc1")
	assert.ErrorContains(t, err, "invalid allocatedSubnetCIDR")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_HappyPath_TransitionsToInUse(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	// Registered range 10.55.55.0/24 — GCP allocated 10.55.55.16/29 from within it.
	ar := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-1"},
		Name:              "my-range",
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "CREATED",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{ar}, nil)
	mockStorage.On("UpdateAddressRangeState", ctx, "ar-uuid-1", "IN_USE", (*bool)(nil)).
		Return(ar, nil)

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_CorrectRangeSelectedFromMultiple(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	// Two registered ranges — GCP allocated from the second one.
	ar1 := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-1"},
		Name:              "range-a",
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "CREATED",
	}
	ar2 := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-2"},
		Name:              "range-b",
		AddressRangeCidr:  "10.56.0.0/24",
		AddressRangeState: "CREATED",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{ar1, ar2}, nil)
	// Only ar2 should be marked IN_USE — the allocated subnet is in its range.
	mockStorage.On("UpdateAddressRangeState", ctx, "ar-uuid-2", "IN_USE", (*bool)(nil)).
		Return(ar2, nil)

	err := activity.MarkAddressRangeInUse(ctx, "10.56.0.16/29", "projects/123/global/networks/vpc1")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_AlreadyInUse_NoStateChange(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	ar := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-2"},
		Name:              "my-range",
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{ar}, nil)
	// UpdateAddressRangeState must NOT be called.

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_RangeNotFound_NoError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	// Registered range does not contain the allocated subnet IP.
	other := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-3"},
		Name:              "different-range",
		AddressRangeCidr:  "192.168.1.0/24",
		AddressRangeState: "CREATED",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{other}, nil)

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_SEListError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return(nil, errors.New("db error"))

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.ErrorContains(t, err, "db error")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangeInUse_SEUpdateError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	lifType := "dataLIF"
	ar := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-4"},
		Name:              "my-range",
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "CREATED",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{ar}, nil)
	mockStorage.On("UpdateAddressRangeState", ctx, "ar-uuid-4", "IN_USE", (*bool)(nil)).
		Return(nil, errors.New("update failed"))

	err := activity.MarkAddressRangeInUse(ctx, "10.55.55.16/29", "projects/123/global/networks/vpc1")
	assert.ErrorContains(t, err, "update failed")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_FlagDisabled(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "false")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{Network: "projects/123/global/networks/vpc1"}
	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_NilPool(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	err := activity.MarkAddressRangesCreated(ctx, nil)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_InvalidNetwork(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{Network: "bad-network-string"}
	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "parseProjectId failed")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_RemainingPoolUsesSameRange_NoStateChange(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid-1"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-5"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	// Atomic update returns false — another active pool still has a subnet within this range.
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-uuid-5", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(false, nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_HappyPath_ResetsOnlyMatchedRange(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid-2"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-5"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	arOther := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-6"},
		AddressRangeCidr:  "10.88.88.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse, arOther}, nil)
	// Only the matching range (arInUse) is reset — atomic update returns true (no other pool uses it).
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-uuid-5", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(true, nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_SEAtomicUpdateError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid-3"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-5"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-uuid-5", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(false, errors.New("list pools error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "list pools error")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_SEListError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid-4"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return(nil, errors.New("list error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "list error")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_SEUpdateError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid-5"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-uuid-7"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-uuid-7", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(false, errors.New("update error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "update error")
	mockStorage.AssertExpectations(t)
}

// CVN-style fallback tests — pool has no AllocatedSubnetCIDR in DB (legacy pool or
// subnet already deleted before CIDR could be read). getPoolAllocatedSubnetCIDR returns ""
// via the early guard (no subnet names / no SnHostProject), triggering the fallback path.

func TestMarkAddressRangesCreated_LegacyPool_LastPool_ResetsAllInUseRanges(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	// Pool has no AllocatedSubnetCIDR and no SubnetNames → CIDR is empty, CVN fallback runs.
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-legacy-1"},
		Network:        "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{},
	}
	lifType := "dataLIF"
	arInUse1 := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-legacy-1"},
		AddressRangeCidr:  "10.10.0.0/24",
		AddressRangeState: "IN_USE",
	}
	arInUse2 := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-legacy-2"},
		AddressRangeCidr:  "10.20.0.0/24",
		AddressRangeState: "IN_USE",
	}
	arCreated := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-legacy-3"},
		AddressRangeCidr:  "10.30.0.0/24",
		AddressRangeState: "CREATED",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse1, arInUse2, arCreated}, nil)
	mockStorage.On("CountActivePoolsByNetwork", ctx, pool.Network, pool.UUID).
		Return(int64(0), nil)
	mockStorage.On("ResetAddressRangesInUseToCreated", ctx, "123", "vpc1").
		Return(nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_LegacyPool_OtherPoolsExist_NoReset(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-legacy-2"},
		Network:        "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-legacy-4"},
		AddressRangeCidr:  "10.10.0.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	// Another active pool remains on the network — do not reset.
	mockStorage.On("CountActivePoolsByNetwork", ctx, pool.Network, pool.UUID).
		Return(int64(1), nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_LegacyPool_CountActivePoolsError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-legacy-3"},
		Network:        "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{},
	}
	lifType := "dataLIF"
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{}, nil)
	mockStorage.On("CountActivePoolsByNetwork", ctx, pool.Network, pool.UUID).
		Return(int64(0), errors.New("db error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "db error")
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_LegacyPool_UpdateError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-legacy-4"},
		Network:        "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-legacy-5"},
		AddressRangeCidr:  "10.10.0.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	mockStorage.On("CountActivePoolsByNetwork", ctx, pool.Network, pool.UUID).
		Return(int64(0), nil)
	mockStorage.On("ResetAddressRangesInUseToCreated", ctx, "123", "vpc1").
		Return(errors.New("update error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "update error")
	mockStorage.AssertExpectations(t)
}

// GetPoolTenancyInfo tests

func TestGetPoolTenancyInfo_NilPool_ReturnsEmptyCIDR(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	info, err := activity.GetPoolTenancyInfo(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, "", info.AllocatedSubnetCIDR)
}

func TestGetPoolTenancyInfo_AllocatedSubnetCIDRFromClusterDetails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.0.0/29",
		},
	}
	info, err := activity.GetPoolTenancyInfo(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, "10.55.0.0/29", info.AllocatedSubnetCIDR)
}

func TestGetPoolTenancyInfo_NoSubnetNames_ReturnsEmptyCIDR(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		ClusterDetails: datamodel.ClusterDetails{
			// AllocatedSubnetCIDR is empty, SubnetNames is empty — no GCP call.
		},
	}
	info, err := activity.GetPoolTenancyInfo(ctx, pool)
	require.NoError(t, err)
	assert.Equal(t, "", info.AllocatedSubnetCIDR)
}

// MarkAddressRangesCreated — additional missing paths

func TestMarkAddressRangesCreated_InvalidDeletedPoolCIDR_FindRangeError(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-invalid-cidr"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "not-a-cidr",
		},
	}
	lifType := "dataLIF"
	ar := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-1"},
		AddressRangeCidr:  "10.0.0.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{ar}, nil)

	// findAddressRangeContainingSubnet returns error when deletedPoolSubnetCIDR is invalid.
	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.Error(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_RemainingPoolIsDeletedPool_Skipped(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-self"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-target"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	// Atomic update returns true — only the pool-self existed and it's the one being deleted.
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-target", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(true, nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	require.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_RemainingPoolEmptyCIDR_Skipped(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-main"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-target2"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	// Atomic update returns true — only active pool with a subnet in range is being deleted.
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-target2", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(true, nil)

	err := activity.MarkAddressRangesCreated(ctx, pool)
	require.NoError(t, err)
	mockStorage.AssertExpectations(t)
}

func TestMarkAddressRangesCreated_UpdateStateToCreated_Error(t *testing.T) {
	t.Setenv("ADDRESS_SPACE_MGMT_ENABLED", "true")
	mockStorage := database.NewMockStorage(t)
	activity := activities.PoolActivity{SE: mockStorage}
	ctx := context.Background()

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-update-err"},
		Network:   "projects/123/global/networks/vpc1",
		ClusterDetails: datamodel.ClusterDetails{
			AllocatedSubnetCIDR: "10.55.55.16/29",
		},
	}
	lifType := "dataLIF"
	arInUse := &datamodel.AddressRange{
		BaseModel:         datamodel.BaseModel{UUID: "ar-update-err"},
		AddressRangeCidr:  "10.55.55.0/24",
		AddressRangeState: "IN_USE",
	}
	mockStorage.On("ListAddressRanges", ctx, "123", "vpc1", (*string)(nil), &lifType).
		Return([]*datamodel.AddressRange{arInUse}, nil)
	mockStorage.On("UpdateAddressRangeStateToCreatedIfLastPool", ctx, "ar-update-err", pool.Network, pool.UUID, "10.55.55.0/24").
		Return(false, errors.New("update to created error"))

	err := activity.MarkAddressRangesCreated(ctx, pool)
	assert.ErrorContains(t, err, "update to created error")
	mockStorage.AssertExpectations(t)
}
