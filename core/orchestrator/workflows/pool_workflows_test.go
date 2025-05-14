package workflows

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
)

type MockPoolActivity struct {
	mock.Mock
}

func (m *MockPoolActivity) CreateTenancy(ctx context.Context, params *common.CreatePoolParams) (*common.TenancyInfo, error) {
	args := m.Called(ctx, params)
	return args.Get(0).(*common.TenancyInfo), args.Error(1)
}

func (m *MockPoolActivity) CreatingPool(ctx context.Context, pool *datamodel.Pool) (*datamodel.Pool, error) {
	args := m.Called(ctx, pool)
	return args.Get(0).(*datamodel.Pool), args.Error(1)
}

func (m *MockPoolActivity) DeployDeploymentManager(ctx context.Context, clusterName, region, zone, network, subnetwork, tenantProject, hostProject string, sizeInGB int) (*[]map[string]string, error) {
	args := m.Called(ctx, clusterName, region, zone, network, subnetwork, tenantProject, hostProject, sizeInGB)
	return args.Get(0).(*[]map[string]string), args.Error(1)
}

func (m *MockPoolActivity) GetOntapVersion(ctx context.Context, node *models.Node) (string, error) {
	args := m.Called(ctx, node)
	return args.String(0), args.Error(1)
}

func (m *MockPoolActivity) SavePoolWithClusterDetails(ctx context.Context, dbPool *datamodel.Pool, clusterDetails *datamodel.ClusterDetails) error {
	args := m.Called(ctx, dbPool, clusterDetails)
	return args.Error(0)
}

func (m *MockPoolActivity) SaveNodeDetails(ctx context.Context, pool *datamodel.Pool, vsaCluster *[]map[string]string) error {
	args := m.Called(ctx, pool, vsaCluster)
	return args.Error(0)
}

func (m *MockPoolActivity) CreateSvmForPool(ctx context.Context, pool *datamodel.Pool, node *models.Node) (*datamodel.Svm, error) {
	args := m.Called(ctx, pool, node)
	return args.Get(0).(*datamodel.Svm), args.Error(1)
}

func (m *MockPoolActivity) CreateLifForSvm(ctx context.Context, node *models.Node, vsaCluster []map[string]string, pool *datamodel.Pool, svm datamodel.Svm) error {
	args := m.Called(ctx, node, vsaCluster, pool, svm)
	return args.Error(0)
}

func (m *MockPoolActivity) GetProxyIP(ctx context.Context, dataLif string) (string, error) {
	args := m.Called(ctx, dataLif)
	return args.String(0), args.Error(1)
}

func (m *MockPoolActivity) CreateNetworkIpRoute(ctx context.Context, node *models.Node, svmName, gateway string) error {
	args := m.Called(ctx, node, svmName, gateway)
	return args.Error(0)
}

func (m *MockPoolActivity) CreatedPool(ctx context.Context, pool *datamodel.Pool) error {
	args := m.Called(ctx, pool)
	return args.Error(0)
}

func (m *MockPoolActivity) CheckForNodes(ctx context.Context, node *models.Node) error {
	args := m.Called(ctx, node)
	return args.Error(0)
}

func (m *MockPoolActivity) CheckForAggr(ctx context.Context, node *models.Node) error {
	args := m.Called(ctx, node)
	return args.Error(0)
}

func (m *MockPoolActivity) EnableIscsiServiceForSVM(ctx context.Context, node *models.Node, svmUUID string) (*datamodel.Svm, error) {
	args := m.Called(ctx, node, svmUUID)
	return args.Get(0).(*datamodel.Svm), args.Error(1)
}

type MockCommonActivities struct {
	mock.Mock
}

func (m *MockCommonActivities) UpdateJobStatus(ctx context.Context, job *datamodel.Job) error {
	args := m.Called(ctx, job)
	return args.Error(0)
}

func (m *MockCommonActivities) GetNode(ctx context.Context, poolId int64) (*datamodel.Node, error) {
	args := m.Called(ctx, poolId)
	return args.Get(0).(*datamodel.Node), args.Error(1)
}

func TestCreatePoolWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)
	// Mock activities
	mockPoolActivity := new(MockPoolActivity)
	mockCommonActivities := new(MockCommonActivities)

	env.RegisterActivity(mockPoolActivity.CreateTenancy)
	env.RegisterActivity(mockPoolActivity.CreatingPool)
	env.RegisterActivity(mockPoolActivity.DeployDeploymentManager)
	env.RegisterActivity(mockPoolActivity.GetOntapVersion)
	env.RegisterActivity(mockPoolActivity.SavePoolWithClusterDetails)
	env.RegisterActivity(mockPoolActivity.SaveNodeDetails)
	env.RegisterActivity(mockPoolActivity.CreateSvmForPool)
	env.RegisterActivity(mockPoolActivity.EnableIscsiServiceForSVM)
	env.RegisterActivity(mockPoolActivity.CreateLifForSvm)
	env.RegisterActivity(mockPoolActivity.GetProxyIP)
	env.RegisterActivity(mockPoolActivity.CreateNetworkIpRoute)
	env.RegisterActivity(mockPoolActivity.CreatedPool)
	env.RegisterActivity(mockCommonActivities.UpdateJobStatus)
	env.RegisterActivity(mockPoolActivity.CheckForNodes)
	env.RegisterActivity(mockPoolActivity.CheckForAggr)

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024, // 1 GB
		Region:      "test-region",
		CurrentZone: "test-zone",
	}
	pool := &datamodel.Pool{
		Username: "test-user",
		Password: "test-password",
	}

	// Mock activity responses
	mockCommonActivities.On("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	mockPoolActivity.On("CreateTenancy", mock.Anything, params).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkName:        "test-subnet",
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)

	mockPoolActivity.On("DeployDeploymentManager", mock.Anything, "test-pool-vsa", "test-region", "test-zone", "test-network", "test-subnet", "test-project", "test-host-project", 1).Return(&[]map[string]string{
		{"Name": "node1", "NodeIp": "192.168.1.1", "Zone": "test-zone", "MachineType": "n1-standard-4"},
	}, nil)

	mockPoolActivity.On("GetOntapVersion", mock.Anything, mock.Anything).Return("9.10.1", nil)
	mockPoolActivity.On("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockPoolActivity.On("SaveNodeDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockPoolActivity.On("CreateSvmForPool", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Svm{Name: "test-svm", SvmDetails: &datamodel.SvmDetails{ExternalUUID: "test_uuid"}}, nil)
	mockPoolActivity.On("EnableIscsiServiceForSVM", mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Svm{Name: "test-svm"}, nil)
	mockPoolActivity.On("CreateLifForSvm", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockPoolActivity.On("CreateNetworkIpRoute", mock.Anything, mock.Anything, "test-svm", "192.168.1.254").Return(nil)
	mockPoolActivity.On("CreatedPool", mock.Anything, mock.Anything).Return(nil)
	mockPoolActivity.On("CheckForNodes", mock.Anything, mock.Anything).Return(nil)
	mockPoolActivity.On("CheckForAggr", mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	// Assert activity calls
	mockPoolActivity.AssertExpectations(t)
	mockCommonActivities.AssertExpectations(t)
}
