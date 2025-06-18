package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

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
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:                    "test-pool",
		AccountName:             "test-account",
		SizeInBytes:             1024 * 1024 * 1024 * 1024, // 1 TB
		Region:                  "test-region",
		PrimaryZone:             "test-zone",
		SecondaryZone:           "test-secondary-zone",
		AllowAutoTiering:        true,
		CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: 1024},
	}
	pool := &datamodel.Pool{
		Username: "test-user",
		Password: "test-password",
		SecretID: "",
	}

	if secretManagerEnabled {
		env.OnActivity("CreateSecret", params.Region, pool.SecretID).Return(nil)
	}
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateTenancy", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkName:        "test-subnet",
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSACluster", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSASVM", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestUpdatePoolWorkflow(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()

	// Setup context propagation and header values
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	// Register required activities
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	// Setup test input data for update workflow.
	params := &common.UpdatePoolParams{
		AccountName:          "test-account",
		PoolId:               "test-pool-id",
		SizeInBytes:          2 * 1024 * 1024 * 1024 * 1024, // For example: 2 TB
		TotalThroughputMibps: 128,
		TotalIops:            2048,
		QosType:              "Manual",
		// Add other parameters as needed.
	}
	pool := &datamodel.Pool{
		Username: "test-user",
		Password: "test-password",
		// Set additional fields if required.
	}

	// Register activity mocks.
	// The UpdateJobStatus calls return nil.
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).
		Return(nil)
	// The UpdatedPool activity is called within updatePoolWorkflow.Run.
	env.OnActivity("UpdatedPool", mock.Anything, mock.Anything).
		Return(nil)

	// Execute the workflow.
	env.ExecuteWorkflow(UpdatePoolWorkflow, params, pool)

	// Optionally query workflow status.
	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert the workflow has completed successfully.
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestDeletePoolWorkflow(t *testing.T) {
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
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	// Set up test data
	params := &common.DeletePoolParams{
		PoolID:      "test-pool",
		AccountName: "test-account",
	}
	pool := &datamodel.Pool{
		Username: "test-user",
		Password: "test-password",
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteVSADeployment", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	env.ExecuteWorkflow(DeletePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func Test_EnableAutoTier_Error_In_CreatePoolWorkflow(t *testing.T) {
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
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:             "test-pool",
		AccountName:      "test-account",
		SizeInBytes:      1024 * 1024 * 1024, // 1 GB
		Region:           "test-region",
		PrimaryZone:      "test-zone",
		AllowAutoTiering: true,
	}
	pool := &datamodel.Pool{
		Username: "test-user",
		Password: "test-password",
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateTenancy", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkName:        "test-subnet",
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSACluster", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSASVM", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Bucket Creation Failed"))

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)
}
