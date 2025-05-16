package workflows

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"testing"
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
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateTenancy", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkName:        "test-subnet",
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSACluster", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateVSASVM", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestRunDeletePoolWorkflow_FailedPoolInvokedOnError(t *testing.T) {
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

	// Register the activity implementation
	env.RegisterActivity(&activities.PoolActivity{})

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "test-pool-id",
	}
	pool := &datamodel.Pool{}

	// Mock activities
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(nil, nil)

	env.OnActivity("FailedPool", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("delete resources failed"))

	// Register the workflow
	env.RegisterWorkflow(func(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (interface{}, error) {
		wf := &PoolWorkflow{}
		return wf.RunDeletePoolWorkflow(ctx, params, pool)
	})

	// Execute workflow
	env.ExecuteWorkflow(func(ctx workflow.Context, params *common.DeletePoolParams, pool *datamodel.Pool) (interface{}, error) {
		wf := &PoolWorkflow{}
		return wf.RunDeletePoolWorkflow(ctx, params, pool)
	}, params, pool)

	// Assert workflow failed
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())

	// Assert FailedPool was called
	env.AssertExpectations(t)
}
