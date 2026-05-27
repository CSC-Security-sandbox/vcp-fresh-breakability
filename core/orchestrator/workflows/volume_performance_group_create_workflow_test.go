package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func setupVPGWorkflowEnv(t *testing.T) *testsuite.TestWorkflowEnvironment {
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	env.RegisterWorkflow(CreateVolumePerformanceGroupWorkflow)

	mockStorage := database.NewMockStorage(t)
	vpgActivity := activities.VolumePerformanceGroupActivity{SE: mockStorage}
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{}

	env.RegisterActivity(vpgActivity.GetVolumePerformanceGroupByUUID)
	env.RegisterActivity(commonActivity.GetPoolBySvmPoolId)
	env.RegisterActivity(commonActivity.GetNode)
	env.RegisterActivity(volumeCreateActivity.GetOntapClusterHealth)
	env.RegisterActivity(vpgActivity.CreateQoSPolicyInONTAP)
	env.RegisterActivity(vpgActivity.UpdateVPGWithOntapID)
	env.RegisterActivity(vpgActivity.UpdateVPGStateInDB)

	return env
}

func TestCreateVolumePerformanceGroupWorkflow_Success(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg",
		PoolID:          poolID,
		ThroughputMibps: 100,
		Iops:            1000,
		IsShared:        true,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}
	qosPolicyID := "ontap-qos-policy-id"
	vpgUpdated := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: vpgUUID},
		Name:             "test-vpg",
		PoolID:           poolID,
		OntapQosPolicyID: qosPolicyID,
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil).Once()
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return(qosPolicyID, nil)
	env.OnActivity("UpdateVPGWithOntapID", mock.Anything, vpgUUID, qosPolicyID).Return(nil)
	env.OnActivity("UpdateVPGStateInDB", mock.Anything, vpgUUID, "READY", "").Return(nil)
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpgUpdated, nil).Once()

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	var result *datamodel.VolumePerformanceGroup
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, vpgUUID, result.UUID)
	assert.Equal(t, qosPolicyID, result.OntapQosPolicyID)
}

func TestCreateVolumePerformanceGroupWorkflow_Success_IsSharedFalse(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-not-shared"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg-not-shared",
		PoolID:          poolID,
		ThroughputMibps: 200,
		Iops:            2000,
		IsShared:        false,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}
	qosPolicyID := "ontap-qos-policy-id-not-shared"
	vpgUpdated := &datamodel.VolumePerformanceGroup{
		BaseModel:        datamodel.BaseModel{UUID: vpgUUID},
		Name:             "test-vpg-not-shared",
		PoolID:           poolID,
		ThroughputMibps:  200,
		Iops:             2000,
		IsShared:         false,
		OntapQosPolicyID: qosPolicyID,
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil).Once()
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return(qosPolicyID, nil)
	env.OnActivity("UpdateVPGWithOntapID", mock.Anything, vpgUUID, qosPolicyID).Return(nil)
	env.OnActivity("UpdateVPGStateInDB", mock.Anything, vpgUUID, "READY", "").Return(nil)
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpgUpdated, nil).Once()

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	var result *datamodel.VolumePerformanceGroup
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.False(t, result.IsShared, "IsShared should be false in workflow result")
	assert.Equal(t, qosPolicyID, result.OntapQosPolicyID)
}

func TestCreateVolumePerformanceGroupWorkflow_GetVPGByUUIDFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(nil, errors.New("vpg not found"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_GetPoolFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(nil, errors.New("pool not found"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_GetNodeFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{ID: poolID},
		DeploymentName: "test-deployment",
	}

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("nodes not found"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_NoNodesForPool(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{ID: poolID},
		DeploymentName: "test-deployment",
	}

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return([]*datamodel.Node{}, nil)

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "no nodes found for pool")
}

func TestCreateVolumePerformanceGroupWorkflow_CreateQoSPolicyFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg",
		PoolID:          poolID,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return("", errors.New("create qos failed"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_UpdateVPGWithOntapIDFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	qosPolicyID := "ontap-qos-id"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg",
		PoolID:          poolID,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return(qosPolicyID, nil)
	env.OnActivity("UpdateVPGWithOntapID", mock.Anything, vpgUUID, qosPolicyID).Return(errors.New("update vpg failed"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_ReFetchVPGFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	qosPolicyID := "ontap-qos-id"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg",
		PoolID:          poolID,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil).Once()
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return(qosPolicyID, nil)
	env.OnActivity("UpdateVPGWithOntapID", mock.Anything, vpgUUID, qosPolicyID).Return(nil)
	env.OnActivity("UpdateVPGStateInDB", mock.Anything, vpgUUID, "READY", "").Return(nil)
	// Re-fetch fails (may be called multiple times due to retries)
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(nil, errors.New("re-fetch failed"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_NoNodeEndpointsForPool(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	// Nodes with no EndpointAddress so CreateNodeForProvider yields empty EndpointAddressesToHostNameMap.
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: ""},
	}

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "VSA cluster node",
		"error should indicate missing node endpoints for pool")
}

func TestCreateVolumePerformanceGroupWorkflow_OntapClusterUnhealthy(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}
	isOntapHealthy := false

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_SetVPGStateToReadyFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	qosPolicyID := "ontap-qos-id"
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel:       datamodel.BaseModel{UUID: vpgUUID},
		Name:            "test-vpg",
		PoolID:          poolID,
		ThroughputMibps: 100,
		Iops:            1000,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}

	isOntapHealthy := true
	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil).Once()
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(&isOntapHealthy, nil)
	env.OnActivity("CreateQoSPolicyInONTAP", mock.Anything, vpg, mock.Anything).Return(qosPolicyID, nil)
	env.OnActivity("UpdateVPGWithOntapID", mock.Anything, vpgUUID, qosPolicyID).Return(nil)
	env.OnActivity("UpdateVPGStateInDB", mock.Anything, vpgUUID, "READY", "").Return(errors.New("state update failed"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestCreateVolumePerformanceGroupWorkflow_OntapClusterHealthCheckFails(t *testing.T) {
	env := setupVPGWorkflowEnv(t)

	vpgUUID := "vpg-uuid-123"
	poolID := int64(1)
	vpg := &datamodel.VolumePerformanceGroup{
		BaseModel: datamodel.BaseModel{UUID: vpgUUID},
		Name:      "test-vpg",
		PoolID:    poolID,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{ID: poolID},
		DeploymentName:  "test-deployment",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	dbNodes := []*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, EndpointAddress: "127.0.0.1"},
	}

	env.OnActivity("GetVolumePerformanceGroupByUUID", mock.Anything, vpgUUID).Return(vpg, nil)
	env.OnActivity("GetPoolBySvmPoolId", mock.Anything, poolID).Return(pool, nil)
	env.OnActivity("GetNode", mock.Anything, poolID).Return(dbNodes, nil)
	env.OnActivity("GetOntapClusterHealth", mock.Anything, mock.Anything).Return(nil, errors.New("health check failed"))

	env.ExecuteWorkflow(CreateVolumePerformanceGroupWorkflow, vpgUUID)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}
