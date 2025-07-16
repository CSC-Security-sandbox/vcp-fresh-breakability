package workflows

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	envs "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
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

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: common.USERNAME_PWD,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateDONE),
	}, nil)
	env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	enableMetrics = true
	// Mock child workflow execution
	env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, RegisterNodeToHarvestFarmWorkflowInput{
		PoolID:            0,
		CustomerProjectID: "test-account",
		MaxNodesPerGroup:  200,
		TenantProjectID:   "test-project",
	}).Return(nil)

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

func TestCreatePoolWorkflow_CreateSubnetJobFailure(t *testing.T) {
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

	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("subnet create failed"))
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCreatePoolWorkflow_PollJobError(t *testing.T) {
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

	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(nil, errors.New("job poll failed"))
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCreatePoolWorkflow_GetTenancyDetailsError(t *testing.T) {
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

	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateDONE),
	}, nil)
	env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(nil, errors.New("get tenancy details failed"))
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
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
		Description:          "Updated pool description",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-id",
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: common.USERNAME_PWD,
		},
		// Set additional fields if required.
		ClusterDetails: datamodel.ClusterDetails{
			ExternalName:          "test-cluster",
			Network:               "test-network",
			RegionalTenantProject: "test-regional-project",
			SnHostProject:         "test-host-project",
		},
		SizeInBytes: 2048 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone:     "test-primary-zone",
			SecondaryZone:   "test-secondary-zone",
			Iops:            1024,
			ThroughputMibps: 64,
		},
		KmsConfig: &datamodel.KmsConfig{
			ServiceAccount: &datamodel.ServiceAccount{
				ServiceAccountEmail: "test-sa-email",
			},
		},
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			BucketName: "test-auto-tier-bucket",
		},
	}

	// Register activity mocks.
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).
		Return(nil)
	env.OnActivity("CreateVlmConfig", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			SPConfig: vlm.SPConfig{
				IOps:       1024,
				Throughput: 64,
				Size:       "1TiB",
			},
		},
	}, nil)
	env.OnActivity("GetOnTapCredentials", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("UpdateVSACluster", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("UpdatedPool", mock.Anything, mock.Anything).
		Return(nil, nil)

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

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	// Set up test data
	params := &common.DeletePoolParams{
		PoolID:      "test-pool",
		AccountName: "test-account",
	}

	pool := &datamodel.Pool{
		Name: "test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			BucketName: "test-bucket",
		},
		ServiceAccountId: "test-service-account",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "test-tenant",
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: common.USERNAME_PWD,
		},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)

	// Mock child workflow
	env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, &unRegisterNodeFromHarvestFarmParams{
		PoolID: 0,
	}).Return(nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

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

func TestDeletePoolWorkflowWithAuthTypeUserPasswordInSecretManager(t *testing.T) {
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

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})

	pool := &datamodel.Pool{
		Name: "test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			BucketName: "test-bucket",
		},
		ServiceAccountId: "test-service-account",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "test-tenant",
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: common.USERNAME_PWD,
		},
	}
	// Set up test data
	params := &common.DeletePoolParams{
		PoolID:      "test-pool",
		AccountName: "test-account",
	}

	originalProjectID := common.SecretManagerProjectID
	common.SecretManagerProjectID = "123456789"

	defer func() {
		common.SecretManagerProjectID = originalProjectID
	}()

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)

	// Mock child workflow
	env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, &unRegisterNodeFromHarvestFarmParams{
		PoolID: 0,
	}).Return(nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

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

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "password",
		},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateDONE),
	}, nil)
	env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Bucket Creation Failed"))
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)
}

func TestConfigureQoSPolicyForSvmActivity(t *testing.T) {
	t.Run("WhenCreateQoSPolicyAndApplyToSVMFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

		// Set up test data
		params := &common.CreatePoolParams{
			Name:                    "test-pool",
			AccountName:             "test-account",
			SizeInBytes:             1024 * 1024 * 1024, // 1 GB
			Region:                  "test-region",
			PrimaryZone:             "test-zone",
			AllowAutoTiering:        true,
			CustomPerformanceParams: &common.CustomPerformanceParams{Enabled: true, ThroughputMibps: 64, Iops: 1024},
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("QoS policy creation failed"))
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		assert.Contains(t, env.GetWorkflowError().Error(), "QoS policy creation failed")
		env.AssertExpectations(t)
	})
}

func TestConfigureKmsConfigForSvmActivity(t *testing.T) {
	enableMetrics = true
	t.Run("WhenGetKmsConfigActivityReturnsNoError", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Mock child workflow execution
		env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, RegisterNodeToHarvestFarmWorkflowInput{
			PoolID:            0,
			CustomerProjectID: "test-account",
			MaxNodesPerGroup:  200,
			TenantProjectID:   "test-project",
		}).Return(nil)

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
	})

	t.Run("WhenGetKmsConfigActivityReturnsErrorNotFound", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}
		cvpKmsConfig := &cvpModels.KmsConfigV1beta{}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("some error", kms_activities.ErrTypeKmsConfigNotFound, errors.New("some error")))
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Mock child workflow execution
		env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, RegisterNodeToHarvestFarmWorkflowInput{
			PoolID:            0,
			CustomerProjectID: "test-account",
			MaxNodesPerGroup:  200,
			TenantProjectID:   "test-project",
		}).Return(nil)

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
	})

	t.Run("WhenGrantRoleActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}
		cvpKmsConfig := &cvpModels.KmsConfigV1beta{}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("some error", kms_activities.ErrTypeKmsConfigNotFound, errors.New("some error")))
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GrantRoleActivity", mock.Anything, mock.Anything).Return(errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenCreateVSAKmsConfigSAKeyActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})

		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}
		cvpKmsConfig := &cvpModels.KmsConfigV1beta{}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("some error", kms_activities.ErrTypeKmsConfigNotFound, errors.New("some error")))
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateVSAKmsConfigSAKeyActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenCreateAndSyncKmsConfigActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}
		cvpKmsConfig := &cvpModels.KmsConfigV1beta{}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("some error", kms_activities.ErrTypeKmsConfigNotFound, errors.New("some error")))
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything).Return(cvpKmsConfig, nil)
		env.OnActivity("CreateAndSyncKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenDescribeKmsConfigurationActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, temporal.NewNonRetryableApplicationError("some error", kms_activities.ErrTypeKmsConfigNotFound, errors.New("some error")))
		env.OnActivity("DescribeSDEKmsConfigurationActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenGetKmsConfigActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenAccessCryptoKeyActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(errors.New("error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenCheckVsaKmsConfigReachableActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenConfigureKmsForSvmActivityError", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenUpdatePoolWithKmsConfigActivityFails", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
		}()
		getSignedJwtToken = func(projectNumber string) (string, error) {
			return "", nil
		}

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})

	t.Run("WhenRunningEnvIsLocal", func(t *testing.T) {
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

		mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
		newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
		defer func() {
			GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		}()

		mockStorage := database.NewMockStorage(t)
		env.RegisterActivity(&SubnetActivity{})
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterActivity(&kms_activities.KmsConfigActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
			KmsConfigId:             "ksmConfigUUID",
		}
		pool := &datamodel.Pool{
			PoolCredentials: &datamodel.PoolCredentials{
				Password: "test-password",
				SecretID: "",
			},
		}
		runningEnv = "local"

		defer func() {
			getSignedJwtToken = auth.GetSignedJwtToken
			runningEnv = envs.GetString("ENV", "")
		}()

		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Execute workflow
		env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		if err != nil {
			t.Fatalf("Failed to query workflow: %v", err)
		}

		// Assert workflow execution
		assert.True(t, env.IsWorkflowCompleted())
		assert.Error(t, env.GetWorkflowError())
		env.AssertExpectations(t)
	})
}

func TestCreatePoolWorkflow_Failure_FindTenancyProject(t *testing.T) {
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
		PoolCredentials: &datamodel.PoolCredentials{Password: "password", SecretID: "secret-id"},
	}
	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status")).Times(10)

	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything, mock.Anything).Return("", errors.New("failed to create tenancy"))
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to update job status")
	env.AssertExpectations(t)
}

func TestCreatePoolWorkflow_InitialFailure_UpdateJobStatus(t *testing.T) {
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
		PoolCredentials: &datamodel.PoolCredentials{Password: "password", SecretID: "secret-id"},
	}
	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to update job status")
	env.AssertExpectations(t)
}

func TestCreatePoolWorkflow_FailureToUpdateFinalJobStatus(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	enableMetrics = true
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	env.SetHeader(mockHeader)

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{Password: "password", SecretID: "secret-id"},
	}

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Times(1)
	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything, mock.Anything).Return("test-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateDONE),
	}, nil)
	env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SetupNetwork", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)

	// Simulate failure in final job status update
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status")).Times(10)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	// Mock child workflow execution
	env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, RegisterNodeToHarvestFarmWorkflowInput{
		PoolID:            0,
		CustomerProjectID: "test-account",
		MaxNodesPerGroup:  200,
		TenantProjectID:   "test-project",
	}).Return(nil)

	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to update job status")
	env.AssertExpectations(t)
}

func TestCreatePoolWorkflow_FailureUpdatePoolSubnet(t *testing.T) {
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
	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&SubnetActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
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
		PoolCredentials: &datamodel.PoolCredentials{Password: "password", SecretID: "secret-id"},
	}
	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status")).Times(10)

	env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything, mock.Anything).Return("tenant-project", nil)
	env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
	mockStorage.EXPECT().GetJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateDONE),
	}, nil)
	env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update pool subnet"))
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to update job status")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow(t *testing.T) {
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
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

	env.OnActivity("CreateOrGetSubnetwork", mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "tenant-project")

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_RunError(t *testing.T) {
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
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("CreateOrGetSubnetwork", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch subnet"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: CreateOrGetSubnetwork, scheduledEventID: 0, startedEventID: 0, identity: ): failed to fetch subnet",
	}).Return(nil).Once()

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "tenant-project")

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_UpdateJobError(t *testing.T) {
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
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("CreateOrGetSubnetwork", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch subnet"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: CreateOrGetSubnetwork, scheduledEventID: 0, startedEventID: 0, identity: ): failed to fetch subnet",
	}).Return(errors.New("failed to update job status")).Once()

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "tenant-project")

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestCreateSubnetJob_Success(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	mockStorage := database.NewMockStorage(t)

	subnetActivity := &SubnetActivity{SE: mockStorage}

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		Region:      "test-region",
		PrimaryZone: "test-zone",
	}
	pool := &datamodel.Pool{
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		},
	}
	tenantProjectNumber := "test-tenant-project"
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := ExecuteWorkflowSeq
	ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return nil
	}
	defer func() { ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	// Mock dependencies if any (none in this method directly)
	env.RegisterActivity(subnetActivity.CreateSubnetJob)

	mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
	}, nil)

	// Execute activity
	future, err := env.ExecuteActivity(subnetActivity.CreateSubnetJob, params, pool, tenantProjectNumber)
	assert.NoError(t, err)

	var result string
	err = future.Get(&result)
	assert.NoError(t, err)
}

func TestCreateSubnetJob_WorkflowError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	mockStorage := database.NewMockStorage(t)

	subnetActivity := &SubnetActivity{SE: mockStorage}

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		Region:      "test-region",
		PrimaryZone: "test-zone",
	}
	pool := &datamodel.Pool{
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		},
	}
	tenantProjectNumber := "test-tenant-project"
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	// Mock ExecuteWorkflowSequentially using ExecuteWorkflowSeq
	origExecuteWorkflowSeq := ExecuteWorkflowSeq
	ExecuteWorkflowSeq = func(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error {
		return errors.New("test workflow error")
	}
	defer func() { ExecuteWorkflowSeq = origExecuteWorkflowSeq }()

	// Mock dependencies if any (none in this method directly)
	env.RegisterActivity(subnetActivity.CreateSubnetJob)

	mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
	}, nil)

	// Execute activity
	_, err := env.ExecuteActivity(subnetActivity.CreateSubnetJob, params, pool, tenantProjectNumber)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test workflow error")
}

func TestCreateSubnetJob_JobError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestActivityEnvironment()
	mockStorage := database.NewMockStorage(t)

	subnetActivity := &SubnetActivity{SE: mockStorage}

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		Region:      "test-region",
		PrimaryZone: "test-zone",
	}
	pool := &datamodel.Pool{
		Account: &datamodel.Account{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
		},
	}
	tenantProjectNumber := "test-tenant-project"
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)

	origFetchTemporalClient := fetchTemporalClient
	// Patch fetchTemporalClient to return mockOntap
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() {
		fetchTemporalClient = origFetchTemporalClient
	}()

	// Mock dependencies if any (none in this method directly)
	env.RegisterActivity(subnetActivity.CreateSubnetJob)

	mockStorage.EXPECT().CreateJob(mock.Anything, mock.Anything).Return(nil, errors.New("test job error"))

	// Execute activity
	_, err := env.ExecuteActivity(subnetActivity.CreateSubnetJob, params, pool, tenantProjectNumber)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test job error")
}

type mockEncVal struct {
	err   bool
	value subnetWorkflowResult
}

func (m mockEncVal) Get(valuePtr interface{}) error {
	if m.err {
		return fmt.Errorf("encoding error for value: %+v", valuePtr)
	}

	v, ok := valuePtr.(*subnetWorkflowResult)
	if !ok {
		return fmt.Errorf("expected *subnetWorkflowResult, got %T", valuePtr)
	}

	*v = m.value
	return nil
}

func (m mockEncVal) HasValue() bool {
	return true
}

func TestSubnetActivity_GetTenancyDetails_Success(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	expectedResult := subnetWorkflowResult{
		TenancyDetails: &common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		},
		WorkflowStatus: &WorkflowStatus{Status: WorkflowStatusCompleted},
	}
	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockEncVal{value: expectedResult}, nil)

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.NoError(t, err)
	assert.Equal(t, expectedResult.TenancyDetails, result)
}

func TestSubnetActivity_GetTenancyDetails_QueryWorkflowError(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("query error"))

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSubnetActivity_GetTenancyDetails_EncodingError(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockEncVal{err: true}, nil)

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSubnetActivity_GetTenancyDetails_WorkflowStatusNil(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockEncVal{value: subnetWorkflowResult{}}, nil)

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSubnetActivity_GetTenancyDetails_WorkflowStatusNotCompleted(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockEncVal{
		value: subnetWorkflowResult{
			WorkflowStatus: &WorkflowStatus{Status: "not-completed"},
		},
	}, nil)

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestSubnetActivity_GetTenancyDetails_ResultNilError(t *testing.T) {
	mockTemp := workflow_engine.NewMockTemporalTestClient(t)
	subnetActivity := &SubnetActivity{}
	origFetchTemporalClient := fetchTemporalClient
	fetchTemporalClient = func(ctx context.Context) client.Client {
		return mockTemp
	}
	defer func() { fetchTemporalClient = origFetchTemporalClient }()

	mockTemp.On("QueryWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(mockEncVal{
		value: subnetWorkflowResult{
			WorkflowStatus: &WorkflowStatus{Status: WorkflowStatusCompleted},
		},
	}, nil)

	result, err := subnetActivity.GetTenancyDetails(context.Background(), "test-workflow-id")
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "returned tenancy details as nil")
}
