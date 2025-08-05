package workflows

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	cvpModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	hyperscalermodels "github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
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
	env.RegisterWorkflow(ConfigureNetworkWorkflow)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.BackupActivity{SE: mockStorage})
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
			AuthType: envs.USERNAME_PWD,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}
	svmName := "svmName"

	defer func() {
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
		WaitForGCPNetworkOperationStatus = _waitForGCPNetworkOperationStatus
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}

	WaitForGCPNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, project string, isRegionalResource bool, operationNames *map[string]bool, timeout time.Duration) error {
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
	env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
	env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
		PrimaryZone:   "test-zone",
		SecondaryZone: "test-secondary-zone",
		Region:        "test-region",
		MediatorZone:  "test-mediator-zone",
	}, nil)
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	oldEnableMetrics := enableMetrics
	enableMetrics = true
	defer func() { enableMetrics = oldEnableMetrics }()
	// Mock child workflow execution
	env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
		return input.PoolID == 0 &&
			input.CustomerProjectID == "test-account" &&
			input.MaxNodesPerGroup == 200 &&
			input.TenantProjectID == "test-project"
	})).Return(nil)

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

func TestCreatePoolWorkflow_RegisterNodeToHarvestFailure(t *testing.T) {
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
	env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
			AuthType: envs.USERNAME_PWD,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}
	svmName := "svmName"

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
	env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
	env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
		PrimaryZone:   "test-zone",
		SecondaryZone: "test-secondary-zone",
		Region:        "test-region",
		MediatorZone:  "test-mediator-zone",
	}, nil)

	// Rollback
	env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, mock.Anything).Return(nil)
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

	oldEnableMetrics := enableMetrics
	enableMetrics = true
	defer func() { enableMetrics = oldEnableMetrics }()
	// Mock child workflow execution
	env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
		return input.PoolID == 0 &&
			input.CustomerProjectID == "test-account" &&
			input.MaxNodesPerGroup == 200 &&
			input.TenantProjectID == "test-project"
	})).Return(errors.New("failed to register node"))

	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to register node")
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

func TestCreatePoolWorkflow_AllocateClusterSerialNumber(t *testing.T) {
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
	env.RegisterWorkflow(ConfigureNetworkWorkflow)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.BackupActivity{SE: mockStorage})
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
			AuthType: envs.USERNAME_PWD,
		},
		PoolAttributes: &datamodel.PoolAttributes{
			Iops:            params.CustomPerformanceParams.Iops,
			ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
		},
	}
	svmName := "svmName"
	oldEnableUniqueSerialNumberGeneration := enableUniqueSerialNumberGeneration
	defer func() {
		enableUniqueSerialNumberGeneration = oldEnableUniqueSerialNumberGeneration
		configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
		isProberProject = utils.IsProberProject
		err := os.Unsetenv("VCP_VSA_ENABLE_SERIAL_NUMBER")
		if err != nil {
			t.Errorf("Failed to unset VCP_VSA_ENABLE_SERIAL_NUMBER")
		}
	}()
	configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
		return nil
	}
	isProberProject = func(projectID string) bool {
		return false
	}
	enableUniqueSerialNumberGeneration = true

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
	env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("AllocateClusterSerialNumber", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			SPConfig: vlm.SPConfig{
				IOps:       1024,
				Throughput: 64,
				Size:       "1TiB",
			},
			SerialNumberPrefix: "93500011111",
		},
	}, nil)
	mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
	env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
	env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
		PrimaryZone:   "test-zone",
		SecondaryZone: "test-secondary-zone",
		Region:        "test-region",
		MediatorZone:  "test-mediator-zone",
	}, nil)
	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

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

func TestCreatePoolWorkflow_ConfigureNetworkWorkflow(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
		env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
		env.RegisterActivity(&activities.BackupActivity{SE: mockStorage})
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
				AuthType: envs.USERNAME_PWD,
			},
			PoolAttributes: &datamodel.PoolAttributes{
				Iops:            params.CustomPerformanceParams.Iops,
				ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			},
		}
		svmName := "svmName"

		defer func() {
			configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
			WaitForGCPNetworkOperationStatus = _waitForGCPNetworkOperationStatus
		}()
		configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
			return nil
		}

		WaitForGCPNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, project string, isRegionalResource bool, operationNames *map[string]bool, timeout time.Duration) error {
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		oldEnableMetrics := enableMetrics
		enableMetrics = true
		defer func() { enableMetrics = oldEnableMetrics }()
		// Mock child workflow execution
		env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
			return input.PoolID == 0 &&
				input.CustomerProjectID == "test-account" &&
				input.MaxNodesPerGroup == 200 &&
				input.TenantProjectID == "test-project"
		})).Return(nil)

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
	t.Run("CreateVPCs_fails", func(t *testing.T) {
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
				AuthType: envs.USERNAME_PWD,
			},
			PoolAttributes: &datamodel.PoolAttributes{
				Iops:            params.CustomPerformanceParams.Iops,
				ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			},
		}

		defer func() {
			configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
			WaitForGCPNetworkOperationStatus = _waitForGCPNetworkOperationStatus
		}()
		configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
			return nil
		}

		WaitForGCPNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, project string, isRegionalResource bool, operationNames *map[string]bool, timeout time.Duration) error {
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create VPCs"))
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
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
		assert.Contains(t, env.GetWorkflowError().Error(), "failed to create VPCs")
		env.AssertExpectations(t)
	})
	t.Run("CreateSubnets_fails", func(t *testing.T) {
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
				AuthType: envs.USERNAME_PWD,
			},
			PoolAttributes: &datamodel.PoolAttributes{
				Iops:            params.CustomPerformanceParams.Iops,
				ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			},
		}

		defer func() {
			configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
			WaitForGCPNetworkOperationStatus = _waitForGCPNetworkOperationStatus
		}()
		configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
			return nil
		}

		WaitForGCPNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, project string, isRegionalResource bool, operationNames *map[string]bool, timeout time.Duration) error {
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, errors.New("failed to create subnets"))
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
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
		assert.Contains(t, env.GetWorkflowError().Error(), "failed to create subnets")
		env.AssertExpectations(t)
	})
	t.Run("CreateFirewalls_fails", func(t *testing.T) {
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
				AuthType: envs.USERNAME_PWD,
			},
			PoolAttributes: &datamodel.PoolAttributes{
				Iops:            params.CustomPerformanceParams.Iops,
				ThroughputMibps: params.CustomPerformanceParams.ThroughputMibps,
			},
		}

		defer func() {
			configureKmsConfigForSvmActivity = _configureKmsConfigForSvmActivity
			WaitForGCPNetworkOperationStatus = _waitForGCPNetworkOperationStatus
		}()
		configureKmsConfigForSvmActivity = func(ctx workflow.Context, pool datamodel.Pool, node *models.Node, svm *datamodel.Svm, params *common.CreatePoolParams) error {
			return nil
		}

		WaitForGCPNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, project string, isRegionalResource bool, operationNames *map[string]bool, timeout time.Duration) error {
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create firewalls"))
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
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
		assert.Contains(t, env.GetWorkflowError().Error(), "failed to create firewalls")
		env.AssertExpectations(t)
	})
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

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
	}()

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
			AuthType: envs.USERNAME_PWD,
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
	env.OnActivity("ConstructCurrentVlmConfig", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			SPConfig: vlm.SPConfig{
				IOps:       1024,
				Throughput: 64,
				Size:       "1TiB",
			},
		},
	}, nil)
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
	mockVSAClientWorkflowManager.On("UpdateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(&vlm.UpdateVSAClusterDeploymentResponse{}, nil)
	env.OnActivity("UpdatedPool", mock.Anything, mock.Anything).
		Return(nil, nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

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

func TestUpdatePoolWorkflowNoVLM(t *testing.T) {
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
			AuthType: envs.USERNAME_PWD,
		},
		// Set additional fields if required.
		ClusterDetails: datamodel.ClusterDetails{
			ExternalName:          "test-cluster",
			Network:               "test-network",
			RegionalTenantProject: "test-regional-project",
			SnHostProject:         "test-host-project",
		},
		SizeInBytes: 2 * 1024 * 1024 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{
			PrimaryZone:     "test-primary-zone",
			SecondaryZone:   "test-secondary-zone",
			Iops:            2048,
			ThroughputMibps: 128,
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
	enableMetrics = true
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		enableMetrics = envs.GetBool("ENABLE_METRICS", false)
	}()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&kms_activities.KmsConfigActivity{})

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
			AuthType: envs.USERNAME_PWD,
		},
		KmsConfig:   &datamodel.KmsConfig{},
		KmsConfigID: sql.NullInt64{Int64: 1, Valid: true},
	}

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil)

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
	enableMetrics = true
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		enableMetrics = envs.GetBool("ENABLE_METRICS", false)
	}()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&kms_activities.KmsConfigActivity{})

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
			AuthType: envs.USERNAME_PWD,
		},
	}
	// Set up test data
	params := &common.DeletePoolParams{
		PoolID:      "test-pool",
		AccountName: "test-account",
	}

	originalProjectID := envs.SecretManagerProjectID
	envs.SecretManagerProjectID = "123456789"

	defer func() {
		envs.SecretManagerProjectID = originalProjectID
	}()

	// Mock activity responses
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(pool, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
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

func TestDeletePoolWorkflow_OntapVersionBranches(t *testing.T) {
	var ts testsuite.WorkflowTestSuite

	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	params := &common.DeletePoolParams{
		PoolID:      "test-pool",
		AccountName: "test-account",
	}

	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetHeader(mockHeader)

	mockVSAClientWorkflowManager := new(vlm.MockVlmWorkflowClient)
	newVSAClientWorkflowManager := GetNewVSAClientWorkflowManager
	enableMetrics = true
	defer func() {
		GetNewVSAClientWorkflowManager = newVSAClientWorkflowManager
		enableMetrics = envs.GetBool("ENABLE_METRICS", false)
	}()

	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&kms_activities.KmsConfigActivity{})

	poolEmpty := &datamodel.Pool{
		Name: "test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			BucketName: "test-bucket",
		},
		ServiceAccountId: "test-service-account",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "test-tenant",
			OntapVersion:          "",
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: envs.USERNAME_PWD,
		},
		KmsConfig:   &datamodel.KmsConfig{},
		KmsConfigID: sql.NullInt64{Int64: 1, Valid: true},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(poolEmpty, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil)

	env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, &unRegisterNodeFromHarvestFarmParams{
		PoolID: 0,
	}).Return(nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	env.ExecuteWorkflow(DeletePoolWorkflow, params, poolEmpty)
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	env = ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	env.SetHeader(mockHeader)
	env.RegisterActivity(&activities.CommonActivities{})
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&kms_activities.KmsConfigActivity{})

	poolNonEmpty := &datamodel.Pool{
		Name: "test-pool",
		AutoTieringConfig: &datamodel.AutoTieringConfig{
			BucketName: "test-bucket",
		},
		ServiceAccountId: "test-service-account",
		ClusterDetails: datamodel.ClusterDetails{
			RegionalTenantProject: "test-tenant",
			OntapVersion:          "9.13.1P2",
		},
		PoolCredentials: &datamodel.PoolCredentials{
			Password: "test-password",
			SecretID: "",
			AuthType: envs.USERNAME_PWD,
		},
		KmsConfig:   &datamodel.KmsConfig{},
		KmsConfigID: sql.NullInt64{Int64: 1, Valid: true},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("GetPool", mock.Anything, mock.Anything).Return(poolNonEmpty, nil)
	env.OnActivity("DeletingPoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("GetCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("VerifyVsaKmsReachabilityActivity", mock.Anything, mock.Anything).Return(nil)

	env.OnWorkflow(UnRegisterNodeFromHarvestFarmWorkflow, mock.Anything, &unRegisterNodeFromHarvestFarmParams{
		PoolID: 0,
	}).Return(nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	env.ExecuteWorkflow(DeletePoolWorkflow, params, poolNonEmpty)
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
	env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
	env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("Bucket Creation Failed"))

	// Rollback activities that will be called when CreateAutoTierBucket fails
	env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	// Execute workflow
	env.ExecuteWorkflow(CreatePoolWorkflow, params, pool)

	// Assert workflow execution - should complete but with error due to bucket creation failure
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "Bucket Creation Failed")
	env.AssertExpectations(t)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("QoS policy creation failed"))
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)

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

	t.Run("WhenGetInterClusterLifsFromVLMConfigFails", func(t *testing.T) {
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
		env.RegisterActivity(&activities.BackupActivity{SE: mockStorage})
		env.RegisterActivity(&activities.PoolActivity{})
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, request vlm.DeleteVSAClusterDeploymentRequest) error {
				return nil
			},
			workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
		)

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
				AuthType: envs.USERNAME_PWD,
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

		// Mock all activities up to the GetInterClusterLifsFromVLMConfig failure
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("FindTenancyProject", mock.Anything, mock.Anything).Return("test-project", nil)
		env.OnActivity("CreateSubnetJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return("test-subnet-id", nil)
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "test-subnet-id"},
			State:     string(models.JobsStateDONE),
		}, nil)
		env.OnActivity("GetTenancyDetails", mock.Anything, mock.Anything).Return(&common.TenancyInfo{
			Network:               "test-network",
			SubnetworkNames:       []string{"test-subnet"},
			RegionalTenantProject: "test-project",
			SnHostProject:         "test-host-project",
			Gateway:               "192.168.1.254",
		}, nil)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context, tenancyDetails *common.TenancyInfo) error {
				return nil
			},
			workflow.RegisterOptions{Name: "ConfigureNetworkWorkflow"},
		)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		// GetInterClusterLifsFromVLMConfig will fail, so the following activities won't be called
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return(nil, errors.New("Failed to get intercluster LIFs from ONTAP"))
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockVSAClientWorkflowManager.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)

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
		assert.Contains(t, env.GetWorkflowError().Error(), "Failed to get intercluster LIFs from ONTAP")
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateAutoTierBucket", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateServiceAccountWithStorageRole", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifyVMs", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockVSAClientWorkflowManager.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything).Return(&vlm.CreateVSAClusterDeploymentResponse{}, nil)
		env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("GetOntapVersion", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("SavePoolWithClusterDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("AccessCryptoKeyWithImpersonationActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("CreateDnsActivity", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ConfigureKmsForSvmActivity", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CheckVsaKmsConfigReachableActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdatePoolWithKmsConfigActivity", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateOnTapCredentials", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Mock child workflow execution
		env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
			return input.PoolID == 0 &&
				input.CustomerProjectID == "test-account" &&
				input.MaxNodesPerGroup == 200 &&
				input.TenantProjectID == "test-project"
		})).Return(nil)

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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)

		GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
			return mockVSAClientWorkflowManager
		}

		// Mock child workflow execution
		env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
			return input.PoolID == 0 &&
				input.CustomerProjectID == "test-account" &&
				input.MaxNodesPerGroup == 200 &&
				input.TenantProjectID == "test-project"
		})).Return(nil)

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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
		env.OnActivity("GetKmsConfigActivity", mock.Anything, mock.Anything).Return(nil, errors.New("some error"))
		env.OnWorkflow(vlm.DeleteVSAClusterDeploymentWorkflowName, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteAutoTierBucket", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteServiceAccount", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ReleaseSubnet", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteOnTapCredentials", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeleteCloudDNSRecords", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("DeletePoolResourcesOnRollback", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
		env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
		svmName := "svmName"
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
		env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
		env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return(svmName, nil)
		mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
		env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, svmName).Return(nil, nil)
		env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
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
		env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
			PrimaryZone:   "test-zone",
			SecondaryZone: "test-secondary-zone",
			Region:        "test-region",
			MediatorZone:  "test-mediator-zone",
		}, nil)
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
	env.RegisterWorkflow(ConfigureNetworkWorkflow)
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
	env.OnActivity("CreateVPCs", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateSubnets", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("CreateFirewalls", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
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
	env.OnActivity("AllocateSVMName", mock.Anything, mock.Anything).Return("svmName", nil)
	mockVSAClientWorkflowManager.On("CreateVSASVM", mock.Anything, mock.Anything).Return(&vlm.CreateSVMResponse{}, nil)
	env.OnActivity("CreateQoSPolicyAndApplyToSVM", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SaveSVMAndLifData", mock.Anything, mock.Anything, mock.Anything, "svmName").Return(nil, nil)
	env.OnActivity("GetInterClusterLifsFromVLMConfig", mock.Anything, mock.Anything).Return([]string{"192.168.1.10", "192.168.1.11"}, nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything).Return(nil, nil)
	env.OnActivity("IdentifySecondaryAndMediatorZone", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&common.LocationInfo{
		PrimaryZone:   "test-zone",
		SecondaryZone: "test-secondary-zone",
		Region:        "test-region",
		MediatorZone:  "test-mediator-zone",
	}, nil)
	// Simulate failure in final job status update
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(errors.New("failed to update job status")).Times(10)

	GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient {
		return mockVSAClientWorkflowManager
	}

	// Mock child workflow execution
	env.OnWorkflow(RegisterNodeToHarvestFarmWorkflow, mock.Anything, mock.MatchedBy(func(input RegisterNodeToHarvestFarmWorkflowInput) bool {
		return input.PoolID == 0 &&
			input.CustomerProjectID == "test-account" &&
			input.MaxNodesPerGroup == 200 &&
			input.TenantProjectID == "test-project"
	})).Return(nil)

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
	env.RegisterActivity(&SubnetActivity{})
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
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return []byte("test-operation-data"), nil
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "",
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("GetSubnetFromOperation", mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{Name: "subnet-name"}, nil)
	env.OnActivity("GetTenancyInfo", mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
		RegionalTenantProject: "test-project",
		SnHostProject:         "test-host-project",
		Gateway:               "192.168.1.254",
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "DONE",
	}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", "tenant-project")

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
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "",
	}, nil)
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch subnet"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: GetCreateDataSubnetOp, scheduledEventID: 0, startedEventID: 0, identity: ): failed to fetch subnet",
	}).Return(nil).Once()

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", "tenant-project")

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
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{Name: ""}, nil)
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to fetch subnet"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: GetCreateDataSubnetOp, scheduledEventID: 0, startedEventID: 0, identity: ): failed to fetch subnet",
	}).Return(errors.New("failed to update job status"))

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", "tenant-project")

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

// Test cases for poolDataSubnetWorkFlow.Run method to improve coverage
func TestPoolDataSubnetWorkFlow_ExistingSubnet1(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	// Mock the UpdateJobStatus activity that gets called during workflow execution
	env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil).Once()

	// Mock existing subnet (name is not empty) - tests the path where GetCreateDataSubnetOp is NOT called
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name:           "existing-subnet",
		Network:        "projects/test-project/global/networks/test-network",
		GatewayAddress: "10.0.0.1",
	}, nil)
	env.OnActivity("GetTenancyInfo", mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{
		RegionalTenantProject: "test-tenant-123",
		Network:               "test-network",
		SubnetworkNames:       []string{"existing-subnet"},
	}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "DONE",
	}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_GetAvailableSubnetError1(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	// Mock the first UpdateJobStatus call (PROCESSING)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()

	// Mock GetAvailableSubnet to return an error
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("subnet lookup failed"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: GetAvailableSubnet, scheduledEventID: 0, startedEventID: 0, identity: ): subnet lookup failed",
	}).Return(nil).Once()

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
	if err != nil {
		t.Fatalf("Failed to query workflow: %v", err)
	}

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_GetCreateDataSubnetOpError(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	// Mock empty subnet response to trigger GetCreateDataSubnetOp path
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "",
	}, nil)
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(nil, fmt.Errorf("create subnet failed"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: GetCreateDataSubnetOp, scheduledEventID: 0, startedEventID: 0, identity: ): create subnet failed"}).Return(nil).Once()

	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "create subnet failed")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_SuccessfulNewSubnetCreation1(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "",
	}, nil)
	operationName := ""
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "failed to create subnet for tenant project: test-tenant-123, operation name is empty",
	}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to create subnet for tenant project: test-tenant-123, operation name is empty")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_WaitFails(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	// Mock successful subnet creation flow
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return nil, errors.New("wait for operation failed")
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "", // Empty name triggers subnet creation path
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "failed to create subnet for tenant project while waiting to get operation status: test-tenant-123: wait for operation failed"}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "wait for operation failed")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_GetSubnet(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	// Mock successful subnet creation flow
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return []byte("test-operation-data"), nil
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "", // Empty name triggers subnet creation path
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("GetSubnetFromOperation", mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "test-subnet",
	}, errors.New("failed to get subnet from operation"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "failed to get subnet from operation for tenant project: test-tenant-123: activity error (type: GetSubnetFromOperation, scheduledEventID: 0, startedEventID: 0, identity: ): failed to get subnet from operation"}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to get subnet from operation")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_GetTenancyInfo(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	// Mock successful subnet creation flow
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return []byte("test-operation-data"), nil
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "", // Empty name triggers subnet creation path
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("GetSubnetFromOperation", mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "test-subnet",
	}, nil)
	env.OnActivity("GetTenancyInfo", mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, errors.New("failed to get tenancy info"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: GetTenancyInfo, scheduledEventID: 0, startedEventID: 0, identity: ): failed to get tenancy info"}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to get tenancy info")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_UpdatePoolSubnet(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	// Mock successful subnet creation flow
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return []byte("test-operation-data"), nil
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "", // Empty name triggers subnet creation path
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("GetSubnetFromOperation", mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "test-subnet",
	}, nil)
	env.OnActivity("GetTenancyInfo", mock.Anything, mock.Anything, mock.Anything).Return(&common.TenancyInfo{}, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update pool subnet"))
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State:        "DONE",
		ErrorDetails: "activity error (type: UpdatePoolSubnet, scheduledEventID: 0, startedEventID: 0, identity: ): failed to update pool subnet"}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.Contains(t, env.GetWorkflowError().Error(), "failed to update pool subnet")
	env.AssertExpectations(t)
}

func TestPoolDataSubnetWorkFlow_SuccessfulNewSubnetCreation(t *testing.T) {
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
	env.RegisterActivity(&activities.PoolActivity{})
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})

	// Mock successful subnet creation flow
	originalWaitForServiceNetworkOperationStatus := WaitForServiceNetworkOperationStatus
	WaitForServiceNetworkOperationStatus = func(ctx workflow.Context, poolActivity *activities.PoolActivity, operationName string, timeout time.Duration) ([]byte, error) {
		return []byte("test-operation-data"), nil
	}
	defer func() {
		WaitForServiceNetworkOperationStatus = originalWaitForServiceNetworkOperationStatus
	}()

	params := &common.CreatePoolParams{
		AccountName: "test-account",
		Region:      "us-central1",
	}
	tenantProjectNumber := "test-tenant-123"

	expectedTenancyInfo := &common.TenancyInfo{
		RegionalTenantProject: "test-project",
		Network:               "test-network",
		SubnetworkNames:       []string{"test-subnet"},
	}

	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "PROCESSING",
	}).Return(nil).Once()
	env.OnActivity("GetAvailableSubnet", mock.Anything, mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "", // Empty name triggers subnet creation path
	}, nil)
	operationName := "test-operation-123"
	env.OnActivity("GetCreateDataSubnetOp", mock.Anything, mock.Anything, mock.Anything).Return(&operationName, nil)
	env.OnActivity("GetSubnetFromOperation", mock.Anything, mock.Anything).Return(&hyperscalermodels.Subnet{
		Name: "test-subnet",
	}, nil)
	env.OnActivity("GetTenancyInfo", mock.Anything, mock.Anything, mock.Anything).Return(expectedTenancyInfo, nil)
	env.OnActivity("UpdatePoolSubnet", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("UpdateJobStatus", mock.Anything, &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID: "default-test-workflow-id",
		},
		State: "DONE",
	}).Return(nil).Once()
	env.ExecuteWorkflow(PoolDataSubnetWorkFlow, params, "pool-uuid", tenantProjectNumber)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func WfTestWaitForServiceNetworkOperationStatus(ctx workflow.Context, operationName string, timeout time.Duration) ([]byte, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
	})
	poolActivity := &activities.PoolActivity{}
	result, err := _waitForServiceNetworkOperationStatus(ctx, poolActivity, operationName, timeout)
	if err != nil {
		return nil, fmt.Errorf("wait for service network operation status test failed: %w", err)
	}
	return result, nil
}

func Test_waitForServiceNetworkOperationStatus_Success_CompletedOperation(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock successful operation completion
	operation := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(`{"result": "success"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(operation, nil)
	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result []byte
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"result": "success"}`), result)
}

func Test_waitForServiceNetworkOperationStatus_OperationWithEmptyResponse(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock operation done but with empty response, then successful completion
	emptyResponseOp := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(""),
	}
	successOperation := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(`{"result": "success"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(emptyResponseOp, nil).Once()
	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(successOperation, nil).Once()

	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result []byte
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"result": "success"}`), result)
}

func Test_waitForServiceNetworkOperationStatus_Timeout_ComprehensiveTest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock operation that is never done
	operation := &hyperscalermodels.ComputeOperation{
		Done:     false,
		Response: []byte(`{"status": "pending"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(operation, nil)
	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Millisecond)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout while confirming compute network google components")
}

func Test_waitForServiceNetworkOperationStatus_GetOperationFails_ComprehensiveTest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(nil, assert.AnError)
	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to get GCP Operation")
}

func Test_waitForServiceNetworkOperationStatus_NotReadyErrorThenSuccess_ComprehensiveTest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock NotReadyErr first, then successful completion
	notReadyErr := errors.NewNotReadyErr("operation not ready")
	successOperation := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(`{"result": "success"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(nil, notReadyErr).Once()
	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(successOperation, nil).Once()

	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result []byte
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"result": "success"}`), result)
}

func Test_waitForServiceNetworkOperationStatus_NotFoundErrorThenSuccess_ComprehensiveTest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock NotFoundErr first, then successful completion
	testOperation := "test-operation"
	notFoundErr := errors.NewNotFoundErr("operation not found", &testOperation)
	successOperation := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(`{"result": "success"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(nil, notFoundErr).Once()
	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(successOperation, nil).Once()

	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result []byte
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"result": "success"}`), result)
}

func Test_waitForServiceNetworkOperationStatus_OperationNotDoneThenSuccess_ComprehensiveTest(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	poolActivity := &activities.PoolActivity{}
	operationName := "test-operation"

	// Mock operation not done first, then successful completion
	notDoneOp := &hyperscalermodels.ComputeOperation{
		Done:     false,
		Response: []byte(`{"status": "in-progress"}`),
	}
	successOperation := &hyperscalermodels.ComputeOperation{
		Done:     true,
		Response: []byte(`{"result": "success"}`),
	}

	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(notDoneOp, nil).Once()
	env.OnActivity(poolActivity.GetServiceNetOpStatus, mock.Anything, operationName).Return(successOperation, nil).Once()

	env.RegisterActivity(poolActivity.GetServiceNetOpStatus)
	env.ExecuteWorkflow(WfTestWaitForServiceNetworkOperationStatus, operationName, 1*time.Minute)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result []byte
	err := env.GetWorkflowResult(&result)
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"result": "success"}`), result)
}

// WfTestWaitForGCPNetworkOperationStatus is a test workflow function for _waitForGCPNetworkOperationStatus
func WfTestWaitForGCPNetworkOperationStatus(ctx workflow.Context, project string, isRegionalResource bool, operationNames map[string]bool, timeout time.Duration) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		}})
	poolActivity := &activities.PoolActivity{}
	err := _waitForGCPNetworkOperationStatus(ctx, poolActivity, project, isRegionalResource, &operationNames, timeout)
	if err != nil {
		return fmt.Errorf("wait for GCP network operation status test failed: %w", err)
	}
	return nil
}

// Comprehensive unit tests for _waitForGCPNetworkOperationStatus

func Test_waitForGCPNetworkOperationStatus_Success_SingleOperation(t *testing.T) {
	t.Run("Success_SingleOperation", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}
		// Mock successful operation completion
		operation := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operation, nil)
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 10*time.Second)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("Success_MultipleOperations", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{
			"operation-1": false,
			"operation-2": false,
			"operation-3": false,
		}

		// Mock successful completion for all operations
		operation1 := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}
		operation2 := &hyperscalermodels.ComputeOperation{
			Name:     "operation-2",
			Status:   "DONE",
			Progress: int64(100),
		}
		operation3 := &hyperscalermodels.ComputeOperation{
			Name:     "operation-3",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operation1, nil)
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-2").Return(operation2, nil)
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-3").Return(operation3, nil)
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("Success_OperationProgressThenComplete", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		// Mock operation in progress first, then completed
		operationInProgress := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "RUNNING",
			Progress: int64(50),
		}
		operationCompleted := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationInProgress, nil).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationCompleted, nil).Once()
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("Success_OperationDoneButIncompleteProgress", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		// Mock operation with DONE status but incomplete progress, then fully complete
		operationDoneIncomplete := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(90), // Not 100, so should continue polling
		}
		operationCompleted := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationDoneIncomplete, nil).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationCompleted, nil).Once()
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("Timeout_ComprehensiveTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		// Mock operation that never completes
		operationPending := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "RUNNING",
			Progress: int64(50),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationPending, nil)
		env.RegisterActivity(poolActivity.GetComputeOpStatus)

		// Create a custom test workflow that sets a longer activity timeout but short workflow timeout
		testWorkflow := func(ctx workflow.Context, project string, isRegionalResource bool, operationNames map[string]bool, timeout time.Duration) error {
			// Set a longer activity timeout so it doesn't timeout before the workflow logic
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second, // Long enough to not interfere with workflow timeout
			})
			poolActivity := &activities.PoolActivity{}
			err := _waitForGCPNetworkOperationStatus(ctx, poolActivity, project, true, &operationNames, timeout)
			if err != nil {
				return fmt.Errorf("wait for GCP network operation status test failed: %w", err)
			}
			return nil
		}

		env.ExecuteWorkflow(testWorkflow, project, true, operationNames, 1*time.Millisecond)

		assert.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout while confirming compute network google components")
	})
	t.Run("GetOperationFails_ComprehensiveTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(nil, assert.AnError)
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get GCP Operation operation-1")
	})
	t.Run("NotReadyErrorThenSuccess_ComprehensiveTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		// Mock NotReadyErr first, then successful completion
		notReadyErr := errors.NewNotReadyErr("operation not ready")
		operationCompleted := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(nil, notReadyErr).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationCompleted, nil).Once()
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("NotFoundErrorThenSuccess_ComprehensiveTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{"operation-1": false}

		// Mock NotFoundErr first, then successful completion
		testOperation := "operation-1"
		notFoundErr := errors.NewNotFoundErr("operation not found", &testOperation)
		operationCompleted := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(nil, notFoundErr).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operationCompleted, nil).Once()
		env.RegisterActivity(poolActivity.GetComputeOpStatus)
		env.ExecuteWorkflow(WfTestWaitForGCPNetworkOperationStatus, project, true, operationNames, 1*time.Minute)

		assert.True(t, env.IsWorkflowCompleted())
		assert.NoError(t, env.GetWorkflowError())
	})
	t.Run("OperationNotDoneThenSuccess_ComprehensiveTest", func(t *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{
			"operation-1": true,  // Already completed
			"operation-2": false, // Not completed
			"operation-3": false, // Not completed
		}
		env.RegisterActivity(poolActivity.GetComputeOpStatus)

		// Mock operation-2 as initially not done, then done
		operation2InProgress := &hyperscalermodels.ComputeOperation{
			Name:     "operation-2",
			Status:   "RUNNING",
			Progress: int64(50),
		}
		operation2Complete := &hyperscalermodels.ComputeOperation{
			Name:     "operation-2",
			Status:   "DONE",
			Progress: int64(100),
		}
		// Mock operation-3 as completed immediately
		operation3 := &hyperscalermodels.ComputeOperation{
			Name:     "operation-3",
			Status:   "DONE",
			Progress: int64(100),
		}
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-2").Return(operation2InProgress, nil).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-2").Return(operation2Complete, nil).Once()
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-3").Return(operation3, nil)

		testWorkflow := func(ctx workflow.Context, project string, isRegionalResource bool, operationNames map[string]bool, timeout time.Duration) error {
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 30 * time.Second, // Long enough to not interfere with workflow timeout
			})
			poolActivity := &activities.PoolActivity{}
			err := _waitForGCPNetworkOperationStatus(ctx, poolActivity, project, true, &operationNames, timeout)
			if err != nil {
				return fmt.Errorf("wait for GCP network operation status test failed: %w", err)
			}
			return nil
		}

		env.ExecuteWorkflow(testWorkflow, project, true, operationNames, 5*time.Second)

		assert.True(t, env.IsWorkflowCompleted())
		err := env.GetWorkflowError()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout while confirming compute network google components")
	})
	t.Run("MultipleOperations_MixedProgressStates", func(t *testing.T) {
		// Create custom workflow for timeout testing
		timeoutTestWorkflow := func(ctx workflow.Context, project string, isRegionalResource bool, operationNames map[string]bool, timeout time.Duration) error {
			// Set activity options with shorter timeout
			ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 5 * time.Second, // Short timeout to trigger timeout error
				RetryPolicy: &temporal.RetryPolicy{
					MaximumAttempts: 1, // No retries to fail fast
				}})
			poolActivity := &activities.PoolActivity{}
			return _waitForGCPNetworkOperationStatus(ctx, poolActivity, project, true, &operationNames, timeout)
		}

		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		poolActivity := &activities.PoolActivity{}
		project := "test-project"
		operationNames := map[string]bool{
			"operation-1": false,
			"operation-2": false,
		}

		operation1Complete := &hyperscalermodels.ComputeOperation{
			Name:     "operation-1",
			Status:   "DONE",
			Progress: int64(100),
		}

		// Second operation is in progress
		operation2InProgress := &hyperscalermodels.ComputeOperation{
			Name:     "operation-2",
			Status:   "RUNNING",
			Progress: int64(75),
		}

		// Set up activity mocks that may not be called due to timeout
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-1").Return(operation1Complete, nil)
		env.OnActivity(poolActivity.GetComputeOpStatus, mock.Anything, project, true, "operation-2").Return(operation2InProgress, nil)
		env.RegisterActivity(poolActivity.GetComputeOpStatus)

		// Execute the custom workflow with timeout
		env.ExecuteWorkflow(timeoutTestWorkflow, project, true, operationNames, 1*time.Minute)

		// The workflow should complete
		assert.True(t, env.IsWorkflowCompleted())
		workflowErr := env.GetWorkflowError()
		if workflowErr == nil {
			// Test passed - operations completed as expected
			assert.NoError(t, workflowErr)
		} else {
			assert.Contains(t, workflowErr.Error(), "timeout")
		}
	})
}
