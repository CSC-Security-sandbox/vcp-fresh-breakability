package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	testVSAImageOCID      = "ocid1.image.oc1.iad.aaaaaaaaef2bc4g6vf4rvsa4vd2e4pnqw2ot2qicxrjo5a3ohglr6i4exdjq"
	testMediatorImageOCID = "ocid1.image.oc1.iad.aaaaaaaagakcrtyceuuvl6ts7xhqzzrdk3lv4z7tcqif3xpa6qsvppzflaaq"
)

// withVSAImageOCIDs sets package-level image OCIDs for the test (init-time env is not re-read; tests must assign).
func withVSAImageOCIDs(t *testing.T, vsa, mediator string) {
	t.Helper()
	origV, origM := vsaImageName, vsaMediatorImageName
	vsaImageName, vsaMediatorImageName = vsa, mediator
	t.Cleanup(func() {
		vsaImageName, vsaMediatorImageName = origV, origM
	})
}

func setTestOCIImageEnv(t *testing.T) {
	t.Helper()
	withVSAImageOCIDs(t, testVSAImageOCID, testMediatorImageOCID)
}

// setOCIExpertModePassword overrides the package-level ociExpertModePassword
// so the workflow skips the GetExpertModeCredentialsForOCI activity and
// uses the preset password directly. The original value is restored via t.Cleanup.
func setOCIExpertModePassword(t *testing.T, pw string) {
	t.Helper()
	orig := ociExpertModePassword
	ociExpertModePassword = pw
	t.Cleanup(func() { ociExpertModePassword = orig })
}

// registerOCICreatePoolVLMRollbackWorkflows registers the VLM delete child workflow used when OCICreatePoolWorkflow
// rolls back after CreateVSAClusterDeployment (or later steps) fail.
func registerOCICreatePoolVLMRollbackWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *vlm.DeleteVSAClusterDeploymentRequest, _ string) error {
			return nil
		},
		workflow.RegisterOptions{Name: vlm.DeleteVSAClusterDeploymentWorkflowName},
	)
}

func withOCIWorkerStartupEnv(t *testing.T, vsa, mediator, adminPassword, region, secret string) {
	t.Helper()
	origVsa, origMediator := vsaImageName, vsaMediatorImageName
	origAdminPassword, origRegion, origSecret := ociOntapAdminPassword, localRegion, secretURI
	vsaImageName = vsa
	vsaMediatorImageName = mediator
	ociOntapAdminPassword = adminPassword
	localRegion = region
	secretURI = secret
	t.Cleanup(func() {
		vsaImageName, vsaMediatorImageName = origVsa, origMediator
		ociOntapAdminPassword, localRegion, secretURI = origAdminPassword, origRegion, origSecret
	})
}

func TestValidateOCIWorkerStartupEnv(t *testing.T) {
	t.Run("ok when all required vars are present", func(t *testing.T) {
		withOCIWorkerStartupEnv(t, testVSAImageOCID, testMediatorImageOCID, "Netapp1!", "us-ashburn-1", "ocid1.vaultsecret.oc1..aaa")
		assert.NoError(t, ValidateOCIWorkerStartupEnv())
	})

	t.Run("fails and lists missing vars", func(t *testing.T) {
		withOCIWorkerStartupEnv(t, "", "", "", "", "")
		err := ValidateOCIWorkerStartupEnv()
		assert.Error(t, err)
		assert.True(t, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(t, err.Error(), "VSA_IMAGE_NAME")
		assert.Contains(t, err.Error(), "VSA_MEDIATOR_IMAGE_NAME")
		assert.Contains(t, err.Error(), "OCI_ONTAP_ADMIN_PASSWORD")
		assert.Contains(t, err.Error(), "LOCAL_REGION")
		assert.Contains(t, err.Error(), "SECRET_URI")
	})
}

func TestPrepareVLMConfig_CustomPerformanceAndSerialPrefix(t *testing.T) {
	setTestOCIImageEnv(t)
	iops := int64(5000)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
		SerialNumberPrefix: "99999",
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}
	cfg, err := prepareVLMConfig(params, pool)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	assert.Equal(t, int64(128), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, int64(5000), cfg.Deployment.SPConfig.IOps)
	assert.Equal(t, "99999", cfg.Deployment.SerialNumberPrefix)
}

func TestPrepareOCIDeleteVSAClusterDeploymentRequest(t *testing.T) {
	req := &vlm.DeleteVSAClusterDeploymentRequest{}
	pool := &datamodel.Pool{
		DeploymentName: "dep-1",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-from-pool"},
	}
	prepareOCIDeleteVSAClusterDeploymentRequest(req, pool, "tenancy-ocid")
	assert.Equal(t, vlm.OCICloud, req.CloudProvider)
	assert.Equal(t, "dep-1", req.DeploymentID)
	assert.Equal(t, "tenancy-ocid", req.ProjectID)
	require.NotNil(t, req.HyperScalerConfig)
	assert.Equal(t, "comp-from-pool", req.HyperScalerConfig.OCIConfig.CompartmentID)

	pool.ClusterDetails.CompartmentOCID = "comp-updated"
	prepareOCIDeleteVSAClusterDeploymentRequest(req, pool, "tenancy-2")
	assert.Equal(t, "comp-updated", req.HyperScalerConfig.OCIConfig.CompartmentID)
	assert.Equal(t, "tenancy-2", req.ProjectID)
}

func TestPrepareCreateVSAClusterDeploymentRequest_InitsNilLabels(t *testing.T) {
	setTestOCIImageEnv(t)
	req := &vlm.CreateVSAClusterDeploymentRequest{}
	vlmConfig := vlm.VLMConfig{
		Deployment: vlm.DeploymentConfig{
			Labels: nil,
		},
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "u1"},
		Name:      "pn",
		PoolOCID:  "ocid.pool",
		Account:   &datamodel.Account{Name: "aname"},
	}
	cred := vlm.OntapCredentials{}
	prepareCreateVSAClusterDeploymentRequest(req, vlmConfig, cred, pool)
	require.NotNil(t, req.VLMConfig.Deployment.Labels)
	assert.Equal(t, "pn", req.VLMConfig.Deployment.Labels["pool_name"])
	assert.Equal(t, "ocid.pool", req.VLMConfig.Deployment.Labels["pool_ocid"])
	assert.Equal(t, "aname", req.VLMConfig.Deployment.Labels["account_id"])
}

func TestOCIDeletePoolWorkflow_Success(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.DeletePoolParams{
		AccountName: "test-account",
		PoolID:      "pool-uuid-del",
	}

	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid-del"},
		Name:           "p",
		DeploymentName: "dep-ocnv-abc",
		ClusterDetails: datamodel.ClusterDetails{CompartmentOCID: "comp-ocid"},
		Account:        &datamodel.Account{Name: "test-account"},
	}

	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_Success(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024, // 1 TB
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Assert workflow execution
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_SetupError(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// Set up test data with invalid params to cause setup error
	params := &common.CreatePoolParams{
		Name:        "",
		AccountName: "",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Mock GetJob activity - return NEW state for workflow job (EnsureJobState)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	// Mock UpdateJob on storage (called by UpdateJobStatus activity)
	mockStorage.On("UpdateJob", mock.Anything, "default-test-workflow-id", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete (setup may succeed but workflow should handle it)
	assert.True(t, env.IsWorkflowCompleted())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_EnsureJobStateError(t *testing.T) {
	setTestOCIImageEnv(t)
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Mock GetJob activity to return ERROR state (should cause EnsureJobState to fail)
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateERROR),
	}, nil).Maybe()
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete with error
	assert.True(t, env.IsWorkflowCompleted())
	// Should have error because job is in ERROR state
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_UpdateJobStatusError(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Mock GetJob activity - return NEW state for workflow job
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Pool)(nil), assert.AnError)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Workflow should complete with error
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_RunMethodCalled(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	// Set up test data
	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Mock GetJob activity - return NEW state for workflow job
	env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
		BaseModel: datamodel.BaseModel{UUID: "test-workflow-id"},
		State:     string(models.JobsStateNEW),
	}, nil).Maybe()

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	// Assert workflow execution completed successfully
	// The Run method should be called and return nil, nil
	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_ExpertModePasswordFromEnv(t *testing.T) {
	setTestOCIImageEnv(t)

	// Simulate OCI_EXPERT_MODE_PASSWORD being set before the binary starts by directly
	// overriding the package-level var (same package, so accessible).
	orig := ociExpertModePassword
	ociExpertModePassword = "preset-env-password"
	defer func() { ociExpertModePassword = orig }()

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	// CreateVSAExpertModeUser is always called; the preset password is used directly
	// without executing the GetExpertModeCredentialsForOCI activity.
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// GetExpertModeCredentialsForOCI must NOT be called when the env var is pre-set.
	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), nil)
	env.OnActivity("CreatedPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_CreateExpertModeCredentialsFails(t *testing.T) {
	setTestOCIImageEnv(t)
	// Ensure ociExpertModePassword is empty so the workflow takes the
	// GetExpertModeCredentialsForOCI activity path.
	setOCIExpertModePassword(t, "")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
		OciAdminPassword: &common.OciAdminPassword{
			Ocid:    "ocid1.vaultsecret.oc1..testadminpw",
			Version: 1,
		},
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// Simulate the activity returning an error (e.g. OCI secret fetch failure).
	env.OnActivity("GetExpertModeCredentialsForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return((*vlm.OntapCredentials)(nil), assert.AnError)
	// Rollback path: ErroredPool is called; SaveVSANodeDetails and CreatedPool are never reached.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_CreateVSAExpertModeUserFails(t *testing.T) {
	setTestOCIImageEnv(t)
	setOCIExpertModePassword(t, "preset-test-password")

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	registerOCICreatePoolVLMRollbackWorkflows(env)

	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})

	params := &common.CreatePoolParams{
		Name:        "test-pool",
		AccountName: "test-account",
		SizeInBytes: 1024 * 1024 * 1024 * 1024,
		Region:      "us-ashburn-1",
		PrimaryZone: "us-ashburn-1-ad-1",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	mockVlmWorkflowClient := vlm.NewMockVlmWorkflowClient(t)
	mockVlmWorkflowClient.On("CreateVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.CreateVSAClusterDeploymentResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	// Password comes from env var, but expert-mode user creation fails.
	mockVlmWorkflowClient.On("CreateVSAExpertModeUser", mock.Anything, mock.Anything).
		Return(vlm.OntapExpertModeUserResponse{}, assert.AnError)
	origVSAClientFactory := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlmWorkflowClient }
	defer func() { workflows.GetNewVSAClientWorkflowManager = origVSAClientFactory }()

	// Rollback path: ErroredPool is called; SaveVSANodeDetails and CreatedPool are never reached.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}
