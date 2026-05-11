package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
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

func TestPrepareVLMConfig_CustomPerformanceAndFixedSerialPrefix(t *testing.T) {
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
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
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

	// SerialNumberPrefix is the concatenation of ociSerialNumberLeadingPrefix ("955")
	// and the 15-zero ociSerialNumberPrefix const, yielding 18 characters total.
	assert.Equal(t, ociSerialNumberLeadingPrefix+ociSerialNumberPrefix, cfg.Deployment.SerialNumberPrefix,
		"SerialNumberPrefix must equal ociSerialNumberLeadingPrefix + ociSerialNumberPrefix")
	assert.Equal(t, "955000000000000000", cfg.Deployment.SerialNumberPrefix,
		"SerialNumberPrefix must be the literal 18-char fixed value")
	assert.Len(t, cfg.Deployment.SerialNumberPrefix, 18,
		"SerialNumberPrefix must be exactly 18 characters")
	assert.Len(t, ociSerialNumberPrefix, 15,
		"ociSerialNumberPrefix const must be exactly 15 zero digits")
}

func TestPrepareVLMConfig_DerivesIopsFromThroughputWhenNil(t *testing.T) {
	setTestOCIImageEnv(t)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            nil, // derived by the validator
		},
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

	const expectedDerivedIOPS = int64(2048)
	assert.Equal(t, int64(128), cfg.Deployment.SPConfig.Throughput)
	assert.Equal(t, expectedDerivedIOPS, cfg.Deployment.SPConfig.IOps,
		"SPConfig.IOps must equal the validator-derived IOPS (ThroughputMibps * IopsPerMiBps)")
}

func TestPrepareVLMConfig_ReturnsErrorWhenIopsValidationFails(t *testing.T) {
	setTestOCIImageEnv(t)
	belowMin := int64(100) // below MinCustomIops (1024)
	params := &common.CreatePoolParams{
		AccountName:     "acct",
		SizeInBytes:     100 * 1024 * 1024 * 1024,
		PrimaryZone:     "ad1",
		SecondaryZone:   "ad2",
		MediatorZone:    "ad3",
		VendorSubNetID:  "subnet",
		CompartmentOCID: "comp",
		HAPairs:         1,
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &belowMin,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}

	cfg, err := prepareVLMConfig(params, pool)
	assert.Error(t, err, "validator must reject IOPS below MinCustomIops")
	assert.Nil(t, cfg)
	assert.Contains(t, err.Error(), "derive iops from throughput")
}

func TestPrepareVLMConfig_RejectsZeroHAPairs(t *testing.T) {
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
		// HAPairs intentionally omitted (zero value).
		CustomPerformanceParams: &common.CustomPerformanceParams{
			ThroughputMibps: 128,
			Iops:            &iops,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "u1"},
		DeploymentName: "dep1",
		Name:           "pool1",
		Account:        &datamodel.Account{Name: "acct"},
	}

	cfg, err := prepareVLMConfig(params, pool)
	require.Error(t, err, "zero HAPairs must be rejected")
	assert.Nil(t, cfg)
	assert.True(t, utilserrors.IsUserInputValidationErr(err),
		"error must be a UserInputValidationErr so it surfaces as a 4xx, not a 5xx")
	assert.Contains(t, err.Error(), "haPairs",
		"error message must mention the offending field")
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

func TestOCIDeletePoolWorkflow_VLMDeleteFailure(t *testing.T) {
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

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestOCIDeletePoolWorkflow_DBCleanupFailure(t *testing.T) {
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

	env.OnActivity("DeletePoolResources", mock.Anything, mock.Anything).Return(nil, assert.AnError)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("DeleteVSAClusterDeployment", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCIDeletePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
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
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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

func TestOCICreatePoolWorkflow_SaveVSANodeDetailsFailure(t *testing.T) {
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
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
	}

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

	env.OnActivity("SaveVSANodeDetails", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return((*datamodel.Node)(nil), assert.AnError)
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

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
		HAPairs:     1,
	}

	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{
			UUID: "test-pool-uuid",
		},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		HAPairs:     1,
		OciAdminPassword: &common.OciAdminPassword{
			Ocid:    "ocid1.vaultsecret.oc1..testadminpw",
			Version: 1,
		},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:            "test-pool",
		AccountID:       12345,
		VendorID:        "test-vendor",
		Account:         &datamodel.Account{Name: "test-account"},
		PoolCredentials: &datamodel.PoolCredentials{Password: "test-pool-password"},
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

func TestOCICreatePoolWorkflow_NilPoolCredentialsRejected(t *testing.T) {
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
		HAPairs:     1,
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "test-pool-uuid"},
		Name:      "test-pool",
		AccountID: 12345,
		VendorID:  "test-vendor",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	// Rollback fires when Run returns an error.
	env.OnActivity("ErroredPool", mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	env.ExecuteWorkflow(OCICreatePoolWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pool credentials are required",
		"workflow should fail with the new pool-credentials guard, not some downstream error")
	env.AssertExpectations(t)
}

func TestOCICreatePoolWorkflow_RunArgsValidation(t *testing.T) {
	validParams := &common.CreatePoolParams{Name: "p", AccountName: "a", HAPairs: 1}
	validPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "u"}, Name: "p"}

	cases := []struct {
		name             string
		args             []interface{}
		wantOriginalSubs string // substring expected in OriginalErr.Error()
		wantTrackingID   int    // 0 = don't assert; otherwise must match
	}{
		{
			name:             "ZeroArgs",
			args:             nil,
			wantOriginalSubs: "expected 2 args, got 0",
		},
		{
			name:             "OneArg",
			args:             []interface{}{validParams},
			wantOriginalSubs: "expected 2 args, got 1",
		},
		{
			name:             "Args0WrongType",
			args:             []interface{}{"not-params", validPool},
			wantOriginalSubs: "args[0] has unexpected type string",
		},
		{
			name:             "Args0TypedNil",
			args:             []interface{}{(*common.CreatePoolParams)(nil), validPool},
			wantOriginalSubs: "args[0] (*common.CreatePoolParams) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
		{
			name:             "Args1WrongType",
			args:             []interface{}{validParams, "not-pool"},
			wantOriginalSubs: "args[1] has unexpected type string",
		},
		{
			name:             "Args1TypedNil",
			args:             []interface{}{validParams, (*datamodel.Pool)(nil)},
			wantOriginalSubs: "args[1] (*datamodel.Pool) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			wf := &ociCreatePoolWorkflow{}
			out, customErr := wf.Run(nil, tc.args...)

			assert.Nil(tt, out, "validation failure should not return a payload")
			require.NotNil(tt, customErr, "expected a *vsaerrors.CustomError, got nil")
			require.NotNil(tt, customErr.OriginalErr, "OriginalErr should preserve the descriptive validation message")
			assert.Contains(tt, customErr.OriginalErr.Error(), tc.wantOriginalSubs)
			if tc.wantTrackingID != 0 {
				assert.Equal(tt, tc.wantTrackingID, customErr.TrackingID,
					"typed-nil cases should be classified as ErrResourceEmptyError so they aren't lumped with generic internal errors")
			}
		})
	}
}

// TestOCIDeletePoolWorkflow_RunArgsValidation mirrors the Create-side coverage
// for (*ociDeletePoolWorkflow).Run. Same validation block, same rationale —
// keeping the two workflows in lock-step for reviewers.
func TestOCIDeletePoolWorkflow_RunArgsValidation(t *testing.T) {
	validParams := &common.DeletePoolParams{}
	validPool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "u"}, Name: "p"}

	cases := []struct {
		name             string
		args             []interface{}
		wantOriginalSubs string
		wantTrackingID   int
	}{
		{
			name:             "ZeroArgs",
			args:             nil,
			wantOriginalSubs: "expected 2 args, got 0",
		},
		{
			name:             "OneArg",
			args:             []interface{}{validParams},
			wantOriginalSubs: "expected 2 args, got 1",
		},
		{
			name:             "Args0WrongType",
			args:             []interface{}{"not-params", validPool},
			wantOriginalSubs: "args[0] has unexpected type string",
		},
		{
			name:             "Args0TypedNil",
			args:             []interface{}{(*common.DeletePoolParams)(nil), validPool},
			wantOriginalSubs: "args[0] (*common.DeletePoolParams) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
		{
			name:             "Args1WrongType",
			args:             []interface{}{validParams, "not-pool"},
			wantOriginalSubs: "args[1] has unexpected type string",
		},
		{
			name:             "Args1TypedNil",
			args:             []interface{}{validParams, (*datamodel.Pool)(nil)},
			wantOriginalSubs: "args[1] (*datamodel.Pool) must not be nil",
			wantTrackingID:   vsaerrors.ErrResourceEmptyError,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(tt *testing.T) {
			wf := &ociDeletePoolWorkflow{}
			out, customErr := wf.Run(nil, tc.args...)

			assert.Nil(tt, out, "validation failure should not return a payload")
			require.NotNil(tt, customErr, "expected a *vsaerrors.CustomError, got nil")
			require.NotNil(tt, customErr.OriginalErr, "OriginalErr should preserve the descriptive validation message")
			assert.Contains(tt, customErr.OriginalErr.Error(), tc.wantOriginalSubs)
			if tc.wantTrackingID != 0 {
				assert.Equal(tt, tc.wantTrackingID, customErr.TrackingID,
					"typed-nil cases should be classified as ErrResourceEmptyError so they aren't lumped with generic internal errors")
			}
		})
	}
}
