package oci

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/vlm"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newSVMTestEnv(t *testing.T) (*testsuite.TestWorkflowEnvironment, *database.MockStorage) {
	t.Helper()
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	mockStorage := database.NewMockStorage(t)
	env.RegisterActivity(&activities.CommonActivities{SE: mockStorage})
	env.RegisterActivity(&activities.PoolActivity{SE: mockStorage})
	env.RegisterActivity(&activities.SvmActivity{SE: mockStorage})
	return env, mockStorage
}

// registerOCICreateSVMVLMRollbackWorkflows registers a no-op stub for the VLM delete
// child workflow used when OCICreateSVMWorkflow rolls back after CreateVSASVM has
// succeeded. Required because the rollback names the workflow by string, and the
// Temporal test environment refuses to dispatch unregistered named workflows.
// The trailing string arg captures the error message the rollback manager appends.
func registerOCICreateSVMVLMRollbackWorkflows(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *vlm.DeleteSVMRequest, _ string) error {
			return nil
		},
		workflow.RegisterOptions{Name: vlm.DeleteVSASVMWorkflowName},
	)
}

// ---------------------------------------------------------------------------
// OCICreateSVMWorkflow
// ---------------------------------------------------------------------------

func TestOCICreateSVMWorkflow_Success(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		EnableIscsi:           true,
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		Name:            "pool1",
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	vlmCfg := vlm.VLMConfig{
		Svm: map[string]vlm.SvmConfig{
			"test-svm": {
				SVMLIFs: vlm.SvmLIFConfigs{
					vlm.LIFTypeSan: {
						{Name: "lif1", IP: "10.0.0.1/24", HomeNode: "node1"},
					},
				},
			},
		},
	}

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.CreateSVMResponse{VLMConfig: vlmCfg}, nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	savedSvm := &datamodel.Svm{
		Name:       "test-svm",
		SvmDetails: &datamodel.SvmDetails{ExternalUUID: "ext-uuid"},
	}
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "svm-admin-pw"}, nil)
	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(savedSvm, nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())

	var result OCICreateSVMResult
	assert.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, "test-svm", result.Name)
	assert.Equal(t, "ocid1.svm..a", result.SvmOCID)
	assert.Len(t, result.Lifs, 1)
	assert.Equal(t, "10.0.0.1", result.Lifs[0].IP)
	env.AssertExpectations(t)
}

// ParseVlmConfig fails after the factory pre-allocated the SVM in CREATING.
// Because the rollback is now registered up-front (before the first activity),
// MarkSvmAsErroredForCreation MUST fire so the row moves CREATING -> ERROR
// rather than being stranded.
func TestOCICreateSVMWorkflow_ParseVlmConfigFails(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), assert.AnError)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForCreation rollback must fire when ParseVlmConfig fails")
	env.AssertExpectations(t)
}

// VLM CreateVSASVM fails after the SVM was pre-allocated in CREATING; the DB
// rollback MUST fire so the row moves CREATING -> ERROR. The cluster delete
// rollback must NOT fire because the cluster SVM was never successfully created.
func TestOCICreateSVMWorkflow_CreateVSASVMFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "svm-admin-pw"}, nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return((*vlm.CreateSVMResponse)(nil), assert.AnError)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForCreation rollback must fire when CreateVSASVM fails")
	env.AssertExpectations(t)
}

func TestOCICreateSVMWorkflow_SaveSVMAndLifDataFails(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	registerOCICreateSVMVLMRollbackWorkflows(env)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "svm-admin-pw"}, nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.CreateSVMResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((*datamodel.Svm)(nil), assert.AnError)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForCreation rollback must fire when SaveSVMAndLifDataWithOCID fails")
	env.AssertExpectations(t)
}

func TestOCICreateSVMWorkflow_PoolCredentialsFallback(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "svm-admin-pw"}, nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.CreateSVMResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	savedSvm := &datamodel.Svm{
		Name:       "test-svm",
		SvmDetails: &datamodel.SvmDetails{ExternalUUID: "ext-uuid"},
	}
	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(savedSvm, nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// When a workflow step after CreateVSASVM fails, the rollback must invoke the VLM
// DeleteVSASVM child workflow with a request that carries the just-created SVM name,
// the VLMConfig returned by CreateVSASVM (the persisted pool config does not yet
// reflect the new SVM at rollback time), and the credentials used to create it.
func TestOCICreateSVMWorkflow_VlmDeleteRollbackFiresOnLaterFailure(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	// Register a stub for the rollback that records the request it received so the
	// test can assert the rollback was invoked with the expected payload.
	var captured *vlm.DeleteSVMRequest
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, req *vlm.DeleteSVMRequest, _ string) error {
			captured = req
			return nil
		},
		workflow.RegisterOptions{Name: vlm.DeleteVSASVMWorkflowName},
	)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}

	createdVlmCfg := vlm.VLMConfig{
		Svm: map[string]vlm.SvmConfig{"test-svm": {Svmname: "test-svm"}},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "svm-admin-pw"}, nil)
	// Both rollbacks must fire: cluster cleanup first (LIFO), then DB row -> ERROR.
	markErroredCalled := false
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.CreateSVMResponse{VLMConfig: createdVlmCfg}, nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	// Force the post-CreateVSASVM DB-persistence step to fail so the deferred
	// rollback runs. SaveSVMAndLifDataWithOCID is the only remaining step after
	// CreateVSASVM, so it is the only path that can exercise the rollback.
	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((*datamodel.Svm)(nil), assert.AnError)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	if assert.NotNil(t, captured, "VLM DeleteVSASVM rollback workflow must be invoked on failure") {
		assert.Equal(t, params.Name, captured.Name)
		assert.Equal(t, createdVlmCfg, captured.VLMConfig,
			"rollback must use VLMConfig returned by CreateVSASVM, not the pre-create config")
		assert.Equal(t, pool.PoolCredentials.Password, captured.OntapCredentials.AdminPassword)
	}
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForCreation rollback must fire so the DB row ends in ERROR")
	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// OCIDeleteSVMWorkflow
// ---------------------------------------------------------------------------

// deleteSVMTestFixtures returns the params/svm/pool/vlmConfig combo shared by the
// OCIDeleteSVMWorkflow test cases. Each test mutates only the activity / VLM mocks.
func deleteSVMTestFixtures() (*common.DeleteSvmParams, *datamodel.Svm, *datamodel.Pool, vlm.VLMConfig) {
	params := &common.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "test-account",
		PoolOCID:    "ocid1.pool..a",
	}
	svm := &datamodel.Svm{
		BaseModel:             datamodel.BaseModel{UUID: "svm-uuid"},
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "test-svm",
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		Name:            "pool1",
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
	}
	vlmCfg := vlm.VLMConfig{}
	return params, svm, pool, vlmCfg
}

// installMockVlmForDelete swaps in a mock VlmWorkflowClient for the duration of the
// test and returns it so callers can program expectations.
func installMockVlmForDelete(t *testing.T) *vlm.MockVlmWorkflowClient {
	t.Helper()
	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	t.Cleanup(func() { workflows.GetNewVSAClientWorkflowManager = orig })
	return mockVlm
}

func TestOCIDeleteSVMWorkflow_Success(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil)

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// ParseVlmConfig fails. The SVM is already in DELETING (set by the orchestrator
// before the workflow started), so the rollback MUST fire to move the row to
// ERROR — otherwise the SVM is stranded in DELETING with no workflow driving
// it.
func TestOCIDeleteSVMWorkflow_ParseVlmConfigFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, _ := deleteSVMTestFixtures()

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), assert.AnError)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForDeletion rollback must fire when ParseVlmConfig fails")
	env.AssertExpectations(t)
}

// VLM DeleteVSASVM fails; the rollback MUST fire so the SVM moves from
// DELETING to ERROR instead of being stranded.
func TestOCIDeleteSVMWorkflow_VlmDeleteFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Return((*vlm.DeleteSVMResponse)(nil), assert.AnError)

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("SoftDeleteSvm should not run when the VLM delete failed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForDeletion rollback must fire when VLM DeleteVSASVM fails")
	env.AssertExpectations(t)
}

// SoftDeleteSvm fails after the VLM delete; the rollback MUST fire to move the
// SVM from DELETING to ERROR so it isn't stranded in a transitional state.
func TestOCIDeleteSVMWorkflow_SoftDeleteFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil)

	markErroredCalled := false
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(assert.AnError)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { markErroredCalled = true }).
		Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	assert.True(t, markErroredCalled, "MarkSvmAsErroredForDeletion rollback must fire when SoftDeleteSvm fails")
	env.AssertExpectations(t)
}

// Asserts the DeleteSVMRequest carries the SVM name, the parsed VLM config, and
// the password from pool.PoolCredentials (the preferred source).
func TestOCIDeleteSVMWorkflow_RequestShape_UsesPoolCredentialsPassword(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, _ := deleteSVMTestFixtures()
	vlmCfg := vlm.VLMConfig{Svm: map[string]vlm.SvmConfig{"test-svm": {Svmname: "test-svm"}}}

	mockVlm := installMockVlmForDelete(t)
	var captured *vlm.DeleteSVMRequest
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.MatchedBy(func(req *vlm.DeleteSVMRequest) bool {
		captured = req
		return true
	})).Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil)

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	if assert.NotNil(t, captured) {
		assert.Equal(t, svm.Name, captured.Name)
		assert.Equal(t, vlmCfg, captured.VLMConfig)
		assert.Equal(t, pool.PoolCredentials.Password, captured.OntapCredentials.AdminPassword)
	}
	env.AssertExpectations(t)
}

func TestOCIDeleteSVMWorkflow_NilPoolCredentialsFailsBeforeVlmDelete(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()
	pool.PoolCredentials = nil

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("DeleteVSASVM should not run when pool credentials are missing") }).
		Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil).Maybe()

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("SoftDeleteSvm should not run when credential resolution failed") }).
		Return(nil).Maybe()
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// buildCreateSVMResult
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// OCICreateSVMWorkflow.Run argument validation
// ---------------------------------------------------------------------------

// Run() is invoked indirectly via the typed workflow entry point. Each branch
// below covers a defensive arg-validation path that returns early without
// running any activities, so the parent workflow surfaces a typed failure
// instead of a runtime panic.

func TestOCICreateSVMWorkflow_Run_RejectsTooFewArgs(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, args ...interface{}) (*OCICreateSVMResult, error) {
			wf := &struct {
				_ workflows.BaseWorkflow
			}{}
			_ = wf
			return invokeOCICreateRun(ctx, args...)
		},
		workflow.RegisterOptions{Name: "test-run-too-few"},
	)
	env.ExecuteWorkflow("test-run-too-few")
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestOCICreateSVMWorkflow_Run_RejectsBadArgTypes(t *testing.T) {
	// args[0] wrong type
	t.Run("args0 not *CreateSvmParams", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) (*OCICreateSVMResult, error) {
				return invokeOCICreateRun(ctx, "not-params", &datamodel.Pool{}, &datamodel.Svm{})
			},
			workflow.RegisterOptions{Name: "test-run-args0"},
		)
		env.ExecuteWorkflow("test-run-args0")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("args1 not *Pool", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) (*OCICreateSVMResult, error) {
				return invokeOCICreateRun(ctx,
					&common.CreateSvmParams{
						SvmAdminPassword: &common.OciAdminPassword{Ocid: "x"},
					},
					"not-a-pool",
					&datamodel.Svm{},
				)
			},
			workflow.RegisterOptions{Name: "test-run-args1"},
		)
		env.ExecuteWorkflow("test-run-args1")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("args2 not *Svm", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) (*OCICreateSVMResult, error) {
				return invokeOCICreateRun(ctx,
					&common.CreateSvmParams{
						SvmAdminPassword: &common.OciAdminPassword{Ocid: "x"},
					},
					&datamodel.Pool{Account: &datamodel.Account{Name: "a"}},
					"not-an-svm",
				)
			},
			workflow.RegisterOptions{Name: "test-run-args2"},
		)
		env.ExecuteWorkflow("test-run-args2")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
}

// pool.Account nil triggers the dedicated validation branch — distinct from
// the args-type checks above.
func TestOCICreateSVMWorkflow_Run_RejectsNilAccount(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) (*OCICreateSVMResult, error) {
			return invokeOCICreateRun(ctx,
				&common.CreateSvmParams{
					Name:             "svm",
					AccountName:      "acct",
					SvmAdminPassword: &common.OciAdminPassword{Ocid: "x"},
				},
				&datamodel.Pool{}, // Account: nil
				&datamodel.Svm{Name: "svm"},
			)
		},
		workflow.RegisterOptions{Name: "test-run-nil-account"},
	)
	env.ExecuteWorkflow("test-run-nil-account")
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// Missing admin password OCID triggers the dedicated validation branch.
func TestOCICreateSVMWorkflow_Run_RejectsEmptyAdminPasswordOCID(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	env.RegisterWorkflowWithOptions(
		func(ctx workflow.Context) (*OCICreateSVMResult, error) {
			return invokeOCICreateRun(ctx,
				&common.CreateSvmParams{
					Name:             "svm",
					AccountName:      "acct",
					SvmAdminPassword: &common.OciAdminPassword{Ocid: ""},
				},
				&datamodel.Pool{Account: &datamodel.Account{Name: "acct"}},
				&datamodel.Svm{Name: "svm"},
			)
		},
		workflow.RegisterOptions{Name: "test-run-empty-admin-pw"},
	)
	env.ExecuteWorkflow("test-run-empty-admin-pw")
	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// Helper to call Run on a fresh workflow instance. The workflow can't call its
// own Run from outside the workflow context, but a registered helper workflow
// is allowed to drive Run with arbitrary args[] to exercise validation branches.
func invokeOCICreateRun(ctx workflow.Context, args ...interface{}) (*OCICreateSVMResult, error) {
	wf := new(ociCreateSVMWorkflow)
	res, vsaErr := wf.Run(ctx, args...)
	if vsaErr != nil {
		return nil, vsaErr
	}
	r, _ := res.(*OCICreateSVMResult)
	return r, nil
}

// ParseVlmConfig returns nil vlmConfig with nil error -> nil-config validation
// branch in OCICreateSVMWorkflow.Run.
func TestOCICreateSVMWorkflow_NilVLMConfigFromActivity(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// GetSvmAdminPasswordSecretForOCI returns nil creds with nil error -> nil-creds
// validation branch.
func TestOCICreateSVMWorkflow_NilAdminCredsFromActivity(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return((*vlm.OntapCredentials)(nil), nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// CreateVSASVM returns nil response with nil error -> nil-response validation
// branch in the post-CreateVSASVM step.
func TestOCICreateSVMWorkflow_NilCreateVSASVMResponse(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pool-pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return(&vlm.OntapCredentials{AdminPassword: "pw"}, nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return((*vlm.CreateSVMResponse)(nil), nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// ---------------------------------------------------------------------------
// OCIDeleteSVMWorkflow.Run argument validation
// ---------------------------------------------------------------------------

func TestOCIDeleteSVMWorkflow_Run_RejectsBadArgs(t *testing.T) {
	t.Run("too few args", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) error {
				return invokeOCIDeleteRun(ctx) // zero args
			},
			workflow.RegisterOptions{Name: "test-del-run-too-few"},
		)
		env.ExecuteWorkflow("test-del-run-too-few")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("args0 not *DeleteSvmParams", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) error {
				return invokeOCIDeleteRun(ctx, "not-params", &datamodel.Svm{}, &datamodel.Pool{})
			},
			workflow.RegisterOptions{Name: "test-del-run-args0"},
		)
		env.ExecuteWorkflow("test-del-run-args0")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("args1 not *Svm", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) error {
				return invokeOCIDeleteRun(ctx, &common.DeleteSvmParams{}, "not-svm", &datamodel.Pool{})
			},
			workflow.RegisterOptions{Name: "test-del-run-args1"},
		)
		env.ExecuteWorkflow("test-del-run-args1")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("args2 not *Pool", func(tt *testing.T) {
		env, _ := newSVMTestEnv(tt)
		env.RegisterWorkflowWithOptions(
			func(ctx workflow.Context) error {
				return invokeOCIDeleteRun(ctx, &common.DeleteSvmParams{}, &datamodel.Svm{}, "not-pool")
			},
			workflow.RegisterOptions{Name: "test-del-run-args2"},
		)
		env.ExecuteWorkflow("test-del-run-args2")
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
}

func invokeOCIDeleteRun(ctx workflow.Context, args ...interface{}) error {
	wf := new(ociDeleteSVMWorkflow)
	_, vsaErr := wf.Run(ctx, args...)
	if vsaErr != nil {
		return vsaErr
	}
	return nil
}

// GetSvmAdminPasswordSecretForOCI returns an error -> the create workflow must
// propagate the activity error via ConvertToVSAError after the workflow has
// already pulled VLM config.
func TestOCICreateSVMWorkflow_GetSvmAdminPasswordFails(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
		SvmAdminPassword:      &common.OciAdminPassword{Ocid: "ocid1.vaultsecret..a", Version: 1},
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig: "{}",
		Account:   &datamodel.Account{Name: "test-account"},
	}
	preallocatedSvm := &datamodel.Svm{Name: "test-svm", SvmExternalIdentifier: "ocid1.svm..a"}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("GetSvmAdminPasswordSecretForOCI", mock.Anything, mock.Anything, mock.Anything).
		Return((*vlm.OntapCredentials)(nil), assert.AnError)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool, preallocatedSvm)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

// ParseVlmConfig returns nil vlmConfig with nil error in OCIDeleteSVMWorkflow.
func TestOCIDeleteSVMWorkflow_NilVLMConfigFromActivity(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, _ := deleteSVMTestFixtures()

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), nil)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestBuildCreateSVMResult_SvmNotInVlmConfig(t *testing.T) {
	params := &common.CreateSvmParams{Name: "missing-svm", SvmExternalIdentifier: "ocid1.svm..x"}
	svm := &datamodel.Svm{Name: "missing-svm"}
	vlmCfg := &vlm.VLMConfig{Svm: map[string]vlm.SvmConfig{}}

	result := buildCreateSVMResult(params, svm, vlmCfg)

	assert.Equal(t, "missing-svm", result.Name)
	assert.Equal(t, "ocid1.svm..x", result.SvmOCID)
	assert.Empty(t, result.Lifs)
}

func TestBuildCreateSVMResult_MultipleLifTypes(t *testing.T) {
	params := &common.CreateSvmParams{Name: "svm1", SvmExternalIdentifier: "ocid1.svm..a"}
	svm := &datamodel.Svm{Name: "svm1"}
	vlmCfg := &vlm.VLMConfig{
		Svm: map[string]vlm.SvmConfig{
			"svm1": {
				SVMLIFs: vlm.SvmLIFConfigs{
					vlm.LIFTypeSan: {
						{Name: "iscsi-lif1", IP: "10.0.0.1/24", HomeNode: "node1"},
					},
					vlm.LIFTypeNas: {
						{Name: "nas-lif1", IP: "10.0.1.1/24", HomeNode: "node2"},
					},
					vlm.LIFTypeIlbNas: {
						{Name: "ilb-nas-lif1", IP: "10.0.2.1/24", HomeNode: "node3"},
					},
				},
			},
		},
	}

	result := buildCreateSVMResult(params, svm, vlmCfg)

	assert.Equal(t, "svm1", result.Name)
	// Only SAN and NAS LIF types are externally exposed; ILB NAS is intentionally
	// filtered out by lifTypeToProtocols and must not appear in the result.
	assert.Len(t, result.Lifs, 2)

	foundNas := false
	foundSan := false
	for _, lif := range result.Lifs {
		assert.NotContains(t, lif.IP, "/")
		assert.NotEqual(t, "ilb-nas-lif1", lif.Name, "ILB NAS LIF must not be exposed in the result")
		switch lif.Name {
		case "nas-lif1":
			assert.ElementsMatch(t, []string{"nfs", "cifs", "s3"}, lif.Protocols)
			foundNas = true
		case "iscsi-lif1":
			assert.ElementsMatch(t, []string{"iscsi", "nvme"}, lif.Protocols)
			foundSan = true
		}
	}
	assert.True(t, foundNas, "NAS LIF should be mapped to nfs+cifs+s3 protocols")
	assert.True(t, foundSan, "SAN LIF should be mapped to iscsi+nvme protocols")
}
