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
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		Name:            "pool1",
		VLMConfig:       "{}",
		PoolCredentials: &datamodel.PoolCredentials{Password: "pw"},
		Account:         &datamodel.Account{Name: "test-account"},
	}

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

	preallocatedSvm := &datamodel.Svm{Name: "test-svm"}
	savedSvm := &datamodel.Svm{
		Name:       "test-svm",
		SvmDetails: &datamodel.SvmDetails{ExternalUUID: "ext-uuid"},
	}
	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(preallocatedSvm, nil)
	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(savedSvm, nil)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

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

// ParseVlmConfig fails before any state has been changed; neither the rollback
// nor MarkSvmAsErroredForCreation should fire.
func TestOCICreateSVMWorkflow_ParseVlmConfigFails(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig: "{}",
	}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), assert.AnError)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			t.Fatalf("CreateSvmInCreatingState should not run when ParseVlmConfig failed")
		}).
		Return((*datamodel.Svm)(nil), nil).Maybe()
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("rollback should not fire when no state has been changed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// CreateSvmInCreatingState fails before its rollback is registered; the rollback
// must NOT fire.
func TestOCICreateSVMWorkflow_CreateSvmInCreatingStateFails_NoRollback(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig: "{}",
	}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((*datamodel.Svm)(nil), assert.AnError)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("rollback should not fire when CreateSvmInCreatingState failed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
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
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig: "{}",
	}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Svm{Name: "test-svm"}, nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return((*vlm.CreateSVMResponse)(nil), assert.AnError)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreateSVMWorkflow_SaveSVMAndLifDataFails(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	registerOCICreateSVMVLMRollbackWorkflows(env)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
	}
	pool := &datamodel.Pool{
		BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig: "{}",
		Account:   &datamodel.Account{Name: "test-account"},
	}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Svm{Name: "test-svm"}, nil)
	env.OnActivity("MarkSvmAsErroredForCreation", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	mockVlm := vlm.NewMockVlmWorkflowClient(t)
	mockVlm.On("CreateVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.CreateSVMResponse{VLMConfig: vlm.VLMConfig{}}, nil)
	orig := workflows.GetNewVSAClientWorkflowManager
	workflows.GetNewVSAClientWorkflowManager = func() vlm.VlmWorkflowClient { return mockVlm }
	defer func() { workflows.GetNewVSAClientWorkflowManager = orig }()

	env.OnActivity("SaveSVMAndLifDataWithOCID", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((*datamodel.Svm)(nil), assert.AnError)

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

func TestOCICreateSVMWorkflow_PoolCredentialsFallback(t *testing.T) {
	env, _ := newSVMTestEnv(t)

	params := &common.CreateSvmParams{
		Name:                  "test-svm",
		AccountName:           "test-account",
		SvmExternalIdentifier: "ocid1.svm..a",
	}
	pool := &datamodel.Pool{
		BaseModel:       datamodel.BaseModel{UUID: "pool-uuid"},
		VLMConfig:       "{}",
		PoolCredentials: nil,
		Account:         &datamodel.Account{Name: "test-account"},
	}

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Svm{Name: "test-svm"}, nil)

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

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

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

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlm.VLMConfig{}, nil)
	env.OnActivity("CreateSvmInCreatingState", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&datamodel.Svm{Name: "test-svm"}, nil)
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

	env.ExecuteWorkflow(OCICreateSVMWorkflow, params, pool)

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
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// ParseVlmConfig fails before any state has been changed; the rollback must NOT fire
// and MarkSvmDeleting must not be invoked.
func TestOCIDeleteSVMWorkflow_ParseVlmConfigFails_NoRollback(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, _ := deleteSVMTestFixtures()

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return((*vlm.VLMConfig)(nil), assert.AnError)
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("MarkSvmDeleting should not run when ParseVlmConfig failed") }).
		Return(nil).Maybe()
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("rollback should not fire when no state has been changed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// MarkSvmDeleting fails before any state has been changed; the rollback must NOT fire.
func TestOCIDeleteSVMWorkflow_MarkSvmDeletingFails_NoRollback(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(assert.AnError)
	// MarkSvmAsErroredForDeletion must not be called: we never transitioned to DELETING.
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("rollback should not fire when MarkSvmDeleting failed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// VLM DeleteVSASVM fails after the DELETING transition; the rollback MUST fire so
// the SVM moves from DELETING to ERROR instead of being stranded.
func TestOCIDeleteSVMWorkflow_VlmDeleteFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Return((*vlm.DeleteSVMResponse)(nil), assert.AnError)

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { t.Fatalf("SoftDeleteSvm should not run when the VLM delete failed") }).
		Return(nil).Maybe()

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// SoftDeleteSvm fails after DELETING transition; the rollback MUST fire to move the
// SVM from DELETING to ERROR so it isn't stranded in a transitional state.
func TestOCIDeleteSVMWorkflow_SoftDeleteFails_RollbackMarksError(t *testing.T) {
	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()

	mockVlm := installMockVlmForDelete(t)
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.Anything).
		Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil)

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(assert.AnError)
	env.OnActivity("MarkSvmAsErroredForDeletion", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
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
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(nil)
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

// When pool.PoolCredentials is nil the workflow must fall back to the
// ociOntapAdminPassword env var.
func TestOCIDeleteSVMWorkflow_RequestShape_FallsBackToEnvPassword(t *testing.T) {
	origAdminPassword := ociOntapAdminPassword
	ociOntapAdminPassword = "env-admin-pw"
	t.Cleanup(func() { ociOntapAdminPassword = origAdminPassword })

	env, _ := newSVMTestEnv(t)
	params, svm, pool, vlmCfg := deleteSVMTestFixtures()
	pool.PoolCredentials = nil

	mockVlm := installMockVlmForDelete(t)
	var captured *vlm.DeleteSVMRequest
	mockVlm.On("DeleteVSASVM", mock.Anything, mock.MatchedBy(func(req *vlm.DeleteSVMRequest) bool {
		captured = req
		return true
	})).Return(&vlm.DeleteSVMResponse{VLMConfig: vlmCfg}, nil)

	env.OnActivity("ParseVlmConfig", mock.Anything, mock.Anything).Return(&vlmCfg, nil)
	env.OnActivity("MarkSvmDeleting", mock.Anything, mock.Anything).Return(nil)
	env.OnActivity("SoftDeleteSvm", mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(OCIDeleteSVMWorkflow, params, svm, pool)

	assert.True(t, env.IsWorkflowCompleted())
	assert.NoError(t, env.GetWorkflowError())
	if assert.NotNil(t, captured) {
		assert.Equal(t, "env-admin-pw", captured.OntapCredentials.AdminPassword)
	}
	env.AssertExpectations(t)
}

// ---------------------------------------------------------------------------
// buildCreateSVMResult
// ---------------------------------------------------------------------------

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
