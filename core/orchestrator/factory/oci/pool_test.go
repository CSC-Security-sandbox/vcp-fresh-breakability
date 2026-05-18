package oci

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"gorm.io/gorm"
)

// withOCIOntapAdminCreds overrides the env-package globals that the OCI factory
// reads and restores them on test cleanup. We override the package vars
// directly (rather than t.Setenv) because env.OCIOntapAdminUsername and
// env.NodePassword are captured once at process start; t.Setenv would not
// affect what the factory observes.
//
// Note: the OCI default (USERNAME_PWD) credential branch reads the cluster
// admin password from env.NodePassword (shared with GCP)
func withOCIOntapAdminCreds(t *testing.T, username, password string) {
	t.Helper()
	origUsername := env.OCIOntapAdminUsername
	origPassword := env.NodePassword
	t.Cleanup(func() {
		env.OCIOntapAdminUsername = origUsername
		env.NodePassword = origPassword
	})
	env.OCIOntapAdminUsername = username
	env.NodePassword = password
}

func TestPreparePool_OntapAdminCredentialsFromEnv(t *testing.T) {
	withOCIOntapAdminCreds(t, "svc-admin", "secret-pass")

	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	iops := int64(100)
	params := &commonparams.CreatePoolParams{
		Name: "pool1",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            &iops,
		},
	}
	pool := preparePool(params, acc, 0)
	assert.NotNil(t, pool.PoolCredentials)
	assert.Equal(t, "svc-admin", pool.PoolCredentials.Username)
	assert.Equal(t, "secret-pass", pool.PoolCredentials.Password)
}

func TestPreparePool_KmsConfigAndActiveDirectory(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "pw")
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	params := &commonparams.CreatePoolParams{
		Name: "pool1",
		KmsConfig: &models.KmsConfig{
			BaseModel: models.BaseModel{ID: 42},
		},
		ActiveDirectoryId: "ad-ext",
		ADExistsInVCP:     true,
		ActiveDirectory: &models.ActiveDirectory{
			BaseModel: models.BaseModel{ID: 99},
		},
	}
	pool := preparePool(params, acc, 0)
	assert.True(t, pool.KmsConfigID.Valid)
	assert.Equal(t, int64(42), pool.KmsConfigID.Int64)
	assert.True(t, pool.ActiveDirectoryID.Valid)
	assert.Equal(t, int64(99), pool.ActiveDirectoryID.Int64)
}

func TestPreparePool_UsesPreGeneratedDeploymentName(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "pw")
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	params := &commonparams.CreatePoolParams{
		Name:           "pool1",
		DeploymentName: "preset-deployment-name",
	}
	pool := preparePool(params, acc, 0)
	assert.Equal(t, "preset-deployment-name", pool.DeploymentName)
}

// withAuthType overrides env.AuthType for the duration of a test so we can
// exercise the non-default credential branches in preparePool without relying
// on VSA_AUTH_TYPE being set in the environment when the process started.
func withAuthType(t *testing.T, authType int) {
	t.Helper()
	orig := env.AuthType
	t.Cleanup(func() { env.AuthType = orig })
	env.AuthType = authType
}

// TestPreparePool_UsernamePwdSecMgrBranch verifies the env.USERNAME_PWD_SEC_MGR
// branch of preparePool: PoolCredentials must be populated with the deployment
// scoped SecretID, the configured admin username, and an empty password (the
// password lives in OCI Vault and is fetched at credential-creation time).
func TestPreparePool_UsernamePwdSecMgrBranch(t *testing.T) {
	withOCIOntapAdminCreds(t, "vault-admin", "ignored-when-secmgr")
	withAuthType(t, env.USERNAME_PWD_SEC_MGR)

	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	params := &commonparams.CreatePoolParams{
		Name:           "pool1",
		DeploymentName: "ocnv-deadbeefcafebabe",
	}

	pool := preparePool(params, acc, 0)

	require.NotNil(t, pool.PoolCredentials)
	assert.Equal(t, env.USERNAME_PWD_SEC_MGR, pool.PoolCredentials.AuthType)
	assert.Equal(t, "ocnv-deadbeefcafebabe-secret", pool.PoolCredentials.SecretID)
	assert.Empty(t, pool.PoolCredentials.CertificateID)
	assert.Empty(t, pool.PoolCredentials.Password,
		"password must be empty for sec-mgr branch; it is fetched from OCI Vault later")
	assert.Equal(t, "vault-admin", pool.PoolCredentials.Username)
}

// TestPreparePool_UserCertificateBranch verifies the env.USER_CERTIFICATE
// branch: both SecretID and CertificateID are derived from the deployment
// name, password/username are intentionally left empty (auth happens via the
// client cert), and AuthType is propagated to the pool record.
func TestPreparePool_UserCertificateBranch(t *testing.T) {
	withOCIOntapAdminCreds(t, "ignored-when-cert", "ignored-when-cert")
	withAuthType(t, env.USER_CERTIFICATE)

	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	params := &commonparams.CreatePoolParams{
		Name:           "pool1",
		DeploymentName: "ocnv-abc123",
	}

	pool := preparePool(params, acc, 0)

	require.NotNil(t, pool.PoolCredentials)
	assert.Equal(t, env.USER_CERTIFICATE, pool.PoolCredentials.AuthType)
	assert.Equal(t, "ocnv-abc123-secret", pool.PoolCredentials.SecretID)
	assert.Equal(t, "ocnv-abc123-cert", pool.PoolCredentials.CertificateID)
	assert.Empty(t, pool.PoolCredentials.Password)
	assert.Empty(t, pool.PoolCredentials.Username)
}

func TestPreparePool_OntapAdminDefaultsWhenEnvUnset(t *testing.T) {
	// "admin" is the env-package default for OCIOntapAdminUsername when
	// OCI_ONTAP_ADMIN_USERNAME is unset at process start; "" represents
	// VSA_NODE_PASSWORD being unset (env.NodePassword default). Override
	// directly so this test stays hermetic regardless of what was set when
	// the process launched.
	withOCIOntapAdminCreds(t, "admin", "")

	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acct"}
	iops := int64(100)
	params := &commonparams.CreatePoolParams{
		Name: "pool1",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            &iops,
		},
	}
	pool := preparePool(params, acc, 0)
	assert.Equal(t, "admin", pool.PoolCredentials.Username)
	assert.Equal(t, "", pool.PoolCredentials.Password)
}

// TestCreatePool_Integration uses a real in-memory database for integration testing
func TestCreatePool_Integration(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "Netapp1!")

	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	store, err := database.SetupStorageForTest(mockLogger)
	assert.NoError(t, err)
	err = database.ClearInMemoryDB(store.DB())
	assert.NoError(t, err)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	orch := &OCIOrchestrator{
		storage:  store,
		temporal: mockTemporal,
	}

	params := &commonparams.CreatePoolParams{
		AccountName:    "test-account",
		Name:           "test-pool",
		SizeInBytes:    1024 * 1024 * 1024, // 1 GiB
		VendorID:       "test-vendor-id",
		VendorSubNetID: "test-subnet",
		Region:         "us-central1",
		ServiceLevel:   "FLEX",
		QosType:        "auto",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(1024); return &v }(),
		},
		PrimaryZone: "us-central1-a",
	}

	// Mock the ExecuteWorkflow call to simulate successful workflow execution
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	result, jobID, err := orch.CreatePool(ctx, params)

	// Verify successful pool creation
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, params.Name, result.Name)
	assert.Equal(t, params.AccountName, result.AccountName)
}

func TestDeletePool_EmptyPoolOCID(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{AccountName: "acc", PoolOCID: ""})
	assert.Error(t, err)
}

func TestDeletePool_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(nil, gorm.ErrRecordNotFound)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestDeletePool_GetAccountNonNotFoundError(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(nil, errors.New("database unavailable"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Equal(t, "database unavailable", err.Error())
}

func TestDeletePool_GetPoolByNameError(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, errors.New("query failed"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Equal(t, "query failed", err.Error())
}

func TestDeletePool_ConflictWhileCreating(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{State: models.LifeCycleStateCreating},
	}, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestDeletePool_ConflictWhenAlreadyDeleting(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "p1",
			State:          models.LifeCycleStateDeleting,
			PoolAttributes: &datamodel.PoolAttributes{},
		},
	}, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	pool, wf, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err), "already-deleting must map to a conflict, not idempotent success")
	assert.Contains(t, err.Error(), "transition state")
	assert.Contains(t, err.Error(), models.LifeCycleStateDeleting,
		"error must surface the actual state (DELETING) so callers can disambiguate from other transitions")
	assert.Nil(t, pool, "no pool model is returned alongside a conflict error")
	assert.Equal(t, "", wf)
}

func TestDeletePool_TransitionalStateConflict(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{State: models.LifeCycleStateUpdating},
	}, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestDeletePool_DeletingPoolFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     models.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().DeletingPool(mock.Anything, mock.Anything).Return(errors.New("state transition failed"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "state transition failed")
}

func TestDeletePool_ExecuteWorkflowFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     models.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().DeletingPool(mock.Anything, mock.Anything).Return(nil)
	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal start failed"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "temporal start failed")
}

func TestDeletePool_PoolNotFound(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.DeletePool(ctx, &commonparams.DeletePoolParams{
		AccountName: "ocid1.compartment..x",
		PoolOCID:    "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
}

func TestCreatePool_GetOrCreateAccountFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "a").Return(nil, gorm.ErrRecordNotFound).Once()
	mockStorage.EXPECT().CreateAccount(mock.Anything, mock.Anything).Return(nil, errors.New("create failed")).Once()
	mockStorage.EXPECT().GetAccount(mock.Anything, "a").Return(nil, errors.New("still down")).Once()

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}

	params := &commonparams.CreatePoolParams{
		AccountName:    "a",
		Name:           "p",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorSubNetID: "s",
		PoolOCID:       "ocid1.pool.oc1..z",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(100); return &v }(),
		},
	}
	_, _, err := orch.CreatePool(ctx, params)
	assert.Error(t, err)
}

func TestCreatePool_CreatingPoolConflictPassesThrough(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "x")

	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	conflictErr := utilserrors.NewConflictErr("pool already exists")
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil).Once()
	mockStorage.EXPECT().CreatingPool(mock.Anything, mock.Anything).Return(nil, conflictErr).Once()

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	params := &commonparams.CreatePoolParams{
		AccountName:    "ocid1.compartment..x",
		Name:           "p",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorSubNetID: "s",
		PoolOCID:       "ocid1.pool.oc1..z",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(100); return &v }(),
		},
	}
	_, _, err := orch.CreatePool(ctx, params)
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestCreatePool_CreatingPoolVCPErrorWrappedConflictPassesThrough(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "x")

	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	wrapped := vsaerrors.NewVCPError(vsaerrors.ErrInputValidationError, utilserrors.NewConflictErr("pool already exists"))
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil).Once()
	mockStorage.EXPECT().CreatingPool(mock.Anything, mock.Anything).Return(nil, wrapped).Once()

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	params := &commonparams.CreatePoolParams{
		AccountName:    "ocid1.compartment..x",
		Name:           "p",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorSubNetID: "s",
		PoolOCID:       "ocid1.pool.oc1..z",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(100); return &v }(),
		},
	}
	_, _, err := orch.CreatePool(ctx, params)
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestCreatePool_CreatingPoolNonConflictReturnsError(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "x")

	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.compartment..x"}
	dbErr := errors.New("pq: internal insert failure")
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.compartment..x").Return(acc, nil).Once()
	mockStorage.EXPECT().CreatingPool(mock.Anything, mock.Anything).Return(nil, dbErr).Once()

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	params := &commonparams.CreatePoolParams{
		AccountName:    "ocid1.compartment..x",
		Name:           "p",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorSubNetID: "s",
		PoolOCID:       "ocid1.pool.oc1..z",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(100); return &v }(),
		},
	}
	_, _, err := orch.CreatePool(ctx, params)
	assert.Error(t, err)
	assert.False(t, utilserrors.IsConflictErr(err))
	assert.Equal(t, dbErr.Error(), err.Error())
}

// TestCreatePool_SucceedsWithoutVSAImageEnv documents that VSA / mediator image OCIDs are validated at OCI
// customer worker startup (see worker/main.go), not in CreatePool — oci-proxy need not set VSA_IMAGE_*.
func TestCreatePool_SucceedsWithoutVSAImageEnv(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "Netapp1!")
	t.Cleanup(func() {
		_ = os.Unsetenv("VSA_IMAGE_NAME")
		_ = os.Unsetenv("VSA_MEDIATOR_IMAGE_NAME")
	})
	_ = os.Unsetenv("VSA_IMAGE_NAME")
	_ = os.Unsetenv("VSA_MEDIATOR_IMAGE_NAME")

	ctx := context.Background()
	mockLogger := log.NewLogger()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, mockLogger)

	store, err := database.SetupStorageForTest(mockLogger)
	assert.NoError(t, err)
	assert.NoError(t, database.ClearInMemoryDB(store.DB()))

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: store, temporal: mockTemporal}
	params := &commonparams.CreatePoolParams{
		AccountName:    "test-account-no-vsa-env",
		Name:           "test-pool-no-vsa-env",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorID:       "test-vendor-no-vsa",
		VendorSubNetID: "test-subnet",
		Region:         "us-central1",
		ServiceLevel:   "FLEX",
		QosType:        "auto",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(1024); return &v }(),
		},
		PrimaryZone: "us-central1-a",
	}

	result, jobID, err := orch.CreatePool(ctx, params)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, jobID)
}

func TestCreatePool_WorkflowStartFails(t *testing.T) {
	withOCIOntapAdminCreds(t, "admin", "Netapp1!")

	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())

	store, err := database.SetupStorageForTest(log.NewLogger())
	assert.NoError(t, err)
	assert.NoError(t, database.ClearInMemoryDB(store.DB()))

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal unavailable"))

	orch := &OCIOrchestrator{storage: store, temporal: mockTemporal}
	params := &commonparams.CreatePoolParams{
		AccountName:    "test-account-wf",
		Name:           "test-pool-wf",
		SizeInBytes:    1024 * 1024 * 1024,
		VendorSubNetID: "test-subnet",
		PoolOCID:       "ocid1.pool.oc1..wf",
		CustomPerformanceParams: &commonparams.CustomPerformanceParams{
			ThroughputMibps: 64,
			Iops:            func() *int64 { v := int64(1024); return &v }(),
		},
		PrimaryZone: "ad1",
	}
	_, _, err = orch.CreatePool(ctx, params)
	assert.Error(t, err)
}

func TestGenerateDeploymentNameFromOCID(t *testing.T) {
	t.Run("empty string returns empty", func(t *testing.T) {
		assert.Equal(t, "", GenerateDeploymentNameFromOCID(""))
	})

	t.Run("known OCID produces deterministic name with prefix and fixed length", func(t *testing.T) {
		ocid := "ocid1.pool.oc1.eu-frankfurt-1.aaaaaaaapat5pvypcyr7xjb33om5j6howstzg2wfztjhbrlxcf2pz2wfen6a"
		got := GenerateDeploymentNameFromOCID(ocid)
		require.NotEmpty(t, got)
		assert.True(t, strings.HasPrefix(got, ociDeploymentPrefix), "got %q", got)
		assert.Len(t, got, len(ociDeploymentPrefix)+ociDeploymentHashLen)
		// Same input twice
		assert.Equal(t, got, GenerateDeploymentNameFromOCID(ocid))
	})

	t.Run("different OCIDs produce different names", func(t *testing.T) {
		a := GenerateDeploymentNameFromOCID("ocid1.pool.oc1..aaa")
		b := GenerateDeploymentNameFromOCID("ocid1.pool.oc1..bbb")
		assert.NotEqual(t, a, b)
	})

	t.Run("output only contains lowercase alphanumeric and hyphens", func(t *testing.T) {
		ocid := "ocid1.compartment.oc1..anything!@#UPPER"
		got := GenerateDeploymentNameFromOCID(ocid)
		for _, r := range got {
			assert.True(t,
				(r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-',
				"invalid char %q in %q", r, got)
		}
	})

	t.Run("output length does not exceed OCINameMaxLen", func(t *testing.T) {
		got := GenerateDeploymentNameFromOCID(strings.Repeat("x", 10000))
		assert.LessOrEqual(t, len(got), ociNameMaxLen)
	})

	t.Run("clamp truncates overly long deployment name", func(t *testing.T) {
		long := strings.Repeat("a", ociNameMaxLen+10)
		clamped := clampOCIDeploymentNameLength(long)
		assert.Len(t, clamped, ociNameMaxLen)
		assert.Equal(t, long[:ociNameMaxLen], clamped)
	})
}
