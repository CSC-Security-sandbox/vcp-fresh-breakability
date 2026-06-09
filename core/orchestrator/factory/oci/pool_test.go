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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/workflowquery"
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
		Pool: datamodel.Pool{State: datamodel.LifeCycleStateCreating},
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
			State:          datamodel.LifeCycleStateDeleting,
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
	assert.Contains(t, err.Error(), datamodel.LifeCycleStateDeleting,
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
		Pool: datamodel.Pool{State: datamodel.LifeCycleStateUpdating},
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
			State:     datamodel.LifeCycleStateREADY,
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
			State:     datamodel.LifeCycleStateREADY,
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

// TestUpdatePool_EmptyPoolExternalIdentifier pins the orchestrator's behavior
// when an empty PoolExternalIdentifier slips past the endpoint layer (which
// already rejects empty PoolOCID in oci-proxy/api/endpoints/pool_endpoints.go).
// The orchestrator no longer carries a redundant guard of its own; instead the
// empty identifier flows into GetPoolByName, which returns ErrRecordNotFound,
// and the factory maps that to a typed NotFoundErr. This proves the path
// degrades safely (no panic, no 500) rather than asserting on a duplicate
// 400-rejection that the endpoint layer already owns.
func TestUpdatePool_EmptyPoolExternalIdentifier(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "acc").Return(
		&datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "acc"}, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)
	orch := &OCIOrchestrator{
		storage:  mockStorage,
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{AccountName: "acc", PoolExternalIdentifier: ""})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestUpdatePool_AccountNotFound(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(nil, gorm.ErrRecordNotFound)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestUpdatePool_GetAccountGenericError(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(nil, errors.New("db down"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Equal(t, "db down", err.Error())
}

func TestUpdatePool_PoolNotFound(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestUpdatePool_PoolNotReady(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{State: datamodel.LifeCycleStateCreating},
	}, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
	assert.Contains(t, err.Error(), "already in progress")
	assert.Contains(t, err.Error(), "CREATING")
}

func TestUpdatePool_UpdatingPoolDbFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(nil, errors.New("state transition failed"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "state transition failed")
}

func TestUpdatePool_WorkflowFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, Name: "p1"}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)
	mockStorage.EXPECT().ErroredResource(mock.Anything, mock.Anything, mock.Anything).Return(pool, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal start failed"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "temporal start failed")
}

func TestUpdatePool_WorkflowFailsAndRollbackFails(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{BaseModel: datamodel.BaseModel{UUID: "pool-uuid"}, Name: "p1"}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)
	mockStorage.EXPECT().ErroredResource(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("rollback db error"))

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal unavailable"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "temporal unavailable")
}

func TestUpdatePool_GetPoolByNameGenericError(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, errors.New("query timeout"))
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.Equal(t, "query timeout", err.Error())
}

func TestUpdatePool_PoolInUpdatingState_Returns409(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateUpdating,
		},
	}, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
	assert.Contains(t, err.Error(), "UPDATING")
	assert.Contains(t, err.Error(), "already in progress")
}

func TestUpdatePool_PoolInDeletedState_Returns400(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{State: datamodel.LifeCycleStateDeleted},
	}, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.False(t, utilserrors.IsConflictErr(err))
	assert.Contains(t, err.Error(), "deleted")
	assert.Contains(t, err.Error(), "ocid1.pool.oc1..y")
}

func TestUpdatePool_PoolInDeletingState(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{State: datamodel.LifeCycleStateDeleting},
	}, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
	assert.Contains(t, err.Error(), "DELETING")
}

// TestUpdatePool_ClusterUpgradePending_Returns409 pins the cluster-upgrade guard:
// the pool's lifecycle is READY (so validateUpdatePoolState would accept it), but a
// cluster_upgrade_jobs row is still PENDING. UpdatePool must reject with 409 and
// must not call UpdatingPool or start a workflow. This catches the race where a
// long-running upgrade has been enqueued but the pool row hasn't transitioned yet.
func TestUpdatePool_ClusterUpgradePending_Returns409(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return(
		[]*datamodel.ClusterUpgradeJob{
			{
				BaseModel: datamodel.BaseModel{UUID: "job-pending-1"},
				ClusterID: "pool-uuid",
				PoolID:    "pool-uuid",
				Status:    string(models.UpgradeStatusPending),
			},
		}, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err), "PENDING upgrade must surface as 409")
	assert.Contains(t, err.Error(), "cluster upgrade is in progress")
	assert.Contains(t, err.Error(), "job-pending-1")
	assert.Contains(t, err.Error(), string(models.UpgradeStatusPending))
}

// TestUpdatePool_ClusterUpgradeInProgress_Returns409 covers the second active-upgrade
// status (IN_PROGRESS / "upgrading"). Same contract as the PENDING case: 409, no
// workflow started, and the conflict message names the offending job.
func TestUpdatePool_ClusterUpgradeInProgress_Returns409(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return(
		[]*datamodel.ClusterUpgradeJob{
			// A terminal-status job for the same pool must not mask the active one; the
			// guard must scan all rows and react to any active status it finds.
			{
				BaseModel: datamodel.BaseModel{UUID: "job-old-completed"},
				ClusterID: "pool-uuid",
				PoolID:    "pool-uuid",
				Status:    string(models.UpgradeStatusCompleted),
			},
			{
				BaseModel: datamodel.BaseModel{UUID: "job-active-1"},
				ClusterID: "pool-uuid",
				PoolID:    "pool-uuid",
				Status:    string(models.UpgradeStatusInProgress),
			},
		}, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err), "IN_PROGRESS upgrade must surface as 409")
	assert.Contains(t, err.Error(), "cluster upgrade is in progress")
	assert.Contains(t, err.Error(), "job-active-1")
	assert.Contains(t, err.Error(), string(models.UpgradeStatusInProgress))
}

// TestUpdatePool_ClusterUpgradeTerminalStatesDoNotBlock proves the guard only fires on
// active statuses. A pool whose only upgrade-job rows are COMPLETED/FAILED/CANCELLED
// must be allowed through to the next validator (and ultimately the workflow). This
// pins the contract that historical upgrade jobs do not permanently freeze a pool.
func TestUpdatePool_ClusterUpgradeTerminalStatesDoNotBlock(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			SizeInBytes:    1024 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return(
		[]*datamodel.ClusterUpgradeJob{
			{BaseModel: datamodel.BaseModel{UUID: "j1"}, ClusterID: "pool-uuid", PoolID: "pool-uuid", Status: string(models.UpgradeStatusCompleted)},
			{BaseModel: datamodel.BaseModel{UUID: "j2"}, ClusterID: "pool-uuid", PoolID: "pool-uuid", Status: string(models.UpgradeStatusFailed)},
			{BaseModel: datamodel.BaseModel{UUID: "j3"}, ClusterID: "pool-uuid", PoolID: "pool-uuid", Status: string(models.UpgradeStatusCancelled)},
		}, nil)
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		SizeInBytes:    1024 * 1024 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
	}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	result, workflowID, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   256,
	})
	assert.NoError(t, err, "only PENDING/IN_PROGRESS may block UpdatePool; terminal-status history must not")
	assert.NotNil(t, result)
	assert.NotEmpty(t, workflowID)
}

// TestUpdatePool_ClusterUpgradeLookupDbError_PropagatesError pins fail-closed behavior
// when the cluster_upgrade_jobs read itself fails: we must NOT silently bypass the
// guard. The error is surfaced as a VCP/database read error (the endpoint layer maps
// this to 5xx), so the caller retries against a healthy DB instead of running the
// update concurrently with a possibly-active upgrade.
func TestUpdatePool_ClusterUpgradeLookupDbError_PropagatesError(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{UUID: "pool-uuid"},
			Name:      "p1",
			State:     datamodel.LifeCycleStateREADY,
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").
		Return(nil, errors.New("db unreachable"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
	})
	assert.Error(t, err)
	var vcpErr *vsaerrors.CustomError
	require.True(t, errors.As(err, &vcpErr), "lookup failure must surface as a VCP error so the endpoint maps it to 5xx, not a silent bypass")
	require.NotNil(t, vcpErr.OriginalErr, "VCP error must wrap the underlying DB failure so ops can triage the root cause")
	assert.Contains(t, vcpErr.OriginalErr.Error(), "db unreachable", "underlying DB error must be preserved in the chain for ops triage")
}

func TestUpdatePool_AllowedFromErrorStateRetry(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateError,
			SizeInBytes:    1024 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		SizeInBytes:    1024 * 1024 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
	}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	result, workflowID, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		SizeInBytes:            2 * 1024 * 1024 * 1024 * 1024,
		TotalThroughputMibps:   256,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, workflowID)
	assert.Equal(t, "p1", result.Name)
}

func TestUpdatePool_HappyPathReturnsPoolAndWorkflowID(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			SizeInBytes:    1024 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		SizeInBytes:    1024 * 1024 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
	}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	result, workflowID, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		SizeInBytes:            2 * 1024 * 1024 * 1024 * 1024,
		TotalThroughputMibps:   256,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, workflowID)
	assert.Equal(t, "p1", result.Name)
}

func TestValidateUpdatePoolThroughput(t *testing.T) {
	// Cap derived from default OCI_THROUGHPUT_THRESHOLD_GBPS=5: int64(5 * 953.67431640625) = 4768 MiBps.
	// We pick test values around that boundary.
	t.Run("rejects zero throughput as non-zero", func(tt *testing.T) {
		err := validateUpdatePoolThroughput(&commonparams.UpdatePoolParams{TotalThroughputMibps: 0})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "non-zero")
	})

	t.Run("allows throughput strictly below the cap", func(tt *testing.T) {
		err := validateUpdatePoolThroughput(&commonparams.UpdatePoolParams{TotalThroughputMibps: 1})
		assert.NoError(tt, err)
		err = validateUpdatePoolThroughput(&commonparams.UpdatePoolParams{TotalThroughputMibps: 4767})
		assert.NoError(tt, err)
	})

	t.Run("rejects throughput equal to the cap", func(tt *testing.T) {
		capMibps := int64(float64(ociThroughputThresholdGBps) * workflowquery.MiBpsPerGBps)
		err := validateUpdatePoolThroughput(&commonparams.UpdatePoolParams{TotalThroughputMibps: capMibps})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "less than")
		assert.Contains(tt, err.Error(), "GBps")
	})

	t.Run("rejects throughput strictly above the cap", func(tt *testing.T) {
		capMibps := int64(float64(ociThroughputThresholdGBps) * workflowquery.MiBpsPerGBps)
		err := validateUpdatePoolThroughput(&commonparams.UpdatePoolParams{TotalThroughputMibps: capMibps + 1})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsBadRequestErr(err))
		assert.Contains(tt, err.Error(), "less than")
	})
}

// poolNodes builds the slice that GetNodesByPoolID returns for tests. The
// trimmed validator only reads Node.UUID for membership/coverage; we don't
// bother stamping Name or State.
func poolNodes(uuids ...string) []*datamodel.Node {
	out := make([]*datamodel.Node, 0, len(uuids))
	for _, u := range uuids {
		out = append(out, &datamodel.Node{BaseModel: datamodel.BaseModel{UUID: u}})
	}
	return out
}

// makeUpdatePool constructs the *datamodel.Pool fixture the validator reads.
// SizeInBytes is the canonical persisted pool size that the shrink guard
// compares against; PoolAttributes must be non-nil because the validator
// dereferences it for the IsRegionalHA mirror-halving branch.
func makeUpdatePool(id int64, ocid, poolUUID string, sizeBytes int64, isRegionalHA bool) *datamodel.Pool {
	return &datamodel.Pool{
		BaseModel:              datamodel.BaseModel{ID: id, UUID: poolUUID},
		PoolExternalIdentifier: ocid,
		SizeInBytes:            sizeBytes,
		PoolAttributes:         &datamodel.PoolAttributes{IsRegionalHA: isRegionalHA},
	}
}

// vcpErrorDetail returns the wrapped original error message for VCP internal errors.
// Customer-facing CustomError.Error() hides operational detail; tests assert on OriginalErr.
func vcpErrorDetail(err error) string {
	if ce := vsaerrors.ExtractCustomError(err); ce != nil && ce.OriginalErr != nil {
		return ce.OriginalErr.Error()
	}
	return err.Error()
}

// TestValidateUpdatePoolNodeCapacities covers the four behavioral rules of the
// trimmed validator: per-node max-cap, pool membership via GetNodesByPoolID,
// full-coverage (request must touch every node), and pool-level shrink guard.
// The validator no longer consults VLM config or HA-pair topology — the DB
// nodes table and pool.SizeInBytes are the only sources of truth.
func TestValidateUpdatePoolNodeCapacities(t *testing.T) {
	ctx := context.Background()
	const (
		poolID   = int64(99)
		poolOCID = "ocid1.pool.oc1..nc"
		poolUUID = "pool-uuid-nc"
	)

	t.Run("no nodeCapacities → no DB call, returns nil", func(tt *testing.T) {
		// Strict mock with zero EXPECTs proves the empty-NC path short-circuits
		// before any DB read.
		mockStorage := database.NewMockStorage(tt)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{})
		assert.NoError(tt, err)
	})

	t.Run("nil pool surfaces as internal error (caller invariant violation, not a 400)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage, nil, &commonparams.UpdatePoolParams{
			NodeCapacities: []commonparams.NodeCapacity{{NodeUUID: "uuid-1", SizeInGiB: 100}},
		})
		assert.Error(tt, err)
		assert.False(tt, utilserrors.IsUserInputValidationErr(err),
			"nil pool is a caller-side invariant violation, not a 400 user-input error")
		assert.Contains(tt, vcpErrorDetail(err), "pool is nil")
	})

	t.Run("rejects sizeInGiB above the configured per-node maximum (fail-fast, no DB call)", func(tt *testing.T) {
		// Strict mock with no GetNodesByPoolID expectation: if the validator
		// did its DB lookup before the cap check, GetNodesByPoolID would
		// trip the mock. This therefore pins both the rejection AND the
		// short-circuit ordering.
		mockStorage := database.NewMockStorage(tt)
		over := int64(ociNodeCapacityMaxTiB*1024) + 1
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: over},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "exceeds the configured per-node maximum")
		assert.Contains(tt, err.Error(), "uuid-1")
	})

	t.Run("DB read failure surfaces as an internal database error (not a 400)", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(nil, errors.New("connection refused"))
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{{NodeUUID: "uuid-1", SizeInGiB: 100}},
			})
		assert.Error(tt, err)
		assert.False(tt, utilserrors.IsUserInputValidationErr(err),
			"DB read failure is an internal 5xx, not a user-input 400")
		assert.Contains(tt, vcpErrorDetail(err), "list nodes for pool")
		assert.Contains(tt, vcpErrorDetail(err), "connection refused")
	})

	t.Run("rejects request that references a node_uuid not in the pool", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 100},
					{NodeUUID: "stranger", SizeInGiB: 100},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "references node_uuid(s) that are not part of pool")
		assert.Contains(tt, err.Error(), `"stranger"`)
	})

	t.Run("collects multiple unknown node_uuids into one 400", func(tt *testing.T) {
		// Single error covers the whole bad payload so the caller fixes
		// everything in one round trip rather than retrying field-by-field.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 100},
					{NodeUUID: "ghost-a", SizeInGiB: 100},
					{NodeUUID: "ghost-b", SizeInGiB: 100},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), `"ghost-a"`)
		assert.Contains(tt, err.Error(), `"ghost-b"`)
	})

	t.Run("rejects coverage mismatch when fewer entries than pool nodes", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 100},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "must cover every node in pool")
		assert.Contains(tt, err.Error(), "pool has 2 nodes")
		assert.Contains(tt, err.Error(), "request has 1 entries")
	})

	t.Run("rejects coverage mismatch when more entries than pool nodes", func(tt *testing.T) {
		// All entries reference real nodes (uuid-1/uuid-2) so membership
		// passes; the entry-count vs node-count check is the one that fires.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 100},
					{NodeUUID: "uuid-2", SizeInGiB: 100},
					{NodeUUID: "uuid-1", SizeInGiB: 100},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "must cover every node in pool")
		assert.Contains(tt, err.Error(), "pool has 2 nodes")
		assert.Contains(tt, err.Error(), "request has 3 entries")
	})

	t.Run("rejects pool shrink on non-HA pool: requested aggregate < current pool size", func(tt *testing.T) {
		// Current 1000 GiB; request 2 × 400 = 800 GiB. Non-HA counts every
		// per-node GiB toward the pool total, so 800 < 1000 is a shrink.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		currentBytes := int64(1000) * 1024 * 1024 * 1024
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, currentBytes, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 400},
					{NodeUUID: "uuid-2", SizeInGiB: 400},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "cannot shrink pool")
		assert.Contains(tt, err.Error(), "current size=1000 GiB")
		assert.Contains(tt, err.Error(), "requested size=800 GiB")
	})

	t.Run("rejects pool shrink on regional-HA pool: half-sum < current pool size", func(tt *testing.T) {
		// Regional HA mirrors across both zones, so only half of the
		// per-node sum contributes to pool capacity. Current 800 GiB,
		// per-node sum 1000, halved → requested 500 GiB < 800 → shrink.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		currentBytes := int64(800) * 1024 * 1024 * 1024
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, currentBytes, true),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 500},
					{NodeUUID: "uuid-2", SizeInGiB: 500},
				},
			})
		assert.Error(tt, err)
		assert.True(tt, utilserrors.IsUserInputValidationErr(err))
		assert.Contains(tt, err.Error(), "cannot shrink pool")
		assert.Contains(tt, err.Error(), "requested size=500 GiB")
	})

	t.Run("accepts grow on non-HA pool: requested aggregate > current pool size", func(tt *testing.T) {
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		currentBytes := int64(500) * 1024 * 1024 * 1024
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, currentBytes, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 600},
					{NodeUUID: "uuid-2", SizeInGiB: 600},
				},
			})
		assert.NoError(tt, err)
	})

	t.Run("accepts grow on regional-HA pool: half-sum > current pool size", func(tt *testing.T) {
		// Current 500 GiB; per-node sum 2000 GiB halved → 1000 GiB > 500 → grow.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		currentBytes := int64(500) * 1024 * 1024 * 1024
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, currentBytes, true),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 1000},
					{NodeUUID: "uuid-2", SizeInGiB: 1000},
				},
			})
		assert.NoError(tt, err)
	})

	t.Run("accepts equal-size update: requested aggregate == current pool size", func(tt *testing.T) {
		// Shrink guard is strictly less-than; matching sizes pass so callers
		// may resubmit the current capacity (e.g. throughput-only update).
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		currentBytes := int64(800) * 1024 * 1024 * 1024
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, currentBytes, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 400},
					{NodeUUID: "uuid-2", SizeInGiB: 400},
				},
			})
		assert.NoError(tt, err)
	})

	t.Run("zero current pool size is treated as unknown and never reads as shrink", func(tt *testing.T) {
		// pool.SizeInBytes == 0 (legacy/uninitialized row); the guard must
		// not falsely accuse the request of shrinking against zero.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: "uuid-1", SizeInGiB: 100},
					{NodeUUID: "uuid-2", SizeInGiB: 100},
				},
			})
		assert.NoError(tt, err)
	})

	t.Run("tolerates leading and trailing whitespace on node_uuid", func(tt *testing.T) {
		// The validator TrimSpace's nc.NodeUUID before the membership check
		// so " uuid-1 " is treated as "uuid-1". Pinning this guards against
		// a future refactor dropping the trim and starting to spuriously
		// reject payloads with stray whitespace.
		mockStorage := database.NewMockStorage(tt)
		mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, poolID).
			Return(poolNodes("uuid-1", "uuid-2"), nil)
		err := validateUpdatePoolNodeCapacities(ctx, mockStorage,
			makeUpdatePool(poolID, poolOCID, poolUUID, 0, false),
			&commonparams.UpdatePoolParams{
				NodeCapacities: []commonparams.NodeCapacity{
					{NodeUUID: " uuid-1 ", SizeInGiB: 100},
					{NodeUUID: "uuid-2", SizeInGiB: 100},
				},
			})
		assert.NoError(tt, err)
	})
}

// TestUpdatePool_NodeCapacityCoverageMismatchRejected drives UpdatePool end-to-end
// to prove the simplified validator's coverage rule reaches the wire: the pool has
// two nodes but the request only mentions one, and the factory must reject the
// request as a 400 before invoking any Temporal workflow.
func TestUpdatePool_NodeCapacityCoverageMismatchRejected(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())

	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 42, UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, int64(42)).Return(
		poolNodes("uuid-1", "uuid-2"), nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, _, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   128,
		NodeCapacities: []commonparams.NodeCapacity{
			{Name: "vm-1", NodeUUID: "uuid-1", SizeInGiB: 200},
		},
	})
	assert.Error(t, err)
	assert.True(t, utilserrors.IsUserInputValidationErr(err))
	assert.Contains(t, err.Error(), "must cover every node in pool")
	assert.Contains(t, err.Error(), "pool has 2 nodes")
	assert.Contains(t, err.Error(), "request has 1 entries")
}

// TestUpdatePool_NodeCapacitiesCollapseToPoolTotal pins the wire contract for the
// nodeCapacities → SizeInBytes collapse the factory performs after validation.
//
// Why this matters: regional-HA mirrors data across both zones, so only half of
// the per-node sum contributes to the pool-wide capacity. CREATE follows the same
// convention. If UPDATE summed across NODES instead of halving on regional-HA,
// the stored pool size would double-count the mirror and would not match what
// CREATE produced for the same physical pool.
//
// This test drives UpdatePool end-to-end with two homogeneous nodeCapacities
// entries (4096 GiB each) against a regional-HA pool and zero pool-level
// SizeInBytes, then asserts post-call params.SizeInBytes is the halved sum
// (4096 GiB), NOT the raw per-node sum (8192 GiB).
func TestUpdatePool_NodeCapacitiesCollapseToPoolTotal(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())

	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 42, UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			SizeInBytes:    200 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128, IsRegionalHA: true},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, int64(42)).Return(
		poolNodes("uuid-1", "uuid-2"), nil)
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(&datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 256, IsRegionalHA: true},
	}, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}

	params := &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   256,
		SizeInBytes:            0,
		NodeCapacities: []commonparams.NodeCapacity{
			{Name: "vm-1", NodeUUID: "uuid-1", SizeInGiB: 4096},
			{Name: "vm-2", NodeUUID: "uuid-2", SizeInGiB: 4096},
		},
	}

	_, _, err := orch.UpdatePool(ctx, params)
	require.NoError(t, err)

	wantBytes := uint64(4096) * 1024 * 1024 * 1024
	assert.Equal(t, wantBytes, params.SizeInBytes,
		"collapse must halve the per-node sum on regional-HA (mirror); summing per-node sizes (8192) would double-count the mirror")
}

// TestUpdatePool_NodeCapacitiesCollapseOverwritesStaleSizeInBytes pins the API
// invariant for the OCI UpdatePool flow: the wire deliberately omits a pool-level
// sizeInGiB/sizeInBytes field — nodeCapacities[] is the ONLY sizing input that
// crosses the network. params.SizeInBytes therefore arrives at the factory as zero
// on every real call, and the per-pair-sum derivation from nodeCapacities is the
// single source of truth.
//
// To keep that invariant tamper-proof against a future upstream refactor that
// might accidentally pre-populate params.SizeInBytes (stale value, leftover from
// create-path bridging, env-driven default, etc.), the collapse runs whenever
// NodeCapacities is non-empty AND overwrites whatever value was there. This test
// proves that contract: we feed a deliberately-wrong explicit SizeInBytes alongside
// valid nodeCapacities, and assert the post-call value matches the per-pair-sum
// derivation rather than the smuggled-in explicit value.
func TestUpdatePool_NodeCapacitiesCollapseOverwritesStaleSizeInBytes(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())

	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{ID: 42, UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			SizeInBytes:    200 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128, IsRegionalHA: true},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, int64(42)).Return(
		poolNodes("uuid-1", "uuid-2"), nil)
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(&datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 256, IsRegionalHA: true},
	}, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}

	// Deliberately wrong "stale" pool-total bytes that should NOT survive: simulates
	// what would happen if a future upstream refactor accidentally smuggled a value
	// into params.SizeInBytes (it normally arrives as zero from the OCI UpdatePool
	// wire). 7777 GiB is intentionally not the collapsed value (8192 GiB / 2 = 4096).
	const staleSmuggledBytes = uint64(7777) * 1024 * 1024 * 1024
	params := &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   256,
		SizeInBytes:            staleSmuggledBytes,
		NodeCapacities: []commonparams.NodeCapacity{
			{Name: "vm-1", NodeUUID: "uuid-1", SizeInGiB: 4096},
			{Name: "vm-2", NodeUUID: "uuid-2", SizeInGiB: 4096},
		},
	}

	_, _, err := orch.UpdatePool(ctx, params)
	require.NoError(t, err)

	const wantBytes = uint64(4096) * 1024 * 1024 * 1024
	assert.Equal(t, wantBytes, params.SizeInBytes,
		"collapse must OVERWRITE a smuggled SizeInBytes with the regional-HA half-sum derivation; the OCI UpdatePool wire does not carry a pool-total, so nodeCapacities is the sole source of truth")
	assert.NotEqual(t, staleSmuggledBytes, params.SizeInBytes,
		"smuggled SizeInBytes value must not survive the collapse")
}

func TestUpdatePool_QueriesByPoolExternalIdentifier(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 7}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.MatchedBy(func(conds [][]interface{}) bool {
		if len(conds) != 2 {
			return false
		}
		return conds[0][0] == "pool_external_identifier = ?" && conds[0][1] == "ocid1.pool.oc1..target" &&
			conds[1][0] == "account_id = ?" && conds[1][1] == int64(7)
	})).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "target-pool",
			State:          datamodel.LifeCycleStateREADY,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "target-pool",
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
	}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)
	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}

	result, workflowID, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..target",
		SizeInBytes:            2 * 1024 * 1024 * 1024 * 1024,
		TotalThroughputMibps:   128,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, workflowID)
}

func TestUpdatePool_HappyPathWithThroughput(t *testing.T) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "ocid1.tenancy..x"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "ocid1.tenancy..x").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
			Name:           "p1",
			State:          datamodel.LifeCycleStateREADY,
			SizeInBytes:    1024 * 1024 * 1024 * 1024,
			PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
		},
	}, nil)
	mockStorage.EXPECT().GetClusterUpgradeJobsByClusterID(mock.Anything, "pool-uuid").Return([]*datamodel.ClusterUpgradeJob{}, nil)
	pool := &datamodel.Pool{
		BaseModel:      datamodel.BaseModel{UUID: "pool-uuid"},
		Name:           "p1",
		SizeInBytes:    1024 * 1024 * 1024 * 1024,
		PoolAttributes: &datamodel.PoolAttributes{ThroughputMibps: 128},
	}
	mockStorage.EXPECT().UpdatingPool(mock.Anything, mock.Anything).Return(pool, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().
		ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything,
			mock.MatchedBy(func(p *commonparams.UpdatePoolParams) bool {
				return p != nil && p.TotalThroughputMibps == 256
			}),
			mock.Anything).
		Return(nil, nil)
	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}

	result, workflowID, err := orch.UpdatePool(ctx, &commonparams.UpdatePoolParams{
		AccountName:            "ocid1.tenancy..x",
		PoolExternalIdentifier: "ocid1.pool.oc1..y",
		TotalThroughputMibps:   256,
	})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.NotEmpty(t, workflowID)
	assert.Equal(t, "p1", result.Name)
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
