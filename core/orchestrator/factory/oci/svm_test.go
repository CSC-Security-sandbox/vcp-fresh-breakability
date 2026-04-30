package oci

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	utilserrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowenginemock "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"gorm.io/gorm"
)

func svmTestCtx() context.Context {
	ctx := context.Background()
	return context.WithValue(ctx, middleware.ContextSLoggerKey, log.NewLogger())
}

// ---------------------------------------------------------------------------
// CreateSvm
// ---------------------------------------------------------------------------

func TestCreateSvm_EmptyPoolOCID(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.Contains(t, err.Error(), "PoolOCID")
}

func TestCreateSvm_EmptySvmOCID(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.Contains(t, err.Error(), "SvmOCID")
}

func TestCreateSvm_EmptyName(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.Contains(t, err.Error(), "Name")
}

func TestCreateSvm_EmptyAccountName(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "   ",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
	assert.Contains(t, err.Error(), "Tenancy-Ocid")
}

func TestCreateSvm_GetOrCreateAccountFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(nil, gorm.ErrRecordNotFound)
	mockStorage.EXPECT().CreateAccount(mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(nil, errors.New("still down"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
}

func TestCreateSvm_DefaultIPSpaceAndProtocol(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	params := &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	}
	_, _ = orch.CreateSvm(svmTestCtx(), params)

	assert.Equal(t, "Default", params.IPSpace)
	assert.True(t, params.EnableIscsi)
}

func TestCreateSvm_PoolNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestCreateSvm_GetPoolByNameGenericError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, errors.New("db error"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.Equal(t, "db error", err.Error())
}

func TestCreateSvm_GetSvmByExternalIdentifierError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).
		Return(nil, errors.New("lookup failed"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookup failed")
}

func TestCreateSvm_DuplicateSvmOCID(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	existingSvm := &datamodel.Svm{SvmExternalIdentifier: "ocid1.svm..a"}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).
		Return(existingSvm, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestCreateSvm_NotFoundProceedsWithCreate(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 10},
			AccountID: 1,
			State:     string(models.LifeCycleStateREADY),
			VLMConfig: "cfg",
		},
	}
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(poolView, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).
		Return(nil, utilserrors.NewNotFoundErr("svm", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)
	mockStorage.EXPECT().GetSvmByNameAndPoolID(mock.Anything, "svm1", int64(10)).
		Return(nil, utilserrors.NewNotFoundErr("not found", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	wfID, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..a",
		Name:                  "svm1",
		AccountName:           "tenancy",
		EnableIscsi:           true,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, wfID)
}

func TestCreateSvm_ValidationFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	// Pool not in READY state triggers validation failure
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 10},
			AccountID: 1,
			State:     "CREATING",
			VLMConfig: "cfg",
		},
	}
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(poolView, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..new", int64(1)).
		Return(nil, utilserrors.NewNotFoundErr("svm", nil))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..new",
		Name:                  "svm1",
		AccountName:           "tenancy",
		EnableIscsi:           true,
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestCreateSvm_WorkflowStartFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 10},
			AccountID: 1,
			State:     string(models.LifeCycleStateREADY),
			VLMConfig: "cfg",
		},
	}
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(poolView, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..new", int64(1)).
		Return(nil, utilserrors.NewNotFoundErr("svm", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)
	mockStorage.EXPECT().GetSvmByNameAndPoolID(mock.Anything, "svm1", int64(10)).
		Return(nil, utilserrors.NewNotFoundErr("not found", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal unavailable"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	_, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..new",
		Name:                  "svm1",
		AccountName:           "tenancy",
		EnableIscsi:           true,
	})
	require.Error(t, err)
}

func TestCreateSvm_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	poolView := &datamodel.PoolView{
		Pool: datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 10, UUID: "pool-uuid"},
			AccountID: 1,
			State:     string(models.LifeCycleStateREADY),
			VLMConfig: "cfg",
		},
	}
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(poolView, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..new", int64(1)).
		Return(nil, utilserrors.NewNotFoundErr("svm", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)
	mockStorage.EXPECT().GetSvmByNameAndPoolID(mock.Anything, "svm1", int64(10)).
		Return(nil, utilserrors.NewNotFoundErr("not found", nil))
	mockStorage.EXPECT().GetNodesByPoolID(mock.Anything, mock.Anything).Return([]*datamodel.Node{
		{BaseModel: datamodel.BaseModel{ID: 1}, Name: "n1", State: models.LifeCycleStateREADY},
		{BaseModel: datamodel.BaseModel{ID: 2}, Name: "n2", State: models.LifeCycleStateREADY},
	}, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	wfID, err := orch.CreateSvm(svmTestCtx(), &commonparams.CreateSvmParams{
		PoolOCID:              "ocid1.pool..a",
		SvmExternalIdentifier: "ocid1.svm..new",
		Name:                  "svm1",
		AccountName:           "tenancy",
		EnableIscsi:           true,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, wfID)
}

// ---------------------------------------------------------------------------
// DeleteSvm
// ---------------------------------------------------------------------------

func TestDeleteSvm_EmptySvmID(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
}

func TestDeleteSvm_EmptyAccountName(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
}

func TestDeleteSvm_EmptyPoolOCID(t *testing.T) {
	orch := &OCIOrchestrator{
		storage:  database.NewMockStorage(t),
		temporal: workflowenginemock.NewMockTemporalTestClient(t),
	}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "  ",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsBadRequestErr(err))
}

func TestDeleteSvm_GetOrCreateAccountFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(nil, gorm.ErrRecordNotFound)
	mockStorage.EXPECT().CreateAccount(mock.Anything, mock.Anything).Return(nil, errors.New("create failed"))
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(nil, errors.New("still down"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
}

func TestDeleteSvm_PoolNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, gorm.ErrRecordNotFound)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestDeleteSvm_GetPoolGenericError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(nil, errors.New("db error"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.Equal(t, "db error", err.Error())
}

func TestDeleteSvm_GetSvmByExternalIdentifierError(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).
		Return(nil, errors.New("lookup failed"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "lookup failed")
}

func TestDeleteSvm_SvmNotFound(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).
		Return(nil, utilserrors.NewNotFoundErr("svm", nil))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestDeleteSvm_SvmBelongsToDifferentPool(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	svm := &datamodel.Svm{SvmExternalIdentifier: "ocid1.svm..a", PoolID: 99}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).Return(svm, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsNotFoundErr(err))
}

func TestDeleteSvm_ConflictWhenDeleting(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	svm := &datamodel.Svm{
		SvmExternalIdentifier: "ocid1.svm..a",
		PoolID:                10,
		State:                 string(models.LifeCycleStateDeleting),
	}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).Return(svm, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestDeleteSvm_ConflictWhenCreating(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	svm := &datamodel.Svm{
		SvmExternalIdentifier: "ocid1.svm..a",
		PoolID:                10,
		State:                 string(models.LifeCycleStateCreating),
	}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).Return(svm, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: workflowenginemock.NewMockTemporalTestClient(t)}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
	assert.True(t, utilserrors.IsConflictErr(err))
}

func TestDeleteSvm_WorkflowStartFails(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	svm := &datamodel.Svm{
		BaseModel:             datamodel.BaseModel{UUID: "svm-uuid"},
		SvmExternalIdentifier: "ocid1.svm..a",
		PoolID:                10,
		State:                 string(models.LifeCycleStateREADY),
		Name:                  "svm1",
	}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).Return(svm, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, errors.New("temporal down"))

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	_, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	require.Error(t, err)
}

func TestDeleteSvm_Success(t *testing.T) {
	mockStorage := database.NewMockStorage(t)
	acc := &datamodel.Account{BaseModel: datamodel.BaseModel{ID: 1}, Name: "tenancy"}
	mockStorage.EXPECT().GetAccount(mock.Anything, "tenancy").Return(acc, nil)
	mockStorage.EXPECT().GetPoolByName(mock.Anything, mock.Anything).Return(&datamodel.PoolView{
		Pool: datamodel.Pool{BaseModel: datamodel.BaseModel{ID: 10}, AccountID: 1},
	}, nil)
	svm := &datamodel.Svm{
		BaseModel:             datamodel.BaseModel{UUID: "svm-uuid"},
		SvmExternalIdentifier: "ocid1.svm..a",
		PoolID:                10,
		State:                 string(models.LifeCycleStateREADY),
		Name:                  "svm1",
	}
	mockStorage.EXPECT().GetSvmByExternalIdentifier(mock.Anything, "ocid1.svm..a", int64(1)).Return(svm, nil)

	mockTemporal := workflowenginemock.NewMockTemporalTestClient(t)
	mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nil)

	orch := &OCIOrchestrator{storage: mockStorage, temporal: mockTemporal}
	wfID, err := orch.DeleteSvm(svmTestCtx(), &commonparams.DeleteSvmParams{
		SvmID:       "ocid1.svm..a",
		AccountName: "tenancy",
		PoolOCID:    "ocid1.pool..a",
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, wfID)
}
