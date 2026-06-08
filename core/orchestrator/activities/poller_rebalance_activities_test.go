package activities

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"go.temporal.io/sdk/testsuite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestDeleteHarvestLeaseConfigFolder_EmptyLeaseNoOp(t *testing.T) {
	err := deleteHarvestLeaseConfigFolder(context.Background(), "   ")
	require.NoError(t, err)
}

func TestDeleteHarvestLeaseConfigFolder_SuccessStatuses(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusNoContent, http.StatusNotFound} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodDelete, r.Method)
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				b, _ := io.ReadAll(r.Body)
				assert.Contains(t, string(b), `"leaseName":"my-lease"`)
				w.WriteHeader(code)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(srv.Close)

			oldHost, oldProto := harvestEndPoint, harvestRestProtocol
			harvestEndPoint = strings.TrimPrefix(strings.TrimPrefix(srv.URL, "http://"), "https://")
			harvestRestProtocol = "http"
			t.Cleanup(func() {
				harvestEndPoint = oldHost
				harvestRestProtocol = oldProto
			})

			err := deleteHarvestLeaseConfigFolder(context.Background(), "my-lease")
			require.NoError(t, err)
		})
	}
}

func TestDeleteHarvestLeaseConfigFolder_ErrorStatuses(t *testing.T) {
	tests := []struct {
		name   string
		status int
	}{
		{"bad_request", http.StatusBadRequest},
		{"conflict", http.StatusConflict},
		{"server_error", http.StatusInternalServerError},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte("details"))
			}))
			t.Cleanup(srv.Close)

			oldHost, oldProto := harvestEndPoint, harvestRestProtocol
			harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
			harvestRestProtocol = "http"
			t.Cleanup(func() {
				harvestEndPoint = oldHost
				harvestRestProtocol = oldProto
			})

			err := deleteHarvestLeaseConfigFolder(context.Background(), "lease-x")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "lease-x")
		})
	}
}

func TestDeleteRebalanceTargetHarvestPoller_EmptyLease(t *testing.T) {
	err := deleteRebalanceTargetHarvestPoller(context.Background(), "", 99)
	require.NoError(t, err)
}

func TestDeleteRebalanceTargetHarvestPoller_OKAndNotFound(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusNotFound} {
		code := code
		t.Run(http.StatusText(code), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodDelete, r.Method)
				assert.Contains(t, r.URL.Path, "/config/lease-a/")
				w.WriteHeader(code)
			}))
			t.Cleanup(srv.Close)

			oldHost, oldProto := harvestEndPoint, harvestRestProtocol
			harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
			harvestRestProtocol = "http"
			t.Cleanup(func() {
				harvestEndPoint = oldHost
				harvestRestProtocol = oldProto
			})

			err := deleteRebalanceTargetHarvestPoller(context.Background(), "lease-a", 7)
			require.NoError(t, err)
		})
	}
}

func TestDeleteRebalanceTargetHarvestPoller_NonSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)

	oldHost, oldProto := harvestEndPoint, harvestRestProtocol
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	harvestRestProtocol = "http"
	t.Cleanup(func() {
		harvestEndPoint = oldHost
		harvestRestProtocol = oldProto
	})

	err := deleteRebalanceTargetHarvestPoller(context.Background(), "lease-b", 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "418")
}

func TestCloneHarvestConfig(t *testing.T) {
	_, err := cloneHarvestConfig(nil)
	require.Error(t, err)

	hc := &datamodel.HarvestConfig{PORT: "12345", LEASE_NAME: "L"}
	out, err := cloneHarvestConfig(hc)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "12345", out.PORT)
	assert.Equal(t, "L", out.LEASE_NAME)
}

func TestPollerRebalanceActivities_GetNodeGroupsWithPollerCountsActivity(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	want := []datamodel.NodeGroupPollerCount{{NodeGroupID: 1, LeaseName: "a", Count: 2}}
	mockSE.On("ListNodeGroupsWithPollerCounts", mock.Anything).Return(want, nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.GetNodeGroupsWithPollerCountsActivity)

	res, err := env.ExecuteActivity(act.GetNodeGroupsWithPollerCountsActivity)
	require.NoError(t, err)

	var got HarvestNodeGroupsSnapshotResult
	require.NoError(t, res.Get(&got))
	require.Len(t, got.Groups, 1)
	assert.Equal(t, int64(1), got.Groups[0].NodeGroupID)
	mockSE.AssertExpectations(t)
}

func TestPollerRebalanceActivities_GetNodeGroupsWithPollerCountsActivity_Error(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("ListNodeGroupsWithPollerCounts", mock.Anything).Return(nil, errors.New("list failed"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.GetNodeGroupsWithPollerCountsActivity)

	_, err := env.ExecuteActivity(act.GetNodeGroupsWithPollerCountsActivity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "list failed")
	mockSE.AssertExpectations(t)
}

func TestListEmptyHarvestLeasesForCleanupActivity_ReturnsOnlyEmpty(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	rows := []datamodel.NodeGroupPollerCount{
		{NodeGroupID: 1, LeaseName: "harvest-l1", Count: 0},
		{NodeGroupID: 2, LeaseName: "harvest-l2", Count: 5},
		{NodeGroupID: 3, LeaseName: "", Count: 0},
		{NodeGroupID: 4, LeaseName: "harvest-l4", Count: 0},
	}
	mockSE.On("ListNodeGroupsWithPollerCounts", mock.Anything).Return(rows, nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.ListEmptyHarvestLeasesForCleanupActivity)

	res, err := env.ExecuteActivity(act.ListEmptyHarvestLeasesForCleanupActivity)
	require.NoError(t, err)

	var got []EmptyHarvestLeaseCandidate
	require.NoError(t, res.Get(&got))
	require.Len(t, got, 2)
	assert.Equal(t, int64(1), got[0].NodeGroupID)
	assert.Equal(t, "harvest-l1", got[0].LeaseName)
	assert.Equal(t, int64(4), got[1].NodeGroupID)
}

func TestListEmptyHarvestLeasesForCleanupActivity_DBError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("ListNodeGroupsWithPollerCounts", mock.Anything).Return(nil, errors.New("db error"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.ListEmptyHarvestLeasesForCleanupActivity)

	_, err := env.ExecuteActivity(act.ListEmptyHarvestLeasesForCleanupActivity)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
}

func TestCleanupEmptyLeaseActivity_NilParams(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, (*CleanupEmptyLeaseParams)(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cleanup empty lease params required")
}

func TestCleanupEmptyLeaseActivity_EmptyLeaseName(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 1, LeaseName: ""})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "node group id and lease name required")
}

func TestCleanupEmptyLeaseActivity_SkipWhenPollersExist(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(3), nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	require.NoError(t, err)
	mockSE.AssertExpectations(t)
}

func TestCleanupEmptyLeaseActivity_GetNodeGroupMapNodeCountError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), errors.New("db err"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db err")
}

func TestCleanupEmptyLeaseActivity_FirstTransactionError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), nil)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(errors.New("tx error")).Once()

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "precheck empty lease cleanup")
}

func TestCleanupEmptyLeaseActivity_FirstTxSucceedsButNotProceed(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), nil)
	// WithTransaction succeeds but shouldProceedCleanup stays false (simulates cnt>0 inside tx)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(nil).Once()

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	// WithTransaction returns nil but shouldProceedCleanup = false → return nil early
	require.NoError(t, err)
}

func TestCleanupEmptyLeaseActivity_HarvestDeleteError(t *testing.T) {
	// Setup: harvest REST endpoint returns error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("disk full"))
	}))
	defer srv.Close()

	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	defer func() { harvestRestProtocol = oldProto; harvestEndPoint = oldEndpoint }()

	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), nil)
	// First WithTransaction: simulate shouldProceedCleanup = true by running callback
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		// Create a sqlmock-backed gorm.DB for the transaction
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		// Expect the SELECT FOR UPDATE query
		smock.ExpectQuery(`SELECT`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(5, "harvest-l5"),
		)
		// Expect the COUNT query → 0 pollers
		smock.ExpectQuery(`SELECT count`).WillReturnRows(
			sqlmock.NewRows([]string{"count"}).AddRow(0),
		)
		_ = fn(mockTx)
	}).Return(nil).Once()

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete harvest lease folder")
}

func TestCleanupEmptyLeaseActivity_FullHappyPath(t *testing.T) {
	// Harvest DELETE returns OK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	defer func() { harvestRestProtocol = oldProto; harvestEndPoint = oldEndpoint }()

	// Override deleteKubernetesLeaseForEmptyHarvestPollers
	oldDeleteK8sLease := deleteKubernetesLeaseForEmptyHarvestPollers
	deleteKubernetesLeaseForEmptyHarvestPollers = func(ctx context.Context, ns, name string) error { return nil }
	defer func() { deleteKubernetesLeaseForEmptyHarvestPollers = oldDeleteK8sLease }()

	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), nil)

	txCallCount := 0
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		txCallCount++
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		if txCallCount == 1 {
			// First tx: SELECT FOR UPDATE + COUNT → proceed
			smock.ExpectQuery(`SELECT`).WillReturnRows(
				sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(5, "harvest-l5"),
			)
			smock.ExpectQuery(`SELECT count`).WillReturnRows(
				sqlmock.NewRows([]string{"count"}).AddRow(0),
			)
		} else {
			// Second tx: SELECT FOR UPDATE + COUNT + DELETE
			smock.ExpectQuery(`SELECT`).WillReturnRows(
				sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(5, "harvest-l5"),
			)
			smock.ExpectQuery(`SELECT count`).WillReturnRows(
				sqlmock.NewRows([]string{"count"}).AddRow(0),
			)
			smock.ExpectExec(`UPDATE`).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		_ = fn(mockTx)
	}).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	require.NoError(t, err)
	assert.Equal(t, 2, txCallCount)
}

func TestCleanupEmptyLeaseActivity_K8sLeaseNotFoundIgnored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	defer func() { harvestRestProtocol = oldProto; harvestEndPoint = oldEndpoint }()

	oldDeleteK8sLease := deleteKubernetesLeaseForEmptyHarvestPollers
	deleteKubernetesLeaseForEmptyHarvestPollers = func(ctx context.Context, ns, name string) error {
		return errors.New("lease not found")
	}
	defer func() { deleteKubernetesLeaseForEmptyHarvestPollers = oldDeleteK8sLease }()

	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroupMapNodeCount", mock.Anything, int64(5)).Return(int64(0), nil)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)
		smock.ExpectQuery(`SELECT`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(5, "harvest-l5"),
		)
		smock.ExpectQuery(`SELECT count`).WillReturnRows(
			sqlmock.NewRows([]string{"count"}).AddRow(0),
		)
		if smock.ExpectationsWereMet() != nil {
			// Second tx: soft delete
			smock.ExpectExec(`UPDATE`).WillReturnResult(sqlmock.NewResult(0, 1))
		}
		_ = fn(mockTx)
	}).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CleanupEmptyLeaseActivity)

	_, err := env.ExecuteActivity(act.CleanupEmptyLeaseActivity, &CleanupEmptyLeaseParams{NodeGroupID: 5, LeaseName: "harvest-l5"})
	// "not found" in k8s lease error is logged but not returned
	require.NoError(t, err)
}

func TestUploadRebalanceMovesToHarvestActivity_NilParams(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, (*UploadRebalanceMovesParams)(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload params required")
}

func TestUploadRebalanceMovesToHarvestActivity_EmptyURL(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
		Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{1}}},
		UploadURL: "",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload URL required")
}

func TestUploadRebalanceMovesToHarvestActivity_EmptyMoves(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	res, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
		Moves:     []HarvestRebalanceMove{},
		UploadURL: "http://localhost:3000",
	})
	require.NoError(t, err)
	var got RebalanceUploadStageResult
	require.NoError(t, res.Get(&got))
	assert.Empty(t, got.Staged)
}

func TestUploadRebalanceMovesToHarvestActivity_TargetGroupMissing(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return((*datamodel.NodeGroup)(nil), nil)

	// Need DB() for the function - use sqlmock
	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	mockSE.On("DB").Return(gormDB)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
		Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{10}}},
		UploadURL: "http://localhost:3000/upload",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target node group 2 missing")
}

func TestUploadRebalanceMovesToHarvestActivity_SourceMismatch(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	targetGroup := &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 2}, LeaseName: "lease-2"}
	mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(targetGroup, nil)
	mockSE.On("GetNodeGroup", mock.Anything, int64(1)).Return(&datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 1}, LeaseName: "lease-1"}, nil)
	// Node 10 is on group 99, not group 1 as expected
	mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(10), mock.Anything).Return(
		&datamodel.NodeNodeGroupMap{NodeID: 10, NodeGroupID: 99, HarvestConfig: &datamodel.HarvestConfig{PORT: "13001"}}, nil)

	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	mockSE.On("DB").Return(gormDB)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
		Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{10}}},
		UploadURL: "http://localhost:3000/upload",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "move expects source 1")
}

func TestUploadRebalanceMovesToHarvestActivity_NilHarvestConfig(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	targetGroup := &datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 2}, LeaseName: "lease-2"}
	mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(targetGroup, nil)
	mockSE.On("GetNodeGroup", mock.Anything, int64(1)).Return(&datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 1}, LeaseName: "lease-1"}, nil)
	mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(10), mock.Anything).Return(
		&datamodel.NodeNodeGroupMap{NodeID: 10, NodeGroupID: 1, HarvestConfig: nil}, nil)

	db, _, _ := sqlmock.New()
	defer func() { _ = db.Close() }()
	gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	mockSE.On("DB").Return(gormDB)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)

	_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
		Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{10}}},
		UploadURL: "http://localhost:3000/upload",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no harvest_config")
}

func TestCommitRebalanceMovesInDBActivity_NilParams(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, (*CommitRebalanceMovesParams)(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "commit params required")
}

func TestCommitRebalanceMovesInDBActivity_EmptyStaged(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{Staged: nil, MaxNodesPerGroup: 200})
	require.NoError(t, err)
}

func TestCommitRebalanceMovesInDBActivity_InvalidMaxNodes(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged:           []RebalanceStagedNode{{NodeID: 1}},
		MaxNodesPerGroup: 0,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max nodes per group must be > 0")
}

func TestCommitRebalanceMovesInDBActivity_TransactionError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(errors.New("tx failed"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged:           []RebalanceStagedNode{{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, Port: "13001"}},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "tx failed")
}

func TestCommitRebalanceMovesInDBActivity_SourceDeleteAfterCommit(t *testing.T) {
	// Transaction succeeds, but source harvest delete fails
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	defer func() { harvestRestProtocol = oldProto; harvestEndPoint = oldEndpoint }()

	mockSE := database.NewMockStorage(t)
	// WithTransaction succeeds (no actual DB changes in this test, simulate success)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		// Don't execute the callback - just return nil to simulate tx success
	}).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src-lease", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "delete source harvest poller after rebalance commit")
}

func TestCommitRebalanceMovesInDBActivity_SkipSameSourceTarget(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	// When SourceLeaseName == TargetLeaseName, source delete is skipped
	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "same-lease", TargetLeaseName: "same-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.NoError(t, err)
}

func TestCommitRebalanceMovesInDBActivity_EmptySourceLease(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	// When SourceLeaseName is empty, source delete is skipped with warning
	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.NoError(t, err)
}

func TestCommitRebalanceMovesInDBActivity_SuccessfulSourceDelete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	defer func() { harvestRestProtocol = oldProto; harvestEndPoint = oldEndpoint }()

	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src-lease", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.NoError(t, err)
}

func restorePollerVerifyGlobals() func() {
	savedMax := rebalancePollerVerifyMaxAttempts
	savedInt := rebalancePollerVerifyIntervalSec
	savedMgmt := harvestPollerVerifyManagementPort
	savedPath := harvestPollerVerifyPrometheusTargetsPath
	oldGetPod := getPodIPForKubernetesLeaseHolder
	return func() {
		rebalancePollerVerifyMaxAttempts = savedMax
		rebalancePollerVerifyIntervalSec = savedInt
		harvestPollerVerifyManagementPort = savedMgmt
		harvestPollerVerifyPrometheusTargetsPath = savedPath
		getPodIPForKubernetesLeaseHolder = oldGetPod
	}
}

func TestVerifyRebalancePollersUpActivity_Validation(t *testing.T) {
	act := &PollerRebalanceActivities{SE: database.NewMockStorage(t)}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.VerifyRebalancePollersUpActivity)

	_, err := env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, (*VerifyRebalancePollersParams)(nil))
	require.Error(t, err)

	_, err = env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, &VerifyRebalancePollersParams{Staged: nil})
	require.NoError(t, err)

	_, err = env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, &VerifyRebalancePollersParams{
		Staged: []RebalanceStagedNode{{NodeID: 1, TargetLeaseName: "", Port: "13001"}},
	})
	require.Error(t, err)

	_, err = env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, &VerifyRebalancePollersParams{
		Staged: []RebalanceStagedNode{{NodeID: 1, TargetLeaseName: "lease-a", Port: "   "}},
	})
	require.Error(t, err)
}

func TestVerifyRebalancePollersUpActivity_GetPodIPError(t *testing.T) {
	cleanup := restorePollerVerifyGlobals()
	t.Cleanup(cleanup)
	rebalancePollerVerifyMaxAttempts = 1
	rebalancePollerVerifyIntervalSec = 0

	oldGet := getPodIPForKubernetesLeaseHolder
	getPodIPForKubernetesLeaseHolder = func(context.Context, string, string, string) (string, error) {
		return "", errors.New("lease holder lookup failed")
	}
	t.Cleanup(func() { getPodIPForKubernetesLeaseHolder = oldGet })

	act := &PollerRebalanceActivities{SE: database.NewMockStorage(t)}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.VerifyRebalancePollersUpActivity)

	_, err := env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, &VerifyRebalancePollersParams{
		Staged: []RebalanceStagedNode{{NodeID: 1, TargetLeaseName: "lease-x", Port: "13001"}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve pod IP")
}

func TestVerifyRebalancePollersUpActivity_SuccessHTTP(t *testing.T) {
	cleanup := restorePollerVerifyGlobals()
	t.Cleanup(cleanup)

	var handlerCalls int
	var prometheusHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalls++
		body, _ := json.Marshal([]harvestPrometheusTargetEntry{
			{Targets: []string{net.JoinHostPort(prometheusHost, "13001")}},
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	prometheusHost, mgmtPort, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	rebalancePollerVerifyMaxAttempts = 2
	harvestPollerVerifyManagementPort = mgmtPort
	harvestPollerVerifyPrometheusTargetsPath = "/pollers/prometheus-targets"

	oldGet := getPodIPForKubernetesLeaseHolder
	getPodIPForKubernetesLeaseHolder = func(context.Context, string, string, string) (string, error) {
		return prometheusHost, nil
	}
	t.Cleanup(func() { getPodIPForKubernetesLeaseHolder = oldGet })

	act := &PollerRebalanceActivities{SE: database.NewMockStorage(t)}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.VerifyRebalancePollersUpActivity)

	_, err = env.ExecuteActivity(act.VerifyRebalancePollersUpActivity, &VerifyRebalancePollersParams{
		Staged: []RebalanceStagedNode{{NodeID: 1, TargetLeaseName: "lease-verify", Port: "13001"}},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, handlerCalls)
}

func TestRollbackRebalanceTargetHarvestActivity(t *testing.T) {
	act := &PollerRebalanceActivities{SE: database.NewMockStorage(t)}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.RollbackRebalanceTargetHarvestActivity)

	_, err := env.ExecuteActivity(act.RollbackRebalanceTargetHarvestActivity, (*RollbackRebalanceTargetHarvestParams)(nil))
	require.NoError(t, err)

	_, err = env.ExecuteActivity(act.RollbackRebalanceTargetHarvestActivity, &RollbackRebalanceTargetHarvestParams{Staged: nil})
	require.NoError(t, err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	oldProto, oldEndpoint := harvestRestProtocol, harvestEndPoint
	harvestRestProtocol = "http"
	harvestEndPoint = strings.TrimPrefix(srv.URL, "http://")
	t.Cleanup(func() {
		harvestRestProtocol = oldProto
		harvestEndPoint = oldEndpoint
	})

	_, err = env.ExecuteActivity(act.RollbackRebalanceTargetHarvestActivity, &RollbackRebalanceTargetHarvestParams{
		Staged: []RebalanceStagedNode{{NodeID: 9, TargetLeaseName: "lease-rb"}},
	})
	require.NoError(t, err)
}

func TestBuildPollerRebalancePlanActivity(t *testing.T) {
	t.Run("nil params", func(t *testing.T) {
		act := &PollerRebalanceActivities{SE: database.NewMockStorage(t)}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		_, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, (*BuildPollerRebalancePlanParams)(nil))
		require.Error(t, err)
	})

	t.Run("empty node groups", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		res, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, &BuildPollerRebalancePlanParams{NodeGroups: nil})
		require.NoError(t, err)
		var out HarvestRebalancePlanOutput
		require.NoError(t, res.Get(&out))
		require.Empty(t, out.Moves)
	})

	t.Run("list maps error", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("ListNodeNodeGroupMapsByNodeGroupID", mock.Anything, int64(10)).Return(nil, errors.New("maps err"))
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		_, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, &BuildPollerRebalancePlanParams{
			NodeGroups:       []datamodel.NodeGroupPollerCount{{NodeGroupID: 10, LeaseName: "L", Count: 1}},
			MaxNodesPerGroup: 200,
			EvictThreshold:   20,
			SoftThreshold:    50,
		})
		require.Error(t, err)
	})

	t.Run("ha sibling error", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("ListNodeNodeGroupMapsByNodeGroupID", mock.Anything, int64(11)).Return([]*datamodel.NodeNodeGroupMap{
			{NodeID: 99, NodeGroupID: 11, HarvestConfig: &datamodel.HarvestConfig{}},
		}, nil)
		mockSE.On("GetHarvestHaSiblingNodeID", mock.Anything, int64(99)).Return(int64(0), errors.New("sibling err"))
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		_, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, &BuildPollerRebalancePlanParams{
			NodeGroups:       []datamodel.NodeGroupPollerCount{{NodeGroupID: 11, LeaseName: "L", Count: 1}},
			MaxNodesPerGroup: 200,
			EvictThreshold:   20,
			SoftThreshold:    50,
		})
		require.Error(t, err)
	})

	t.Run("count zero skips list", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		res, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, &BuildPollerRebalancePlanParams{
			NodeGroups:       []datamodel.NodeGroupPollerCount{{NodeGroupID: 1, LeaseName: "a", Count: 0}},
			MaxNodesPerGroup: 200,
			EvictThreshold:   20,
			SoftThreshold:    50,
		})
		require.NoError(t, err)
		var out HarvestRebalancePlanOutput
		require.NoError(t, res.Get(&out))
		require.Empty(t, out.Moves)
	})

	t.Run("success minimal", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("ListNodeNodeGroupMapsByNodeGroupID", mock.Anything, int64(1)).Return([]*datamodel.NodeNodeGroupMap{
			{NodeID: 10, NodeGroupID: 1, HarvestConfig: &datamodel.HarvestConfig{}},
		}, nil)
		mockSE.On("GetHarvestHaSiblingNodeID", mock.Anything, int64(10)).Return(int64(0), nil)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.BuildPollerRebalancePlanActivity)
		_, err := env.ExecuteActivity(act.BuildPollerRebalancePlanActivity, &BuildPollerRebalancePlanParams{
			NodeGroups:       []datamodel.NodeGroupPollerCount{{NodeGroupID: 1, LeaseName: "L", Count: 1}},
			MaxNodesPerGroup: 200,
			EvictThreshold:   20,
			SoftThreshold:    50,
		})
		require.NoError(t, err)
	})
}

func TestUploadRebalanceMovesToHarvestActivity_GetNodeGroupErrors(t *testing.T) {
	db, _, _ := sqlmock.New()
	t.Cleanup(func() { _ = db.Close() })
	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
	require.NoError(t, err)

	t.Run("get target group error", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(nil, errors.New("db read"))
		mockSE.On("DB").Return(gormDB)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)
		_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
			Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{10}}},
			UploadURL: "http://localhost/upload",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "db read")
	})

	t.Run("target empty lease", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(&datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 2}, LeaseName: ""}, nil)
		mockSE.On("DB").Return(gormDB)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)
		_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
			Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, NodeIDs: []int64{10}}},
			UploadURL: "http://localhost/upload",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty lease name")
	})

	t.Run("resolve source lease get node group error", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(&datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 2}, LeaseName: "t"}, nil)
		mockSE.On("GetNodeGroup", mock.Anything, int64(1)).Return(nil, errors.New("src group err"))
		mockSE.On("DB").Return(gormDB)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)
		_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
			Moves:     []HarvestRebalanceMove{{SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: " ", NodeIDs: []int64{10}}},
			UploadURL: "http://localhost/upload",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "src group err")
	})

	t.Run("get active mapping error", func(t *testing.T) {
		mockSE := database.NewMockStorage(t)
		mockSE.On("GetNodeGroup", mock.Anything, int64(2)).Return(&datamodel.NodeGroup{BaseModel: datamodel.BaseModel{ID: 2}, LeaseName: "t"}, nil)
		mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(10), mock.Anything).Return(nil, errors.New("no mapping"))
		mockSE.On("DB").Return(gormDB)
		act := &PollerRebalanceActivities{SE: mockSE}
		ts := &testsuite.WorkflowTestSuite{}
		env := ts.NewTestActivityEnvironment()
		env.RegisterActivity(act.UploadRebalanceMovesToHarvestActivity)
		_, err := env.ExecuteActivity(act.UploadRebalanceMovesToHarvestActivity, &UploadRebalanceMovesParams{
			Moves: []HarvestRebalanceMove{{
				SourceGroupID:   1,
				TargetGroupID:   2,
				SourceLeaseName: "already-set-lease",
				NodeIDs:         []int64{10},
			}},
			UploadURL: "http://localhost/upload",
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no mapping")
	})
}

func TestCommitRebalanceMovesInDBActivity_TargetGroupMissingAfterLock(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		// Lock returns no rows for target group 2
		smock.ExpectQuery(`SELECT.*FROM "node_groups"`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}),
		)
		_ = fn(mockTx)
	}).Return(errors.New("target node group 2 missing or deleted"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src", TargetLeaseName: "tgt", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "target node group 2 missing or deleted")
}

func TestCommitRebalanceMovesInDBActivity_TargetGroupEmptyLeaseNameAfterLock(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		// Lock returns target with empty lease_name
		smock.ExpectQuery(`SELECT.*FROM "node_groups"`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(2, ""),
		)
		_ = fn(mockTx)
	}).Return(errors.New("target node group 2 missing or has empty lease name"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src", TargetLeaseName: "tgt", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty lease name")
}

func TestCommitRebalanceMovesInDBActivity_CountQueryError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(errors.New("count query failed"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src", TargetLeaseName: "tgt", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "count query failed")
}

func TestCommitRebalanceMovesInDBActivity_NodeAlreadyOnTargetMatchingConfig(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		smock.ExpectQuery(`SELECT.*FROM "node_groups"`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(2, "tgt-lease"),
		)
		smock.ExpectQuery(`SELECT.*COUNT`).WillReturnRows(
			sqlmock.NewRows([]string{"node_group_id", "cnt"}).AddRow(2, 10),
		)
		_ = fn(mockTx)
	}).Return(nil)

	// Node already on target with matching config
	mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(1), mock.Anything).Return(
		&datamodel.NodeNodeGroupMap{
			NodeID:      1,
			NodeGroupID: 2,
			HarvestConfig: &datamodel.HarvestConfig{
				LEASE_NAME: "tgt-lease",
				PORT:       "13001",
			},
		}, nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	// Idempotent case: node already on target with matching config
	// Use empty SourceLeaseName so it doesn't try to delete
	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.NoError(t, err) // Should succeed without trying to delete from source
}

func TestCommitRebalanceMovesInDBActivity_NodeAlreadyOnTargetMismatchedConfig(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(dbutils.Transaction) error)
		db, smock, _ := sqlmock.New()
		defer func() { _ = db.Close() }()
		gormDB, _ := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
		mockTx := dbutils.NewMockTransaction(t)
		mockTx.On("GORM").Return(gormDB)

		smock.ExpectQuery(`SELECT.*FROM "node_groups"`).WillReturnRows(
			sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(2, "tgt-lease"),
		)
		smock.ExpectQuery(`SELECT.*COUNT`).WillReturnRows(
			sqlmock.NewRows([]string{"node_group_id", "cnt"}).AddRow(2, 10),
		)
		_ = fn(mockTx)
	}).Return(errors.New("rebalance commit: node 1 already on target group 2 but harvest_config does not match expected lease/port"))

	// Node on target but config doesn't match
	mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(1), mock.Anything).Return(
		&datamodel.NodeNodeGroupMap{
			NodeID:      1,
			NodeGroupID: 2,
			HarvestConfig: &datamodel.HarvestConfig{
				LEASE_NAME: "wrong-lease",
				PORT:       "99999",
			},
		}, nil)

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "already on target group")
}

func TestCommitRebalanceMovesInDBActivity_PortConflictRetryable(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("GetActiveNodeNodeGroupMapByNodeID", mock.Anything, int64(1), mock.Anything).Return(
		&datamodel.NodeNodeGroupMap{
			NodeID:        1,
			NodeGroupID:   1,
			HarvestConfig: &datamodel.HarvestConfig{LEASE_NAME: "src-lease", PORT: "13000"},
		}, nil)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(
		func(_ context.Context, fn func(dbutils.Transaction) error) error {
			db, smock, err := sqlmock.New()
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: db}), &gorm.Config{})
			if err != nil {
				return err
			}
			mockTx := dbutils.NewMockTransaction(t)
			mockTx.On("GORM").Return(gormDB)

			smock.ExpectQuery(`SELECT.*FROM "node_groups"`).WillReturnRows(
				sqlmock.NewRows([]string{"id", "lease_name"}).AddRow(2, "tgt-lease"),
			)
			smock.ExpectQuery(`SELECT.*COUNT`).WillReturnRows(
				sqlmock.NewRows([]string{"node_group_id", "cnt"}).AddRow(2, 10),
			)
			smock.ExpectBegin()
			smock.ExpectExec(`UPDATE "node_node_group_maps"`).
				WillReturnError(errors.New(`duplicate key value violates unique constraint "idx_node_node_group_maps_group_port_active_uq"`))
			smock.ExpectRollback()
			return fn(mockTx)
		})

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, SourceLeaseName: "src-lease", TargetLeaseName: "tgt-lease", Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "HarvestRebalancePortConflict")
}

func TestCommitRebalanceMovesInDBActivity_CapacityExceededRetryable(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(
		errors.New("rebalance commit capacity changed: target group 2 has 195 assigned, incoming 6 exceeds max 200"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	// Try to move 6 nodes to target that has 195, max is 200 (exceeds by 1)
	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, Port: "13001"},
			{NodeID: 2, SourceGroupID: 1, TargetGroupID: 2, Port: "13002"},
			{NodeID: 3, SourceGroupID: 1, TargetGroupID: 2, Port: "13003"},
			{NodeID: 4, SourceGroupID: 1, TargetGroupID: 2, Port: "13004"},
			{NodeID: 5, SourceGroupID: 1, TargetGroupID: 2, Port: "13005"},
			{NodeID: 6, SourceGroupID: 1, TargetGroupID: 2, Port: "13006"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "capacity changed")
}

func TestCommitRebalanceMovesInDBActivity_GetActiveMappingError(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(errors.New("mapping lookup failed"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mapping lookup failed")
}

func TestCommitRebalanceMovesInDBActivity_SourceGroupMismatch(t *testing.T) {
	mockSE := database.NewMockStorage(t)
	mockSE.On("WithTransaction", mock.Anything, mock.Anything).Return(
		errors.New("rebalance commit: node 1 on group 99, expected source 1 (drift or duplicate commit)"))

	act := &PollerRebalanceActivities{SE: mockSE}
	ts := &testsuite.WorkflowTestSuite{}
	env := ts.NewTestActivityEnvironment()
	env.RegisterActivity(act.CommitRebalanceMovesInDBActivity)

	_, err := env.ExecuteActivity(act.CommitRebalanceMovesInDBActivity, &CommitRebalanceMovesParams{
		Staged: []RebalanceStagedNode{
			{NodeID: 1, SourceGroupID: 1, TargetGroupID: 2, Port: "13001"},
		},
		MaxNodesPerGroup: 200,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "on group 99")
}
