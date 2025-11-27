package tasks

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	supervisorhandler "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/tasks/supervisor-handler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type testHandler struct {
	jobTypes  []models.JobType
	events    []supervisorhandler.Event
	jobs      []*datamodel.Job
	returnErr error
}

func newTestHandler(jobTypes ...models.JobType) *testHandler {
	return &testHandler{jobTypes: jobTypes}
}

func (h *testHandler) JobTypes() []models.JobType {
	return h.jobTypes
}

func (h *testHandler) Handle(ctx context.Context, job *datamodel.Job, event supervisorhandler.Event, storage database.Storage) error {
	h.events = append(h.events, event)
	h.jobs = append(h.jobs, job)
	return h.returnErr
}

func expectJobLock(t *testing.T, storage *database.MockStorage, job *datamodel.Job) {
	storage.EXPECT().WithTransaction(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, fn func(dbutils.Transaction) error) error {
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
		require.NoError(t, err)
		require.NoError(t, db.AutoMigrate(&datamodel.Job{}))

		dbJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      job.UUID,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			WorkflowID: job.WorkflowID,
			State:      job.State,
			TrackingID: job.TrackingID,
		}
		require.NoError(t, db.Create(dbJob).Error)

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		return fn(tx)
	})
}

func TestWorkflowSupervisorTaskRunnerRegisterHandlers(t *testing.T) {
	runner := &workflowSupervisorTaskRunner{handlers: make(map[string]supervisorhandler.Handler)}
	handler := newTestHandler(models.JobTypeCreatePool)

	runner.registerHandlers(handler, nil)

	require.Equal(t, handler, runner.handlers[string(models.JobTypeCreatePool)])
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobNotFoundAfterGraceTriggersCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID:      "job-2",
			CreatedAt: time.Now().Add(-2 * time.Minute),
		},
		WorkflowID: "wf-2",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 7,
	}

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), serviceerror.NewNotFound("missing"))
	expectJobLock(t, storage, job)
	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)).Return(nil)

	runner.evaluateJob(context.Background(), job, handler)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerScanAppliesGracePeriodFilter(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	handler := newTestHandler(models.JobTypeCreatePool)

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		if len(filter.Conditions) != 3 {
			return false
		}

		foundCreatedAt := false
		for _, condition := range filter.Conditions {
			if condition.Field == "created_at" && condition.Op == "<=" {
				val, ok := condition.Value.(string)
				if !ok {
					return false
				}
				if _, err := time.Parse(time.RFC3339, val); err != nil {
					return false
				}
				foundCreatedAt = true
			}
		}
		return foundCreatedAt
	})).Return([]*datamodel.Job{}, nil)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobTimedOutTriggersCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-3"},
		WorkflowID: "wf-3",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 42,
	}

	detail := fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)
	expectJobLock(t, storage, job)
	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, detail).Return(nil)

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
	}, nil)

	runner.evaluateJob(context.Background(), job, handler)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestRunWorkflowSupervisorTaskUsesProvidedHandlers(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "type" && condition.Op == "IN" {
				if types, ok := condition.Value.([]string); ok && len(types) == 1 && types[0] == string(models.JobTypeCreatePool) {
					return true
				}
			}
		}
		return false
	})).Return([]*datamodel.Job{}, nil).Once()

	runWorkflowSupervisorTask(context.Background(), storage, temporal, "corr-id", handler)
}

func TestRunWorkflowSupervisorTaskRegistersDefaultHandlers(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "type" && condition.Op == "IN" {
				if types, ok := condition.Value.([]string); ok {
					expected := map[string]struct{}{
						string(models.JobTypeCreateKmsConfig): {},
						string(models.JobTypeCreatePool):      {},
					}
					for key := range expected {
						found := false
						for _, t := range types {
							if t == key {
								found = true
								break
							}
						}
						if !found {
							return false
						}
					}
					return true
				}
			}
		}
		return false
	})).Return([]*datamodel.Job{}, nil).Once()

	runWorkflowSupervisorTask(context.Background(), storage, temporal, "corr-id")
}

func TestWorkflowSupervisorTaskRunnerScanNoHandlers(t *testing.T) {
	runner := &workflowSupervisorTaskRunner{
		handlers: make(map[string]supervisorhandler.Handler),
	}

	assert.NotPanics(t, func() {
		runner.scan(context.Background())
	})
}

func TestWorkflowSupervisorTaskRunnerScanFetchError(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return(nil, errors.New("db failure"))

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())
}

func TestWorkflowSupervisorTaskRunnerScanWarnsOnMissingHandler(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{
		{
			BaseModel: datamodel.BaseModel{UUID: "job-unknown"},
			Type:      "UNKNOWN",
		},
	}, nil)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobMissingExecutionInfo(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-missing-info"},
		WorkflowID: "wf-missing-info",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
	}

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{}, nil)

	runner.evaluateJob(context.Background(), job, handler)
	require.Empty(t, handler.events)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobTerminateNotFound(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-terminate-notfound"},
		WorkflowID: "wf-terminate-notfound",
		State:      string(models.JobsStateNEW),
		TrackingID: 13,
	}

	storage.EXPECT().WithTransaction(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, fn func(dbutils.Transaction) error) error {
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
		require.NoError(t, err)
		require.NoError(t, db.AutoMigrate(&datamodel.Job{}))

		dbJob := &datamodel.Job{
			BaseModel: datamodel.BaseModel{
				UUID:      job.UUID,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
			State:      string(models.JobsStateNEW),
			WorkflowID: job.WorkflowID,
		}
		require.NoError(t, db.Create(dbJob).Error)

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		return fn(tx)
	})

	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(serviceerror.NewNotFound("missing"))
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)).Return(nil)

	runner := &workflowSupervisorTaskRunner{
		storage:  storage,
		temporal: temporal,
	}

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))
	require.Len(t, handler.events, 1)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobLockFailure(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-lock-fail"},
		WorkflowID: "wf-lock-fail",
		State:      string(models.JobsStateNEW),
	}

	storage.EXPECT().WithTransaction(mock.Anything, mock.Anything).Return(fmt.Errorf("lock failure"))

	runner := &workflowSupervisorTaskRunner{
		storage:  storage,
		temporal: temporal,
	}

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))
	require.Empty(t, handler.events)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobMarkErrorFailure(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-mark-fail"},
		WorkflowID: "wf-mark-fail",
		TrackingID: 99,
	}

	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)).Return(errors.New("update failed"))

	runner := &workflowSupervisorTaskRunner{
		storage: storage,
	}

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))
	require.Len(t, handler.events, 1)
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobDescribeTimeoutTriggersCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-7"},
		WorkflowID: "wf-7",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 101,
	}

	detail := fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), context.DeadlineExceeded)
	expectJobLock(t, storage, job)
	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, detail).Return(nil)

	runner.evaluateJob(context.Background(), job, handler)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobDescribeErrorTriggersCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID:      "job-8",
			CreatedAt: time.Now().Add(-2 * workflowNotFoundGracePeriod),
		},
		WorkflowID: "wf-8",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 202,
	}

	detail := fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), serviceerror.NewInternal("describe failed"))
	expectJobLock(t, storage, job)
	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, detail).Return(nil)

	runner.evaluateJob(context.Background(), job, handler)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerEvaluateJobNonTimeoutStatusSkipsCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-4"},
		WorkflowID: "wf-4",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
	}

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}, nil)

	runner.evaluateJob(context.Background(), job, handler)

	require.Empty(t, handler.events)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobHandlerError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := newTestHandler(models.JobTypeCreatePool)
	handler.returnErr = errors.New("cleanup failed")

	runner := &workflowSupervisorTaskRunner{storage: storage}
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-5"}, WorkflowID: "wf-5"}

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))

	require.Len(t, handler.events, 1)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobMarksError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{storage: storage}
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-6"}, WorkflowID: "wf-6", TrackingID: 9}
	detail := fmt.Sprintf("%s: %s", supervisorhandler.WorkflowTimeoutDetail, job.WorkflowID)

	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, detail).Return(nil)

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))

	require.Len(t, handler.events, 1)
	assert.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerCleanupJobSkipsTerminateWhenJobNotNew(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	runner := &workflowSupervisorTaskRunner{
		storage:  storage,
		temporal: temporal,
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-9"},
		WorkflowID: "wf-9",
		State:      string(models.JobsStatePROCESSING),
		TrackingID: 11,
	}

	storage.EXPECT().WithTransaction(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, fn func(dbutils.Transaction) error) error {
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
		require.NoError(t, err)
		require.NoError(t, db.AutoMigrate(&datamodel.Job{}))

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		err = fn(tx)
		require.ErrorIs(t, err, gorm.ErrRecordNotFound)
		return err
	})

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, util.GetLogger(context.Background()))

	require.Empty(t, handler.events)
	temporal.AssertNotCalled(t, "TerminateWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}
