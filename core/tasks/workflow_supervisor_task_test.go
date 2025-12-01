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
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil)

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

	detail := supervisorhandler.WorkflowTimeoutDetail
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

func TestWorkflowSupervisorTaskRunnerScanProcessesJobsInIDOrder(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	now := time.Now()
	olderJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        10,
			UUID:      "job-old",
			CreatedAt: now,
		},
		WorkflowID: "",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 1,
	}
	newerJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        20,
			UUID:      "job-new",
			CreatedAt: now.Add(time.Minute),
		},
		WorkflowID: "",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 2,
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{newerJob, olderJob}, nil).Once()

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, olderJob.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), context.DeadlineExceeded).Once()
	storage.EXPECT().UpdateJob(mock.Anything, olderJob.UUID, string(models.JobsStateERROR), olderJob.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, newerJob.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), context.DeadlineExceeded).Once()
	storage.EXPECT().UpdateJob(mock.Anything, newerJob.UUID, string(models.JobsStateERROR), newerJob.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())

	require.Len(t, handler.jobs, 2)
	require.Equal(t, olderJob.ID, handler.jobs[0].ID)
	require.Equal(t, newerJob.ID, handler.jobs[1].ID)
}

func TestWorkflowSupervisorTaskRunnerScanMaintainsInputOrderForDuplicateIDs(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	now := time.Now()
	firstJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        42,
			UUID:      "job-first",
			CreatedAt: now,
		},
		WorkflowID: "",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 1,
	}
	secondJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        42,
			UUID:      "job-second",
			CreatedAt: now.Add(time.Minute),
		},
		WorkflowID: "",
		Type:       string(models.JobTypeCreatePool),
		State:      string(models.JobsStateNEW),
		TrackingID: 2,
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{secondJob, firstJob}, nil).Once()

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, secondJob.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), context.DeadlineExceeded).Once()
	storage.EXPECT().UpdateJob(mock.Anything, secondJob.UUID, string(models.JobsStateERROR), secondJob.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, firstJob.WorkflowID, "").Return((*workflowservice.DescribeWorkflowExecutionResponse)(nil), context.DeadlineExceeded).Once()
	storage.EXPECT().UpdateJob(mock.Anything, firstJob.UUID, string(models.JobsStateERROR), firstJob.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())

	require.Len(t, handler.jobs, 2)
	require.Equal(t, secondJob.UUID, handler.jobs[0].UUID)
	require.Equal(t, firstJob.UUID, handler.jobs[1].UUID)
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

func TestWorkflowSupervisorTaskRunnerScanSkipsJobWithActiveOverrideGracePeriod(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeCreatePool)

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID:      "job-override-skip",
			CreatedAt: time.Now().Add(-5 * time.Minute),
		},
		Type:  string(models.JobTypeCreatePool),
		State: string(models.JobsStateNEW),
		JobAttributes: &datamodel.JobAttributes{
			SupervisorAttributes: &datamodel.SupervisorAttributes{
				OverrideGracePeriod: 15 * time.Minute,
			},
		},
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeCreatePool): handler,
		},
	}

	runner.scan(context.Background())

	require.Empty(t, handler.events)
	temporal.AssertNotCalled(t, "DescribeWorkflowExecution", mock.Anything, mock.Anything, mock.Anything)
}

func TestShouldSkipJobForOverrideGracePeriod(t *testing.T) {
	now := time.Now().UTC()
	grace := 10 * time.Minute
	createdAt := now.Add(-5 * time.Minute)

	tests := []struct {
		name     string
		job      *datamodel.Job
		wantSkip bool
		wantTime time.Time
	}{
		{
			name: "skip when within override grace period",
			job: &datamodel.Job{
				BaseModel: datamodel.BaseModel{CreatedAt: createdAt},
				JobAttributes: &datamodel.JobAttributes{
					SupervisorAttributes: &datamodel.SupervisorAttributes{OverrideGracePeriod: grace},
				},
			},
			wantSkip: true,
			wantTime: createdAt.Add(grace),
		},
		{
			name: "process when override grace period elapsed",
			job: &datamodel.Job{
				BaseModel: datamodel.BaseModel{CreatedAt: now.Add(-2 * grace)},
				JobAttributes: &datamodel.JobAttributes{
					SupervisorAttributes: &datamodel.SupervisorAttributes{OverrideGracePeriod: grace},
				},
			},
			wantSkip: false,
			wantTime: time.Time{},
		},
		{
			name: "no supervisor attributes",
			job: &datamodel.Job{
				BaseModel:     datamodel.BaseModel{CreatedAt: createdAt},
				JobAttributes: &datamodel.JobAttributes{},
			},
			wantSkip: false,
			wantTime: time.Time{},
		},
		{
			name:     "nil job attributes",
			job:      &datamodel.Job{BaseModel: datamodel.BaseModel{CreatedAt: createdAt}},
			wantSkip: false,
			wantTime: time.Time{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			skip, resumeAt, _ := shouldSkipJobForOverrideGracePeriod(tt.job, now)
			assert.Equal(t, tt.wantSkip, skip)
			assert.Equal(t, tt.wantTime, resumeAt)
		})
	}
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
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil)

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

	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(errors.New("update failed"))

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

	detail := supervisorhandler.WorkflowTimeoutDetail

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

	detail := supervisorhandler.WorkflowTimeoutDetail

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
	detail := supervisorhandler.WorkflowTimeoutDetail

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
