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
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	supervisorhandler "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/tasks/supervisor-handler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	temporalConfig "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
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

func enableProcessingTimeout(t *testing.T) {
	t.Helper()
	processingTimeoutEnabled = true
	t.Cleanup(func() { processingTimeoutEnabled = true })
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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerScanAppliesGracePeriodFilter(t *testing.T) {
	enableProcessingTimeout(t)
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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)

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

	// NEW state scan
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "state" && condition.Op == "=" && condition.Value == string(models.JobsStateNEW) {
				return true
			}
		}
		return false
	})).Return([]*datamodel.Job{newerJob, olderJob}, nil).Once()

	// CreatePool is not eligible for PROCESSING scan, so no second GetJobsWithCondition call

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

	// NEW state scan
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "state" && condition.Op == "=" && condition.Value == string(models.JobsStateNEW) {
				return true
			}
		}
		return false
	})).Return([]*datamodel.Job{secondJob, firstJob}, nil).Once()

	// CreatePool is not eligible for PROCESSING scan, so no second GetJobsWithCondition call

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

	// NEW state scan for the provided CreatePool handler
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

	// CreatePool is not eligible for PROCESSING scan, so no second call

	runWorkflowSupervisorTask(context.Background(), storage, temporal, "corr-id", handler)
}

func TestRunWorkflowSupervisorTaskRegistersDefaultHandlers(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	// First call for NEW state jobs
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

	// Second call for PROCESSING state jobs
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "state" && condition.Op == "=" && condition.Value == string(models.JobsStatePROCESSING) {
				return true
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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)
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

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))
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

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))
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

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))
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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)

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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)

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

	runner.evaluateJob(context.Background(), job, handler, models.JobsStateNEW)

	require.Empty(t, handler.events)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowSupervisorTaskRunnerCleanupJobHandlerError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := newTestHandler(models.JobTypeCreatePool)
	handler.returnErr = errors.New("cleanup failed")

	runner := &workflowSupervisorTaskRunner{storage: storage}
	job := &datamodel.Job{BaseModel: datamodel.BaseModel{UUID: "job-5"}, WorkflowID: "wf-5"}

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))

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

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))

	require.Len(t, handler.events, 1)
	assert.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerCleanupJobSkipsTerminateWhenJobStateChanged(t *testing.T) {
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

	// This test verifies that when we try to lock a job with expected state NEW
	// but the job is actually in PROCESSING state, the lock fails and cleanup is skipped
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

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))

	require.Empty(t, handler.events)
	temporal.AssertNotCalled(t, "TerminateWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestMarkJobAsErrorWithZeroTrackingID verifies that when a job has TrackingID 0
// (i.e., it never started processing), the supervisor uses ErrWorkflowSupervisorTimeout
// so customers get a proper error message instead of "undefined error".
func TestMarkJobAsErrorWithZeroTrackingID(t *testing.T) {
	storage := database.NewMockStorage(t)

	runner := &workflowSupervisorTaskRunner{storage: storage}
	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-zero-tracking"},
		WorkflowID: "wf-zero-tracking",
		TrackingID: 0, // Job never started processing
	}

	// Expect UpdateJob to be called with ErrWorkflowSupervisorTimeout (1018) instead of 0
	storage.EXPECT().UpdateJob(
		mock.Anything,
		job.UUID,
		string(models.JobsStateERROR),
		vsaerrors.ErrWorkflowSupervisorTimeout, // Should use 1018, not 0
		supervisorhandler.WorkflowTimeoutDetail,
	).Return(nil)

	err := runner.markJobAsError(context.Background(), job)
	require.NoError(t, err)
}

// TestMarkJobAsErrorPreservesNonZeroTrackingID verifies that when a job has a non-zero
// TrackingID (i.e., it started processing and got a proper error code), the supervisor
// preserves the original tracking ID.
func TestMarkJobAsErrorPreservesNonZeroTrackingID(t *testing.T) {
	storage := database.NewMockStorage(t)

	runner := &workflowSupervisorTaskRunner{storage: storage}
	originalTrackingID := 5001 // Some error that occurred during processing
	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-with-tracking"},
		WorkflowID: "wf-with-tracking",
		TrackingID: originalTrackingID,
	}

	// Expect UpdateJob to be called with the original TrackingID
	storage.EXPECT().UpdateJob(
		mock.Anything,
		job.UUID,
		string(models.JobsStateERROR),
		originalTrackingID, // Should preserve original tracking ID
		supervisorhandler.WorkflowTimeoutDetail,
	).Return(nil)

	err := runner.markJobAsError(context.Background(), job)
	require.NoError(t, err)
}

// TestCleanupJobWithZeroTrackingIDUsesTimeoutError verifies the full cleanup flow
// when a job with TrackingID 0 is cleaned up by the supervisor.
func TestCleanupJobWithZeroTrackingIDUsesTimeoutError(t *testing.T) {
	storage := database.NewMockStorage(t)
	handler := newTestHandler(models.JobTypeCreateVolume)

	runner := &workflowSupervisorTaskRunner{storage: storage}
	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-cleanup-zero"},
		WorkflowID: "wf-cleanup-zero",
		Type:       string(models.JobTypeCreateVolume),
		TrackingID: 0, // Job stuck in NEW state, never got a tracking ID
	}

	// Expect UpdateJob to be called with ErrWorkflowSupervisorTimeout
	storage.EXPECT().UpdateJob(
		mock.Anything,
		job.UUID,
		string(models.JobsStateERROR),
		vsaerrors.ErrWorkflowSupervisorTimeout,
		supervisorhandler.WorkflowTimeoutDetail,
	).Return(nil)

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStateNEW, util.GetLogger(context.Background()))

	require.Len(t, handler.events, 1)
	assert.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

// Tests for PROCESSING state timeout handling

func TestWorkflowSupervisorTaskRunnerScanProcessingStateTimeoutsFindsTimedOutJobs(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeDeletePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        1,
			UUID:      "job-processing-timeout",
			CreatedAt: time.Now().Add(-4 * time.Hour), // Well past global timeout
		},
		WorkflowID: "wf-processing-timeout",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
		TrackingID: 42,
	}

	// Expect query for PROCESSING state jobs
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "state" && condition.Op == "=" && condition.Value == string(models.JobsStatePROCESSING) {
				return true
			}
		}
		return false
	})).Return([]*datamodel.Job{job}, nil).Once()

	// Expect Temporal describe to return TIMED_OUT status
	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
	}, nil).Once()

	// Expect cleanup to acquire lock for PROCESSING state
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
			State:      string(models.JobsStatePROCESSING),
			WorkflowID: job.WorkflowID,
		}
		require.NoError(t, db.Create(dbJob).Error)

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		return fn(tx)
	}).Once()

	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil).Once()

	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeDeletePool)})

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerScanProcessingStateTimeoutsSkipsRunningWorkflows(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeDeletePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        1,
			UUID:      "job-still-running",
			CreatedAt: time.Now().Add(-4 * time.Hour),
		},
		WorkflowID: "wf-still-running",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil).Once()

	// Temporal reports workflow is still RUNNING
	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}, nil).Once()

	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeDeletePool)})

	require.Empty(t, handler.events)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowSupervisorTaskRunnerScanProcessingStateTimeoutsSkipsOnDescribeError(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeDeletePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			ID:        1,
			UUID:      "job-describe-error",
			CreatedAt: time.Now().Add(-4 * time.Hour),
		},
		WorkflowID: "wf-describe-error",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{job}, nil).Once()

	// Temporal describe fails - for PROCESSING jobs we skip rather than cleanup
	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(
		(*workflowservice.DescribeWorkflowExecutionResponse)(nil),
		serviceerror.NewInternal("describe failed"),
	).Once()

	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeDeletePool)})

	// Unlike NEW state jobs, PROCESSING jobs should NOT trigger cleanup on describe error
	require.Empty(t, handler.events)
	storage.AssertNotCalled(t, "UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestWorkflowSupervisorTaskRunnerEvaluateProcessingJobTimedOutTriggersCleanup(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers: map[string]supervisorhandler.Handler{
			string(models.JobTypeDeletePool): handler,
		},
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-proc-timed-out"},
		WorkflowID: "wf-proc-timed-out",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
		TrackingID: 99,
	}

	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, job.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
	}, nil)

	// Expect lock for PROCESSING state
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
			State:      string(models.JobsStatePROCESSING),
			WorkflowID: job.WorkflowID,
		}
		require.NoError(t, db.Create(dbJob).Error)

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		return fn(tx)
	})

	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil)

	runner.evaluateProcessingJob(context.Background(), job, handler)

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestWorkflowSupervisorTaskRunnerCleanupJobWithProcessingStateLock(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:  storage,
		temporal: temporal,
	}

	job := &datamodel.Job{
		BaseModel:  datamodel.BaseModel{UUID: "job-proc-cleanup"},
		WorkflowID: "wf-proc-cleanup",
		State:      string(models.JobsStatePROCESSING),
		TrackingID: 77,
	}

	// Expect lock query to look for PROCESSING state
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
			State:      string(models.JobsStatePROCESSING),
			WorkflowID: job.WorkflowID,
		}
		require.NoError(t, db.Create(dbJob).Error)

		tx := dbutils.NewMockTransaction(t)
		tx.EXPECT().GORM().Return(db)
		return fn(tx)
	})

	temporal.EXPECT().TerminateWorkflow(mock.Anything, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail).Return(nil)
	storage.EXPECT().UpdateJob(mock.Anything, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail).Return(nil)

	runner.cleanupJob(context.Background(), job, handler, supervisorhandler.EventTimeout, models.JobsStatePROCESSING, util.GetLogger(context.Background()))

	require.Len(t, handler.events, 1)
	require.Equal(t, supervisorhandler.EventTimeout, handler.events[0])
}

func TestGetWorkflowTimeoutForJobType(t *testing.T) {
	globalTimeout := temporalConfig.GetWorkflowGlobalTimeout()
	require.NotZero(t, globalTimeout, "global workflow timeout must be configured")

	tests := []struct {
		name    string
		jobType string
	}{
		{name: "DeletePool returns global timeout", jobType: string(models.JobTypeDeletePool)},
		{name: "DeleteLargePool returns global timeout", jobType: string(models.JobTypeDeleteLargePool)},
		{name: "DeleteVolume returns global timeout", jobType: string(models.JobTypeDeleteVolume)},
		{name: "DeleteLargeVolume returns global timeout", jobType: string(models.JobTypeDeleteLargeVolume)},
		{name: "Unknown job type returns global timeout", jobType: "UNKNOWN_JOB_TYPE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeout := getWorkflowTimeoutForJobType(tt.jobType)
			assert.Equal(t, globalTimeout, timeout)
		})
	}
}

// TestGetWorkflowTimeoutForJobType_UnhandledJobTypesFallBackToGlobal verifies that
// real job types not explicitly handled in the switch statement fall back to the
// global workflow timeout. This catches cases where a new job type is added to the
// system but not given a specific timeout in getWorkflowTimeoutForJobType.
func TestGetWorkflowTimeoutForJobType_UnhandledJobTypesFallBackToGlobal(t *testing.T) {
	globalTimeout := temporalConfig.GetWorkflowGlobalTimeout()
	require.NotZero(t, globalTimeout, "global workflow timeout must be configured")

	unhandledJobTypes := []models.JobType{
		models.JobTypeCreatePool,
		models.JobTypeCreateLargePool,
		models.JobTypeUpdatePool,
		models.JobTypeUpdateLargePool,
		models.JobTypeCreateVolume,
		models.JobTypeCreateLargeVolume,
		models.JobTypeDeletePool,
		models.JobTypeDeleteLargePool,
		models.JobTypeDeleteVolume,
		models.JobTypeDeleteLargeVolume,
		models.JobTypeUpdateVolume,
		models.JobTypeCreateVolumeReplication,
		models.JobTypeDeleteVolumeReplication,
		models.JobTypeCreateBackup,
		models.JobTypeDeleteBackup,
		models.JobTypeCreateSnapshot,
		models.JobTypeDeleteSnapshot,
		models.JobTypeCreateKmsConfig,
		models.JobTypeSdeKmsCreate,
		models.JobTypeDeleteKmsConfig,
		models.JobTypeMigrateKmsConfig,
		models.JobTypeRestoreBackup,
		models.JobTypeCreateActiveDirectory,
	}

	for _, jt := range unhandledJobTypes {
		t.Run(string(jt), func(t *testing.T) {
			timeout := getWorkflowTimeoutForJobType(string(jt))
			assert.Equal(t, globalTimeout, timeout,
				"job type %s is not handled in the switch and should fall back to global timeout (%v), got %v",
				jt, globalTimeout, timeout,
			)
		})
	}
}

func TestFilterEligibleProcessingJobTypes(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name: "filters to only eligible delete types",
			input: []string{
				string(models.JobTypeCreatePool),
				string(models.JobTypeDeletePool),
				string(models.JobTypeCreateKmsConfig),
				string(models.JobTypeDeleteVolume),
				string(models.JobTypeCreateBackup),
			},
			expected: []string{
				string(models.JobTypeDeletePool),
				string(models.JobTypeDeleteVolume),
			},
		},
		{
			name: "all pool and volume delete types pass",
			input: []string{
				string(models.JobTypeDeletePool),
				string(models.JobTypeDeleteLargePool),
				string(models.JobTypeDeleteVolume),
				string(models.JobTypeDeleteLargeVolume),
			},
			expected: []string{
				string(models.JobTypeDeletePool),
				string(models.JobTypeDeleteLargePool),
				string(models.JobTypeDeleteVolume),
				string(models.JobTypeDeleteLargeVolume),
			},
		},
		{
			name: "create types are not eligible",
			input: []string{
				string(models.JobTypeCreatePool),
				string(models.JobTypeCreateLargePool),
				string(models.JobTypeCreateVolume),
				string(models.JobTypeCreateLargeVolume),
				string(models.JobTypeCreateKmsConfig),
				string(models.JobTypeCreateBackup),
				string(models.JobTypeCreateSnapshot),
			},
			expected: nil,
		},
		{
			name:     "empty input returns nil",
			input:    []string{},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterEligibleProcessingJobTypes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestScanProcessingStateTimeouts_SkipsIneligibleJobTypes(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	// Register only CMEK handler (not eligible for PROCESSING scan)
	handler := newTestHandler(models.JobTypeCreateKmsConfig)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}
	runner.registerHandlers(handler)

	// No DB call should be made since CMEK is not eligible
	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeCreateKmsConfig)})

	storage.AssertNotCalled(t, "GetJobsWithCondition", mock.Anything, mock.Anything)
}

func TestScanProcessingStateTimeouts_UsesJobSpecificTimeouts(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}
	runner.registerHandlers(handler)

	// Job created 50 minutes ago - should NOT be evaluated because
	// DELETE_POOL timeout is ~60 minutes (global) + 5 min grace
	recentJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID:      "recent-job",
			CreatedAt: time.Now().Add(-50 * time.Minute),
		},
		WorkflowID: "wf-recent",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
	}

	// Job created 4 hours ago - should be evaluated because
	// it exceeds the 60 minute timeout + grace period
	oldJob := &datamodel.Job{
		BaseModel: datamodel.BaseModel{
			UUID:      "old-job",
			CreatedAt: time.Now().Add(-4 * time.Hour),
		},
		WorkflowID: "wf-old",
		Type:       string(models.JobTypeDeletePool),
		State:      string(models.JobsStatePROCESSING),
	}

	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.MatchedBy(func(filter dbutils.Filter) bool {
		for _, condition := range filter.Conditions {
			if condition.Field == "state" && condition.Value == string(models.JobsStatePROCESSING) {
				return true
			}
		}
		return false
	})).Return([]*datamodel.Job{recentJob, oldJob}, nil).Once()

	// Only the old job should trigger a DescribeWorkflowExecution call
	temporal.EXPECT().DescribeWorkflowExecution(mock.Anything, oldJob.WorkflowID, "").Return(&workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}, nil).Once()

	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeDeletePool)})

	storage.AssertExpectations(t)
	temporal.AssertExpectations(t)
}

func TestScanProcessingStateTimeouts_DisabledByFlag(t *testing.T) {
	processingTimeoutEnabled = false
	t.Cleanup(func() { processingTimeoutEnabled = true })
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	handler := newTestHandler(models.JobTypeDeletePool)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "corr-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}
	runner.registerHandlers(handler)

	runner.scanProcessingStateTimeouts(context.Background(), []string{string(models.JobTypeDeletePool)})

	storage.AssertNotCalled(t, "GetJobsWithCondition", mock.Anything, mock.Anything)
}
