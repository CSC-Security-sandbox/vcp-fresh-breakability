// Package tasks provides background job supervisors and supporting helpers that
// coordinate Temporal workflow cleanup when jobs stall or time out.
package tasks

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	supervisorhandler "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/tasks/supervisor-handler"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	temporalDescribeTimeout  = 15 * time.Second
	resourceCleanupTimeout   = 30 * time.Second
	temporalTerminateTimeout = 10 * time.Second
)

var (
	// WorkflowSupervisorTask is the entry point used by cron/background runners.
	WorkflowSupervisorTask = runWorkflowSupervisorTask
	// workflowNotFoundGracePeriod controls how long the supervisor waits before
	// treating a missing Temporal workflow as an error condition.
	workflowNotFoundGracePeriod = env.GetDuration("WORKFLOW_SUPERVISOR_NOT_FOUND_GRACE_PERIOD", 5*time.Minute)
)

type workflowSupervisorTaskRunner struct {
	storage       database.Storage
	temporal      client.Client
	correlationID string

	handlers   map[string]supervisorhandler.Handler
	handlersMu sync.RWMutex
}

// runWorkflowSupervisorTask is the cron entry point that scans for stalled jobs
// and delegates cleanup to registered supervisor handlers.
func runWorkflowSupervisorTask(ctx context.Context, storage database.Storage, temporal client.Client, correlationID string, handlers ...supervisorhandler.Handler) {
	loggerFields := log.Fields{string(middleware.RequestCorrelationID): correlationID}

	ctx = context.WithValue(ctx, middleware.CorrelationContextKey, correlationID)
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)

	logger := util.GetLogger(ctx)
	logger.Infof("[WorkflowSupervisorTask] Starting workflow supervisor task - CorrelationID: %s", correlationID)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: correlationID,
		handlers:      make(map[string]supervisorhandler.Handler),
	}

	if len(handlers) == 0 {
		handlers = append(
			handlers,
			supervisorhandler.NewCmekHandler(),
			supervisorhandler.NewPoolHandler(),
			supervisorhandler.NewVolumeHandler(),
			supervisorhandler.NewBackupHandler(),
			supervisorhandler.NewSnapshotHandler(),
			supervisorhandler.NewReplicationHandler(),
			supervisorhandler.NewNetworkHandler(),
		)
	}

	runner.registerHandlers(handlers...)
	runner.scan(ctx)
}

// registerHandlers installs one or more handler implementations keyed by their
// advertised job types.
func (r *workflowSupervisorTaskRunner) registerHandlers(handlers ...supervisorhandler.Handler) {
	r.handlersMu.Lock()
	defer r.handlersMu.Unlock()

	for _, handler := range handlers {
		if handler == nil {
			continue
		}
		for _, jobType := range handler.JobTypes() {
			r.handlers[string(jobType)] = handler
		}
	}
}

// handlerFor retrieves the handler registered for the supplied job type.
func (r *workflowSupervisorTaskRunner) handlerFor(jobType string) (supervisorhandler.Handler, bool) {
	r.handlersMu.RLock()
	handler, ok := r.handlers[jobType]
	r.handlersMu.RUnlock()
	return handler, ok
}

// supportedJobTypes returns the set of job type identifiers known to the runner.
func (r *workflowSupervisorTaskRunner) supportedJobTypes() []string {
	r.handlersMu.RLock()
	defer r.handlersMu.RUnlock()

	jobTypes := make([]string, 0, len(r.handlers))
	for jobType := range r.handlers {
		jobTypes = append(jobTypes, jobType)
	}
	return jobTypes
}

// scan locates timed-out jobs and invokes the registered handler for each.
func (r *workflowSupervisorTaskRunner) scan(ctx context.Context) {
	logger := util.GetLogger(ctx).With(log.Fields{string(middleware.RequestCorrelationID): r.correlationID})
	jobTypes := r.supportedJobTypes()
	if len(jobTypes) == 0 {
		logger.Debug("workflow-supervisor-task: no handlers registered")
		return
	}

	cutoffTimestamp := time.Now().Add(-workflowNotFoundGracePeriod).UTC().Format(time.RFC3339)
	filter := dbutils.CreateFilterWithConditions(
		dbutils.NewFilterCondition("state", "=", string(models.JobsStateNEW)),
		dbutils.NewFilterCondition("type", "IN", jobTypes),
		dbutils.NewFilterCondition("created_at", "<=", cutoffTimestamp),
	)

	jobs, err := r.storage.GetJobsWithCondition(ctx, *filter)

	if err != nil {
		logger.Errorf("workflow-supervisor-task: failed to fetch jobs: %v", err)
		return
	}

	if len(jobs) == 0 {
		logger.Infof("workflow-supervisor-task: no candidate jobs found")
		return
	}

	sort.SliceStable(jobs, func(i, j int) bool {
		return jobs[i].ID < jobs[j].ID
	})

	now := time.Now().UTC()
	for _, job := range jobs {
		if skip, resumeAt, grace := shouldSkipJobForOverrideGracePeriod(job, now); skip {
			logger.With(log.Fields{
				"jobUUID":      job.UUID,
				"jobType":      job.Type,
				"createdAt":    job.CreatedAt.UTC(),
				"resumeAt":     resumeAt,
				"overrideWait": grace.String(),
			}).Info("workflow-supervisor-task: deferring job due to override grace period")
			continue
		}

		handler, ok := r.handlerFor(job.Type)
		if !ok {
			logger.With(log.Fields{"jobUUID": job.UUID, "jobType": job.Type}).Warn(
				"workflow-supervisor-task: no handler registered for job type",
			)
			continue
		}
		logger.Infof("workflow-supervisor-task: processing job type %s", job.Type)
		r.evaluateJob(ctx, job, handler)
	}
}

// evaluateJob inspects Temporal state for a job and triggers cleanup when
// timeout conditions are met.
func (r *workflowSupervisorTaskRunner) evaluateJob(ctx context.Context, job *datamodel.Job, handler supervisorhandler.Handler) {
	describeCtx, cancel := context.WithTimeout(ctx, temporalDescribeTimeout)
	defer cancel()

	logger := util.GetLogger(ctx).With(log.Fields{
		"jobUUID":                               job.UUID,
		"workflowID":                            job.WorkflowID,
		"jobType":                               job.Type,
		"jobState":                              job.State,
		string(middleware.RequestCorrelationID): r.correlationID,
	})

	resp, err := r.temporal.DescribeWorkflowExecution(describeCtx, job.WorkflowID, "")
	if err != nil {
		logger.Errorf("workflow-supervisor-task: temporal describe failed; starting cleanup: %v", err)
		r.cleanupJob(ctx, job, handler, supervisorhandler.EventTimeout, logger)
		return
	}

	if resp == nil || resp.WorkflowExecutionInfo == nil {
		logger.Warnf("workflow-supervisor-task: describe response missing execution info")
		return
	}

	status := resp.WorkflowExecutionInfo.GetStatus()
	logger.Infof("workflow-supervisor-task: workflow: %s status: %s", job.WorkflowID, status)
	if status != enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT {
		logger.Debugf("workflow-supervisor-task: unsupported workflow status=%s; ignoring", status.String())
		return
	}

	logger.Warnf("workflow-supervisor-task: detected timed-out workflow; starting cleanup")

	r.cleanupJob(ctx, job, handler, supervisorhandler.EventTimeout, logger)
}

// markJobAsError updates the job state and detail to record a timeout failure.
func (r *workflowSupervisorTaskRunner) markJobAsError(ctx context.Context, job *datamodel.Job) error {
	return r.storage.UpdateJob(ctx, job.UUID, string(models.JobsStateERROR), job.TrackingID, supervisorhandler.WorkflowTimeoutDetail)
}

// cleanupJob terminates the workflow if needed, delegates compensating actions,
// and marks the job as failed.
func (r *workflowSupervisorTaskRunner) cleanupJob(ctx context.Context, job *datamodel.Job, handler supervisorhandler.Handler, event supervisorhandler.Event, logger log.Logger) {
	if r.temporal != nil && job.WorkflowID != "" {
		lockErr := r.storage.WithTransaction(ctx, func(tx dbutils.Transaction) error {
			var dbJob datamodel.Job
			if err := tx.GORM().Clauses(clause.Locking{Strength: "UPDATE"}).
				Where("uuid = ? AND state = ?", job.UUID, string(models.JobsStateNEW)).
				First(&dbJob).Error; err != nil {
				return err
			}

			terminateCtx, cancelTerminate := context.WithTimeout(ctx, temporalTerminateTimeout)
			defer cancelTerminate()

			if err := r.temporal.TerminateWorkflow(terminateCtx, job.WorkflowID, "", supervisorhandler.WorkflowTimeoutDetail); err != nil {
				var notFound *serviceerror.NotFound
				if errors.As(err, &notFound) {
					logger.Debugf("workflow-supervisor-task: terminate skipped; workflow already missing")
				}
			}
			return nil
		})

		if lockErr != nil {
			if errors.Is(lockErr, gorm.ErrRecordNotFound) {
				logger.Debugf("workflow-supervisor-task: job missing while acquiring lock; skipping terminate and cleanup")
			} else {
				logger.Warnf("workflow-supervisor-task: failed to lock job for terminate: %v", lockErr)
			}
			return
		}
	}

	cleanupCtx, cancelCleanup := context.WithTimeout(ctx, resourceCleanupTimeout)
	defer cancelCleanup()

	if err := handler.Handle(cleanupCtx, job, event, r.storage); err != nil {
		logger.Errorf("workflow-supervisor-task: cleanup failed: %v", err)
		return
	}

	if err := r.markJobAsError(ctx, job); err != nil {
		logger.Errorf("workflow-supervisor-task: failed to mark job error after cleanup: %v", err)
	}
}

// shouldSkipJobForOverrideGracePeriod reports whether a job should be deferred
// based on the override grace period attribute.
func shouldSkipJobForOverrideGracePeriod(job *datamodel.Job, now time.Time) (bool, time.Time, time.Duration) {
	if job == nil || job.JobAttributes == nil || job.JobAttributes.SupervisorAttributes == nil {
		return false, time.Time{}, 0
	}

	grace := job.JobAttributes.SupervisorAttributes.OverrideGracePeriod
	if grace <= 0 {
		return false, time.Time{}, 0
	}

	createdAt := job.CreatedAt
	if createdAt.IsZero() {
		return false, time.Time{}, 0
	}

	resumeAt := createdAt.Add(grace)
	if now.Before(resumeAt) {
		return true, resumeAt, grace
	}

	return false, time.Time{}, 0
}
