package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	fetchCCFEPoolsActivityStartToCloseTimeoutSec = env.GetUint64("FETCH_CCFE_POOLS_ACTIVITY_START_TO_CLOSE_TIMEOUT_SEC", 300)
	fetchCCFEPoolsActivityHeartbeatTimeoutSec    = env.GetUint64("FETCH_CCFE_POOLS_ACTIVITY_HEARTBEAT_TIMEOUT_SEC", 120)
	fetchCCFEPoolsActivityMaxAttempts            = env.GetInt("FETCH_CCFE_POOLS_ACTIVITY_MAX_ATTEMPTS", 3)
)

// FetchCCFEPoolsWorkflow returns the live CCFE pool snapshot for a single
// project across multiple locations. It fans out one activity per location
// in parallel (each FetchStoragePoolsActivity.FetchStoragePools call hits
// the CCFE list endpoint for its (projectID, location) pair) and
// aggregates the per-location results into a map keyed by location.
//
// Invoked synchronously by core's leaked-resources pool detector once per
// VCP account per tick (instead of once per (project, location) pair) so
// CCFE data is always fresh and the Temporal UI shows a single workflow
// run per project. The previous cron-driven cache (clh_resources /
// SyncCLHResourcesWorkflow) has been removed entirely.
//
// Per-location failures are tolerated: a location whose activity exhausts
// its retries is logged and omitted from the returned map. The caller
// treats a missing key the same way it treats a CCFE-disabled (nil)
// response: skip the diff for that pair, do not false-flag every VCP pool
// as in_vcp_not_in_ccfe. The workflow itself only returns an overall
// error for catastrophic conditions (Temporal infra failures, retry
// policy population failures) — the typical "one location failed" case
// is reported via map cardinality, not via error.
func FetchCCFEPoolsWorkflow(ctx workflow.Context, projectID string, locations []string) (map[string][]poolpairs.CachedPool, error) {
	requestID := utils.RandomUUID()
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID":                            workflow.GetInfo(ctx).WorkflowExecution.ID,
		"requestID":                             requestID,
		string(middleware.RequestCorrelationID): requestID,
		"projectID":                             projectID,
		"locationCount":                         len(locations),
	})
	logger := util.GetLogger(ctx)

	if len(locations) == 0 {
		logger.Infof("FetchCCFEPoolsWorkflow: no locations supplied; returning empty map projectID=%s", projectID)
		return map[string][]poolpairs.CachedPool{}, nil
	}

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(fetchCCFEPoolsActivityStartToCloseTimeoutSec) * time.Second,
		HeartbeatTimeout:    time.Duration(fetchCCFEPoolsActivityHeartbeatTimeoutSec) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(fetchCCFEPoolsActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	act := &backgroundactivities.FetchStoragePoolsActivity{}

	// Fire one activity per location concurrently. ExecuteActivity returns
	// immediately with a Future the workflow Get()s on later — Temporal
	// schedules the activities in parallel on the worker side as long as
	// task-queue concurrency allows. Each activity has its own retry
	// policy (defined above), so a transient failure on one location does
	// not stall any of the others.
	futures := make([]workflow.Future, len(locations))
	for i, loc := range locations {
		futures[i] = workflow.ExecuteActivity(ctx, act.FetchStoragePools, projectID, loc)
	}

	// Aggregate. Successful activities (including ones that returned
	// (nil, nil) to signal "CCFE disabled") populate the map; activities
	// that exhausted retries are logged and dropped from the map so the
	// caller can distinguish "fetch failed for this location" from
	// "fetch succeeded with empty list".
	result := make(map[string][]poolpairs.CachedPool, len(locations))
	failedCount := 0
	for i, f := range futures {
		var pools []poolpairs.CachedPool
		if err := f.Get(ctx, &pools); err != nil {
			logger.Warnf("FetchCCFEPoolsWorkflow: activity failed projectID=%s location=%s after retries: %v",
				projectID, locations[i], err)
			failedCount++
			continue
		}
		result[locations[i]] = pools
	}

	logger.Infof("FetchCCFEPoolsWorkflow: completed projectID=%s locations=%d successful=%d failed=%d",
		projectID, len(locations), len(result), failedCount)
	return result, nil
}
