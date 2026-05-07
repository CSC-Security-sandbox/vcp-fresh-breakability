package detectors

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/backgroundworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// temporalCCFEPoolFetcher is the production CCFEPoolFetcher used by
// leakedresources.Run. Each FetchCCFEPools call kicks off a fresh
// FetchCCFEPoolsWorkflow on the background task queue (one workflow per
// VCP account, fanning the per-location CCFE list calls out as parallel
// activities) and blocks until it returns, giving the pool detector live
// CCFE data on every tick — the previous 6h-cron clh_resources cache has
// been removed.
type temporalCCFEPoolFetcher struct {
	client client.Client
}

// NewTemporalCCFEPoolFetcher returns a Temporal-backed CCFEPoolFetcher.
// The supplied client is the same client that core constructs at startup
// (see core/app.go) and shares with the rest of the orchestrator.
func NewTemporalCCFEPoolFetcher(c client.Client) CCFEPoolFetcher {
	return &temporalCCFEPoolFetcher{client: c}
}

// FetchCCFEPools starts FetchCCFEPoolsWorkflow synchronously for one
// project across the given locations and returns the per-location pool
// snapshot map. A nil client (e.g. in tests that didn't wire a real
// Temporal connection) yields (nil, error) instead of panicking; the
// detector treats that as "skip every location of this project" rather
// than false-flagging VCP rows.
//
// Per-location activity failures inside the workflow are absorbed there
// (the location is omitted from the returned map) so a single transient
// CCFE error doesn't poison the whole project's diff.
func (f *temporalCCFEPoolFetcher) FetchCCFEPools(ctx context.Context, projectID string, locations []string) (map[string][]poolpairs.CachedPool, error) {
	logger := util.GetLogger(ctx)
	if f.client == nil {
		return nil, fmt.Errorf("temporal client is not configured")
	}
	if len(locations) == 0 {
		return map[string][]poolpairs.CachedPool{}, nil
	}

	opts := client.StartWorkflowOptions{
		TaskQueue:                temporalutils.BackgroundTaskQueue,
		ID:                       fmt.Sprintf("fetch-ccfe-pools-%s-%s", projectID, utils.RandomUUID()),
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:       temporalutils.GetWorkflowGlobalTimeout(),
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	run, err := f.client.ExecuteWorkflow(ctx, opts, backgroundworkflows.FetchCCFEPoolsWorkflow, projectID, locations)
	if err != nil {
		logger.Warnf("temporalCCFEPoolFetcher: ExecuteWorkflow failed project=%s locations=%d: %v", projectID, len(locations), err)
		return nil, err
	}

	var poolsByLocation map[string][]poolpairs.CachedPool
	if err := run.Get(ctx, &poolsByLocation); err != nil {
		logger.Warnf("temporalCCFEPoolFetcher: workflow Get failed project=%s locations=%d: %v", projectID, len(locations), err)
		return nil, err
	}
	return poolsByLocation, nil
}
