package detectors

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/poolpairs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/backgroundworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// temporalZoneFetcher is the production poolpairs.ZoneFetcher used by
// leakedresources.Run. Each GetRegionZones call kicks off a fresh
// GetRegionZonesWorkflow on the background task queue and blocks until it
// returns, so the GCP compute.zones.list call stays on the worker (where
// GCP credentials live) and core only ever holds the Temporal client.
type temporalZoneFetcher struct {
	client client.Client
}

// NewTemporalZoneFetcher returns a Temporal-backed ZoneFetcher. The
// supplied client is the same one core constructs at startup (see
// core/app.go) and shares with the rest of the orchestrator.
func NewTemporalZoneFetcher(c client.Client) poolpairs.ZoneFetcher {
	return &temporalZoneFetcher{client: c}
}

// GetRegionZones starts GetRegionZonesWorkflow synchronously and returns
// its result. A nil client (e.g. in tests that didn't wire a real Temporal
// connection) yields (nil, error) instead of panicking, so the caller
// gracefully falls back to region-only enumeration.
func (f *temporalZoneFetcher) GetRegionZones(ctx context.Context, region string) ([]string, error) {
	if f.client == nil {
		return nil, fmt.Errorf("temporal client is not configured")
	}

	opts := client.StartWorkflowOptions{
		TaskQueue:                temporalutils.BackgroundTaskQueue,
		ID:                       fmt.Sprintf("get-region-zones-%s-%s", region, utils.RandomUUID()),
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:       temporalutils.GetWorkflowGlobalTimeout(),
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	run, err := f.client.ExecuteWorkflow(ctx, opts, backgroundworkflows.GetRegionZonesWorkflow, region)
	if err != nil {
		return nil, err
	}

	var zones []string
	if err := run.Get(ctx, &zones); err != nil {
		return nil, err
	}
	return zones, nil
}

// projectLocationLister is the production ProjectLocationLister: it pairs
// the storage handle (for ListAccountsForTelemetry) with a Temporal-backed
// ZoneFetcher (for GetRegionZonesWorkflow) and forwards both to
// poolpairs.EnumerateProjectLocationKeys. Lives next to temporalZoneFetcher
// so the wiring all stays in one file.
type projectLocationLister struct {
	storage     database.Storage
	zoneFetcher poolpairs.ZoneFetcher
}

// NewProjectLocationLister returns the production lister. It defers all
// decision-making (region resolution, zones lookup, deduplication) to
// poolpairs.EnumerateProjectLocationKeys so the detector itself stays free
// of env / GCP concerns.
func NewProjectLocationLister(storage database.Storage, zoneFetcher poolpairs.ZoneFetcher) ProjectLocationLister {
	return &projectLocationLister{storage: storage, zoneFetcher: zoneFetcher}
}

// ListProjectLocations implements ProjectLocationLister.
func (l *projectLocationLister) ListProjectLocations(ctx context.Context) ([]poolpairs.PoolProjectLocation, error) {
	return poolpairs.EnumerateProjectLocationKeys(ctx, l.storage, l.zoneFetcher)
}
