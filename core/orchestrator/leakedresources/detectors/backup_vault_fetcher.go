package detectors

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/leakedresources/resourcescope"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/backgroundworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	temporalutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// CCFEBackupVaultFetcher returns the live CCFE backup vault snapshot for
// one project across multiple locations in a single call. Production
// wires this to a Temporal client that synchronously executes
// FetchCCFEBackupVaultsWorkflow on the background task queue (which fans
// the per-location CCFE list calls out as parallel activities); tests
// substitute an in-process fake.
//
// The returned map's keys are the locations the workflow successfully
// fetched. A missing key signals "fetch failed for this location after
// retries" — the detector skips the diff for that pair so a transient
// CCFE outage cannot false-flag every VCP vault as a leak.
type CCFEBackupVaultFetcher interface {
	FetchCCFEBackupVaults(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedBackupVault, error)
}

// temporalCCFEBackupVaultFetcher is the production CCFEBackupVaultFetcher.
// Each call kicks off a fresh FetchCCFEBackupVaultsWorkflow on the
// background task queue and blocks until it returns.
type temporalCCFEBackupVaultFetcher struct {
	client client.Client
}

// NewTemporalCCFEBackupVaultFetcher returns a Temporal-backed
// CCFEBackupVaultFetcher. The supplied client is the same client that
// core constructs at startup (see core/app.go).
func NewTemporalCCFEBackupVaultFetcher(c client.Client) CCFEBackupVaultFetcher {
	return &temporalCCFEBackupVaultFetcher{client: c}
}

// FetchCCFEBackupVaults starts FetchCCFEBackupVaultsWorkflow
// synchronously for one project across the given locations and returns
// the per-location vault snapshot map.
func (f *temporalCCFEBackupVaultFetcher) FetchCCFEBackupVaults(ctx context.Context, projectID string, locations []string) (map[string][]resourcescope.CachedBackupVault, error) {
	if f.client == nil {
		return nil, fmt.Errorf("temporal client is not configured")
	}
	if len(locations) == 0 {
		return map[string][]resourcescope.CachedBackupVault{}, nil
	}

	opts := client.StartWorkflowOptions{
		TaskQueue:                temporalutils.BackgroundTaskQueue,
		ID:                       fmt.Sprintf("fetch-ccfe-backup-vaults-%s-%s", projectID, utils.RandomUUID()),
		WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		WorkflowRunTimeout:       temporalutils.GetWorkflowGlobalTimeout(),
		WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
	}

	run, err := f.client.ExecuteWorkflow(ctx, opts, backgroundworkflows.FetchCCFEBackupVaultsWorkflow, projectID, locations)
	if err != nil {
		return nil, err
	}

	var vaultsByLocation map[string][]resourcescope.CachedBackupVault
	if err := run.Get(ctx, &vaultsByLocation); err != nil {
		return nil, err
	}
	return vaultsByLocation, nil
}
