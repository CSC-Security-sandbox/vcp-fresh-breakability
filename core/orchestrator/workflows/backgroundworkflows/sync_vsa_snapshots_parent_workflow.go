package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/workflow"
)

var (
	// SnapshotSyncParentWorkflowBatchSize is the number of pools each child workflow processes
	SnapshotSyncParentWorkflowBatchSize = env.GetInt("SNAPSHOT_SYNC_PARENT_WORKFLOW_BATCH_SIZE", 1000)
	// SnapshotSyncChildWorkflowTimeout is the timeout for each child workflow
	SnapshotSyncChildWorkflowTimeout = time.Duration(env.GetInt("SNAPSHOT_SYNC_CHILD_WORKFLOW_TIMEOUT_MINUTES", 10)) * time.Minute
)

// SnapshotsSyncParentWorkflow coordinates multiple child workflows to process all pools
func SnapshotsSyncParentWorkflow(ctx workflow.Context) (*GenericParentWorkflowResult, error) {
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	config := GenericParentWorkflowConfig{
		WorkflowName:          "snapshots-sync-parent",
		BatchSize:             SnapshotSyncParentWorkflowBatchSize,
		ChildWorkflowTimeout:  SnapshotSyncChildWorkflowTimeout,
		GetTotalCountActivity: syncSnapshotActivity.GetTotalPoolCount,
		ChildWorkflowFunc:     SnapshotsSyncChildWorkflow,
	}

	return GenericParentWorkflow(ctx, config)
}
