package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/workflow"
)

var (
	// SnapshotSyncChildWorkflowActivityBatchSize is the number of pools to process in each activity call
	SnapshotSyncChildWorkflowActivityBatchSize = env.GetInt("SNAPSHOT_SYNC_CHILD_WORKFLOW_ACTIVITY_BATCH_SIZE", 250)
	// SnapshotSyncChildWorkflowActivityTimeoutMinutes is the timeout for batch activities in minutes
	SnapshotSyncChildWorkflowActivityTimeoutMinutes = env.GetInt("SNAPSHOT_SYNC_CHILD_WORKFLOW_ACTIVITY_TIMEOUT_MINUTES", 10)
	// SnapshotSyncMaxConcurrentActivitiesPerChild is the maximum number of concurrent activities per child workflow
	SnapshotSyncMaxConcurrentActivitiesPerChild = env.GetInt("SNAPSHOT_SYNC_MAX_CONCURRENT_ACTIVITIES_PER_CHILD", 20)
)

// SnapshotsSyncChildWorkflow processes a specific range of pools with chunked fetching
func SnapshotsSyncChildWorkflow(ctx workflow.Context, offset, limit int) (*GenericChildWorkflowResult, error) {
	syncSnapshotActivity := &backgroundactivities.SyncSnapshotActivity{}

	config := GenericChildWorkflowConfig{
		WorkflowName:            "snapshots-sync-child",
		ActivityBatchSize:       SnapshotSyncChildWorkflowActivityBatchSize,
		MaxConcurrentActivities: SnapshotSyncMaxConcurrentActivitiesPerChild,
		ActivityTimeoutMinutes:  SnapshotSyncChildWorkflowActivityTimeoutMinutes,
		ListDataActivity:        syncSnapshotActivity.ListPoolsUUIDPaginated,
		ProcessBatchActivity:    syncSnapshotActivity.SyncSnapshotsForPoolBatchActivity,
	}

	// Call the generic child workflow
	genericResult, err := GenericChildWorkflow(ctx, offset, limit, config)
	if err != nil {
		return nil, err
	}

	// Return the generic result directly
	return genericResult, nil
}
