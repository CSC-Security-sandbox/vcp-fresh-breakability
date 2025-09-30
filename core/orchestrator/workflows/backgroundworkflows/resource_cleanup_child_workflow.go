package backgroundworkflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/workflow"
)

var (
	// ResourceCleanupChildWorkflowActivityBatchSize is the number of pools to process in each activity call
	ResourceCleanupChildWorkflowActivityBatchSize = env.GetInt("RESOURCE_CLEANUP_CHILD_WORKFLOW_ACTIVITY_BATCH_SIZE", 25)
	// ResourceCleanupChildWorkflowActivityTimeoutMinutes is the timeout for batch activities in minutes
	ResourceCleanupChildWorkflowActivityTimeoutMinutes = env.GetInt("RESOURCE_CLEANUP_CHILD_WORKFLOW_ACTIVITY_TIMEOUT_MINUTES", 60)
	// ResourceCleanupMaxConcurrentActivitiesPerChild is the maximum number of concurrent activities per child workflow
	ResourceCleanupMaxConcurrentActivitiesPerChild = env.GetInt("RESOURCE_CLEANUP_MAX_CONCURRENT_ACTIVITIES_PER_CHILD", 5)
)

// ResourceCleanupChildWorkflow processes a specific range of resources with chunked fetching
func ResourceCleanupChildWorkflow(ctx workflow.Context, offset, limit int) (*GenericChildWorkflowResult, error) {
	resourceDeleteActivity := &backgroundactivities.ResourceDeleteActivity{}

	config := GenericChildWorkflowConfig{
		WorkflowName:            "resource-cleanup-child",
		ActivityBatchSize:       ResourceCleanupChildWorkflowActivityBatchSize,
		MaxConcurrentActivities: ResourceCleanupMaxConcurrentActivitiesPerChild,
		ActivityTimeoutMinutes:  ResourceCleanupChildWorkflowActivityTimeoutMinutes,
		ListDataActivity:        resourceDeleteActivity.ListResourcesPaginated,
		ProcessBatchActivity:    resourceDeleteActivity.CleanupPendingResources,
	}

	// Call the generic child workflow
	genericResult, err := GenericChildWorkflow(ctx, offset, limit, config)
	if err != nil {
		return nil, err
	}

	// Return the generic result directly
	return genericResult, nil
}
