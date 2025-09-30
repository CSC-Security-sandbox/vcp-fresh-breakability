package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"go.temporal.io/sdk/workflow"
)

var (
	// ResourceCleanupParentWorkflowBatchSize is the number of resources each child workflow processes
	ResourceCleanupParentWorkflowBatchSize = env.GetInt("RESOURCE_CLEANUP_PARENT_WORKFLOW_BATCH_SIZE", 100)
	// ResourceCleanupChildWorkflowTimeout is the timeout for each child workflow
	ResourceCleanupChildWorkflowTimeout = time.Duration(env.GetInt("RESOURCE_CLEANUP_CHILD_WORKFLOW_TIMEOUT_MINUTES", 240)) * time.Minute
)

// ResourceCleanupParentWorkflow coordinates multiple child workflows to process all pending resource deletions
func ResourceCleanupParentWorkflow(ctx workflow.Context) (*GenericParentWorkflowResult, error) {
	resourceDeleteActivity := &backgroundactivities.ResourceDeleteActivity{}

	config := GenericParentWorkflowConfig{
		WorkflowName:          "resource-cleanup",
		BatchSize:             ResourceCleanupParentWorkflowBatchSize,
		ChildWorkflowTimeout:  ResourceCleanupChildWorkflowTimeout,
		GetTotalCountActivity: resourceDeleteActivity.GetTotalResourceCount,
		ChildWorkflowFunc:     ResourceCleanupChildWorkflow,
	}

	return GenericParentWorkflow(ctx, config)
}
