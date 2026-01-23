package common

import (
	"fmt"

	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/workflow"
)

// ExecuteDeferredCleanup handles the common defer block pattern for create workflows.
// It checks for errors or cancellation and executes rollback with appropriate error handling.
// Parameters:
//   - ctx: The workflow context
//   - cancellationHandler: The cancellation handler to check for cancellation
//   - rollbackManager: The rollback manager to execute rollback
//   - err: The error from the workflow (can be nil)
//   - logger: Optional logger. If nil, logger will be obtained from context using util.GetLogger
//   - resourceType: The type of resource being created (e.g., "volume", "pool", "active directory")
//   - resourceUUID: The UUID of the resource being created
//   - updateErrorState: Optional callback function to execute when there's an error (not cancellation).
//     This is typically used to update resource state in DB before rollback.
//   - onCancellationCallback: Optional callback function to execute when cancellation is detected,
//     before executing rollback. This allows adding activities or performing cleanup before rollback.
//   - shouldRollbackOnError: Optional callback function that returns whether to execute rollback on error.
//     If nil, rollback will always be executed on error. If provided, rollback is only executed if it returns true.
func ExecuteDeferredCleanup(ctx workflow.Context, cancellationHandler *WorkflowCancellationHandler, rollbackManager *RollbackManager, err error, logger log.Logger, resourceType string, resourceUUID string, updateErrorState func(ctx workflow.Context) error, onCancellationCallback func(ctx workflow.Context, cancelErr error), shouldRollbackOnError func(ctx workflow.Context, err error) bool) {
	if err == nil && !cancellationHandler.IsCancelled() {
		return
	}
	disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
	if logger == nil {
		logger = util.GetLogger(ctx)
	}

	if cancellationHandler.IsCancelled() {
		logger.Infof("%s creation cancelled, executing rollback: %s", resourceType, resourceUUID)
		cancelErr := vsaerrors.New(fmt.Sprintf("%s creation cancelled by delete request", resourceType))
		if onCancellationCallback != nil {
			onCancellationCallback(disconnectedCtx, cancelErr)
		}
		rollbackManager.ExecuteRollback(disconnectedCtx, cancelErr)
		return
	}

	// Handle error case
	if err != nil {
		// Check if rollback should be executed
		if shouldRollbackOnError != nil {
			if !shouldRollbackOnError(disconnectedCtx, err) {
				return
			}
		}
		// Update error state if callback provided
		if updateErrorState != nil {
			_ = updateErrorState(disconnectedCtx)
		}
		rollbackManager.ExecuteRollback(disconnectedCtx, err)
	}
}
