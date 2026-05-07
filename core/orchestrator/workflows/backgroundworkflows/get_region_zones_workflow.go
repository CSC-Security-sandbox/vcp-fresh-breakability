package backgroundworkflows

import (
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	getRegionZonesWorkflowStartToCloseTimeoutSec = env.GetUint64("GET_REGION_ZONES_WORKFLOW_START_TO_CLOSE_TIMEOUT_SEC", 120)
	getRegionZonesWorkflowHeartbeatTimeoutSec    = env.GetUint64("GET_REGION_ZONES_WORKFLOW_HEARTBEAT_TIMEOUT_SEC", 60)
	getRegionZonesActivityMaxAttempts            = env.GetInt("GET_REGION_ZONES_ACTIVITY_MAX_ATTEMPTS", 3)
)

// GetRegionZonesWorkflow returns the list of zones in region as reported
// by GCP compute.zones.list. It is invoked synchronously by core during
// the leaked-resources pool detector run so the detector knows which
// zones it should fan FetchStoragePoolsWorkflow out to per VCP account
// (independent of which zones currently happen to have VCP pools).
//
// The workflow is intentionally thin — one activity call — so the bulk of
// the logic (control-plane project resolution, retry, error handling)
// lives in GetRegionZonesActivity and stays unit-testable without
// spinning up Temporal.
func GetRegionZonesWorkflow(ctx workflow.Context, region string) ([]string, error) {
	requestID := utils.RandomUUID()
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{
		"workflowID":                            workflow.GetInfo(ctx).WorkflowExecution.ID,
		"requestID":                             requestID,
		string(middleware.RequestCorrelationID): requestID,
		"region":                                region,
	})
	logger := util.GetLogger(ctx)

	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(getRegionZonesWorkflowStartToCloseTimeoutSec) * time.Second,
		HeartbeatTimeout:    time.Duration(getRegionZonesWorkflowHeartbeatTimeoutSec) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(getRegionZonesActivityMaxAttempts),
			NonRetryableErrorTypes: []string{"PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	act := &backgroundactivities.GetRegionZonesActivity{}

	var zones []string
	if err := workflow.ExecuteActivity(ctx, act.GetRegionZones, region).Get(ctx, &zones); err != nil {
		logger.Errorf("GetRegionZonesWorkflow: GetRegionZones failed region=%s: %v", region, err)
		return nil, err
	}
	logger.Infof("GetRegionZonesWorkflow: completed region=%s zone_count=%d", region, len(zones))
	return zones, nil
}
