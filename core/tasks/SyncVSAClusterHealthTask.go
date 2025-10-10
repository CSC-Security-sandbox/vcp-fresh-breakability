package tasks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/inmemotasksprocessor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	ontapRest "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// Default takeover reasons that require ephemeral disk JSWAP
// ref: https://confluence.ngage.netapp.com/pages/viewpage.action?pageId=1268443852#id-1PControlPlaneIntegrationforManuallySwitchingDataONTAPWriteModes-StopGap
var defaultRequiredTakeoverReasons = []string{
	"disabled",
	"Storage failover mailbox disks are in a degraded state",
	"Local node has encountered errors while reading the storage failover partner's mailbox disks",
	"Storage failover interconnect error",
	"Partner node halted after disabling takeover",
	"Mailbox disks are not healthy",
	"Local node missing partner disks",
	"Default",
}

// Unplanned failover detection constant
const UnplannedFailoverTakeoverReason = "Local node is already in takeover state"

// getRequiredTakeoverReasons returns the list of takeover reasons from environment variable or defaults
func getRequiredTakeoverReasons() []string {
	// Try to get from environment variable first
	if envReasons := env.GetString("REQUIRED_TAKEOVER_REASONS", ""); envReasons != "" {
		// Comma-separated format only
		reasons := strings.Split(envReasons, ",")
		for i, reason := range reasons {
			reasons[i] = strings.TrimSpace(reason)
		}
		return reasons
	}

	// Return default reasons if environment variable is not set or invalid
	return defaultRequiredTakeoverReasons
}

var (
	SyncVSAClusterHealth                    = _syncVSAClusterHealth
	SyncVSAClusterHealthTask                = _syncVSAClusterHealthTask
	DetermineJSwapAction                    = determineJSwapAction
	ShouldJSwapToDiskForTakeoverStates      = shouldJSwapToDiskForTakeoverStates
	ShouldJSwapToDiskForTakeoverNotPossible = shouldJSwapToDiskForTakeoverNotPossible
	ShouldJSwapToDiskForUnplannedFailover   = shouldJSwapToDiskForUnplannedFailover
	ShouldJSwapToMemoryForTakeoverPossible  = shouldJSwapToMemoryForTakeoverPossible
	ExecuteJSwapAction                      = executeJSwapAction
	PerformJSwapToDisk                      = performJSwapToDisk
	PerformJSwapToMemory                    = performJSwapToMemory
	UpdatePoolToReadyState                  = updatePoolToReadyState
	UpdatePoolState                         = updatePoolState
	IsRequiredTakeoverReason                = isRequiredTakeoverReason
	HasNodeRequiredTakeoverReasonFromHealth = hasNodeRequiredTakeoverReasonFromHealth
	GetVSAProviderUnit                      = _getVSAProviderUnit
	TriggerTakeoverCheckUnit                = _triggerTakeoverCheckUnit
	GetClusterHealthStatusUnit              = _getClusterHealthStatusUnit
	JSwapUnit                               = _jSwapUnit
)

func _syncVSAClusterHealth(ctx context.Context, se database.Storage, correlationID string) {
	// Set up logger fields with correlation ID for proper context propagation
	loggerFields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}

	// Add both correlation ID and logger fields to context for proper propagation
	ctx = context.WithValue(ctx, middleware.CorrelationContextKey, correlationID)
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, loggerFields)
	logger := util.GetLogger(ctx)

	logger.Infof("[SyncVSAClusterHealth] Starting VSA Cluster Health Synchronization - CorrelationID: %s", correlationID)

	filter := utils2.CreateFilterWithConditions(
		utils2.NewFilterCondition("state", "in", []string{models.LifeCycleStateREADY, models.LifeCycleStateDegraded}),
	)
	pools, err := se.ListPoolUUIDs(ctx, filter)
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealth] CorrelationID: %s - Failed to list pools: %v", correlationID, err)
		return
	}

	logger.Infof("[SyncVSAClusterHealth] CorrelationID: %s - Found %d pools to process", correlationID, len(pools))

	inMemoTasksProcessor, err := inmemotasksprocessor.NewInMemoTasksProcessor(len(pools), len(pools))
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealth] CorrelationID: %s - Failed to create in-memory tasks processor: %v", correlationID, err)
		return
	}

	for _, pool := range pools {
		taskId := pool.UUID
		// Set task timeout to 30 seconds - this ensures the entire task is forcefully terminated within 30 seconds
		taskCtx := inmemotasksprocessor.NewTaskCtxWithID(30*time.Second, taskId)
		// Add correlation ID and logger fields to the task context
		taskCtxWithCorrelationID := context.WithValue(ctx, middleware.CorrelationContextKey, correlationID)
		taskCtxWithCorrelationID = context.WithValue(taskCtxWithCorrelationID, middleware.TemporalSLoggerKey, loggerFields)
		inMemoTasksProcessor.Add(inmemotasksprocessor.TaskFunc(SyncVSAClusterHealthTask), taskCtx, pool, se, taskCtxWithCorrelationID)
	}

	inMemoTasksProcessor.Run()
	logger.Infof("[SyncVSAClusterHealth] CorrelationID: %s - Completed VSA Cluster Health Synchronization", correlationID)
}

func _syncVSAClusterHealthTask(imtpCtx interface{}, inputs ...interface{}) {
	ctx := imtpCtx.(*inmemotasksprocessor.IMTPContext)
	poolIdentifier := inputs[0].(*database.PoolIdentifier)
	se := inputs[1].(database.Storage)
	contextWithCorrelationID := inputs[2].(context.Context)

	// Extract correlation ID from context using utility function
	correlationID := utils.GetCoRelationIDFromContext(contextWithCorrelationID)

	// Set up logger fields with correlation ID
	loggerFields := log.Fields{
		string(middleware.RequestCorrelationID): correlationID,
	}

	// Create a background context with correlation ID and logger fields for the unit functions
	bgCtx := context.WithValue(context.Background(), middleware.CorrelationContextKey, correlationID)
	bgCtx = context.WithValue(bgCtx, middleware.TemporalSLoggerKey, loggerFields)
	logger := util.GetLogger(bgCtx)

	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Processing pool %s", correlationID, poolIdentifier.UUID)

	// Get VSA Provider
	providerResult := ctx.RunUnit(GetVSAProviderUnit, inmemotasksprocessor.UnitOptions{}, poolIdentifier, se, bgCtx)
	if providerResult.Err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to get VSA Provider: %v", correlationID, poolIdentifier.UUID, providerResult.Err)
		return
	}
	provider := providerResult.Result.(vsa.Provider)

	// Create a single REST client for this task to reuse across all ONTAP API calls
	ontapClient, err := provider.CreateRESTClient()
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to create ONTAP REST client: %v", correlationID, poolIdentifier.UUID, err)
		return
	}
	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Created reusable ONTAP REST client", correlationID, poolIdentifier.UUID)

	// Trigger takeover check for all nodes to refresh takeover_check status
	takeoverCheckResult := ctx.RunUnit(TriggerTakeoverCheckUnit, inmemotasksprocessor.UnitOptions{}, provider, poolIdentifier.UUID, ontapClient, bgCtx)
	if takeoverCheckResult.Err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to trigger takeover check: %v", correlationID, poolIdentifier.UUID, takeoverCheckResult.Err)
		return
	}

	// Get cluster health status
	clusterHealthResult := ctx.RunUnit(GetClusterHealthStatusUnit, inmemotasksprocessor.UnitOptions{}, provider, poolIdentifier.UUID, ontapClient, bgCtx)
	if clusterHealthResult.Err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to get cluster health status: %v", correlationID, poolIdentifier.UUID, clusterHealthResult.Err)
		return
	}
	clusterHealth := clusterHealthResult.Result.(*vsa.ClusterHealthStatusResponse)

	// Determine and execute JSWAP action
	jswapAction := determineJSwapAction(clusterHealth, poolIdentifier.UUID, logger, correlationID)
	executeJSwapAction(ctx, jswapAction, clusterHealth, provider, se, poolIdentifier, logger, correlationID, bgCtx, ontapClient)

	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Task completed", correlationID, poolIdentifier.UUID)
}

// JSwapAction represents the type of JSWAP action to be performed
type JSwapAction int

const (
	JSwapActionNone JSwapAction = iota
	JSwapActionToDisk
	JSwapActionToMemory
)

// determineJSwapAction analyzes cluster health and determines the required JSWAP action
func determineJSwapAction(clusterHealth *vsa.ClusterHealthStatusResponse, poolUUID string, logger log.Logger, correlationID string) JSwapAction {
	// Check for unplanned failover scenario: takeover state "not_possible" with reason "Local node is already in takeover state"
	// and backing type is ephemeral_memory - this indicates unplanned failover requiring swap to disk
	if shouldJSwapToDiskForUnplannedFailover(clusterHealth.Records, poolUUID, logger, correlationID) {
		return JSwapActionToDisk
	}

	// Check for problematic takeover states
	if shouldJSwapToDiskForTakeoverStates(clusterHealth.Records, poolUUID, logger, correlationID) {
		return JSwapActionToDisk
	}

	// Check if any node has takeover_possible false
	if shouldJSwapToDiskForTakeoverNotPossible(clusterHealth.Records, poolUUID, logger, correlationID) {
		return JSwapActionToDisk
	}

	// Check if shouldJSwapToMemoryForTakeoverPossible is true for both nodes
	if shouldJSwapToMemoryForTakeoverPossible(clusterHealth.Records, poolUUID, logger, correlationID) {
		return JSwapActionToMemory
	}

	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - No JSWAP action required, cluster is healthy", correlationID, poolUUID)
	return JSwapActionNone
}

// shouldJSwapToDiskForTakeoverStates checks for problematic takeover states
func shouldJSwapToDiskForTakeoverStates(nodes []vsa.NodeHealthStatus, poolUUID string, logger log.Logger, correlationID string) bool {
	for _, node := range nodes {
		if node.Ha != nil && node.Ha.Takeover != nil {
			switch node.Ha.Takeover.State {
			case vsa.TakeoverStateNotPossible:
				// Check if node has required takeover reasons
				if hasNodeRequiredTakeoverReasonFromHealth(node) {
					logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s takeover 'not_possible' with critical reason → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID)
					return true
				}
			case vsa.TakeoverStateInTakeover:
				logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s takeover 'in_takeover' → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID)
				return true
			case vsa.TakeoverStateInProgress:
				logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s takeover 'in_progress' → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID)
				return true
			case vsa.TakeoverStateFailed:
				logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s takeover 'failed' → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID)
				return true
			}
		}
	}
	return false
}

// shouldJSwapToDiskForTakeoverNotPossible checks if any node has takeover_possible false
func shouldJSwapToDiskForTakeoverNotPossible(nodes []vsa.NodeHealthStatus, poolUUID string, logger log.Logger, correlationID string) bool {
	for _, node := range nodes {
		if node.Ha != nil && node.Ha.TakeoverCheck != nil && !node.Ha.TakeoverCheck.TakeoverPossible {
			logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s takeover_possible is false → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID)
			return true
		}
	}
	return false
}

// shouldJSwapToDiskForUnplannedFailover checks for unplanned failover scenario:
// takeover state "not_possible" with reason UnplannedFailoverTakeoverReason and backing type is ephemeral_memory
func shouldJSwapToDiskForUnplannedFailover(nodes []vsa.NodeHealthStatus, poolUUID string, logger log.Logger, correlationID string) bool {
	for _, node := range nodes {
		if node.Ha != nil && node.Ha.Takeover != nil && node.Ha.TakeoverCheck != nil {
			// Check for takeover state "not_possible" with specific reason
			if node.Ha.Takeover.State == vsa.TakeoverStateNotPossible {
				for _, reason := range node.Ha.TakeoverCheck.Reasons {
					if reason == UnplannedFailoverTakeoverReason {
						// Check if current backing type is ephemeral_memory (indicates unplanned failover)
						if node.NVLog != nil && node.NVLog.BackingType == string(vsa.JSWAPBackingTypeEphemeralMemory) {
							logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Node %s unplanned failover detected (takeover state 'not_possible', reason '%s', backing type 'ephemeral_memory') → JSWAP to ephemeral_disk", correlationID, poolUUID, node.UUID, UnplannedFailoverTakeoverReason)
							return true
						}
					}
				}
			}
		}
	}
	return false
}

// shouldJSwapToMemoryForTakeoverPossible checks if both nodes have takeover_possible true
func shouldJSwapToMemoryForTakeoverPossible(nodes []vsa.NodeHealthStatus, poolUUID string, logger log.Logger, correlationID string) bool {
	if len(nodes) == 0 {
		return false
	}

	for _, node := range nodes {
		// If any node doesn't have the required fields or takeover_possible is not true, return false
		if node.Ha == nil || node.Ha.TakeoverCheck == nil || !node.Ha.TakeoverCheck.TakeoverPossible {
			return false
		}
	}

	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Both nodes takeover_possible is true → JSWAP to ephemeral_memory", correlationID, poolUUID)
	return true
}

// executeJSwapAction executes the determined JSWAP action and updates pool state accordingly
func executeJSwapAction(ctx *inmemotasksprocessor.IMTPContext, action JSwapAction, clusterHealth *vsa.ClusterHealthStatusResponse,
	provider vsa.Provider, se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string, bgCtx context.Context, ontapClient ontapRest.RESTClient) {
	switch action {
	case JSwapActionToDisk:
		performJSwapToDisk(ctx, clusterHealth, provider, se, poolIdentifier, logger, correlationID, bgCtx, ontapClient)
	case JSwapActionToMemory:
		performJSwapToMemory(ctx, clusterHealth, provider, se, poolIdentifier, logger, correlationID, bgCtx, ontapClient)
	case JSwapActionNone:
		updatePoolToReadyState(se, poolIdentifier, logger, correlationID)
	}
}

// performJSwapToDisk performs JSWAP to ephemeral_disk and updates pool to DEGRADED state
func performJSwapToDisk(ctx *inmemotasksprocessor.IMTPContext, clusterHealth *vsa.ClusterHealthStatusResponse,
	provider vsa.Provider, se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string, bgCtx context.Context, ontapClient ontapRest.RESTClient) {
	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Executing JSWAP to ephemeral_disk", correlationID, poolIdentifier.UUID)

	jswapCount := 0
	for _, node := range clusterHealth.Records {
		if node.NVLog != nil && node.NVLog.BackingType == string(vsa.JSWAPBackingTypeEphemeralMemory) {
			jswapResult := ctx.RunUnit(JSwapUnit, inmemotasksprocessor.UnitOptions{}, provider, node.UUID, vsa.JSWAPBackingTypeEphemeralDisk, ontapClient, bgCtx)
			if jswapResult.Err != nil {
				logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - JSWAP to disk failed for node %s: %v", correlationID, poolIdentifier.UUID, node.UUID, jswapResult.Err)
			} else {
				logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - JSWAP to disk completed for node %s", correlationID, poolIdentifier.UUID, node.UUID)
				jswapCount++
			}
		}
	}

	// Update pool state to DEGRADED
	err := updatePoolState(se, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails)
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to update pool state to DEGRADED: %v", correlationID, poolIdentifier.UUID, err)
	} else {
		logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Pool state updated to DEGRADED (%d nodes processed)", correlationID, poolIdentifier.UUID, jswapCount)
	}
}

// performJSwapToMemory performs JSWAP to ephemeral_memory and updates pool to READY state
func performJSwapToMemory(ctx *inmemotasksprocessor.IMTPContext, clusterHealth *vsa.ClusterHealthStatusResponse,
	provider vsa.Provider, se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string, bgCtx context.Context, ontapClient ontapRest.RESTClient) {
	logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Executing JSWAP to ephemeral_memory", correlationID, poolIdentifier.UUID)

	jswapCount := 0
	for _, node := range clusterHealth.Records {
		if node.NVLog != nil && node.NVLog.BackingType == string(vsa.JSWAPBackingTypeEphemeralDisk) {
			jswapResult := ctx.RunUnit(JSwapUnit, inmemotasksprocessor.UnitOptions{}, provider, node.UUID, vsa.JSWAPBackingTypeEphemeralMemory, ontapClient, bgCtx)
			if jswapResult.Err != nil {
				logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - JSWAP to memory failed for node %s: %v", correlationID, poolIdentifier.UUID, node.UUID, jswapResult.Err)
			} else {
				logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - JSWAP to memory completed for node %s", correlationID, poolIdentifier.UUID, node.UUID)
				jswapCount++
			}
		}
	}

	// Update pool state to READY
	err := updatePoolState(se, poolIdentifier, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to update pool state to READY: %v", correlationID, poolIdentifier.UUID, err)
	} else {
		logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Pool state updated to READY (%d nodes processed)", correlationID, poolIdentifier.UUID, jswapCount)
	}
}

// updatePoolToReadyState updates pool to READY state when no JSWAP is needed
func updatePoolToReadyState(se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string) {
	err := updatePoolState(se, poolIdentifier, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
	if err != nil {
		logger.Errorf("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Failed to update pool state to READY: %v", correlationID, poolIdentifier.UUID, err)
	} else {
		logger.Infof("[SyncVSAClusterHealthTask] CorrelationID: %s - Pool %s - Pool state updated to READY", correlationID, poolIdentifier.UUID)
	}
}

// updatePoolState updates the lifecycle state of a pool using optimistic concurrency control
// Only updates pools that are in READY or DEGRADED state to prevent race conditions
func updatePoolState(se database.Storage, poolIdentifier *database.PoolIdentifier, newState string, stateDetails string) error {
	ctx := context.Background()
	logger := util.GetLogger(ctx)

	// Get current pool state for validation
	poolView, err := se.GetPool(ctx, poolIdentifier.UUID, poolIdentifier.AccountID)
	if err != nil {
		return fmt.Errorf("failed to get pool for state update: %v", err)
	}

	pool := database.ConvertPoolViewToPool(poolView)
	currentState := pool.State

	// Only update if pool is in expected states (READY or DEGRADED)
	// This prevents race conditions where pool may have transitioned to DELETING, UPDATING, etc.
	if currentState != models.LifeCycleStateREADY && currentState != models.LifeCycleStateDegraded {
		logger.Infof("Skipping pool state update - pool %s is in state %s (not READY/DEGRADED)", poolIdentifier.UUID, currentState)
		return nil // Not an error, just skip the update
	}

	// Check if the new state is the same as current state to avoid unnecessary database updates
	if currentState == newState {
		logger.Infof("Skipping pool state update - pool %s is already in state %s", poolIdentifier.UUID, currentState)
		return nil
	}

	// Use UpdatePoolFields with conditional update to ensure atomic state transition
	// This method includes the state validation as part of the database transaction
	updates := map[string]interface{}{
		"state":         newState,
		"state_details": stateDetails,
	}

	err = se.UpdatePoolFields(ctx, poolIdentifier.UUID, updates)
	if err != nil {
		return fmt.Errorf("failed to conditionally update pool state to %s: %v", newState, err)
	}

	logger.Infof("Successfully updated pool %s state from %s to %s with details: %s", poolIdentifier.UUID, currentState, newState, stateDetails)
	return nil
}

// isRequiredTakeoverReason checks if the given reason matches any of the configured takeover reasons that require JSWAP to ephemeral_disk
func isRequiredTakeoverReason(reason string) bool {
	requiredReasons := getRequiredTakeoverReasons()

	for _, requiredReason := range requiredReasons {
		if reason == requiredReason {
			return true
		}
	}
	return false
}

// hasNodeRequiredTakeoverReasonFromHealth checks if a specific node from consolidated health status has any required takeover reasons
func hasNodeRequiredTakeoverReasonFromHealth(node vsa.NodeHealthStatus) bool {
	// If ha field is absent, it means takeover_possible: true (healthy node) - no required takeover reasons
	if node.Ha != nil && node.Ha.TakeoverCheck != nil {
		for _, reason := range node.Ha.TakeoverCheck.Reasons {
			if isRequiredTakeoverReason(reason) {
				return true
			}
		}
	}
	// If ha field is absent or no reasons present, no required takeover reasons
	return false
}

// _getVSAProviderUnit gets the pool and creates ONTAP provider
func _getVSAProviderUnit(ctx context.Context, inputs ...interface{}) (interface{}, error) {
	if len(inputs) < 3 {
		return nil, fmt.Errorf("insufficient parameters for GetVSAProviderUnit")
	}

	poolIdentifier := inputs[0].(*database.PoolIdentifier)
	se := inputs[1].(database.Storage)
	contextWithCorrelationID := inputs[2].(context.Context)

	logger := util.GetLogger(contextWithCorrelationID)

	// Extract correlation ID for logging using utility function
	correlationID := utils.GetCoRelationIDFromContext(contextWithCorrelationID)

	logger.Infof("[GetVSAProviderUnit] CorrelationID: %s - Getting VSA provider for pool: %s", correlationID, poolIdentifier.UUID)

	poolView, err := se.GetPool(contextWithCorrelationID, poolIdentifier.UUID, poolIdentifier.AccountID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pool: %v", err)
	}

	pool := database.ConvertPoolViewToPool(poolView)
	provider, err := backgroundactivities.GetOntapRestProviderForPoolFastConn(contextWithCorrelationID, se, pool)
	if err != nil || provider == nil {
		return nil, fmt.Errorf("failed to get ONTAP rest provider for pool %v: %v", pool.UUID, err)
	}

	return provider, nil
}

// _triggerTakeoverCheckUnit triggers takeover check for all nodes in the cluster
func _triggerTakeoverCheckUnit(ctx context.Context, inputs ...interface{}) (interface{}, error) {
	if len(inputs) < 4 {
		return nil, fmt.Errorf("insufficient parameters for TriggerTakeoverCheckUnit")
	}

	provider := inputs[0].(vsa.Provider)
	poolUUID := inputs[1].(string)
	ontapClient := inputs[2].(ontapRest.RESTClient)
	contextWithCorrelationID := inputs[3].(context.Context)

	logger := util.GetLogger(contextWithCorrelationID)

	// Extract correlation ID for logging using utility function
	correlationID := utils.GetCoRelationIDFromContext(contextWithCorrelationID)

	logger.Infof("[TriggerTakeoverCheckUnit] CorrelationID: %s - Triggering takeover check for all nodes in pool: %s", correlationID, poolUUID)

	nodes, err := provider.GetNodesWithClient(ontapClient)
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes for pool %s: %v", poolUUID, err)
	}

	type result struct {
		nodeUUID string
		success  bool
		err      error
	}

	results := make(chan result, len(nodes))

	for _, node := range nodes {
		go func(nodeUUID string) {
			logger.Infof("[TriggerTakeoverCheckUnit] CorrelationID: %s - Triggering takeover check for node %s in pool %s", correlationID, nodeUUID, poolUUID)
			success, err := provider.TriggerTakeoverCheckWithClient(nodeUUID, ontapClient)
			results <- result{nodeUUID: nodeUUID, success: success, err: err}
		}(node.ExternalUUID)
	}

	completedNodes := 0
	for completedNodes < len(nodes) {
		res := <-results
		completedNodes++

		if res.err != nil {
			logger.Errorf("[TriggerTakeoverCheckUnit] CorrelationID: %s - Failed to trigger takeover check for node %s: %v", correlationID, res.nodeUUID, res.err)
		} else if res.success {
			logger.Infof("[TriggerTakeoverCheckUnit] CorrelationID: %s - Successfully triggered takeover check for node %s - returning immediately", correlationID, res.nodeUUID)
			return true, nil
		} else {
			logger.Warnf("[TriggerTakeoverCheckUnit] CorrelationID: %s - Takeover check for node %s did not complete successfully", correlationID, res.nodeUUID)
		}
	}

	return false, nil
}

// _getClusterHealthStatusUnit gets the consolidated cluster health status from the provider
func _getClusterHealthStatusUnit(ctx context.Context, inputs ...interface{}) (interface{}, error) {
	if len(inputs) < 4 {
		return nil, fmt.Errorf("insufficient parameters for GetClusterHealthStatusUnit")
	}

	provider := inputs[0].(vsa.Provider)
	poolUUID := inputs[1].(string)
	ontapClient := inputs[2].(ontapRest.RESTClient)
	contextWithCorrelationID := inputs[3].(context.Context)

	logger := util.GetLogger(contextWithCorrelationID)

	// Extract correlation ID for logging using utility function
	correlationID := utils.GetCoRelationIDFromContext(contextWithCorrelationID)

	logger.Infof("[GetClusterHealthStatusUnit] CorrelationID: %s - Getting consolidated cluster health status for pool: %s", correlationID, poolUUID)

	clusterHealthResponse, err := provider.GetClusterHealthStatusWithClient(ontapClient)
	if err != nil {
		return nil, fmt.Errorf("[GetClusterHealthStatusUnit] CorrelationID: %s - failed to get cluster health status for pool %s: %v", correlationID, poolUUID, err)
	}

	return clusterHealthResponse, nil
}

// _jSwapUnit performs JSWAP operation on a specific node
func _jSwapUnit(ctx context.Context, inputs ...interface{}) (interface{}, error) {
	if len(inputs) < 5 {
		return nil, fmt.Errorf("insufficient parameters for JSwapUnit")
	}

	provider := inputs[0].(vsa.Provider)
	nodeUUID := inputs[1].(string)
	backingType := inputs[2].(vsa.JSWAPBackingType)
	ontapClient := inputs[3].(ontapRest.RESTClient)
	contextWithCorrelationID := inputs[4].(context.Context)

	logger := util.GetLogger(contextWithCorrelationID)

	// Extract correlation ID for logging using utility function
	correlationID := utils.GetCoRelationIDFromContext(contextWithCorrelationID)

	logger.Infof("[JSwapUnit] CorrelationID: %s - Performing JSWAP operation for node %s to backing type %s", correlationID, nodeUUID, backingType)

	success, err := provider.UpdateJSwapModeWithClient(nodeUUID, backingType, ontapClient)
	if err != nil {
		return nil, fmt.Errorf("[JSwapUnit] CorrelationID: %s - failed to perform JSWAP for node %s to backing type %s: %v", correlationID, nodeUUID, backingType, err)
	}

	if !success {
		return nil, fmt.Errorf("[JSwapUnit] CorrelationID: %s - JSWAP operation failed for node %s to backing type %s", correlationID, nodeUUID, backingType)
	}

	logger.Infof("[JSwapUnit] CorrelationID: %s - Successfully completed JSWAP for node %s to backing type %s", correlationID, nodeUUID, backingType)
	return success, nil
}
