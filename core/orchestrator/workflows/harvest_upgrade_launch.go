package workflows

import (
	"context"
	"errors"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// HarvestTemplateSHAAppConfigKey is the app_config key used to persist the embedded harvest template hash.
const HarvestTemplateSHAAppConfigKey = "harvest_template_sha"

// LaunchHarvestRefreshIfNeeded compares the running binary's embedded harvest template SHA to the
// value stored in app_config. When RefreshHarvestOnUpgrade is enabled and the template changed
// (or no row exists yet), it starts HarvestPollerUpgradeWorkFlow on CustomerTaskQueue, waits for completion,
// and persists the new SHA only after successful completion.
// The Core API (core/app.go) invokes this at process startup after the Temporal client is ready.
func LaunchHarvestRefreshIfNeeded(ctx context.Context, cfg *common.Config, db database.Storage, temporalClient client.Client, logger log.Logger) error {
	if cfg == nil {
		return errors.New("LaunchHarvestRefreshIfNeeded: config is nil")
	}
	if !cfg.RefreshHarvestOnUpgrade {
		logger.Info("Harvest refresh on upgrade skipped because RefreshHarvestOnUpgrade=false")
		return nil
	}
	if db == nil {
		return errors.New("LaunchHarvestRefreshIfNeeded: database storage is nil but RefreshHarvestOnUpgrade is enabled")
	}
	if temporalClient == nil {
		return errors.New("LaunchHarvestRefreshIfNeeded: temporal client is nil but RefreshHarvestOnUpgrade is enabled")
	}

	currentHarvestSHA := utils.HarvestTemplateSHA

	stored, err := db.GetAppConfig(ctx, HarvestTemplateSHAAppConfigKey)
	if err != nil {
		var customErr *vsaerrors.CustomError
		if !(vsaerrors.As(err, &customErr) && customErr.IsError(vsaerrors.ErrResourceNotFound)) {
			return fmt.Errorf("failed to read harvest template SHA from DB: %w", err)
		}
		// Not found — first deploy, proceed to trigger
	} else if stored != nil && stored.Value == currentHarvestSHA {
		logger.Info("Harvest template unchanged, skipping refresh", "sha", currentHarvestSHA)
		return nil
	}

	storedHarvestSHA := "<none>"
	if stored != nil {
		storedHarvestSHA = stored.Value
	}
	logger.Info("Harvest template changed, triggering HarvestPollerUpgradeWorkFlow",
		"currentHarvestSHA", currentHarvestSHA,
		"storedHarvestSHA", storedHarvestSHA)

	run, err := temporalClient.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			TaskQueue:                workflowengine.CustomerTaskQueue,
			ID:                       HarvestPollerUpgradeWorkflowID,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		},
		HarvestPollerUpgradeWorkFlow,
		&HarvestPollerUpgradeParams{},
	)
	if err != nil {
		return fmt.Errorf("failed to execute HarvestPollerUpgradeWorkFlow: %w", err)
	}
	if err := run.Get(ctx, nil); err != nil {
		return fmt.Errorf("harvest refresh workflow failed: %w", err)
	}

	if err := db.UpsertAppConfig(ctx, HarvestTemplateSHAAppConfigKey, currentHarvestSHA); err != nil {
		return fmt.Errorf("failed to persist harvest template SHA: %w", err)
	}

	logger.Info("Successfully triggered harvest refresh and persisted new SHA", "sha", currentHarvestSHA)
	return nil
}
