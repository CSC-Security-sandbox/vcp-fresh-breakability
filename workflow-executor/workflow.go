package executor

import (
	"time"

	"github.com/labstack/gommon/log"
	"go.temporal.io/sdk/workflow"
)

const (
	DeployVsaJob      = "DeployVsaJob"
	OrderChoiceBanana = "banana"
	OrderChoiceCherry = "cherry"
	OrderChoiceOrange = "orange"
)

// JobWorkflow Workflow definition.
func JobWorkflow(ctx workflow.Context) error {
	// Get basket order.
	options := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, options)
	var jobs *Jobs // Used to call activities by function pointer

	var choices []string
	err := workflow.ExecuteActivity(ctx, jobs.CreateVsaCluster).Get(ctx, &choices)
	if err != nil {
		return err
	}

	log.Info("Workflow completed.")
	return nil
}
