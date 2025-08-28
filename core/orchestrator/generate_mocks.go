package orchestrator

import (
	"context"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

type monkeyMethods interface {
	// Utility methods
	utilGetLogger(ctx interface{}) log.Logger
	utilsGetLocationFromVendorID(vendorID string) (string, error)

	// Helper methods
	getOrCreateAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error)
	validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error
	workflowsExecuteWorkflowSequentially(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error

	// FlexCache specific methods
	createFlexCacheVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error)
}
