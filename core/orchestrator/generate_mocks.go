package orchestrator

import (
	"context"

	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	gcpserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

type monkeyMethods interface {
	// Utility methods
	utilGetLogger(ctx interface{}) log.Logger
	utilsGetLocationFromVendorID(vendorID string) (string, error)
	utilsGetRequestIDFromContext(ctx context.Context) string
	utilsGetCorrelationIDFromContext(ctx context.Context) string

	// Helper methods
	getOrCreateAccount(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error)
	validateCreateVolumeParams(ctx context.Context, se database.Storage, params *common.CreateVolumeParams, pool *datamodel.PoolView) error
	workflowsExecuteWorkflowSequentially(temporal client.Client, ctx context.Context, sequenceWfOptions client.StartWorkflowOptions, wfFunction interface{}, wfOptions workflow.ChildWorkflowOptions, wfArgs ...interface{}) error
	envIsLocalEnv() bool
	isEstablishVolumePeeringNeeded(ctx context.Context, se database.Storage, params *common.EstablishVolumePeeringParams, dbVolume *datamodel.Volume) (string, error)
	verifyVolumeState(ctx context.Context, dbVolume *datamodel.Volume) error
	verifyFlexCacheParameters(ctx context.Context, params *common.EstablishVolumePeeringParams, dbVolume *datamodel.Volume) error
	verifyClusterPeering(ctx context.Context, dbVolume *datamodel.Volume) bool
	checkForFlexCacheJobInProgress(ctx context.Context, se database.Storage, dbVolume *datamodel.Volume, params *common.EstablishVolumePeeringParams) (bool, string, error)
	identicalParams(dbVolume *datamodel.Volume, params *common.EstablishVolumePeeringParams) bool

	// FlexCache specific methods
	createFlexCacheVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error)
	establishFlexCacheVolumePeering(ctx context.Context, se database.Storage, temporal client.Client, params *common.EstablishVolumePeeringParams) (*models.Volume, string, error)

	// Volume replication methods
	getAccountWithName(ctx context.Context, se database.Storage, accountName string) (*datamodel.Account, error)
	utilParseAndValidateRegionAndZone(locationID string) (string, string, *gcpserver.Error)
	utilsGetPairedRegionURI(region string) (string, error)
	authGetSignedJwtToken(accountName string) (string, error)
	utilsParseProjectNumberFromURI(uri string) (string, error)
	getReplicationObjects(ctx context.Context, regionReplicationMap map[string][]*datamodel.VolumeReplication, logger log.Logger, params common.GetMultipleReplicationsParams) ([]*googleproxyclient.VolumeReplicationInternalV1beta, []googleproxyclient.InternalJobV1beta, error)
}
