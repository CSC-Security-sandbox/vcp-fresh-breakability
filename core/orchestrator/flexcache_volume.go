package orchestrator

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/flexcache_workflows"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
)

var (
	createFlexCacheVolume                = _createFlexCacheVolume
	utilGetLogger                        = util.GetLogger
	utilsGetLocationFromVendorID         = utils.GetLocationFromVendorID
	workflowsExecuteWorkflowSequentially = workflows.ExecuteWorkflowSequentially
)

func (o *Orchestrator) CreateFlexCacheVolume(ctx context.Context, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	return createFlexCacheVolume(ctx, o.storage, o.temporal, params)
}

func _createFlexCacheVolume(ctx context.Context, se database.Storage, temporal client.Client, params *common.CreateVolumeParams) (*models.Volume, string, error) {
	logger := utilGetLogger(ctx)

	account, err := getOrCreateAccount(ctx, se, params.AccountName)
	if err != nil {
		return nil, "", err
	}

	pool, err := se.GetPool(ctx, params.PoolID, account.ID)
	if err != nil {
		return nil, "", err
	}

	err = validateCreateVolumeParams(ctx, se, params, pool)
	if err != nil {
		return nil, "", err
	}

	svm, err := se.GetSvmForPoolID(ctx, pool.ID)
	if err != nil {
		return nil, "", err
	}

	dbPool := database.ConvertPoolViewToPool(pool)
	volumeObj := &datamodel.Volume{
		Name:        params.Name,
		Account:     account,
		AccountID:   account.ID,
		SizeInBytes: int64(params.QuotaInBytes),
		Description: params.Description,
		PoolID:      pool.ID,
		SvmID:       svm.ID,
		Pool:        dbPool,
		VolumeAttributes: &datamodel.VolumeAttributes{
			CreationToken:  params.CreationToken,
			Protocols:      params.Protocols,
			VendorSubnetID: params.Network,
			Labels:         params.Labels,
		},
	}

	if params.FileProperties != nil {
		junctionPath := common.CreateJunctionPath(params.CreationToken)
		volumeObj.VolumeAttributes.FileProperties = &datamodel.FileProperties{
			JunctionPath: junctionPath,
		}
		if params.FileProperties.ExportPolicy != nil {
			exportRules := make([]*datamodel.ExportRule, 0, len(params.FileProperties.ExportPolicy.ExportRules))
			for _, rule := range params.FileProperties.ExportPolicy.ExportRules {
				exportRules = append(exportRules, &datamodel.ExportRule{
					AllowedClients: rule.AllowedClients,
					AccessType:     rule.AccessType,
					CIFS:           rule.CIFS,
					NFSv3:          rule.NFSv3,
					NFSv4:          rule.NFSv4,
					Index:          rule.Index,
				})
			}
			volumeObj.VolumeAttributes.FileProperties = &datamodel.FileProperties{
				ExportPolicy: &datamodel.ExportPolicy{
					ExportPolicyName: params.FileProperties.ExportPolicy.ExportPolicyName,
					ExportRules:      exportRules,
				},
				JunctionPath: junctionPath,
			}
		}
	}

	if params.CacheParameters != nil {
		volumeObj.CacheParameters = &datamodel.CacheParameters{
			PeerSvmName:     params.CacheParameters.PeerSvmName,
			PeerVolumeName:  params.CacheParameters.PeerVolumeName,
			PeerClusterName: params.CacheParameters.PeerClusterName,
			PeerIpAddresses: params.CacheParameters.PeerIPAddresses,
		}
	}

	dbVolume, err := se.CreateVolume(ctx, volumeObj)
	if err != nil {
		return nil, "", err
	}

	location, err := utilsGetLocationFromVendorID(dbVolume.Pool.VendorID)
	if err != nil {
		logger.Errorf("Failed to get location from vendor ID for pool %s, error: %v", dbVolume.Pool.Name, err)
		return nil, "", err
	}

	job := &datamodel.Job{
		Type:          string(models.JobTypeFlexCacheCreateVolume),
		State:         string(models.JobsStateNEW),
		ResourceName:  params.Name,
		AccountID:     sql.NullInt64{Int64: account.ID, Valid: true},
		CorrelationID: utils.GetCoRelationIDFromContext(ctx),
		RequestID:     utils.GetRequestIDFromContext(ctx),
	}
	createdJob, err := se.CreateJob(ctx, job)
	if err != nil {
		logger.Errorf("Failed to create job in database, error: %v", err)
		return nil, "", err
	}

	// controlWorkflowID defines the workflow ID for the control workflow
	controlWorkflowID := fmt.Sprintf(workflows.VolumeCreateDeleteSnapshotDeleteSeq, dbVolume.Account.ID, location, dbVolume.Pool.Name)
	err = workflowsExecuteWorkflowSequentially(
		temporal,
		ctx,
		client.StartWorkflowOptions{
			TaskQueue: workflowengine.CustomerTaskQueue,
			ID:        controlWorkflowID,
		},
		flexcache_workflows.CreateFlexCacheWorkflow,
		workflow.ChildWorkflowOptions{
			TaskQueue:             workflowengine.CustomerTaskQueue,
			WorkflowID:            createdJob.WorkflowID,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		},
		params,
		dbVolume,
	)
	if err != nil {
		logger.Errorf("Failed to start create FlexCache volume workflow, error: %v", err)
		return nil, "", err
	}
	return convertDatastoreVolumeToModel(dbVolume, nil), createdJob.UUID, nil
}
