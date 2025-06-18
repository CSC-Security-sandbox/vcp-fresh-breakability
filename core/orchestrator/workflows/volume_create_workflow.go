package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeCreateWorkflow struct {
	BaseWorkflow
}

// Enforcing the WorkflowInterface on volumeCreateWorkflow
var _ WorkflowInterface = &volumeCreateWorkflow{}

// CreateVolumeWorkflow Volume Workflow process volume related requests from a customer.
func CreateVolumeWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	volumeWf := new(volumeCreateWorkflow)
	err := volumeWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	volumeWf.Status = WorkflowStatusRunning
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStatePROCESSING), nil)
	if err != nil {
		return nil, err
	}
	_, err = volumeWf.Run(ctx, volume, params.Region)
	if err != nil {
		volumeWf.Status = WorkflowStatusFailed
		err2 := volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), err)
		if err2 != nil {
			return nil, err2
		}
		return nil, err
	}
	volumeWf.Status = WorkflowStatusCompleted
	err = volumeWf.UpdateJobStatus(ctx, string(models.JobsStateDONE), nil)
	return nil, err
}

func (wf *volumeCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreateVolumeParams)
	info := workflow.GetInfo(ctx)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = createPoolParams.AccountName
	wf.Status = WorkflowStatusCreated
	ctx = util.AddExtraLoggerFields(ctx, map[string]interface{}{"workflowID": wf.ID, "customerID": wf.CustomerID})
	logger := util.GetLogger(ctx)
	wf.Logger = logger

	return workflow.SetQueryHandler(ctx, "status", func() (*WorkflowStatus, error) {
		return &WorkflowStatus{
			ID:         wf.ID,
			Status:     wf.Status,
			CustomerID: wf.CustomerID,
		}, nil
	})
}

func (wf *volumeCreateWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	log := util.GetLogger(ctx)
	dbVolume := args[0].(*datamodel.Volume)
	region := args[1].(string)
	volumeActivity := &activities.VolumeCreateActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    retryPolicy.InitialInterval,
			BackoffCoefficient: retryPolicy.BackoffCoefficient,
			MaximumInterval:    retryPolicy.MaximumInterval,
			MaximumAttempts:    int32(retryPolicy.MaximumAttempts),
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	rollbackManager := common.NewRollbackManager()
	defer func() {
		if err != nil {
			err2 := workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeStateInDB, dbVolume.UUID, models.LifeCycleStateError, models.LifeCycleStateCreationErrorDetails).Get(ctx, nil)
			if err2 != nil {
				log.Errorf("Failed to update volume state in DB to error: %v", err2)
			}
			disconnectedCtx, _ := workflow.NewDisconnectedContext(ctx)
			rollbackManager.ExecuteRollback(disconnectedCtx, err)
		}
	}()

	var hostGroups []*datamodel.HostGroup
	err = workflow.ExecuteActivity(ctx, volumeActivity.GetHosts, &dbVolume).Get(ctx, &hostGroups)
	if err != nil {
		return nil, err
	}

	var dbNode *datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNode)
	if err != nil {
		return nil, err
	}
	node := CreateNodeForProvider(dbNode, dbVolume)

	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateSnapshotPolicyInONTAP, &dbVolume, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var volCreateResponse *vsa.VolumeResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateVolumeInONTAP, &dbVolume, &node).Get(ctx, &volCreateResponse)
	if err != nil {
		return nil, err
	}
	rollbackManager.Add(activities.VolumeDeleteActivity.DeleteVolumeInONTAP, volCreateResponse.ExternalUUID, dbVolume.Name, &node) // This will delete the lunMap & lun if exists

	hostParams := createHostParamsFromHostGroups(hostGroups, dbVolume)
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateIgroup, &dbVolume, &hostParams, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var lun *vsa.LunResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLun, &dbVolume, &node, volCreateResponse.AvailableSpace).Get(ctx, &lun)
	if err != nil {
		return nil, err
	}

	lunMapParams := createLunMapParams(lun.Name, dbVolume.Svm.Name, hostParams)
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLunMap, &dbVolume, &lunMapParams, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	if dbVolume.DataProtection != nil && dbVolume.DataProtection.BackupVaultID != "" {
		tenancyDetails := &common.TenancyInfo{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.FindTenancy, dbVolume.VolumeAttributes.VendorSubnetID, dbVolume.Account.Name, &region).Get(ctx, &tenancyDetails)
		if err != nil {
			return nil, err
		}

		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckBackupVaultExistsInVCP, &dbVolume, &region).Get(ctx, nil)
		if err != nil {
			return nil, err
		}

		bucketDetails := &common.BucketDetails{}
		err = workflow.ExecuteActivity(ctx, volumeActivity.CheckForBucketResourceName, &dbVolume).Get(ctx, &bucketDetails)
		if err != nil {
			return nil, err
		}
		if bucketDetails.BucketName == "" && bucketDetails.ServiceAccountName == "" && bucketDetails.TenantProjectNumber == "" {
			resourceName := &common.ResourceNames{}
			err = workflow.ExecuteActivity(ctx, volumeActivity.GenerateResourceNames, &dbVolume, &tenancyDetails, region).Get(ctx, &resourceName)
			if err != nil {
				return nil, err
			}

			BucketDetails := &common.BucketDetails{}
			err = workflow.ExecuteActivity(ctx, volumeActivity.CreateBucket, &resourceName, &tenancyDetails, region).Get(ctx, &BucketDetails)
			if err != nil {
				return nil, err
			}
			bucketDetails.BucketName = BucketDetails.BucketName
			bucketDetails.ServiceAccountName = BucketDetails.ServiceAccountName
			bucketDetails.TenantProjectNumber = BucketDetails.TenantProjectNumber
			bucketDetails.VendorSubnetID = BucketDetails.VendorSubnetID
		}

		err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateBackupVaultWithBucketDetails, &dbVolume, &bucketDetails).Get(ctx, nil)
		if err != nil {
			return nil, err
		}
	}

	dbVolume.VolumeAttributes.BlockProperties.LunSerialNumber = lun.SerialNumber
	dbVolume.VolumeAttributes.BlockProperties.LunUUID = lun.ExternalUUID
	err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeDetails, &dbVolume, &volCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func CreateNodeForProvider(dbNode *datamodel.Node, volume *datamodel.Volume) *models.Node {
	node := &models.Node{
		EndpointAddress: dbNode.EndpointAddress,
		Username:        volume.Pool.Username,
		Password:        volume.Pool.Password,
	}
	return node
}

func createHostParamsFromHostGroups(hostGroups []*datamodel.HostGroup, volume *datamodel.Volume) []*common.HostParams {
	var hostParamsArray []*common.HostParams

	for _, hostGroup := range hostGroups {
		hostParams := &common.HostParams{
			HostName: hostGroup.Name,
			HostIQNs: hostGroup.Hosts.Hosts,
			OsType:   volume.VolumeAttributes.BlockProperties.OSType,
		}
		hostParamsArray = append(hostParamsArray, hostParams)
	}

	return hostParamsArray
}

func createLunMapParams(lunName string, svmName string, hostParams []*common.HostParams) *common.CreateLunMapParams {
	var hostNames []string

	for _, hostParam := range hostParams {
		hostNames = append(hostNames, hostParam.HostName)
	}

	lunMapParam := &common.CreateLunMapParams{
		LunName:   lunName,
		SvmName:   svmName,
		HostNames: hostNames,
	}

	return lunMapParam
}
