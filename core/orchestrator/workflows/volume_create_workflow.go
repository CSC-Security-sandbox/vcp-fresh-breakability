package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type volumeCreateWorkflow struct {
	// add fields needed for volume workflow
	ID         string
	customerID string
	status     string
	logger     log.Logger
}

type volumeCreateWorkflowStatus struct {
	ID         string
	customerID string
	status     string
}

// CreateVolumeWorkflow Volume Workflow process volume related requests from a customer.
func CreateVolumeWorkflow(ctx workflow.Context, params *common.CreateVolumeParams, volume *datamodel.Volume) (gcpgenserver.V1betaDescribeVolumeRes, error) {
	volumeWf := new(volumeCreateWorkflow)
	err := volumeWf.Setup(ctx, params)
	if err != nil {
		return nil, err
	}
	volumeWf.status = WorkflowStatusRunning
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStatePROCESSING), "")
	// if err != nil {
	//	return nil, err
	// }
	_, err = volumeWf.Run(ctx, volume)
	if err != nil {
		volumeWf.status = WorkflowStatusFailed
	}
	// poolWF.status = WorkflowStatusCompleted
	// err = poolWF.UpdateStatus(ctx, string(models.JobsStateDONE), "")
	// if err != nil {
	//	return nil, err
	// }
	return nil, err
}

func (wf *volumeCreateWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	createPoolParams := input.(*common.CreateVolumeParams)
	wf.customerID = createPoolParams.AccountName
	wf.status = "created"
	wf.logger = log.With(
		workflow.GetLogger(ctx),
		"workflowID", wf.ID,
		"customerID", wf.customerID,
	)

	return workflow.SetQueryHandler(ctx, "status", func() (*volumeCreateWorkflowStatus, error) {
		return &volumeCreateWorkflowStatus{
			ID:         wf.ID,
			status:     wf.status,
			customerID: wf.customerID,
		}, nil
	})
}

func (wf *volumeCreateWorkflow) Run(ctx workflow.Context, volume *datamodel.Volume) (interface{}, error) {
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

	dbVolume := &datamodel.Volume{}
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateVolume, &volume).Get(ctx, &dbVolume)
	if err != nil {
		return nil, err
	}

	var hostGroups []*datamodel.HostGroup
	err = workflow.ExecuteActivity(ctx, volumeActivity.GetHosts, &volume).Get(ctx, &hostGroups)
	if err != nil {
		return nil, err
	}

	var dbNode *datamodel.Node
	err = workflow.ExecuteActivity(ctx, activities.CommonActivities.GetNode, &dbVolume.Pool.ID).Get(ctx, &dbNode)
	if err != nil {
		return nil, err
	}
	node := createNodeForProvider(dbNode, dbVolume)

	var volCreateResponse *vsa.ProviderResponse
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateVolumeInONTAP, &dbVolume, &node).Get(ctx, &volCreateResponse)
	if err != nil {
		return nil, err
	}

	hostParams := createHostParamsFromHostGroups(hostGroups, volume)
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateIgroup, &dbVolume, &hostParams, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	var lunName string
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLun, &dbVolume, &node).Get(ctx, &lunName)
	if err != nil {
		return nil, err
	}

	lunMapParams := createLunMapParams(lunName, dbVolume.Svm.Name, hostParams)
	err = workflow.ExecuteActivity(ctx, volumeActivity.CreateLunMap, &lunMapParams, &node).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	err = workflow.ExecuteActivity(ctx, volumeActivity.UpdateVolumeDetails, &dbVolume, &volCreateResponse).Get(ctx, nil)
	if err != nil {
		return nil, err
	}

	return nil, err
}

func createNodeForProvider(dbNode *datamodel.Node, volume *datamodel.Volume) *models.Node {
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
