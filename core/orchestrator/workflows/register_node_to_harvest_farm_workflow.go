package workflows

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// RegisterNodeToHarvestFarmWorkflowInput holds input parameters for the workflow
type RegisterNodeToHarvestFarmWorkflowInput struct {
	PoolID           int64
	MaxNodesPerGroup int

	CustomerProjectID string
	TenantProjectID   string
}

type registerNodeToHarvestFarmWorkflow struct {
	BaseWorkflow
	SE database.Storage
}

// var uploadURL = env.GetString("UPLOAD_URL", "http://harvest-farm-service:3000/config/upload")

// Enforcing the WorkflowInterface on registerNodeToHarvestFarmWorkflow
var _ WorkflowInterface = &registerNodeToHarvestFarmWorkflow{}

// RegisterNodeToHarvestFarmWorkflow is a Temporal workflow that registers a node to the Harvest farm
func RegisterNodeToHarvestFarmWorkflow(ctx workflow.Context, input RegisterNodeToHarvestFarmWorkflowInput) error {
	wf := new(registerNodeToHarvestFarmWorkflow)
	err := wf.Setup(ctx, input)
	if err != nil {
		return err
	}
	wf.Status = WorkflowStatusRunning
	// Optionally update the job status here if needed
	_, err = wf.Run(ctx, input)
	if err != nil {
		wf.Status = WorkflowStatusFailed
		return err
	}
	wf.Status = WorkflowStatusCompleted
	return nil
}

func (wf *registerNodeToHarvestFarmWorkflow) Setup(ctx workflow.Context, input interface{}) error {
	info := workflow.GetInfo(ctx)
	workflowInput := input.(RegisterNodeToHarvestFarmWorkflowInput)
	wf.ID = info.WorkflowExecution.ID
	wf.CustomerID = workflowInput.CustomerProjectID // Set if you have a customer/account field
	wf.Status = WorkflowStatusCreated
	return nil
}

func (wf *registerNodeToHarvestFarmWorkflow) Run(ctx workflow.Context, args ...interface{}) (interface{}, error) {
	input := args[0].(RegisterNodeToHarvestFarmWorkflowInput)
	registerActivity := &activities.RegisterNodeToHarvestFarmActivity{}
	retryPolicy, err := PopulateRetryPolicyParams()
	if err != nil {
		return nil, err
	}
	// Set activity options
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
			NonRetryableErrorTypes: []string{"not enough nodes found for pool",
				"node group assignment returned insufficient mappings for pool",
				"node1 and node2 must be different nodes",
				"failed to fetch node group details from nodeGroup Map table",
			},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	// 1. Register nodes to Harvest farm
	var nodeMappings []*datamodel.NodeNodeGroupMap
	err = workflow.ExecuteActivity(ctx, registerActivity.RegisterNodeToHarvestFarm,
		activities.RegisterNodeToHarvestFarmInput{
			PoolID:            input.PoolID,
			MaxNodesPerGroup:  input.MaxNodesPerGroup,
			CustomerProjectID: input.CustomerProjectID,
			TenantProjectID:   input.TenantProjectID,
		}).Get(ctx, &nodeMappings)
	if err != nil {
		return nil, err
	}

	// 2. Validate and create kubernetes lease if required.
	err = workflow.ExecuteActivity(ctx, registerActivity.ValidateAndCreateKubernetesLease, nodeMappings).Get(ctx, nil)
	if err != nil {
		return nil, err
	}
	//  This is will be addressed by PR #683
	// // 2. Render and upload a Harvest template for each node mapping (sequentially)
	// uploadInput := activities.UploadHarvestTemplateInput{
	//	 NodeMappings: nodeMappings,
	//	 UploadURL:    uploadURL,
	// }
	// uploadActivity := &activities.UploadHarvestTemplateActivity{}
	// err = workflow.ExecuteActivity(ctx, uploadActivity.UploadHarvestTemplate, uploadInput).Get(ctx, nil)
	// if err != nil {
	//	return nil, err
	// }

	return nodeMappings, nil
}
