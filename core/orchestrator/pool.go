package orchestrator

import (
	"context"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	workflow_engine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

func ListPool(ctx context.Context, params gcpgenserver.V1betaDescribePoolParams, orchestrator *Orchestrator) (gcpgenserver.V1betaDescribePoolRes, error) {
	//1. Prevalidation steps needs to be implemented

	//2. Create a job in the database
	job, err := orchestrator.storage.CreateJob(ctx, &datamodel.Job{})
	if err != nil {
		return nil, err
	}

	//3. Create a workflow execution
	retryPolicy := workflow_engine.GetRetryPolicy(&workflow_engine.RetryPolicyConfig{})
	wf, err := orchestrator.temporal.ExecuteWorkflow(ctx,
		client.StartWorkflowOptions{
			ID:                    job.ID,
			TaskQueue:             workflow_engine.CustomerTaskQueue,
			WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			RetryPolicy:           retryPolicy,
		},
		workflows.CreatePool,
		params,
	)
	if err != nil {
		fmt.Println("Unable to execute workflow", err)
		return nil, err
	}
	fmt.Println("Started workflow", wf.GetID(), wf.GetRunID())

	// 3. Implement workflow response processing
	return nil, nil
}
