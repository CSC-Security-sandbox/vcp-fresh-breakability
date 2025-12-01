package ontap_rest

import (
	"context"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	t "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/client/cluster"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/ontap-rest/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/ontap-rest/transport"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

var (
	timeout = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_TIMEOUT_MINUTES", 30)) * time.Minute
	wait    = time.Duration(env.GetUint("ONTAP_REST_ASYNC_POLL_WAIT_SECONDS", 3)) * time.Second
)

// Poller describes a poller that polls a job
type Poller interface { // generate:mock
	Poll(UUID string) error
}

type poller struct {
	api          cluster.ClientService
	logger       log.Logger
	clientParams RESTClientParams
}

var fetchTemporalClient = _fetchTemporalClient

func _fetchTemporalClient(ctx context.Context) client.Client {
	return activity.GetClient(ctx)
}

// Poll polls an ontap job given UUID
// Since this poller is being used from within many nested functions,
// it is not possible to extract it out at the workflow level. Hence, it is
// being passed an activity context internally that will be used to fetch
// the temporal client and execute a non-blocking polling workflow from within.
func (p *poller) Poll(UUID string) error {
	ctx := p.clientParams.Ctx
	if !activity.IsActivity(p.clientParams.Ctx) {
		return errors.New("Context is not an activity context, cannot poll job in non-blocking way")
	}

	tempClient := fetchTemporalClient(ctx)

	fut, err := tempClient.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:                       "ontap-rest-job-poll-" + UUID,
			TaskQueue:                temporal.CustomerTaskQueue,
			WorkflowExecutionTimeout: timeout,
		},
		PollOntapJob,
		p.clientParams,
		UUID,
	)
	if err != nil {
		p.logger.Errorf("Failed to start job poll workflow for UUID %s, error: %v", UUID, err)
		return errors.New("failed to start ontap-rest job poll workflow")
	}

	// Non-blocking wait for the workflow to complete.
	if err = fut.Get(ctx, nil); err != nil {
		p.logger.Errorf("Failed to poll job with UUID %s, error: %v", UUID, err)
		return errors.New("failed to poll ontap-rest job")
	}

	return nil
}

var workflowSleep = workflow.Sleep

// PollOntapJobActivity is an activity that polls a single ONTAP job
func PollOntapJobActivity(ctx context.Context, clientParams RESTClientParams, UUID string) (*models.Job, error) {
	logger := util.GetLogger(ctx)
	clientParams.Trace = logger
	api, err := NewOntapRestClient(clientParams)
	if err != nil {
		logger.Errorf("Failed to create Ontap REST client, error: %v", err)
		return nil, errors.NewNonRetryableErr("failed to create ontap-rest client")
	}

	rsp, err := api.Cluster().GetJob(UUID)
	if err != nil {
		logger.Errorf("Failed to poll job for UUID %s, error: %v", UUID, err)
		return nil, errors.NewNonRetryableErr("failed to poll job")
	}

	if *rsp.Payload.State == models.JobStateFailure {
		return nil, errors.NewNonRetryableErr(transport.ConvertFromRESTError(logger, rsp).Error())
	}

	if *rsp.Payload.State == models.JobStateSuccess {
		return nil, nil
	}

	return nil, errors.New("Job is still processing")
}

// PollOntapJob is a workflow that polls an ONTAP REST job until it is either successful or failed.
func PollOntapJob(ctx workflow.Context, clientParams RESTClientParams, UUID string) error {
	logger := util.GetLogger(ctx)
	// Since logger is not serializable, it becomes nil when sent as workflow param.
	// Hence, we need to fetch and set it again in the clientParams.
	clientParams.Trace = logger

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &t.RetryPolicy{
			InitialInterval:        wait,
			BackoffCoefficient:     0,
			MaximumInterval:        wait,
			NonRetryableErrorTypes: []string{"NonRetryableErr", "PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	err := workflow.ExecuteActivity(ctx, PollOntapJobActivity, clientParams, UUID).Get(ctx, nil)
	if err != nil {
		logger.Errorf("Failed to poll job for UUID %s, error: %v", UUID, err)
		return err
	}

	return nil
}
