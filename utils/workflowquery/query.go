// Package workflowquery queries a Temporal workflow by ID and returns status, error message (on failure), and metadata (on success).
package workflowquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/grpc"
)

// historyFetcher is the minimal Temporal API used to page workflow execution history (test doubles implement this).
type historyFetcher interface {
	GetWorkflowExecutionHistory(ctx context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error)
}

// temporalQuerier is the subset of client.Client used by Query (test doubles implement this).
type temporalQuerier interface {
	DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error)
	History() historyFetcher
}

type sdkClientWrap struct{ c client.Client }

func (w sdkClientWrap) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return w.c.DescribeWorkflowExecution(ctx, workflowID, runID)
}

func (w sdkClientWrap) History() historyFetcher {
	return w.c.WorkflowService()
}

// WorkflowError holds a structured error with a primary message and an optional cause.
type WorkflowError struct {
	Message string `json:"message"`
	Cause   string `json:"cause,omitempty"`
}

type OCICreatePoolMetadata struct {
	InterclusterIPs []string `json:"interclusterIPs,omitempty"`
	NodeIPs         []string `json:"nodeIPs,omitempty"`
}

type WorkflowStatus string

const (
	WorkflowStatusInProgress WorkflowStatus = "in_progress"
	WorkflowStatusCompleted  WorkflowStatus = "completed"
	WorkflowStatusFailed     WorkflowStatus = "failed"
	WorkflowStatusTimedOut   WorkflowStatus = "timed_out"
)

// Result is the JSON-friendly workflow query result: status, optional error, optional metadata.
type Result struct {
	Status       WorkflowStatus         `json:"status"`
	WorkflowType string                 `json:"workflow_type,omitempty"`
	Error        *WorkflowError         `json:"error,omitempty"`
	Metadata     *OCICreatePoolMetadata `json:"metadata,omitempty"`
}

// Query returns the workflow status result for the given workflow ID (and optional run ID).
// It reuses the Temporal client's underlying gRPC connection for both Describe and history RPCs.
func Query(ctx context.Context, c client.Client, workflowID, runID string) (Result, error) {
	return queryWithClient(ctx, sdkClientWrap{c}, workflowID, runID)
}

func queryWithClient(ctx context.Context, c temporalQuerier, workflowID, runID string) (Result, error) {
	out := Result{}
	resp, err := c.DescribeWorkflowExecution(ctx, workflowID, runID)
	if err != nil {
		return out, err
	}
	if resp == nil || resp.WorkflowExecutionInfo == nil {
		out.Status = WorkflowStatusFailed
		out.Error = &WorkflowError{Message: "no workflow execution info"}
		return out, nil
	}
	info := resp.WorkflowExecutionInfo
	exec := info.GetExecution()
	if exec != nil {
		workflowID = exec.GetWorkflowId()
		runID = exec.GetRunId()
	}
	if wt := info.GetType(); wt != nil {
		out.WorkflowType = wt.GetName()
	}

	out.Status = normalizedStatus(info.GetStatus())

	svc := c.History()
	namespace := env.GetString("TEMPORAL_NAMESPACE", "default")

	switch out.Status {
	case WorkflowStatusFailed, WorkflowStatusTimedOut:
		out.Error = getWorkflowFailureReason(ctx, svc, namespace, workflowID, runID)
	case WorkflowStatusCompleted:
		out.Metadata = getWorkflowInputMetadata(ctx, svc, namespace, workflowID, runID)
	}
	return out, nil
}

func normalizedStatus(s enums.WorkflowExecutionStatus) WorkflowStatus {
	switch s {
	case enums.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return WorkflowStatusCompleted
	case enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return WorkflowStatusTimedOut
	case enums.WORKFLOW_EXECUTION_STATUS_FAILED,
		enums.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enums.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		return WorkflowStatusFailed
	case enums.WORKFLOW_EXECUTION_STATUS_RUNNING:
		return WorkflowStatusInProgress
	default:
		return WorkflowStatusInProgress
	}
}

func getWorkflowInputMetadata(ctx context.Context, svc historyFetcher, namespace, workflowID, runID string) *OCICreatePoolMetadata {
	allEvents, err := fetchAllHistory(ctx, svc, namespace, workflowID, runID)
	if err != nil || len(allEvents) == 0 {
		return nil
	}

	var meta *OCICreatePoolMetadata
	var parentWorkflowType string

	for _, ev := range allEvents {
		if ev.EventType == enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			a := ev.GetWorkflowExecutionStartedEventAttributes()
			if a == nil {
				continue
			}
			if a.GetWorkflowType() != nil && a.GetWorkflowType().GetName() != "" {
				parentWorkflowType = a.GetWorkflowType().GetName()
			}
		}

		// Capture the vlm.CreateVSAClusterDeploymentWorkflow child result only when parent is OCICreatePoolWorkflow
		if parentWorkflowType == "OCICreatePoolWorkflow" && ev.EventType == enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED {
			a := ev.GetChildWorkflowExecutionCompletedEventAttributes()
			if a == nil || a.GetWorkflowType() == nil {
				continue
			}
			childType := a.GetWorkflowType().GetName()
			if childType != "vlm.CreateVSAClusterDeploymentWorkflow" {
				continue
			}
			if a.GetResult() != nil && len(a.GetResult().GetPayloads()) > 0 {
				if childMeta := vsaClusterChildMetadataFromPayloads(a.GetResult().GetPayloads()); childMeta != nil {
					meta = childMeta
				}
			}
		}
	}
	return meta
}

func fetchAllHistory(ctx context.Context, svc historyFetcher, namespace, workflowID, runID string) ([]*historypb.HistoryEvent, error) {
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:       namespace,
		Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		MaximumPageSize: 200,
	}
	var allEvents []*historypb.HistoryEvent
	for {
		resp, err := svc.GetWorkflowExecutionHistory(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp == nil || resp.History == nil {
			break
		}
		allEvents = append(allEvents, resp.History.Events...)
		if len(resp.NextPageToken) == 0 {
			break
		}
		req.NextPageToken = resp.NextPageToken
	}
	return allEvents, nil
}

// payloadDataJSONBytes returns the workflow result JSON bytes from the first Temporal payload
// (same unwrap as decodeWorkflowInput: raw JSON or base64-wrapped JSON in Data).
func payloadDataJSONBytes(payloads []*commonpb.Payload) []byte {
	if len(payloads) == 0 || payloads[0] == nil {
		return nil
	}
	data := payloads[0].GetData()
	if len(data) == 0 {
		return nil
	}
	decoded := data
	if b, err := base64.StdEncoding.DecodeString(string(data)); err == nil && len(b) > 0 {
		decoded = b
	}
	return decoded
}

func decodeWorkflowInput(payloads []*commonpb.Payload) map[string]interface{} {
	decoded := payloadDataJSONBytes(payloads)
	if len(decoded) == 0 {
		return nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(decoded, &out); err != nil {
		return nil
	}
	return out
}

func getWorkflowFailureReason(ctx context.Context, svc historyFetcher, namespace, workflowID, runID string) *WorkflowError {
	allEvents, err := fetchAllHistory(ctx, svc, namespace, workflowID, runID)
	if err != nil {
		return &WorkflowError{Message: fmt.Sprintf("could not fetch history: %v", err)}
	}

	var lastFailure *historypb.HistoryEvent
	for _, ev := range allEvents {
		switch ev.EventType {
		case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
			lastFailure = ev
		}
	}
	if lastFailure == nil {
		return nil
	}
	return formatFailureEvent(lastFailure)
}

func formatFailureEvent(ev *historypb.HistoryEvent) *WorkflowError {
	msg := ""
	var cause string
	var retryState, errorType string

	switch ev.EventType {
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		a := ev.GetWorkflowExecutionFailedEventAttributes()
		if a != nil && a.GetFailure() != nil {
			f := a.GetFailure()
			msg = f.GetMessage()
			if a.RetryState != enums.RETRY_STATE_UNSPECIFIED {
				retryState = a.RetryState.String()
			}
			if f.GetApplicationFailureInfo() != nil {
				errorType = f.GetApplicationFailureInfo().GetType()
			}
			if f.GetCause() != nil && f.GetCause().GetMessage() != "" {
				cause = f.GetCause().GetMessage()
			}
		}
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		msg = "workflow execution timed out"
		if a := ev.GetWorkflowExecutionTimedOutEventAttributes(); a != nil && a.RetryState != enums.RETRY_STATE_UNSPECIFIED {
			retryState = a.RetryState.String()
		}
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		if a := ev.GetWorkflowExecutionCanceledEventAttributes(); a != nil && a.GetDetails() != nil {
			if payloads := a.GetDetails().GetPayloads(); len(payloads) > 0 && payloads[0] != nil {
				msg = fmt.Sprintf("canceled (details: %s)", string(payloads[0].GetData()))
			} else {
				msg = "workflow execution canceled"
			}
		} else {
			msg = "workflow execution canceled"
		}
	case enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		if a := ev.GetWorkflowExecutionTerminatedEventAttributes(); a != nil && a.GetReason() != "" {
			msg = a.GetReason()
		} else {
			msg = "workflow execution terminated"
		}
	}

	if msg == "" {
		return nil
	}
	out := msg
	if errorType != "" {
		out = fmt.Sprintf("[%s] %s", errorType, out)
	}
	if retryState != "" {
		out = fmt.Sprintf("%s (retry: %s)", out, retryState)
	}
	return &WorkflowError{Message: out, Cause: cause}
}
