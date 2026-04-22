package workflowquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enums "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

func TestNormalizedStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   enums.WorkflowExecutionStatus
		want WorkflowStatus
	}{
		{enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, WorkflowStatusCompleted},
		{enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, WorkflowStatusTimedOut},
		{enums.WORKFLOW_EXECUTION_STATUS_FAILED, WorkflowStatusFailed},
		{enums.WORKFLOW_EXECUTION_STATUS_CANCELED, WorkflowStatusFailed},
		{enums.WORKFLOW_EXECUTION_STATUS_TERMINATED, WorkflowStatusFailed},
		{enums.WORKFLOW_EXECUTION_STATUS_RUNNING, WorkflowStatusInProgress},
		{enums.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED, WorkflowStatusInProgress},
		{enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, WorkflowStatusInProgress},
	}
	for _, tt := range tests {
		t.Run(string(tt.want), func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, normalizedStatus(tt.in))
		})
	}
}

func TestDecodeWorkflowInput(t *testing.T) {
	t.Parallel()
	t.Run("nil and empty", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, decodeWorkflowInput(nil))
		require.Nil(t, decodeWorkflowInput([]*commonpb.Payload{}))
		require.Nil(t, decodeWorkflowInput([]*commonpb.Payload{nil}))
		require.Nil(t, decodeWorkflowInput([]*commonpb.Payload{{Data: nil}}))
		require.Nil(t, decodeWorkflowInput([]*commonpb.Payload{{Data: []byte{}}}))
	})
	t.Run("raw JSON object", func(t *testing.T) {
		t.Parallel()
		raw := []byte(`{"k":"v","n":1}`)
		out := decodeWorkflowInput([]*commonpb.Payload{{Data: raw}})
		require.Equal(t, map[string]interface{}{"k": "v", "n": float64(1)}, out)
	})
	t.Run("base64-encoded JSON", func(t *testing.T) {
		t.Parallel()
		enc := base64.StdEncoding.EncodeToString([]byte(`{"x":true}`))
		out := decodeWorkflowInput([]*commonpb.Payload{{Data: []byte(enc)}})
		require.Equal(t, map[string]interface{}{"x": true}, out)
	})
	t.Run("invalid JSON", func(t *testing.T) {
		t.Parallel()
		require.Nil(t, decodeWorkflowInput([]*commonpb.Payload{{Data: []byte("not-json")}}))
	})
}

func TestFormatFailureEvent(t *testing.T) {
	t.Parallel()
	t.Run("failed with application type cause and retry", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
					Failure: &failurepb.Failure{
						Message: "top",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
								Type: "AppErr",
							},
						},
						Cause: &failurepb.Failure{Message: "underlying"},
					},
					RetryState: enums.RETRY_STATE_IN_PROGRESS,
				},
			},
		}
		werr := formatFailureEvent(ev)
		require.NotNil(t, werr)
		require.Equal(t, "underlying", werr.Cause)
		require.Contains(t, werr.Message, "[AppErr]")
		require.Contains(t, werr.Message, "top")
		require.Contains(t, werr.Message, "retry:")
		require.Contains(t, werr.Message, "InProgress")
	})
	t.Run("timed out", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
				WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{
					RetryState: enums.RETRY_STATE_NON_RETRYABLE_FAILURE,
				},
			},
		}
		werr := formatFailureEvent(ev)
		require.NotNil(t, werr)
		require.Contains(t, werr.Message, "workflow execution timed out")
		require.Contains(t, werr.Message, "retry:")
	})
	t.Run("canceled with details payload", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
				WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{
					Details: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{{Data: []byte("reason-xyz")}},
					},
				},
			},
		}
		werr := formatFailureEvent(ev)
		require.NotNil(t, werr)
		require.Contains(t, werr.Message, "reason-xyz")
	})
	t.Run("canceled without details", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
				WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{},
			},
		}
		werr := formatFailureEvent(ev)
		require.NotNil(t, werr)
		require.Equal(t, "workflow execution canceled", werr.Message)
	})
	t.Run("terminated with reason", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
				WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{
					Reason: "admin stop",
				},
			},
		}
		werr := formatFailureEvent(ev)
		require.NotNil(t, werr)
		require.Equal(t, "admin stop", werr.Message)
	})
	t.Run("empty failed attributes returns nil", func(t *testing.T) {
		t.Parallel()
		ev := &historypb.HistoryEvent{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
				WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{},
			},
		}
		require.Nil(t, formatFailureEvent(ev))
	})
}

// fakeHistoryFetcher implements historyFetcher for tests.
type fakeHistoryFetcher struct {
	pages [][]*historypb.HistoryEvent
	err   error

	calls         int
	lastNamespace string
}

func (f *fakeHistoryFetcher) GetWorkflowExecutionHistory(ctx context.Context, in *workflowservice.GetWorkflowExecutionHistoryRequest, opts ...grpc.CallOption) (*workflowservice.GetWorkflowExecutionHistoryResponse, error) {
	f.calls++
	f.lastNamespace = in.GetNamespace()
	if f.err != nil {
		return nil, f.err
	}
	if f.calls > len(f.pages) {
		return &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: nil}}, nil
	}
	events := f.pages[f.calls-1]
	token := []byte{}
	if f.calls < len(f.pages) {
		token = []byte("more")
	}
	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       &historypb.History{Events: events},
		NextPageToken: token,
	}, nil
}

func TestFetchAllHistory_Pagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &fakeHistoryFetcher{
		pages: [][]*historypb.HistoryEvent{
			{{EventId: 1, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED}},
			{{EventId: 2, EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED}},
		},
	}
	got, err := fetchAllHistory(ctx, f, "ns", "wf-1", "run-1")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, int64(1), got[0].EventId)
	require.Equal(t, int64(2), got[1].EventId)
	require.Equal(t, 2, f.calls)
}

func TestFetchAllHistory_Error(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &fakeHistoryFetcher{err: errors.New("rpc down")}
	_, err := fetchAllHistory(ctx, f, "ns", "wf", "run")
	require.Error(t, err)
	require.Contains(t, err.Error(), "rpc down")
}

func TestGetWorkflowFailureReason_HistoryError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := &fakeHistoryFetcher{err: errors.New("no history")}
	werr := getWorkflowFailureReason(ctx, f, "ns", "wf", "run")
	require.NotNil(t, werr)
	require.Contains(t, werr.Message, "could not fetch history")
}

func TestGetWorkflowFailureReason_LastFailureWins(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	firstFail := &historypb.HistoryEvent{
		EventId:   10,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: "first"},
			},
		},
	}
	secondFail := &historypb.HistoryEvent{
		EventId:   20,
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: "second"},
			},
		},
	}
	f := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{{firstFail, secondFail}}}
	werr := getWorkflowFailureReason(ctx, f, "ns", "wf", "run")
	require.NotNil(t, werr)
	require.Contains(t, werr.Message, "second")
}

func TestGetWorkflowInputMetadata_OCIPoolChildResult(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	childJSON := map[string]interface{}{
		"vlm_config": map[string]interface{}{
			"cloud": map[string]interface{}{
				"ha_pair": []interface{}{
					map[string]interface{}{
						"vm1": map[string]interface{}{
							"name":              "FsnIdocnv-vm-01",
							"serial_number":     "1234501",
							"vsa_management_ip": "150.136.212.147",
							"lifs": map[string]interface{}{
								"intercluster":     map[string]interface{}{"ip": "10.38.25.146"},
								"nodemgmtinternal": map[string]interface{}{"ip": "10.38.0.1"},
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(childJSON)
	require.NoError(t, err)
	events := []*historypb.HistoryEvent{
		{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "OCICreatePoolWorkflow"},
				},
			},
		},
		{
			EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
				ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "vlm.CreateVSAClusterDeploymentWorkflow"},
					Result: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{{Data: raw}},
					},
				},
			},
		},
	}
	f := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{events}}
	meta := getWorkflowInputMetadata(ctx, f, "ns", "wf", "run")
	require.NotNil(t, meta)
	require.NotEmpty(t, meta.Vms)
	require.Equal(t, "FsnIdocnv-vm-01", meta.Vms[0].Name)
	require.Equal(t, "1234501", meta.Vms[0].SerialNumber)
	require.Equal(t, "150.136.212.147", meta.Vms[0].VSAManagementIP)
}

func TestGetWorkflowInputMetadata_WrongParentOrChildSkipped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	raw, err := json.Marshal(map[string]string{"x": "y"})
	require.NoError(t, err)
	events := []*historypb.HistoryEvent{
		{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "OtherWorkflow"},
				},
			},
		},
		{
			EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
				ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "vlm.CreateVSAClusterDeploymentWorkflow"},
					Result:       &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: raw}}},
				},
			},
		},
	}
	f := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{events}}
	meta := getWorkflowInputMetadata(ctx, f, "ns", "wf", "run")
	require.Nil(t, meta, "no metadata keys when parent is not OCICreatePoolWorkflow and child branch is skipped")
}

type fakeTemporalQuerier struct {
	describe *workflowservice.DescribeWorkflowExecutionResponse
	descErr  error
	hist     historyFetcher
}

func withTemporalNamespace(t *testing.T, ns string) {
	t.Helper()
	orig := temporalNamespace
	temporalNamespace = ns
	t.Cleanup(func() {
		temporalNamespace = orig
	})
}

func (f *fakeTemporalQuerier) DescribeWorkflowExecution(ctx context.Context, workflowID, runID string) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return f.describe, f.descErr
}

func (f *fakeTemporalQuerier) History() historyFetcher {
	return f.hist
}

func TestQueryWithClient_DescribeError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := &fakeTemporalQuerier{descErr: errors.New("not found")}
	_, err := queryWithClient(ctx, q, "wf", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestQueryWithClient_NoExecutionInfo(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	q := &fakeTemporalQuerier{describe: &workflowservice.DescribeWorkflowExecutionResponse{}}
	res, err := queryWithClient(ctx, q, "wf", "")
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusFailed, res.Status)
	require.NotNil(t, res.Error)
	require.Equal(t, "no workflow execution info", res.Error.Message)
}

func TestQueryWithClient_CompletedSetsWorkflowTypeAndMetadata(t *testing.T) {
	withTemporalNamespace(t, "test-ns")

	ctx := context.Background()
	childJSON := map[string]interface{}{
		"vlm_config": map[string]interface{}{
			"cloud": map[string]interface{}{
				"ha_pair": []interface{}{
					map[string]interface{}{
						"vm1": map[string]interface{}{
							"name":              "single-vm",
							"serial_number":     "9001",
							"vsa_management_ip": "10.0.0.3",
							"lifs": map[string]interface{}{
								"intercluster":     map[string]interface{}{"ip": "10.0.0.1"},
								"nodemgmtinternal": map[string]interface{}{"ip": "10.0.0.2"},
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(childJSON)
	require.NoError(t, err)
	histEvents := []*historypb.HistoryEvent{
		{
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "OCICreatePoolWorkflow"},
				},
			},
		},
		{
			EventType: enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_ChildWorkflowExecutionCompletedEventAttributes{
				ChildWorkflowExecutionCompletedEventAttributes: &historypb.ChildWorkflowExecutionCompletedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "vlm.CreateVSAClusterDeploymentWorkflow"},
					Result:       &commonpb.Payloads{Payloads: []*commonpb.Payload{{Data: raw}}},
				},
			},
		},
	}
	hf := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{histEvents}}
	q := &fakeTemporalQuerier{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status: enums.WORKFLOW_EXECUTION_STATUS_COMPLETED,
				Type:   &commonpb.WorkflowType{Name: "OCICreatePoolWorkflow"},
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "resolved-wf",
					RunId:      "run-abc",
				},
			},
		},
		hist: hf,
	}
	res, err := queryWithClient(ctx, q, "wf-in", "")
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusCompleted, res.Status)
	require.Equal(t, "OCICreatePoolWorkflow", res.WorkflowType)
	require.Nil(t, res.Error)
	require.Equal(t, "test-ns", hf.lastNamespace)
	require.NotNil(t, res.Metadata)
	require.NotEmpty(t, res.Metadata.Vms)
	require.Equal(t, "single-vm", res.Metadata.Vms[0].Name)
}

func TestQueryWithClient_FailedIncludesError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	failEv := &historypb.HistoryEvent{
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
				Failure: &failurepb.Failure{Message: "activity blew up"},
			},
		},
	}
	hf := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{{failEv}}}
	q := &fakeTemporalQuerier{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status: enums.WORKFLOW_EXECUTION_STATUS_FAILED,
				Type:   &commonpb.WorkflowType{Name: "OCICreatePoolWorkflow"},
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "wf",
					RunId:      "r1",
				},
			},
		},
		hist: hf,
	}
	res, err := queryWithClient(ctx, q, "wf", "")
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusFailed, res.Status)
	require.Equal(t, "OCICreatePoolWorkflow", res.WorkflowType)
	require.NotNil(t, res.Error)
	require.Contains(t, res.Error.Message, "activity blew up")
}

func TestQueryWithClient_TimedOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ev := &historypb.HistoryEvent{
		EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{},
		},
	}
	hf := &fakeHistoryFetcher{pages: [][]*historypb.HistoryEvent{{ev}}}
	q := &fakeTemporalQuerier{
		describe: &workflowservice.DescribeWorkflowExecutionResponse{
			WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
				Status: enums.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
				Type:   &commonpb.WorkflowType{Name: "SomeWorkflow"},
			},
		},
		hist: hf,
	}
	res, err := queryWithClient(ctx, q, "wf", "")
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusTimedOut, res.Status)
	require.Equal(t, "SomeWorkflow", res.WorkflowType)
	require.NotNil(t, res.Error)
	require.Contains(t, res.Error.Message, "timed out")
}
