package replicationWorkflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestStopReplicationWorkflow(t *testing.T) {
	t.Run("TestStopReplicationWorkflow", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		stopReplicationActivity := replicationActivities.StopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(stopReplicationActivity.GetSrcBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetDstBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedSrcTokenStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedDstTokenStop)
		env.RegisterActivity(stopReplicationActivity.StopReplicationOnDestination)
		env.RegisterActivity(stopReplicationActivity.DescribeDestJobStop)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.StopReplicationParams{}

		event := &replication.StopReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("StopReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeDestJobStop", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(StopReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("TestStopReplicationWorkflow_WhenIsSrcForHybridReplicationIsTrue", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		stopReplicationActivity := replicationActivities.StopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(stopReplicationActivity.SetHybridReplicationVariablesStop)
		env.RegisterActivity(stopReplicationActivity.GetSrcBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetDstBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedSrcTokenStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedDstTokenStop)
		env.RegisterActivity(stopReplicationActivity.HandleHybridReplicationStopWhenGcnvIsSrc)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.StopReplicationParams{}

		reverseType := string(datamodel.HybridReplicationParametersReplicationTypeREVERSE)
		replicationModel := &datamodel.VolumeReplication{
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType: &reverseType,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "",
			},
		}

		event := &replication.StopReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: replicationModel,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesStop", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.StopReplicationResult) (*replication.StopReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = true
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("HandleHybridReplicationStopWhenGcnvIsSrc", mock.Anything, mock.Anything).Return(nil, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(StopReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	// Test: Quota rule failure from internal stop results in partial success (DONE with error details)
	t.Run("TestStopReplicationWorkflow_QuotaRuleFailure_PartialSuccess", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		mockStorage := database.NewMockStorage(tt)
		commonActivity := activities.CommonActivities{SE: mockStorage}
		stopReplicationActivity := replicationActivities.StopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(stopReplicationActivity.SetHybridReplicationVariablesStop)
		env.RegisterActivity(stopReplicationActivity.GetSrcBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetDstBasePathStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedSrcTokenStop)
		env.RegisterActivity(stopReplicationActivity.GetSignedDstTokenStop)
		env.RegisterActivity(stopReplicationActivity.StopReplicationOnDestination)
		env.RegisterActivity(stopReplicationActivity.DescribeDestJobStop)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.StopReplicationParams{}

		event := &replication.StopReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		// Track UpdateJob calls to verify the exact parameters
		var updateJobCalls []struct {
			status       string
			trackingID   int
			errorDetails string
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				status := args.Get(2).(string)
				trackingID := args.Get(3).(int)
				errorDetails := args.Get(4).(string)
				updateJobCalls = append(updateJobCalls, struct {
					status       string
					trackingID   int
					errorDetails string
				}{status, trackingID, errorDetails})
			}).
			Return(nil)

		env.OnActivity("SetHybridReplicationVariablesStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSrcBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenStop", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("StopReplicationOnDestination", mock.Anything, mock.Anything).Return(nil, nil)

		// Return error containing quota failure message (simulates internal describe returning Error)
		quotaErr := vsaerrors.NewVCPError(
			vsaerrors.ErrJobFailed,
			errors.New("job failed with error: "+datamodel.VolumeReplicationBreakRelationshipQuotaRuleFailure),
		)
		env.OnActivity("DescribeDestJobStop", mock.Anything, mock.Anything).Return(
			vsaerrors.WrapAsNonRetryableTemporalApplicationError(quotaErr),
		)

		env.ExecuteWorkflow(StopReplicationWorkflow, params, event)

		// Assert: Workflow should complete successfully (partial success)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())

		// Verify UpdateJob was called with DONE status and correct TrackingID/ErrorDetails
		foundDoneWithQuotaRuleError := false
		for _, call := range updateJobCalls {
			if call.status == string(datamodel.JobsStateDONE) {
				assert.Equal(tt, vsaerrors.ErrBreakReplicationQuotaRuleFailure, call.trackingID,
					"TrackingID should be ErrBreakReplicationQuotaRuleFailure")
				assert.Contains(tt, call.errorDetails, datamodel.VolumeReplicationBreakRelationshipQuotaRuleFailure,
					"Error details should contain quota rule failure message")
				foundDoneWithQuotaRuleError = true
				break
			}
		}
		assert.True(tt, foundDoneWithQuotaRuleError,
			"UpdateJob should be called with DONE status and quota rule error. This verifies isQuotaRuleFailure() correctly detected the error")
		env.AssertExpectations(tt)
	})
}
