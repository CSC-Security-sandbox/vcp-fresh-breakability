package replicationWorkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestResumeReplicationWorkflow(t *testing.T) {
	t.Run("TestResumeReplicationWorkflow", func(tt *testing.T) {
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
		resumeReplicationActivity := replicationActivities.ResumeVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenResume)
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("TestResumeReplicationWorkflow_WhenIsSrcForHybridReplicationIsTrue", func(tt *testing.T) {
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
		resumeReplicationActivity := replicationActivities.ResumeVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(resumeReplicationActivity.SetHybridReplicationVariablesResume)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenResume)
		env.RegisterActivity(resumeReplicationActivity.HandleHybridReplicationResumeWhenGcnvIsSrc)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		reverseType := string(coreModels.HybridReplicationParametersReplicationTypeREVERSE)
		replicationModel := &datamodel.VolumeReplication{
			HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
				HybridReplicationType: &reverseType,
			},
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationLocation: "",
			},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: replicationModel,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = true
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("HandleHybridReplicationResumeWhenGcnvIsSrc", mock.Anything, mock.Anything).Return(nil, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}
