package replicationWorkflows

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	googleproxyclient "github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/google-proxy-client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	coreModels "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/replication"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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
		env.RegisterActivity(resumeReplicationActivity.SetHybridReplicationVariablesResume)
		env.RegisterActivity(resumeReplicationActivity.GetSrcBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetDstBasePathResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedSrcTokenResume)
		env.RegisterActivity(resumeReplicationActivity.GetSignedDstTokenResume)
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel:         &datamodel.VolumeReplication{},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{}, nil)
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

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_Success", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-2"}, Name: "src-rule-2"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{
				ResourceId: "dst-volume",
				VolumeId:   googleproxyclient.NewOptString("dst-volume-uuid"),
			}
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DestinationQuotaRules = sourceQuotaRules
			return result, nil
		})
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_ErrorListingSourceQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStatePROCESSING), mock.Anything, mock.Anything).Return(nil)
		// Quota rule failures should result in DONE state (partial success), not ERROR
		// UpdateJob signature: (ctx, jobID, status, trackingID, errorDetails)
		// When temporal.NewApplicationError is used, trackingID is 0 and errorDetails contains the error message
		// The error message may have additional suffix from ApplicationError
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStateDONE), 0, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			basePath := "https://src-base.com"
			result.SrcBasePath = &basePath
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			basePath := "https://dst-base.com"
			result.DstBasePath = &basePath
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			token := "src-token"
			result.SrcJwtToken = &token
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			token := "dst-token"
			result.DstJwtToken = &token
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.SrcVolume = &googleproxyclient.VolumeV1beta{ResourceId: "src-vol"}
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			jobId := "job-123"
			result.JobId = &jobId
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		// Quota rule failures should not cause workflow to fail - it completes successfully with partial success
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_ErrorListingDestinationQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStatePROCESSING), mock.Anything, mock.Anything).Return(nil)
		// Quota rule failures should result in DONE state (partial success), not ERROR
		// UpdateJob signature: (ctx, jobID, status, trackingID, errorDetails)
		// When temporal.NewApplicationError is used, trackingID is 0 and errorDetails contains the error message
		// The error message may have additional suffix from ApplicationError
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStateDONE), 0, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			basePath := "https://src-base.com"
			result.SrcBasePath = &basePath
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			basePath := "https://dst-base.com"
			result.DstBasePath = &basePath
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			token := "src-token"
			result.SrcJwtToken = &token
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			token := "dst-token"
			result.DstJwtToken = &token
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.SrcVolume = &googleproxyclient.VolumeV1beta{ResourceId: "src-vol"}
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			jobId := "job-123"
			result.JobId = &jobId
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		// Quota rule failures should not cause workflow to fail - it completes successfully with partial success
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_ErrorDehydratingQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		projectNumber := "123456789"
		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		// Dehydration failures now return nil error (partial failure handling)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError()) // Dehydration failures don't cause workflow failure
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_PartialDehydrationFailure", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-2"}, Name: "src-rule-2"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-2"}, Name: "dst-rule-2"},
		}

		// Partial dehydration - only one rule dehydrated
		partiallyDehydratedRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		// Return partial dehydration with nil error (partial failure handling)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(partiallyDehydratedRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			// Verify recovery mode: SourceQuotaRules should be nil and DestinationQuotaRules should be the dehydrated list
			assert.Nil(tt, result.SourceQuotaRules, "SourceQuotaRules should be nil in recovery mode")
			assert.NotNil(tt, result.DestinationQuotaRules, "DestinationQuotaRules should contain dehydrated rules")
			assert.Len(tt, result.DestinationQuotaRules, 1, "Should have 1 dehydrated quota rule")
			result.DestinationQuotaRules = partiallyDehydratedRules
			return result, nil
		})
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError()) // Partial dehydration failures don't cause workflow failure
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_ErrorSyncingSourceQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
			},
		}

		projectNumber := "123456789"
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStatePROCESSING), mock.Anything, mock.Anything).Return(nil)
		// Quota rule failures should result in DONE state (partial success), not ERROR
		// UpdateJob signature: (ctx, jobID, status, trackingID, errorDetails)
		// When temporal.NewApplicationError is used, trackingID is 0 and errorDetails contains the error message
		// The error message may have additional suffix from ApplicationError
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStateDONE), 0, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(nil, assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		// Quota rule failures should not cause workflow to fail - it completes successfully with partial success
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_ErrorHydratingQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
			},
		}

		projectNumber := "123456789"
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStatePROCESSING), mock.Anything, mock.Anything).Return(nil)
		// Quota rule failures should result in DONE state (partial success), not ERROR
		// UpdateJob signature: (ctx, jobID, status, trackingID, errorDetails)
		// When temporal.NewApplicationError is used, trackingID is 0 and errorDetails contains the error message
		// The error message may have additional suffix from ApplicationError
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStateDONE), 0, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DestinationQuotaRules = sourceQuotaRules
			return result, nil
		})
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		// Quota rule failures should not cause workflow to fail - it completes successfully with partial success
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_NoDestinationQuotaRules", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		// No destination quota rules
		destQuotaRules := []*datamodel.QuotaRule{}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{
				ResourceId: "dst-volume",
				VolumeId:   googleproxyclient.NewOptString("dst-volume-uuid"),
			}
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		// Dehydration skipped - go straight to sync
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DestinationQuotaRules = sourceQuotaRules
			return result, nil
		})
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestResumeReplicationWorkflow_HybridReplicationVolumeSkipsQuotaRules", func(tt *testing.T) {
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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		migrationType := string(coreModels.HybridReplicationParametersReplicationTypeMIGRATION)
		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
					HybridReplicationAttributes: &datamodel.HybridReplicationAttribute{
						HybridReplicationType: &migrationType,
					},
				},
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = true // This is hybrid replication
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		// ListQuotaRules activities should NOT be called for hybrid replication
	})

	t.Run("TestResumeReplicationWorkflow_WithQuotaRuleSync_QuotaRuleFailureTreatedAsPartialSuccess", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStatePROCESSING), mock.Anything, mock.Anything).Return(nil)
		// Quota rule failures should result in DONE state (partial success), not ERROR
		// UpdateJob signature: (ctx, jobID, status, trackingID, errorDetails)
		// When temporal.NewApplicationError is used, trackingID is 0 and errorDetails contains the error message
		// The error message may have additional suffix from ApplicationError
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, string(coreModels.JobsStateDONE), 0, mock.Anything).Return(nil)
		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DestinationQuotaRules = sourceQuotaRules
			return result, nil
		})
		// Hydration fails - should be treated as partial success
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		assert.True(tt, env.IsWorkflowCompleted())
		// Quota rule failures should not cause workflow to fail - it completes successfully with partial success
		assert.NoError(tt, env.GetWorkflowError())
	})

	// This test explicitly verifies that quota rule failure error (ErrResumeReplicationQuotaRuleFailure)
	// returned from Run() is properly detected by isResumeQuotaRuleFailure() and results in DONE status
	t.Run("TestResumeReplicationWorkflow_QuotaRuleFailureError_IsDetectedAndTreatedAsPartialSuccess", func(tt *testing.T) {
		// Set quotaRuleSync to true for this test
		cleanup := setQuotaRuleSyncTrue()
		defer cleanup()

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
		env.RegisterActivity(resumeReplicationActivity.VerifyDstVolume)
		env.RegisterActivity(resumeReplicationActivity.ResizeVolumeIfNeeded)
		env.RegisterActivity(resumeReplicationActivity.ResumeReplicationOnDestination)
		env.RegisterActivity(resumeReplicationActivity.DescribeRemoteJobResume)
		env.RegisterActivity(resumeReplicationActivity.MountReplicationAfterResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnSourceResume)
		env.RegisterActivity(resumeReplicationActivity.ListQuotaRulesOnDestinationResume)
		env.RegisterActivity(resumeReplicationActivity.DehydrateQuotaRulesResume)
		env.RegisterActivity(resumeReplicationActivity.AddSrcQuotaRulesToDstDB)
		env.RegisterActivity(resumeReplicationActivity.HydrateQuotaRulesResume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := &commonparams.ResumeReplicationParams{}

		projectNumber := "123456789"
		sourceQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "src-quota-1"}, Name: "src-rule-1"},
		}

		destQuotaRules := []*datamodel.QuotaRule{
			{BaseModel: datamodel.BaseModel{UUID: "dst-quota-1"}, Name: "dst-rule-1"},
		}

		event := &replication.ResumeReplicationEvent{
			CommonReplicationEventParams: replication.CommonReplicationEventParams{
				ReplicationModel: &datamodel.VolumeReplication{
					ReplicationAttributes: &datamodel.ReplicationDetails{
						DestinationLocation: "us-central1",
					},
				},
				DestinationProjectNumber: projectNumber,
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
				}{
					status:       status,
					trackingID:   trackingID,
					errorDetails: errorDetails,
				})
			}).
			Return(nil)

		env.OnActivity("SetHybridReplicationVariablesResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			if result != nil {
				result.IsSrcForHybridReplication = false
				result.IsHybridReplicationVolume = false
			}
			return result, nil
		})
		env.OnActivity("GetSrcBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetDstBasePathResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedSrcTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("GetSignedDstTokenResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("VerifyDstVolume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DstVolume = &googleproxyclient.VolumeV1beta{ResourceId: "dst-vol"}
			result.DstProjectNumber = &projectNumber
			return result, nil
		})
		env.OnActivity("ResizeVolumeIfNeeded", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ResumeReplicationOnDestination", mock.Anything, mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult, params *commonparams.ResumeReplicationParams) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("DescribeRemoteJobResume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("MountReplicationAfterResume", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			return result, nil
		})
		env.OnActivity("ListQuotaRulesOnSourceResume", mock.Anything, mock.Anything).Return(sourceQuotaRules, nil)
		env.OnActivity("ListQuotaRulesOnDestinationResume", mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("DehydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(destQuotaRules, nil)
		env.OnActivity("AddSrcQuotaRulesToDstDB", mock.Anything, mock.Anything).Return(func(ctx context.Context, result *replication.ResumeReplicationResult) (*replication.ResumeReplicationResult, error) {
			result.DestinationQuotaRules = sourceQuotaRules
			return result, nil
		})
		// Hydration fails - this should trigger the quota rule failure error path
		// Run() returns vsaerrors.NewVCPError(vsaerrors.ErrResumeReplicationQuotaRuleFailure, ...)
		// which should be detected by isResumeQuotaRuleFailure() and result in DONE status
		env.OnActivity("HydrateQuotaRulesResume", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)

		env.ExecuteWorkflow(ResumeReplicationWorkflow, params, event)

		// Verify workflow completed successfully (partial success)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError(), "Workflow should complete without error for quota rule failure (partial success)")

		// Verify that UpdateJob was called with DONE status (not ERROR)
		// This confirms that isResumeQuotaRuleFailure() correctly detected the quota rule failure error
		foundDoneWithQuotaRuleError := false
		for _, call := range updateJobCalls {
			if call.status == string(coreModels.JobsStateDONE) {
				// When temporal.NewApplicationError is used, trackingID should be 0
				assert.Equal(tt, 0, call.trackingID, "TrackingID should be 0 when using temporal.NewApplicationError")
				// errorDetails should contain the quota rule error message (may have additional suffix from ApplicationError)
				assert.Contains(tt, call.errorDetails, replicationQuotaRuleError, "Error details should contain replicationQuotaRuleError")
				foundDoneWithQuotaRuleError = true
				break
			}
		}
		assert.True(tt, foundDoneWithQuotaRuleError, "UpdateJob should be called with DONE status and quota rule error message. This verifies isResumeQuotaRuleFailure() correctly detected the error")

		// Verify no ERROR status was set (quota rule failures should NOT cause workflow failure)
		for _, call := range updateJobCalls {
			assert.NotEqual(tt, string(coreModels.JobsStateERROR), call.status, "Job should not be marked as ERROR for quota rule failure - should be DONE with partial success")
		}
	})
}
