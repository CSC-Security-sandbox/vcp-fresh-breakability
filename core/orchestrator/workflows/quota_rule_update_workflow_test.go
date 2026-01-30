package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestUpdateQuotaRuleWorkflow(t *testing.T) {
	// Common test data
	volumeID := int64(1)
	poolID := int64(1)
	svmID := int64(1)

	createBaseVolume := func(isDataProtection bool, protocols []string) *datamodel.Volume {
		return &datamodel.Volume{
			BaseModel: datamodel.BaseModel{
				ID:   volumeID,
				UUID: "volume-uuid-1",
			},
			Name:        "test-volume",
			PoolID:      poolID,
			SizeInBytes: 200 * 1024 * 1024,
			Pool: &datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID: poolID,
				},
				DeploymentName: "test-deployment",
				PoolCredentials: &datamodel.PoolCredentials{
					Password: "test-password",
				},
			},
			Svm: &datamodel.Svm{
				BaseModel: datamodel.BaseModel{
					ID: svmID,
				},
				Name: "test-svm",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				IsDataProtection: isDataProtection,
				Protocols:        protocols,
			},
		}
	}

	createBaseQuotaRule := func() *datamodel.QuotaRule {
		return &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{
				ID:   1,
				UUID: "quota-rule-uuid-1",
			},
			Name:           "test-quota-rule",
			QuotaType:      "INDIVIDUAL_USER_QUOTA",
			DiskLimitInKib: 100 * 1024, // Original limit
			QuotaTarget:    "1001",
			VolumeID:       volumeID,
			AccountID:      1, // Add AccountID for GetVolumeByID calls
			State:          models.LifeCycleStateUpdating,
			StateDetails:   models.LifeCycleStateUpdatingDetails,
			RQuota:         true,
		}
	}

	createBaseParams := func(diskLimitInMib int64) *commonparams.UpdateQuotaRulesParam {
		return &commonparams.UpdateQuotaRulesParam{
			QuotaRuleUUID:  "quota-rule-uuid-1",
			ProjectId:      "test-project",
			LocationId:     "us-central1-a",
			DiskLimitInMib: diskLimitInMib,
		}
	}

	createBaseParamsDescriptionOnly := func() *commonparams.UpdateQuotaRulesParam {
		return &commonparams.UpdateQuotaRulesParam{
			QuotaRuleUUID:  "quota-rule-uuid-1",
			ProjectId:      "test-project",
			LocationId:     "us-central1-a",
			DiskLimitInMib: 0, // Only description update
			Description:    "Updated description",
		}
	}

	createBaseNodes := func() []*datamodel.Node {
		return []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				EndpointAddress: "127.0.0.1",
			},
		}
	}

	t.Run("WhenGetVolumeByIDFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		// Mock GetVolumeByID to fail
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(nil, errors.New("volume not found"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateQuotaRuleStateFailsForDPVolume", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParamsDescriptionOnly()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(true, []string{"NFSV3"})

		// Create expected quota rule with updated description for mock
		expectedQuotaRule := createBaseQuotaRule()
		expectedQuotaRule.Description = "Updated description"

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.MatchedBy(func(qr datamodel.QuotaRule) bool {
			return qr.UUID == expectedQuotaRule.UUID && qr.Description == expectedQuotaRule.Description
		}), mock.Anything).Return(errors.New("failed to update quota rule state"))
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateQuotaRuleStateFailsForDescriptionOnly", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParamsDescriptionOnly()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		// Create expected quota rule with updated description for mock
		expectedQuotaRule := createBaseQuotaRule()
		expectedQuotaRule.Description = "Updated description"

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.MatchedBy(func(qr datamodel.QuotaRule) bool {
			return qr.UUID == expectedQuotaRule.UUID && qr.Description == expectedQuotaRule.Description
		}), mock.Anything).Return(errors.New("failed to update quota rule state"))
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenGetNodeFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get nodes"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenGetOntapQuotaUUIDFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("", errors.New("failed to get quota UUID"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateQuotaRulesOnOntapFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(errors.New("failed to update quota rule on ONTAP"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenGetVolumeReplicationFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(nil, errors.New("failed to get volume replication"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenReplicationIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				// ReplicationAttributes is nil
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenVerifyReplicationStateFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(false, errors.New("replication state validation failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenGetMatchingQuotaRuleOnDestinationFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.RevertQuotaRulesOnSource)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		jwtToken := "test-jwt-token"
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(nil, errors.New("failed to get matching quota rule"))
		env.OnActivity("RevertQuotaRulesOnSource", mock.Anything, "quota-uuid-123", mock.Anything, quotaRule.DiskLimitInKib).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateQuotaRuleOnDestinationFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleActivity.RevertQuotaRulesOnSource)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		destinationQuotaRuleId := "dest-quota-rule-uuid-123"

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		jwtToken := "test-jwt-token"
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destinationQuotaRuleId, nil)
		env.OnActivity("UpdateQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destinationQuotaRuleId, int64(200*1024*1024), "us-west1-a", "987654321", &jwtToken).Return(nil, errors.New("failed to update quota rule on destination"))
		env.OnActivity("RevertQuotaRulesOnSource", mock.Anything, "quota-uuid-123", mock.Anything, quotaRule.DiskLimitInKib).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// After revert succeeds, workflow returns original error so defer marks quota rule in error state
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateQuotaRuleStateFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update quota rule state"))
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaUpdateWorkflowSucceeds", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		destinationQuotaRuleId := "dest-quota-rule-uuid-123"

		// Mock all activities to succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		jwtToken := "test-jwt-token"
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destinationQuotaRuleId, nil)
		env.OnActivity("UpdateQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destinationQuotaRuleId, int64(200*1024*1024), "us-west1-a", "987654321", &jwtToken).Return(&activities.QuotaRuleOperationResult{IsDone: true}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// Verify workflow completed successfully
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaUpdateWorkflowSucceedsForDPVolume", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(true, []string{"NFSV3"}) // Data protection volume
		nodes := createBaseNodes()

		// Mock all activities to succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.MatchedBy(func(qr datamodel.QuotaRule) bool {
			return qr.UUID == quotaRule.UUID
		}), mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// Verify workflow completed successfully
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaUpdateWorkflowSucceedsForDescriptionOnly", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParamsDescriptionOnly()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock all activities to succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(100*1024)).Return(nil) // Uses original disk limit for description-only
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.MatchedBy(func(qr datamodel.QuotaRule) bool {
			return qr.UUID == quotaRule.UUID
		}), mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// Verify workflow completed successfully
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenPopulateRetryPolicyParamsFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		// Set invalid environment variable to cause PopulateRetryPolicyParams to fail
		originalStartToCloseTimeout := StartToCloseTimeout
		StartToCloseTimeout = "invalid-duration"
		defer func() { StartToCloseTimeout = originalStartToCloseTimeout }()

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()

		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenNoReplicationsFound", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenReplicationIsNotEligible", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(false, nil) // Not eligible
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenParseProjectNumberFails", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "invalid-uri-format", // Invalid URI format
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDestinationQuotaRuleIdIsNil", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.RevertQuotaRulesOnSource)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		var nilString *string = nil

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		jwtToken := "test-jwt-token"
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(nilString, nil) // Returns nil
		env.OnActivity("RevertQuotaRulesOnSource", mock.Anything, "quota-uuid-123", mock.Anything, quotaRule.DiskLimitInKib).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDestinationQuotaRuleIdIsEmpty", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.RevertQuotaRulesOnSource)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					DestinationVolumeUUID: "dest-volume-uuid",
					DestinationLocation:   "us-west1-a",
				},
				RemoteUri: "projects/987654321/locations/us-west1-a/volumes/dest-volume-uuid/replications/replication-1",
			},
		}

		emptyString := ""
		emptyStringPtr := &emptyString

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		jwtToken := "test-jwt-token"
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(emptyStringPtr, nil) // Returns empty string
		// Empty destination ID causes UpdateQuotaRuleOnDestination to fail; workflow reverts and returns error so defer marks quota rule in error state
		env.OnActivity("UpdateQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "", int64(200*1024*1024), "us-west1-a", "987654321", &jwtToken).Return(nil, errors.New("destination quota rule id is empty"))
		env.OnActivity("RevertQuotaRulesOnSource", mock.Anything, "quota-uuid-123", mock.Anything, quotaRule.DiskLimitInKib).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// When destination quota rule ID is empty, UpdateQuotaRuleOnDestination fails; workflow reverts source and returns error (defer marks quota rule in error state)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaUpdateWorkflowSucceedsWithNoReplicationSync", func(tt *testing.T) {
		var ts testsuite.WorkflowTestSuite
		env := ts.NewTestWorkflowEnvironment()
		env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
		encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
		mockHeader := &commonpb.Header{
			Fields: map[string]*commonpb.Payload{
				"logParam": encodedValue,
			},
		}
		env.SetHeader(mockHeader)

		mockStorage := database.NewMockStorage(tt)
		quotaRuleActivity := activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRulesOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)

		params := createBaseParams(200 * 1024)
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		// Mock all activities to succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "update").Return("quota-uuid-123", nil)
		env.OnActivity("UpdateQuotaRulesOnOntap", mock.Anything, "quota-uuid-123", mock.Anything, int64(200*1024*1024)).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil) // No replications
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(UpdateQuotaRuleWorkflow, params, quotaRule)

		// Verify workflow completed successfully
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
