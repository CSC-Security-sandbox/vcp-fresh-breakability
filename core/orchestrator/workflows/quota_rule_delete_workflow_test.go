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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestDeleteQuotaRuleWorkflow(t *testing.T) {
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
			AccountID:   1,
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
			DiskLimitInKib: 100 * 1024,
			QuotaTarget:    "1001",
			VolumeID:       volumeID,
			AccountID:      1,
			State:          models.LifeCycleStateDeleting,
			StateDetails:   models.LifeCycleStateDeletingDetails,
			RQuota:         false,
		}
	}

	createBaseParams := func() *commonparams.DeleteQuotaRulesParam {
		return &commonparams.DeleteQuotaRulesParam{
			QuotaRuleUUID: "quota-rule-uuid-1",
			ProjectId:     "test-project",
			LocationId:    "us-central1-a",
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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(nil, errors.New("volume not found"))
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDataProtectionVolume", func(tt *testing.T) {
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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(true, []string{"NFSV3"})

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
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
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(nil, errors.New("failed to get volume replication"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(false, errors.New("replication state validation failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		jwtToken := "test-jwt-token"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(nil, errors.New("failed to get matching quota rule"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteQuotaRuleOnDestinationFails", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(nil, errors.New("failed to delete quota rule on destination"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

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
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get nodes"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("", errors.New("failed to get quota UUID"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaUUIDIsEmpty", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("", nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteQuotaRuleOnOntapFails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(nil, errors.New("failed to delete quota rule on ONTAP"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenListQuotaRulesForVolumeFails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return(nil, errors.New("failed to list quota rules"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteFailsWithLastQuotaRequiringDisable", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: vsa.DeletedRuleEnforced,
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{quotaRule}, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume, false).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteFailsRequiringReinitialization", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.QuotaReinitialization)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()
		otherQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{UUID: "other-quota-rule-uuid"},
		}

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: vsa.DeletedRuleEnforced,
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{quotaRule, otherQuotaRule}, nil)
		env.OnActivity("QuotaReinitialization", mock.Anything, mock.Anything, volume).Return(nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteFailsWithUnexpectedError", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "unexpected error",
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		// Workflow fails; defer sees err != nil and marks quota rule in error state
		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenHandleQuotaEnableDisableFails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: vsa.DeletedRuleEnforced,
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{quotaRule}, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume, false).Return(nil, errors.New("failed to disable quota"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaDisableFailsWithFailureState", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: vsa.DeletedRuleEnforced,
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{quotaRule}, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume, false).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: "quota disable failed",
		}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaReinitializationFails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.QuotaReinitialization)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()
		otherQuotaRule := &datamodel.QuotaRule{
			BaseModel: datamodel.BaseModel{UUID: "other-quota-rule-uuid"},
		}

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{
			State:   vsa.JobRespFailure,
			Message: vsa.DeletedRuleEnforced,
		}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{quotaRule, otherQuotaRule}, nil)
		env.OnActivity("QuotaReinitialization", mock.Anything, mock.Anything, volume).Return(errors.New("quota reinitialization failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenVolumeHasNoSVMDetails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		volume.Svm = nil // No SVM
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenUpdateRQuotaOnSvmFails", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(errors.New("failed to update RQuota on SVM"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenSuccessWithRQuotaEnabled", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		quotaRule.RQuota = true // RQuota enabled, so UpdateRQuotaOnSvm should be skipped
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenSuccessWithReplication", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock successful deletion with operation result (synchronous completion)
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			IsDone: true,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenReplicationNotEligible", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(false, nil) // Not eligible
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenSuccessWithoutReplication", func(tt *testing.T) {
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
		_ = activities.QuotaRuleUpdateActivity{SE: mockStorage}
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenHydrateQuotaRuleDeleteFails_NonFatal", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock successful deletion with operation result (synchronous completion)
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			IsDone: true,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		// Mock HydrateQuotaRuleDelete failure (non-fatal - should not fail workflow)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(errors.New("hydration failed"))
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow should succeed even if hydration fails (non-fatal)
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenRevertWithHydrateQuotaRuleCreate_Success", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.RevertQuotaRuleOnDestinationForDelete)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleCreate)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock successful deletion with operation result (synchronous completion)
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			IsDone: true,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get node"))
		// Mock revert and hydration after source deletion failure (synchronous completion)
		revertedQuotaRule := &datamodel.QuotaRule{
			Name:           quotaRule.Name,
			QuotaType:      quotaRule.QuotaType,
			QuotaTarget:    quotaRule.QuotaTarget,
			DiskLimitInKib: quotaRule.DiskLimitInKib,
			Description:    quotaRule.Description,
			RQuota:         quotaRule.RQuota,
		}
		revertResult := &activities.RevertQuotaRuleResult{
			OperationResult: &activities.QuotaRuleOperationResult{
				IsDone: true, // Synchronous completion
			},
			QuotaRule: revertedQuotaRule,
		}
		env.OnActivity("RevertQuotaRuleOnDestinationForDelete", mock.Anything, "dest-volume-uuid", mock.Anything, "us-west1-a", "987654321", &jwtToken).Return(revertResult, nil)
		env.OnActivity("HydrateQuotaRuleCreate", mock.Anything, mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow should fail due to GetNode failure, but revert should be called
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenRevertRequiresPolling", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.RevertQuotaRuleOnDestinationForDelete)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleCreate)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"
		revertOperationName := "revert-operation-123"

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
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock successful deletion with operation result (synchronous completion)
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			IsDone: true,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get node"))
		// Mock revert with async operation that requires polling
		revertedQuotaRule := &datamodel.QuotaRule{
			Name:           quotaRule.Name,
			QuotaType:      quotaRule.QuotaType,
			QuotaTarget:    quotaRule.QuotaTarget,
			DiskLimitInKib: quotaRule.DiskLimitInKib,
			Description:    quotaRule.Description,
			RQuota:         quotaRule.RQuota,
		}
		revertResult := &activities.RevertQuotaRuleResult{
			OperationResult: &activities.QuotaRuleOperationResult{
				OperationName: revertOperationName,
				IsDone:        false, // Async operation
			},
			QuotaRule: revertedQuotaRule,
		}
		env.OnActivity("RevertQuotaRuleOnDestinationForDelete", mock.Anything, "dest-volume-uuid", mock.Anything, "us-west1-a", "987654321", &jwtToken).Return(revertResult, nil)
		// Mock polling for revert operation completion (JWT token is reused from above)
		env.OnActivity("DescribeQuotaRuleRemoteJob", mock.Anything, revertOperationName, "us-west1-a", "987654321", &jwtToken).Return(nil)
		env.OnActivity("HydrateQuotaRuleCreate", mock.Anything, mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow should fail due to GetNode failure, but revert should be called and polling should complete
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenRevertWithHydrateQuotaRuleCreateFails_NonFatal", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.RevertQuotaRuleOnDestinationForDelete)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleCreate)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		destQuotaRuleID := "dest-quota-rule-uuid"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		jwtToken := "test-jwt-token"
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock successful deletion with operation result (synchronous completion)
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			IsDone: true,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get node"))
		// Mock revert success but hydration failure (non-fatal) - synchronous completion
		revertedQuotaRule := &datamodel.QuotaRule{
			Name:           quotaRule.Name,
			QuotaType:      quotaRule.QuotaType,
			QuotaTarget:    quotaRule.QuotaTarget,
			DiskLimitInKib: quotaRule.DiskLimitInKib,
			Description:    quotaRule.Description,
			RQuota:         quotaRule.RQuota,
		}
		revertResult := &activities.RevertQuotaRuleResult{
			OperationResult: &activities.QuotaRuleOperationResult{
				IsDone: true, // Synchronous completion
			},
			QuotaRule: revertedQuotaRule,
		}
		env.OnActivity("RevertQuotaRuleOnDestinationForDelete", mock.Anything, "dest-volume-uuid", mock.Anything, "us-west1-a", "987654321", &jwtToken).Return(revertResult, nil)
		env.OnActivity("HydrateQuotaRuleCreate", mock.Anything, mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321").Return(errors.New("hydration failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow should fail due to GetNode failure, but revert should be called and hydration failure should not cause additional error
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteQuotaRuleOnDestinationRequiresPolling", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.HydrateQuotaRuleDelete)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"
		operationName := "delete-operation-123"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock async deletion operation that requires polling
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			OperationName: operationName,
			IsDone:        false,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		// Mock polling for operation completion
		env.OnActivity("DescribeQuotaRuleRemoteJob", mock.Anything, operationName, "us-west1-a", "987654321", &jwtToken).Return(nil)
		env.OnActivity("HydrateQuotaRuleDelete", mock.Anything, destQuotaRuleID, "dest-volume-uuid", "us-west1-a", "987654321").Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid-123", nil)
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid-123", mock.Anything).Return(&vsa.JobStatus{State: vsa.JobRespSuccess}, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, false).Return(nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("WhenDeleteQuotaRuleOnDestinationPollingFails", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleCommonActivity.GetSignedDstTokenForQuotaRule)
		env.RegisterActivity(quotaRuleCommonActivity.GetMatchingQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleCommonActivity.DescribeQuotaRuleRemoteJob)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		destQuotaRuleID := "dest-quota-rule-uuid"
		jwtToken := "test-jwt-token"
		operationName := "delete-operation-123"

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

		// Mock GetJob for EnsureJobState
		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications[0], params.LocationId).Return(true, nil)
		env.OnActivity("GetSignedDstTokenForQuotaRule", mock.Anything, "987654321").Return(&jwtToken, nil)
		env.OnActivity("GetMatchingQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", "us-west1-a", "987654321", quotaRule.Name, &jwtToken).Return(&destQuotaRuleID, nil)
		// Mock async deletion operation that requires polling
		deleteOperationResult := &activities.QuotaRuleOperationResult{
			OperationName: operationName,
			IsDone:        false,
		}
		env.OnActivity("DeleteQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", destQuotaRuleID, "us-west1-a", "987654321", &jwtToken).Return(deleteOperationResult, nil)
		// Mock polling failure
		env.OnActivity("DescribeQuotaRuleRemoteJob", mock.Anything, operationName, "us-west1-a", "987654321", &jwtToken).Return(errors.New("polling failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})
	t.Run("WhenQuotaRuleIsInCreatingStateAndCancellationHandlingSucceeds", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		poolActivity := activities.PoolActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(poolActivity.GetCreateJobByResourceUUID)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		quotaRule.State = models.LifeCycleStateCreating
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetCreateJobByResourceUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "create-job-uuid"},
			State:     string(models.JobsStatePROCESSING),
		}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("", nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("WhenQuotaRuleIsInCreatingStateAndCorrelationIDErrorOccurs", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		poolActivity := activities.PoolActivity{SE: mockStorage}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(poolActivity.GetCreateJobByResourceUUID)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		quotaRule.State = models.LifeCycleStateCreating
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetCreateJobByResourceUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "create-job-uuid"},
			State:     string(models.JobsStatePROCESSING),
		}, nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("", nil)
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		// Workflow should still succeed even if correlation ID error occurs
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
	t.Run("WhenQuotaRuleIsInCreatingStateAndCancellationHandlingFails", func(tt *testing.T) {
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
		quotaRuleDeleteActivity := activities.QuotaRuleDeleteActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}
		poolActivity := activities.PoolActivity{SE: mockStorage}
		cancellationActivity := &activities.CancellationActivity{}

		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleCommonActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(commonActivity.GetJob)
		env.RegisterActivity(poolActivity.GetCreateJobByResourceUUID)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleCommonActivity.GetOntapQuotaUUID)
		env.RegisterActivity(quotaRuleDeleteActivity.DeleteQuotaRuleOnOntap)
		env.RegisterActivity(quotaRuleDeleteActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(cancellationActivity.IsWorkflowRunningActivity)
		env.RegisterActivity(cancellationActivity.SendCancelSignalActivity)
		env.RegisterActivity(cancellationActivity.WaitForWorkflowCancellationAckActivity)
		env.RegisterActivity(cancellationActivity.ForceCancelWorkflowActivity)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		quotaRule.State = models.LifeCycleStateCreating
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetJob", mock.Anything, mock.Anything).Return(&datamodel.Job{
			BaseModel: datamodel.BaseModel{UUID: "default-test-workflow-id"},
			State:     string(models.JobsStateNEW),
		}, nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID, int64(1)).Return(volume, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		// Return a create job result to trigger cancellation handling
		createJobResult := &commonparams.CreateJobResult{
			JobUUID:    "create-job-uuid",
			WorkflowID: "create-workflow-id",
		}
		env.OnActivity("GetCreateJobByResourceUUID", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(createJobResult, nil)
		// Make IsWorkflowRunningActivity fail to trigger error path in HandleCancellationInDeleteWorkflow (covers line 133)
		env.OnActivity("IsWorkflowRunningActivity", mock.Anything, mock.Anything).Return(false, errors.New("failed to check workflow status"))
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("GetOntapQuotaUUID", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.QuotaTarget, "delete").Return("quota-uuid", nil)
		deleteResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota deleted",
		}
		env.OnActivity("DeleteQuotaRuleOnOntap", mock.Anything, "quota-uuid", mock.Anything).Return(deleteResp, nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, volume.UUID).Return([]*datamodel.QuotaRule{}, nil)
		// UpdateRQuotaOnSvm is called when RQuota is false (line 472 in workflow)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(DeleteQuotaRuleWorkflow, params, quotaRule)

		// Workflow should still succeed even if cancellation handling fails (covers line 133)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
