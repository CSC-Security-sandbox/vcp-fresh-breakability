package replicationWorkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func TestStopInternalVolumeReplicationWorkflow(t *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})

	t.Run("TestStopInternalVolumeReplicationWorkflow_DeferCase_UpdateReplicationState", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		replicationCommonActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(replicationCommonActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, assert.AnError)
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_Success", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		destinationVolumeUUID := "destination-volume-uuid"
		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_ListQuotaRulesError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(nil, errors.New("failed to list quota rules"))
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		// Now ListQuotaRulesForVolume error is treated as quota error, so workflow completes with DONE status
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_EmptyList", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return([]*datamodel.QuotaRule{}, nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_ProcessQuotaRulesError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(errors.New("failed to enable RQuota"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_DefaultQuotaUpdate", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		// When HandleDefaultQuotaRuleUpdate succeeds, no quota rule is created, so HandleQuotaEnablementAndReinitialization is not called
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_DefaultQuotaNotFound_Create", func(tt *testing.T) {
		// Note: This test simulates the case where default quota is not found and needs to be created.
		// We test the creation path by using an individual quota rule (bypasses default quota update).
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)
		// ParseQuotaRuleErrors not needed for this test since it should succeed

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		// Use individual quota rule to test the creation path (bypasses the IsNotFoundErr issue with Temporal-wrapped errors)
		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_DefaultQuotaNotFound_WithErrorMessage", func(tt *testing.T) {
		// This test verifies that when HandleDefaultQuotaRuleUpdate returns an error with "not found"
		// in the message, it's treated as a not found error and the quota rule is created.
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		// Use default quota rule (QuotaTarget == "") to test the not found error message handling
		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "", // Default quota
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		// Return error with "not found" in message - should be treated as not found and trigger creation
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("default quota rule not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_MultipleRules", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-2",
				},
				Name:           "quota-rule-2",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "2000",
				DiskLimitInKib: 2097152,
			},
		}

		quotaRuleResponse1 := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}
		quotaRuleResponse2 := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == "quota-rule-uuid-1"
		})).Return(quotaRuleResponse1, nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == "quota-rule-uuid-2"
		})).Return(quotaRuleResponse2, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse2).Return(nil)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_WithErrors", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		// ParseQuotaRuleErrors will fail to serialize, but workflow handles it gracefully
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_DefaultQuotaUpdateError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		// Note: ParseQuotaRuleErrors is not registered here because Temporal can't serialize
		// the error field in QuotaRuleCreationError. The workflow will handle the error gracefully.

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update default quota"))
		// ParseQuotaRuleErrors will fail to serialize, but workflow handles it gracefully
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_RQuotaError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(errors.New("failed to enable RQuota"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		// ParseQuotaRuleErrors will fail to serialize, but workflow handles it gracefully
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_QuotaEnablementError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleCommonActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(errors.New("failed to enable quota subsystem"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		// ParseQuotaRuleErrors will fail to serialize, but workflow handles it gracefully
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopReplicationWorkflow_WithQuotaRules_ParseErrorsError", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleCommonActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(commonActivity.UpdateJobStatus)
		env.RegisterActivity(quotaRuleCommonActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleCommonActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)

		volume := &datamodel.Volume{
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"}},
		}
		destinationVolumeUUID := "destination-volume-uuid"
		replicationDb := &datamodel.VolumeReplication{
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				Name: "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: destinationVolumeUUID,
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)
		// ParseQuotaRuleErrors will fail to serialize, but workflow handles it gracefully
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopInternalVolumeReplicationWorkflow_QuotaErrorHandling_Success", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}

		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(quotaRuleActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID:    "volume-external-uuid",
				BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "test-quota-rule",
				QuotaType:      "tree",
				QuotaTarget:    "target1",
				DiskLimitInKib: 1048576,
				AccountID:      int64(1),
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	// TestStopInternalVolumeReplicationWorkflow_QuotaRuleFailureErrorHandler_Success verifies that
	// with DONE status, making the stop operation successful despite quota rule failures.
	t.Run("TestStopInternalVolumeReplicationWorkflow_QuotaRuleFailureErrorHandler_Success", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}

		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(quotaRuleActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID:    "volume-external-uuid",
				BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "test-quota-rule",
				QuotaType:      "tree",
				QuotaTarget:    "target1",
				DiskLimitInKib: 1048576,
				AccountID:      int64(1),
			},
		}

		// Mock UpdateJobStatus - first call for PROCESSING, second call for DONE (when error handler catches quota rule failure)
		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow should complete successfully (no error) because error handler catches quota rule failure
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopInternalVolumeReplicationWorkflow_UpdateQuotaRulesStateToError_Failure", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		replicationCommonActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}

		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(quotaRuleActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(replicationCommonActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID:    "volume-external-uuid",
				BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "test-quota-rule",
				QuotaType:      "tree",
				QuotaTarget:    "target1",
				DiskLimitInKib: 1048576,
				AccountID:      int64(1),
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(errors.New("database error"))
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow completes successfully after updating job status to ERROR when UpdateQuotaRulesStateToError fails
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})

	t.Run("TestStopInternalVolumeReplicationWorkflow_UpdateVolumeReplicationForQuotaError_Failure", func(tt *testing.T) {
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
		internalStopReplicationActivity := replicationActivities.InternalStopVolumeReplicationActivity{SE: mockStorage}
		quotaRuleActivity := activities.QuotaRuleCommonActivity{SE: mockStorage}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		replicationCommonActivity := replicationActivities.VolumeReplicationCreateActivity{SE: mockStorage}

		env.SetHeader(mockHeader)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(internalStopReplicationActivity.AbortVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.BreakVolumeReplication)
		env.RegisterActivity(internalStopReplicationActivity.GetSnapMirrorFromOntap)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationStopDetails)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeToNonDPVolume)
		env.RegisterActivity(internalStopReplicationActivity.UpdateQuotaRulesStateToError)
		env.RegisterActivity(internalStopReplicationActivity.UpdateVolumeReplicationForQuotaError)
		env.RegisterActivity(quotaRuleActivity.ListQuotaRulesForVolume)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(replicationCommonActivity.UpdateReplicationState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)},
				PoolCredentials: &datamodel.PoolCredentials{
					Password:      "password",
					SecretID:      "",
					CertificateID: "",
				}},
			Svm: &datamodel.Svm{
				Name: "svm_test",
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
			VolumeAttributes: &datamodel.VolumeAttributes{
				ExternalUUID:    "volume-external-uuid",
				BlockProperties: &datamodel.BlockProperties{OSType: "LINUX"},
			},
		}
		replicationDb := &datamodel.VolumeReplication{
			BaseModel: datamodel.BaseModel{UUID: "replication-uuid"},
			Account: &datamodel.Account{
				BaseModel: datamodel.BaseModel{ID: 1},
				Name:      "test-account",
			},
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "quota-rule-uuid-1",
				},
				Name:           "test-quota-rule",
				QuotaType:      "tree",
				QuotaTarget:    "target1",
				DiskLimitInKib: 1048576,
				AccountID:      int64(1),
			},
		}

		mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetNode", mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
		env.OnActivity("AbortVolumeReplication", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeReplication{}, nil)
		env.OnActivity("BreakVolumeReplication", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("GetSnapMirrorFromOntap", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
		env.OnActivity("UpdateVolumeReplicationStopDetails", mock.Anything, mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeToNonDPVolume", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("ListQuotaRulesForVolume", mock.Anything, replicationDb).Return(quotaRules, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))
		env.OnActivity("UpdateQuotaRulesStateToError", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateVolumeReplicationForQuotaError", mock.Anything, replicationDb).Return(errors.New("database error"))
		env.OnActivity("UpdateReplicationState", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(StopInternalVolumeReplicationWorkflow, replicationDb, false)

		assert.True(tt, env.IsWorkflowCompleted())
		// Workflow completes successfully after updating job status to ERROR when UpdateVolumeReplicationForQuotaError fails
		assert.NoError(tt, env.GetWorkflowError())
		env.AssertExpectations(tt)
	})
}

// ProcessQuotaRulesPostBreakReplicationTestWorkflow is a test workflow wrapper for processQuotaRulesPostBreakReplication
func ProcessQuotaRulesPostBreakReplicationTestWorkflow(ctx workflow.Context, replication *datamodel.VolumeReplication, quotaRules []*datamodel.QuotaRule, node *models.Node) ([]*datamodel.QuotaRule, error) {
	// Set up activity options similar to Run() method
	retryPolicy, err := workflows.PopulateRetryPolicyParams()
	if err != nil {
		return nil, workflows.ConvertToVSAError(err)
	}
	ao := workflow.ActivityOptions{
		StartToCloseTimeout: retryPolicy.StartToCloseTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retryPolicy.InitialInterval,
			BackoffCoefficient:     retryPolicy.BackoffCoefficient,
			MaximumInterval:        retryPolicy.MaximumInterval,
			MaximumAttempts:        int32(retryPolicy.MaximumAttempts),
			NonRetryableErrorTypes: []string{"NonRetryableError", "PanicError"},
		},
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	failedQuotaRules := processQuotaRulesPostBreakReplication(ctx, replication, quotaRules, node)
	return failedQuotaRules, nil
}

func TestProcessQuotaRulesPostBreakReplication(t *testing.T) {
	t.Run("TestProcessQuotaRulesPostBreakReplication_IndividualQuotaRule_Success", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Empty(tt, result, "No failed quota rules expected")
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_DefaultQuotaUpdate_Success", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "", // Default quota
				DiskLimitInKib: 1048576,
			},
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Empty(tt, result, "No failed quota rules expected when update succeeds")
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_DefaultQuotaNotFound_Create", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "", // Default quota
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("default quota rule not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Empty(tt, result, "No failed quota rules expected when not found triggers creation")
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_DefaultQuotaUpdateError_Failure", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "", // Default quota
				DiskLimitInKib: 1048576,
			},
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update default quota"))

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1, "Expected one failed quota rule")
		assert.Equal(tt, "quota-rule-uuid-1", result[0].UUID)
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_RQuotaEnableError_Failure", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(errors.New("failed to enable RQuota"))

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1, "Expected one failed quota rule")
		assert.Equal(tt, "quota-rule-uuid-1", result[0].UUID)
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_QuotaRuleCreationError_Failure", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("failed to create quota rule"))

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1, "Expected one failed quota rule")
		assert.Equal(tt, "quota-rule-uuid-1", result[0].UUID)
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_QuotaEnablementError_Failure", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(errors.New("failed to enable quota"))

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1, "Expected one failed quota rule")
		assert.Equal(tt, "quota-rule-uuid-1", result[0].UUID)
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_MultipleRules_PartialFailure", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule-1",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "1000",
				DiskLimitInKib: 1048576,
			},
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-2"},
				Name:           "test-quota-rule-2",
				QuotaType:      "INDIVIDUAL_USER_QUOTA",
				QuotaTarget:    "2000",
				DiskLimitInKib: 2097152,
			},
		}

		quotaRuleResponse1 := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		// First rule succeeds, second rule fails
		// Note: HandleQuotaEnablementAndReinitialization is only called for the last rule after successful creation
		// Since the second rule (last) fails, it won't be called
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == "quota-rule-uuid-1"
		})).Return(quotaRuleResponse1, nil).Once()
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil).Once()
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(qr *datamodel.QuotaRule) bool {
			return qr.UUID == "quota-rule-uuid-2"
		})).Return(nil, errors.New("failed to create quota rule")).Once()

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Len(tt, result, 1, "Expected one failed quota rule")
		assert.Equal(tt, "quota-rule-uuid-2", result[0].UUID)
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_EmptyQuotaRules_Success", func(tt *testing.T) {
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

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{}

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Empty(tt, result, "No failed quota rules expected for empty list")
		env.AssertExpectations(tt)
	})

	t.Run("TestProcessQuotaRulesPostBreakReplication_DefaultQuotaNotFound_WithErrorMessage", func(tt *testing.T) {
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

		quotaRuleActivity := activities.QuotaRuleCommonActivity{}
		quotaRuleCreateActivity := activities.QuotaRuleCreateActivity{}

		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleCreateActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleCreateActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnablementAndReinitialization)

		volume := &datamodel.Volume{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			Svm: &datamodel.Svm{
				SvmDetails: &datamodel.SvmDetails{
					ExternalUUID: "svm-external-uuid",
				},
			},
		}
		replication := &datamodel.VolumeReplication{
			Volume: volume,
			ReplicationAttributes: &datamodel.ReplicationDetails{
				DestinationVolumeUUID: "dest-volume-uuid",
			},
		}
		node := &models.Node{EndpointAddress: "127.0.0.1"}

		quotaRules := []*datamodel.QuotaRule{
			{
				BaseModel:      datamodel.BaseModel{UUID: "quota-rule-uuid-1"},
				Name:           "test-quota-rule",
				QuotaType:      "DEFAULT_USER_QUOTA",
				QuotaTarget:    "", // Default quota
				DiskLimitInKib: 1048576,
			},
		}

		quotaRuleResponse := &vsa.QuotaRuleProviderResponse{
			State:   vsa.JobRespSuccess,
			Message: "Quota rule created successfully",
		}

		// Test with error message containing "not found" (case-insensitive check)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("resource NOT FOUND in system"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(quotaRuleResponse, nil)
		env.OnActivity("HandleQuotaEnablementAndReinitialization", mock.Anything, mock.Anything, mock.Anything, quotaRuleResponse).Return(nil)

		env.ExecuteWorkflow(ProcessQuotaRulesPostBreakReplicationTestWorkflow, replication, quotaRules, node)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
		var result []*datamodel.QuotaRule
		err := env.GetWorkflowResult(&result)
		assert.NoError(tt, err)
		assert.Empty(tt, result, "No failed quota rules expected when error message contains 'not found'")
		env.AssertExpectations(tt)
	})
}
