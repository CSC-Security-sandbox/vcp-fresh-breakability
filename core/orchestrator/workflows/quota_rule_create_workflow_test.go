package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
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

func TestCreateQuotaRuleWorkflow(t *testing.T) {
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
			DiskLimitInKib: 100 * 1024,
			QuotaTarget:    "1001",
			VolumeID:       volumeID,
			State:          models.LifeCycleStateCreating,
			StateDetails:   models.LifeCycleStateCreatingDetails,
			RQuota:         true,
		}
	}

	createBaseParams := func() *commonparams.CreateQuotaRulesParam {
		return &commonparams.CreateQuotaRulesParam{
			ProjectId:  "test-project",
			LocationId: "us-central1-a",
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()

		// Mock GetVolumeByID to fail
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(nil, errors.New("volume not found"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenCreateQuotaRuleForDataProtectionVolumeFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleForDataProtectionVolume)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(true, []string{"NFSV3"})

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("CreateQuotaRuleForDataProtectionVolume", mock.Anything, quotaRule).Return(errors.New("failed to create DP quota rule"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenValidateQuotaTargetByProtocolFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(errors.New("protocol validation failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nil, errors.New("failed to get nodes"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(errors.New("failed to update rquota"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenHandleDefaultQuotaRuleUpdateFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		quotaRule.QuotaTarget = "" // Default quota
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(errors.New("unexpected error updating default quota"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenCreateQuotaRuleOnONTAPFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(nil, errors.New("failed to create quota rule on ONTAP"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenGetQuotaStatusFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(nil, errors.New("failed to get quota status"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(nil, errors.New("failed to enable quota"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.QuotaReinitialization)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespFailure,
			ExternalUUID: "quota-external-uuid",
			Message:      "Quota policy rule create operation succeeded, however quota resize failed",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOn,
			Enabled: true,
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("QuotaReinitialization", mock.Anything, mock.Anything, volume).Return(errors.New("failed to reinitialize quota"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		quotaEnableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota enabled",
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(quotaEnableResp, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(nil, errors.New("failed to get volume replication"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		quotaEnableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota enabled",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(quotaEnableResp, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications, params.LocationId).Return(nil, errors.New("replication state validation failed"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenCreateQuotaRuleOnDestinationFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		quotaEnableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota enabled",
		}

		replications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					ID: 1,
				},
				VolumeID: volumeID,
			},
		}

		eligibleReplications := []*activities.ReplicationSyncEligibility{
			{
				DestinationVolumeUUID: "dest-volume-uuid",
				DestinationLocation:   "us-west1-a",
				DestinationProjectNum: "987654321",
			},
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(quotaEnableResp, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications, params.LocationId).Return(eligibleReplications, nil)
		env.OnActivity("CreateQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", quotaRule, "us-west1-a", "987654321").Return(errors.New("failed to create quota rule on destination"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenFinishQuotaRuleJobFails", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleActivity.FinishQuotaRuleJob)
		env.RegisterActivity(quotaRuleActivity.UpdateQuotaRuleState)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		quotaEnableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota enabled",
		}

		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(quotaEnableResp, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return([]*datamodel.VolumeReplication{}, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, []*datamodel.VolumeReplication{}, params.LocationId).Return([]*activities.ReplicationSyncEligibility{}, nil)
		env.OnActivity("FinishQuotaRuleJob", mock.Anything, quotaRule.UUID, mock.Anything, "quota-external-uuid", quotaRule.Description).Return(errors.New("failed to finish quota rule job"))
		env.OnActivity("UpdateQuotaRuleState", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		assert.True(tt, env.IsWorkflowCompleted())
		assert.Error(tt, env.GetWorkflowError())
	})

	t.Run("WhenQuotaCreateWorkflowSucceeds", func(tt *testing.T) {
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
		quotaRuleActivity := activities.QuotaRuleCreateActivity{SE: mockStorage}
		commonActivity := activities.CommonActivities{SE: mockStorage}

		env.RegisterActivity(quotaRuleActivity.GetVolumeByID)
		env.RegisterActivity(quotaRuleActivity.ValidateQuotaTargetByProtocol)
		env.RegisterActivity(commonActivity.GetNode)
		env.RegisterActivity(quotaRuleActivity.UpdateRQuotaOnSvm)
		env.RegisterActivity(quotaRuleActivity.HandleDefaultQuotaRuleUpdate)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnONTAP)
		env.RegisterActivity(quotaRuleActivity.GetQuotaStatus)
		env.RegisterActivity(quotaRuleActivity.HandleQuotaEnableDisable)
		env.RegisterActivity(quotaRuleActivity.GetVolumeReplication)
		env.RegisterActivity(quotaRuleActivity.VerifyReplicationState)
		env.RegisterActivity(quotaRuleActivity.CreateQuotaRuleOnDestination)
		env.RegisterActivity(quotaRuleActivity.FinishQuotaRuleJob)
		env.RegisterActivity(commonActivity.UpdateJobStatus)

		params := createBaseParams()
		quotaRule := createBaseQuotaRule()
		volume := createBaseVolume(false, []string{"NFSV3"})
		nodes := createBaseNodes()

		quotaRuleResp := &vsa.QuotaRuleProviderResponse{
			State:        vsa.JobRespSuccess,
			ExternalUUID: "quota-external-uuid",
			Message:      "Quota rule created successfully",
		}

		quotaStatus := &vsa.QuotaStatus{
			State:   vsa.QuotaStateOff,
			Enabled: false,
		}

		quotaEnableResp := &vsa.JobStatus{
			State:   vsa.JobRespSuccess,
			Message: "quota enabled",
		}

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
			},
		}

		eligibleReplications := []*activities.ReplicationSyncEligibility{
			{
				DestinationVolumeUUID: "dest-volume-uuid",
				DestinationLocation:   "us-west1-a",
				DestinationProjectNum: "987654321",
			},
		}

		// Mock all activities to succeed
		env.OnActivity("UpdateJobStatus", mock.Anything, mock.Anything).Return(nil)
		env.OnActivity("GetVolumeByID", mock.Anything, volumeID).Return(volume, nil)
		env.OnActivity("ValidateQuotaTargetByProtocol", mock.Anything, quotaRule, []string{"NFSV3"}).Return(nil)
		env.OnActivity("GetNode", mock.Anything, poolID).Return(nodes, nil)
		env.OnActivity("UpdateRQuotaOnSvm", mock.Anything, "svm-external-uuid", mock.Anything, true).Return(nil)
		env.OnActivity("HandleDefaultQuotaRuleUpdate", mock.Anything, volume, mock.Anything, quotaRule.QuotaType, quotaRule.DiskLimitInKib).Return(vsaerrors.New("not found"))
		env.OnActivity("CreateQuotaRuleOnONTAP", mock.Anything, mock.Anything, volume, quotaRule).Return(quotaRuleResp, nil)
		env.OnActivity("GetQuotaStatus", mock.Anything, mock.Anything, volume).Return(quotaStatus, nil)
		env.OnActivity("HandleQuotaEnableDisable", mock.Anything, mock.Anything, volume).Return(quotaEnableResp, nil)
		env.OnActivity("GetVolumeReplication", mock.Anything, volumeID).Return(replications, nil)
		env.OnActivity("VerifyReplicationState", mock.Anything, replications, params.LocationId).Return(eligibleReplications, nil)
		env.OnActivity("CreateQuotaRuleOnDestination", mock.Anything, "dest-volume-uuid", quotaRule, "us-west1-a", "987654321").Return(nil)
		env.OnActivity("FinishQuotaRuleJob", mock.Anything, quotaRule.UUID, mock.Anything, "quota-external-uuid", quotaRule.Description).Return(nil)

		env.ExecuteWorkflow(CreateQuotaRuleWorkflow, params, quotaRule)

		// Verify workflow completed successfully
		_, err := env.QueryWorkflowByID("default-test-workflow-id", "status")
		assert.Nil(tt, err)
		assert.True(tt, env.IsWorkflowCompleted())
		assert.NoError(tt, env.GetWorkflowError())
	})
}
