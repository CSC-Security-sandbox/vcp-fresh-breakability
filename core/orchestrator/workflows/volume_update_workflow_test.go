package workflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type VolumeUpdateTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *VolumeUpdateTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	mockHeader := &commonpb.Header{
		Fields: map[string]*commonpb.Payload{
			"logParam": encodedValue,
		},
	}
	s.env.SetHeader(mockHeader)

	// Register workflow
	s.env.RegisterWorkflow(UpdateVolumeWorkflow)
}

func (s *VolumeUpdateTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 1000,
		Size:           1000,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 1000,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 2000,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_NoSizeChange() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           200,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow with no size change
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1)}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 100,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	// Assert workflow completed successfully
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func (s *VolumeUpdateTestSuite) Test_UpdateVolumeWorkflow_Failure() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	updateActivity := activities.VolumeUpdateActivity{SE: mockStorage}

	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(2)
	mockStorage.On("UpdateJob", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed updating job"))

	// Register activities
	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInONTAP)
	s.env.RegisterActivity(updateActivity.UpdateLun)
	s.env.RegisterActivity(updateActivity.UpdateVolumeInDB)

	// Mock activities
	s.env.OnActivity(commonActivity.GetNode, mock.Anything, mock.Anything).Return(&datamodel.Node{EndpointAddress: "127.0.0.1"}, nil)
	s.env.OnActivity(updateActivity.GetVolumeFromONTAP, mock.Anything, mock.Anything, mock.Anything).Return(&vsa.VolumeResponse{
		ProviderResponse: vsa.ProviderResponse{
			ExternalUUID: "test-external-uuid",
			Name:         "test_volume",
		},
		AvailableSpace: 100,
		Size:           100,
		State:          "online",
	}, nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInONTAP, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("ONTAP error"))
	s.env.OnActivity(updateActivity.UpdateLun, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(updateActivity.UpdateVolumeInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Execute workflow
	volume := &datamodel.Volume{
		Pool: &datamodel.Pool{BaseModel: datamodel.BaseModel{ID: int64(1), UUID: "test-volume-uuid"}},
		Account: &datamodel.Account{
			Name: "test_account",
		},
		SizeInBytes: 100,
	}
	params := &common.UpdateVolumeParams{
		QuotaInBytes: 200,
	}
	s.env.ExecuteWorkflow(UpdateVolumeWorkflow, params, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	// Assert workflow failed
	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	assert.Contains(s.T(), s.env.GetWorkflowError().Error(), "workflow execution error")
	mockStorage.AssertNumberOfCalls(s.T(), "UpdateJob", 2)
}

func TestVolumeUpdateTestSuite(t *testing.T) {
	suite.Run(t, new(VolumeUpdateTestSuite))
}
