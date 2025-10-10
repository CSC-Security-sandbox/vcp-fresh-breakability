package flexcache_workflows

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/flexcache_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/flexcache"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type FlexCacheUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment

	// store activities to use in tests so we don't need to mock them every time
	commonActivity                *activities.CommonActivities
	volumeCreateActivity          *activities.VolumeCreateActivity
	flexCacheVolumeCreateActivity *flexcache_activities.FlexCacheVolumeCreateActivity
}

func (s *FlexCacheUnitTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(CreateFlexCacheWorkflow)

	// Register activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	flexCacheVolumeCreateActivity := flexcache_activities.FlexCacheVolumeCreateActivity{SE: mockStorage}

	s.commonActivity = &commonActivity
	s.flexCacheVolumeCreateActivity = &flexCacheVolumeCreateActivity
	s.volumeCreateActivity = &volumeCreateActivity

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.CreateExportPolicyInOntap)
	// Register all flexcache related activities used in workflow so mocks match
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.WaitForClusterPeerActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity)
}

func CreateTestVolume() *datamodel.Volume {
	return &datamodel.Volume{
		Account:   &datamodel.Account{Name: "account-1"},
		BaseModel: datamodel.BaseModel{UUID: "test-volume-uuid"},
		Name:      "test-flexcache-volume",
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: int64(1)},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
			},
			DeploymentName: "test-deployment",
		},
		CacheParameters: &datamodel.CacheParameters{},
	}
}

func (s *FlexCacheUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_Success() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, mock.Anything, mock.Anything).Return(result, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_GetNodeFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateClusterPeerFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeForClusterPeeringActivityFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForClusterPeerActivityFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForClusterPeerActivityTimeout() {
	volume := CreateTestVolume()
	// Ensure CacheParameters is initialized to avoid nil pointer when workflow sets timeout details
	volume.CacheParameters = &datamodel.CacheParameters{}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
	}
	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, fmt.Errorf("timed out")))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult, "expected UpdateVolumeDetailsOnErrorActivity to be called") {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters, "cache parameters must be set") {
			assert.Equal(s.T(), models.ClusterPeeringExpiredCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), "Cluster peering expired", capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateSVMPeerFailure() {
	volume := CreateTestVolume()
	// Ensure CacheParameters is initialized to avoid nil pointer when workflow sets timeout details
	volume.CacheParameters = &datamodel.CacheParameters{}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
	}
	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(nil, errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerError, fmt.Errorf("some_error"))))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult, "expected UpdateVolumeDetailsOnErrorActivity to be called") {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.ErrorDuringSVMPeeringCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.ErrorDuringSVMPeering, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeForSVMPeeringActivityFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForSVMPeerActivityFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}
	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerError, fmt.Errorf("some_error"))))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult) {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.ErrorDuringSVMPeeringCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.ErrorDuringSVMPeering, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForSVMPeerActivityTimeout() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}
	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, fmt.Errorf("timed out")))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult) {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.SVMPeeringExpiredCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.SVMPeeringExpired, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateFlexCacheVolumeFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateExportPolicyFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeDetailsFailure() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeDetailsOnErrorFailure() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureBeforeRun() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("some_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureAfterFailedRun() {
	volume := CreateTestVolume()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{}}, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureAfterCompletedRun() {
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}

		return errors.New("some_error")
	})
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeDetailsActivity, mock.Anything, mock.Anything).Return(result, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func TestFlexCacheUnitTestSuite(t *testing.T) {
	suite.Run(t, new(FlexCacheUnitTestSuite))
}
