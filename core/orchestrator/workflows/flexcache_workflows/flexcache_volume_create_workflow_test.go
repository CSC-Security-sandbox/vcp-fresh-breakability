package flexcache_workflows

import (
	"context"
	"fmt"
	"testing"
	"time"

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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
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
	flexCacheVolumeDeleteActivity *flexcache_activities.FlexCacheVolumeDeleteActivity
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

	// default force encryption to true in tests
	originalForceEncryption := shouldForceEncryption
	shouldForceEncryption = func() bool { return true }
	s.T().Cleanup(func() {
		shouldForceEncryption = originalForceEncryption
	})

	// Register activities
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	flexCacheVolumeCreateActivity := flexcache_activities.FlexCacheVolumeCreateActivity{SE: mockStorage}
	flexCacheVolumeDeleteActivity := flexcache_activities.FlexCacheVolumeDeleteActivity{SE: mockStorage}

	s.commonActivity = &commonActivity
	s.volumeCreateActivity = &volumeCreateActivity
	s.flexCacheVolumeCreateActivity = &flexCacheVolumeCreateActivity
	s.flexCacheVolumeDeleteActivity = &flexCacheVolumeDeleteActivity

	s.env.RegisterActivity(commonActivity.UpdateJobStatus)
	s.env.RegisterActivity(commonActivity.GetNode)
	s.env.RegisterActivity(volumeCreateActivity.CreateExportPolicyInOntap)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity)
	// Register all flexcache related activities used in workflow so mocks match
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreatePeeringJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CompletePeeringJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.StartInternalJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CompleteInternalJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.FailJobActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.WaitForClusterPeerActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity)

	s.env.RegisterActivity(flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity)
	s.env.RegisterActivity(flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume)

	// Register child workflows for testing
	s.env.RegisterWorkflow(workflows.PostFileVolumeWorkflow)
	s.env.RegisterWorkflow(workflows.PostFileVolumeWorkflowForSMB)
}

func CreateTestVolume() *datamodel.Volume {
	peerExpiry := time.Now().Add(1 * time.Hour)
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
		CacheParameters: &datamodel.CacheParameters{
			CommandExpiryTime: &peerExpiry,
		},
		VolumeAttributes: &datamodel.VolumeAttributes{},
	}
}

func createTestEvent() *flexcache.CreateFlexCacheEvent {
	return &flexcache.CreateFlexCacheEvent{
		LocationID:    "location-id",
		ProjectNumber: "project-number",
		RequestUri:    "https://example.com/",
		CorrelationID: nillable.ToPointer("correlation-id"),
	}
}

func createPeeringResult(volume *datamodel.Volume) *flexcache.CreateFlexCacheResult {
	return &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		SVMPeerAction:     flexcache.ActionCreate,
	}
}

func assertErrorDuringTest(t *testing.T, expectedErr error, actualErr error) {
	var workflowErr *temporal.WorkflowExecutionError
	if errors.As(actualErr, &workflowErr) {
		unwrapped := workflowErr.Unwrap()
		assertErrorDuringTest(t, expectedErr, unwrapped)
		return
	}

	var applicationErr *temporal.ApplicationError
	if errors.As(actualErr, &applicationErr) {
		if applicationErr.Type() == "CustomError" {
			// Do not unwrap CustomError, just compare the error messages
			assert.Equal(t, applicationErr.Error(), actualErr.Error())
			return
		}

		unwrapped := applicationErr.Unwrap()
		if unwrapped == nil {
			// There is no inner error to compare, so just compare the error messages
			assert.Equal(t, expectedErr.Error(), applicationErr.Error())
		} else {
			assertErrorDuringTest(t, expectedErr, unwrapped)
		}
		return
	}

	assert.Equal(t, expectedErr, actualErr, "expected errors to match")
}

func (s *FlexCacheUnitTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_Success() {
	volume := CreateTestVolume()
	clusterPeerUUID := "cluster-peer-uuid"
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: clusterPeerUUID,
		State:         models.CvpClusterPeeringStatusCREATING,
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeeringRow

	peerExpiryTime := time.Now()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteInternalJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{CacheParameters: &models.CacheParameters{PeerExpiryTime: &peerExpiryTime}}, volume, event)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_Create_New_Success() {
	volume := CreateTestVolume()
	clusterPeeringRow := &datamodel.ClusterPeerings{
		OntapPeerUUID: "cluster-peer-uuid",
	}
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		SVMPeerAction:     flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeeringRow,
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteInternalJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

// GetNode failure should trigger error handling, UpdateVolumeDetailsOnErrorActivity, and FailJobActivity
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_GetNodeFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_GetClusterPeeringRowFromDBActivityFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateClusterPeeringRowInDBActivityFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateClusterPeeringInVolumeForCreateActionFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(nil, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateClusterPeeringInVolumeForReadyActionFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	result := createPeeringResult(volume)
	expectedErr := assert.AnError

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil).Once()
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(nil, expectedErr)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
}

// CreateClusterPeer failure
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateClusterPeerFailure() {
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
		OntapPeerUUID: "cluster-peer-uuid",
	}
	volume := CreateTestVolume()
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeerAction: flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeerRow,
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{
		BaseModel:       datamodel.BaseModel{ID: 1},
		EndpointAddress: "127.0.0.1",
	}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateFlexCacheVolumeForClusterPeeringActivity failure
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeForClusterPeeringActivityFailure() {
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
		OntapPeerUUID: "cluster-peer-uuid",
	}
	volume := CreateTestVolume()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateClusterPeeringRowStatePendingInDBActivityFailure() {
	volume := CreateTestVolume()
	clusterPeerUUID := "cluster-peer-uuid"
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		State:         models.CvpClusterPeeringStatusCREATING,
		OntapPeerUUID: clusterPeerUUID,
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeerRow,
	}
	event := createTestEvent()

	// Expectations up to the failure point (no UpdateClusterPeeringInVolume or FailJobActivity in this path)
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForClusterPeerActivityFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel: datamodel.BaseModel{ID: 1},
		State:     models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionCreate

	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, errors.WrapAsNonRetryableTemporalApplicationError(assert.AnError))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateClusterPeeringRowStatePeeredInDBActivityFailure() {
	volume := CreateTestVolume()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
		OntapPeerUUID: "cluster-peer-uuid",
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeeringRow: clusterPeerRow,
		ClusterPeerAction: flexcache.ActionCreate,
	}
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity, mock.Anything, mock.Anything).Return(result, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

// WaitForClusterPeer timeout should map to ClusterPeeringExpired codes
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForClusterPeerActivityTimeout() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel: datamodel.BaseModel{ID: 1},
		State:     models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	expectedErr := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, fmt.Errorf("timed out"))

	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, expectedErr)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult, "expected UpdateVolumeDetailsOnErrorActivity to be called") {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters, "cache parameters must be set") {
			assert.Equal(s.T(), models.ClusterPeeringExpiredCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.ClusterPeeringExpired, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_EnsureSVMPeerInOntapActivityFailure() {
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	volume := CreateTestVolume()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeerAction: flexcache.ActionReady,
		ClusterPeeringRow: clusterPeerRow,
	}
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

// Create SVM peering failure should map to ErrorDuringSVMPeering codes
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateSVMPeerFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerUUID := "cluster-peer-uuid"
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: clusterPeerUUID,
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	volume.CacheParameters = &datamodel.CacheParameters{}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeeringRow,
	}
	expectedErr := errors.WrapAsTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerError, fmt.Errorf("some_error")))
	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, expectedErr)

	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult, "expected UpdateVolumeDetailsOnErrorActivity to be called") {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.ErrorDuringSVMPeeringCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.ErrorDuringSVMPeering, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

// UpdateFlexCacheVolumeForSVMPeeringActivity failure
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeForSVMPeeringActivityFailure() {
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	volume := CreateTestVolume()
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_HydrateFlexCacheStateForPeeredFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(result, assert.AnError)

	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// WaitForSVMPeering failure with non-retryable error
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForSVMPeerActivityFailure() {
	volume := CreateTestVolume()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeerRow,
	}
	expectedErr := errors.WrapAsNonRetryableTemporalApplicationError(errors.NewVCPError(errors.ErrSVMPeerError, fmt.Errorf("some_error")))

	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, expectedErr)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult) {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.ErrorDuringSVMPeeringCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.ErrorDuringSVMPeering, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeForVolumeCreationActivityFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionReady,
		ClusterPeeringRow: clusterPeerRow,
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "0.0.0.0"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// WaitForSVMPeering timeout mapping to SVMPeeringExpired
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_WaitForSVMPeerActivityTimeout() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume:          volume,
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeerRow,
	}
	expectedErr := temporal.NewTimeoutError(enums.TIMEOUT_TYPE_START_TO_CLOSE, fmt.Errorf("timed out"))

	var capturedResult *flexcache.CreateFlexCacheResult

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(nil, expectedErr)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		capturedResult = args.Get(1).(*flexcache.CreateFlexCacheResult)
	})

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
	if assert.NotNil(s.T(), capturedResult) {
		if assert.NotNil(s.T(), capturedResult.DBVolume.CacheParameters) {
			assert.Equal(s.T(), models.SVMPeeringExpiredCode, capturedResult.DBVolume.CacheParameters.CacheStateDetailsCode)
			assert.Equal(s.T(), models.SVMPeeringExpired, capturedResult.DBVolume.CacheParameters.CacheStateDetails)
		}
	}
}

// CreateFlexCacheVolume failure
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateFlexCacheVolumeFailure() {
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	volume := CreateTestVolume()
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		SVMPeerAction:     flexcache.ActionCreate,
		ClusterPeeringRow: clusterPeerRow,
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateVolumeAttributesInDB failure after CreateFlexCacheVolumeInOntapActivity
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeAttributesInDBFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateVolumeAttributesInDB failure after CreateFlexCacheVolumeInOntapActivity
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeAttributesInDBFailure_AfterChildWorkflows() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError).Once()
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// VerifyVolumeEncryptionActivity failure after UpdateVolumeAttributesInDB
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_VerifyVolumeEncryptionActivityFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_CreateExportPolicyFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	result := createPeeringResult(volume)
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
	}
	result.ClusterPeeringRow = clusterPeerRow

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForClusterPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePendingInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStatePeeredInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForClusterPeerActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything).Return(result, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_HydrateFlexCacheStateForSVMPeeringFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionCreate

	// Only mock activities that are actually called in the ready path
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).
		Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{AccountName: "test-account"}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateFlexCacheVolumeLifecycleStateActivity failure at end triggers error handler and FailJob
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateFlexCacheVolumeLifecycleStateActivityFailure() {
	volume := CreateTestVolume()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionCreate

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateVolumeDetailsOnError failure still returns the original error from the workflow
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateVolumeDetailsOnErrorFailure() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeerRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
	}
	result := createPeeringResult(volume)
	result.ClusterPeeringRow = clusterPeerRow

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(errors.New("update_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything).Return(result, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateClusterPeeringRowStateErrorInDBActivityFailure() {
	volume := CreateTestVolume()
	peerRow := &datamodel.ClusterPeerings{
		State: models.CvpClusterPeeringStatusPENDINGCLUSTERPEERING,
	}
	result := &flexcache.CreateFlexCacheResult{DBVolume: volume, ClusterPeeringRow: peerRow}
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringRowStateErrorInDBActivity, mock.Anything, mock.Anything).Return(nil, errors.New("update_error"))

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

// UpdateJobStatus failure before entering Run should short-circuit the workflow
func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureBeforeRun() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(assert.AnError)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_UpdateJobStatusFailureAfterFailedRun() {
	volume := CreateTestVolume()
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(func(ctx context.Context, job *datamodel.Job) error {
		if job.State == string(models.JobsStatePROCESSING) {
			return nil
		}
		return assert.AnError
	})
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{}}, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.FailJobActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), assert.AnError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_DeleteClusterPeerInOntapActivity_Fails() {
	volume := CreateTestVolume()
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		ClusterPeer:       &vsa.ClusterPeer{PeerClusterName: "peer-name"},
		ClusterPeeringRow: clusterPeeringRow,
	}

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity, mock.Anything,
		mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, errors.New("some_error"))
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_DeleteClusterPeeringRowInDBActivity_Fails() {
	volume := CreateTestVolume()
	event := createTestEvent()
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionCreate,
		ClusterPeer:       &vsa.ClusterPeer{PeerClusterName: "peer-name"},
		ClusterPeeringRow: clusterPeeringRow,
	}

	expectedErr := assert.AnError

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteClusterPeeringRowInDBActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, expectedErr)

	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), expectedErr, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_DeleteSVMPeeringInOntapActivity_Fails() {
	volume := CreateTestVolume()
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	event := createTestEvent()
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionCreate,
		SVMPeer:           &vsa.SvmPeer{PeerSvmName: "svm-peer-name"},
		ClusterPeeringRow: clusterPeeringRow,
	}

	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, assert.AnError)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NotNil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_DeleteSVMPeeringInOntapActivity_Success() {
	volume := CreateTestVolume()
	clusterPeeringRow := &datamodel.ClusterPeerings{
		BaseModel:     datamodel.BaseModel{ID: 1},
		OntapPeerUUID: "cluster-peer-uuid",
		State:         models.CvpClusterPeeringStatusPEERED,
	}
	result := &flexcache.CreateFlexCacheResult{
		DBVolume: volume,
		VolumeResponse: &vsa.VolumeResponse{
			ProviderResponse: vsa.ProviderResponse{ExternalUUID: "external-uuid"},
		},
		ClusterPeerAction: flexcache.ActionReady,
		SVMPeerAction:     flexcache.ActionCreate,
		SVMPeer:           &vsa.SvmPeer{PeerSvmName: "svm-peer-name"},
		ClusterPeeringRow: clusterPeeringRow,
	}
	event := createTestEvent()

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeDeleteActivity.DeleteSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(&flexcache.DeleteFlexCacheResult{}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateSVMPeeringInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.WaitForSVMPeeringActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	_, err := s.env.QueryWorkflowByID("default-test-workflow-id", "status")
	assert.Nil(s.T(), err)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

// setupMocksBeforeChildWorkflows sets up all activity mocks needed to reach child workflow execution
func (s *FlexCacheUnitTestSuite) setupMocksBeforeChildWorkflows(result *flexcache.CreateFlexCacheResult) {
	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.HydrateFlexCacheState, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.VerifyVolumeEncryptionActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateREADY).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteInternalJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(result, nil)
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_PostFileVolumeWorkflow_Success() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolNFSv3},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	// Create an updated volume that will be returned by the child workflow
	updatedVolume := *volume
	updatedVolume.Name = "updated-volume-name"

	s.setupMocksBeforeChildWorkflows(result)

	// Mock child workflow execution
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflow, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(&updatedVolume, nil)

	// Mock UpdateVolumeAttributesInDB which is called after child workflow
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_PostFileVolumeWorkflowForSMB_Success() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolSMB},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	// Create an updated volume that will be returned by the child workflow
	updatedVolume := *volume
	updatedVolume.Name = "updated-volume-name"

	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = true
	s.setupMocksBeforeChildWorkflows(result)

	// Mock child workflow execution
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflowForSMB, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(&updatedVolume, nil)

	// Mock UpdateVolumeAttributesInDB which is called after child workflow
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_Both_Child_Workflows_Success() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolNFSv3, utils.ProtocolSMB},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	// Create updated volumes that will be returned by the child workflows
	updatedVolumeAfterNFS := *volume
	updatedVolumeAfterNFS.Name = "updated-after-nfs"
	updatedVolumeAfterSMB := updatedVolumeAfterNFS
	updatedVolumeAfterSMB.Name = "updated-after-smb"

	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = true
	s.setupMocksBeforeChildWorkflows(result)

	// Mock both child workflow executions - NFS first, then SMB
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflow, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(&updatedVolumeAfterNFS, nil).Once()
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflowForSMB, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(&updatedVolumeAfterSMB, nil).Once()

	// Mock UpdateVolumeAttributesInDB which is called after child workflows
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_PostFileVolumeWorkflow_Error() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolNFSv3},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow to return an error
	childWorkflowError := fmt.Errorf("child workflow failed")
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflow, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(nil, childWorkflowError)

	// Mock error handling activities
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), childWorkflowError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_PostFileVolumeWorkflowForSMB_Error() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolSMB},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady
	originalEnableSmb := enableSmb
	defer func() { enableSmb = originalEnableSmb }()
	enableSmb = true

	s.env.OnActivity(s.commonActivity.UpdateJobStatus, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompleteFlexCacheCreateJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreatePeeringJobActivity, mock.Anything, mock.Anything).Return(nil, nil)
	s.env.OnActivity(s.commonActivity.GetNode, mock.Anything, mock.Anything).Return([]*datamodel.Node{{EndpointAddress: "127.0.0.1"}}, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.GetClusterPeeringRowFromDBActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureClusterPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateClusterPeeringInVolume, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.EnsureSVMPeerInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateFlexCacheVolumeLifecycleStateActivity, mock.Anything, mock.Anything, models.LifeCycleStateCreating).Return(result, nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CompletePeeringJobActivity, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.CreateFlexCacheVolumeInOntapActivity, mock.Anything, mock.Anything).Return(result, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.volumeCreateActivity.CreateExportPolicyInOntap, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.StartInternalJobActivity, mock.Anything, mock.Anything).Return(nil, nil)

	// Mock child workflow to return an error
	childWorkflowError := fmt.Errorf("child workflow failed")
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflowForSMB, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(nil, childWorkflowError)

	// Mock error handling activities
	s.env.OnActivity(s.flexCacheVolumeCreateActivity.UpdateVolumeDetailsOnErrorActivity, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assertErrorDuringTest(s.T(), childWorkflowError, s.env.GetWorkflowError())
}

func (s *FlexCacheUnitTestSuite) Test_CreateFlexCacheWorkflow_ChildWorkflowReturnsNilVolumeNoUpdate() {
	volume := CreateTestVolume()
	volume.VolumeAttributes = &datamodel.VolumeAttributes{
		Protocols: []string{utils.ProtocolNFSv3},
	}
	event := createTestEvent()
	result := createPeeringResult(volume)
	result.ClusterPeerAction = flexcache.ActionReady
	result.SVMPeerAction = flexcache.ActionReady

	s.setupMocksBeforeChildWorkflows(result)

	// Mock child workflow to return nil volume
	s.env.OnWorkflow(workflows.PostFileVolumeWorkflow, mock.Anything, mock.AnythingOfType("*datamodel.Volume"), mock.AnythingOfType("*models.Node")).Return(nil, nil)
	s.env.OnActivity(s.volumeCreateActivity.UpdateVolumeAttributesInDB, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(CreateFlexCacheWorkflow, &common.CreateVolumeParams{}, volume, event)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Nil(s.T(), s.env.GetWorkflowError())
}

func TestCopyInputCacheParameters(t *testing.T) {
	t.Run("CopyBoth", func(t *testing.T) {
		peer := time.Now().Add(30 * time.Minute)
		pass := "secret"
		params := &common.CreateVolumeParams{
			CacheParameters: &models.CacheParameters{
				PeerExpiryTime:  &peer,
				Passphrase:      &pass,
				PeerIPAddresses: []string{"192.0.2.1", "198.51.100.2"},
			},
		}
		res := &flexcache.CreateFlexCacheResult{
			DBVolume: &datamodel.Volume{
				CacheParameters: &datamodel.CacheParameters{},
			},
		}

		copyInputCacheParameters(params, res)

		out := res.DBVolume.CacheParameters
		assert.NotNil(t, out.CommandExpiryTime)
		assert.Equal(t, peer, *out.CommandExpiryTime)
		assert.NotSame(t, params.CacheParameters.PeerExpiryTime, out.CommandExpiryTime)
		assert.NotNil(t, out.PeerIpAddresses)
		assert.Equal(t, params.CacheParameters.PeerIPAddresses, out.PeerIpAddresses)
		assert.NotSame(t, &params.CacheParameters.PeerIPAddresses, &out.PeerIpAddresses)
	})

	t.Run("ClearBothWhenInputNil", func(t *testing.T) {
		params := &common.CreateVolumeParams{
			CacheParameters: &models.CacheParameters{},
		}
		oldTime := time.Now().Add(10 * time.Minute)
		oldPass := "old"
		res := &flexcache.CreateFlexCacheResult{
			DBVolume: &datamodel.Volume{
				CacheParameters: &datamodel.CacheParameters{
					CommandExpiryTime: &oldTime,
					Passphrase:        &oldPass,
				},
			},
		}

		copyInputCacheParameters(params, res)

		out := res.DBVolume.CacheParameters
		assert.Nil(t, out.CommandExpiryTime)
	})

	t.Run("CopyOnlyPeerExpiryTime", func(t *testing.T) {
		peer := time.Now().Add(45 * time.Minute)
		params := &common.CreateVolumeParams{
			CacheParameters: &models.CacheParameters{
				PeerExpiryTime: &peer,
			},
		}
		oldPass := "old-pass"
		res := &flexcache.CreateFlexCacheResult{
			DBVolume: &datamodel.Volume{
				CacheParameters: &datamodel.CacheParameters{
					Passphrase: &oldPass,
				},
			},
		}

		copyInputCacheParameters(params, res)

		out := res.DBVolume.CacheParameters
		assert.NotNil(t, out.CommandExpiryTime)
		assert.Equal(t, peer, *out.CommandExpiryTime)
	})

	t.Run("OverwriteExisting", func(t *testing.T) {
		initialTime := time.Now().Add(10 * time.Minute)
		initialPass := "init"
		newTime := time.Now().Add(50 * time.Minute)
		newPass := "new-pass"

		params := &common.CreateVolumeParams{
			CacheParameters: &models.CacheParameters{
				PeerExpiryTime: &newTime,
				Passphrase:     &newPass,
			},
		}
		res := &flexcache.CreateFlexCacheResult{
			DBVolume: &datamodel.Volume{
				CacheParameters: &datamodel.CacheParameters{
					CommandExpiryTime: &initialTime,
					Passphrase:        &initialPass,
				},
			},
		}

		copyInputCacheParameters(params, res)

		out := res.DBVolume.CacheParameters
		assert.NotNil(t, out.CommandExpiryTime)
		assert.Equal(t, newTime, *out.CommandExpiryTime)
	})

	t.Run("Idempotent", func(t *testing.T) {
		peer := time.Now().Add(25 * time.Minute)
		pass := "same"
		params := &common.CreateVolumeParams{
			CacheParameters: &models.CacheParameters{
				PeerExpiryTime: &peer,
				Passphrase:     &pass,
			},
		}
		res := &flexcache.CreateFlexCacheResult{
			DBVolume: &datamodel.Volume{
				CacheParameters: &datamodel.CacheParameters{},
			},
		}

		copyInputCacheParameters(params, res)
		first := *res.DBVolume.CacheParameters.CommandExpiryTime
		copyInputCacheParameters(params, res)
		second := *res.DBVolume.CacheParameters.CommandExpiryTime
		assert.Equal(t, first, second)
	})
}

func TestFlexCacheUnitTestSuite(t *testing.T) {
	suite.Run(t, new(FlexCacheUnitTestSuite))
}
