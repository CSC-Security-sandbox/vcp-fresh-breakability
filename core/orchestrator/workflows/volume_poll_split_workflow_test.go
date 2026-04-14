package workflows

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type VolumePollSplitUnitTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env *testsuite.TestWorkflowEnvironment
}

func (s *VolumePollSplitUnitTestSuite) SetupTest() {
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
	s.env.RegisterWorkflow(VolumePollSplitWorkflow)
}

func (s *VolumePollSplitUnitTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

// testVolume returns a standard volume used across tests.
func testSplitVolume() *datamodel.Volume {
	return &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-volume-uuid",
				ParentSnapshotUUID: "parent-snapshot-uuid",
				State:              models.CloneStateCloned,
			},
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{
				ID: 1,
			},
			PoolCredentials: &datamodel.PoolCredentials{
				Password:      "password",
				SecretID:      "",
				CertificateID: "",
				AuthType:      env.USERNAME_PWD,
			},
			DeploymentName: "test-deployment",
		},
		Account: &datamodel.Account{
			Name: "test-account",
		},
	}
}

// testNode returns a standard ONTAP node used across tests.
func testSplitNode() *models.Node {
	return &models.Node{
		Name:            "test-node",
		EndpointAddress: "127.0.0.1",
	}
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_Success_WithOntapJob() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-123"

	// Register activities
	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	// Mock storage for UpdateCloneParentStateInDB
	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// Mock GetOntapJob to return success
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "success",
	}, nil)

	// Mock CleanupSplitSnapshot
	mockStorage.On("GetVolume", mock.Anything, volume.VolumeAttributes.CloneParentInfo.ParentVolumeUUID).Return(volume, nil).Maybe()
	s.env.OnActivity(volumeSplitActivity.CleanupSplitSnapshot, mock.Anything, mock.Anything).Return(nil)

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_Success_NoOntapJob() {
	// When ontapJobUUID is empty, ONTAP completed synchronously — no polling needed.
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "" // empty = sync completion

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	s.env.OnActivity(volumeSplitActivity.CleanupSplitSnapshot, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_OntapJobFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-456"

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// GetOntapJob returns a failure state
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "failure",
		Error: &vsa.OntapError{Message: "ONTAP split failed: insufficient space"},
	}, nil)

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_OntapJobActivityError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-789"

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// GetOntapJob activity itself errors
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(nil, errors.New("failed to reach ONTAP"))

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_UpdateJobStatusProcessingError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()

	s.env.RegisterActivity(&commonActivity)

	// Fail on UpdateJobStatus PROCESSING
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, "")

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_UpdateJobStatusDoneError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once() // PROCESSING
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update job status"))
	s.env.OnActivity(volumeSplitActivity.CleanupSplitSnapshot, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, "")

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	// When UpdateJobStatus for DONE fails, the workflow logs the error but still completes successfully (returns nil)
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_SetupError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}

	// Volume with nil Account causes setup error
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{
			ID:   1,
			UUID: "volume-uuid",
		},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID: "external-volume-uuid",
			CloneParentInfo: &datamodel.CloneParentInfo{
				ParentVolumeUUID:   "parent-volume-uuid",
				ParentSnapshotUUID: "parent-snapshot-uuid",
				State:              models.CloneStateCloned,
			},
		},
		Pool: &datamodel.Pool{
			BaseModel: datamodel.BaseModel{ID: 1},
		},
		// Account is nil to cause setup error
	}
	node := testSplitNode()

	s.env.RegisterActivity(&commonActivity)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, "")

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_CleanupSnapshotFailureIsNonFatal() {
	// Snapshot cleanup failure should not fail the workflow.
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// CleanupSplitSnapshot fails — workflow should still succeed
	s.env.OnActivity(volumeSplitActivity.CleanupSplitSnapshot, mock.Anything, mock.Anything).Return(errors.New("snapshot cleanup failed"))
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, "")

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_UpdateCloneParentStateInDBError() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-abc"

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "failure",
		Error: &vsa.OntapError{Message: "split failed"},
	}, nil)

	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update clone parent state"))

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}



// Test_VolumePollSplitWorkflow_ContinueAsNewPropagation covers lines 60 and 133-134: when
// GetContinueAsNewSuggested() returns true, VolumePollSplitWorkflow propagates the
// ContinueAsNewError directly without marking the job as ERROR or calling
// UpdateCloneParentStateInDB.
func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_ContinueAsNewPropagation() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-can-wf"

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// Signal ContinueAsNew suggested so pollONTAPSplitJob fires it on the first iteration.
	s.env.SetContinueAsNewSuggested(true)

	// UpdateJobStatus for PROCESSING succeeds; ERROR must NOT be called.
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	// UpdateCloneParentStateInDB must NOT be called (defer skips on ContinueAsNew).

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.True(s.T(), workflow.IsContinueAsNewError(s.env.GetWorkflowError()),
		"expected ContinueAsNewError, got: %v", s.env.GetWorkflowError())
}

// Test_VolumePollSplitWorkflow_UpdateJobStatusErrorFails covers line 66: when UpdateJobStatus
// for ERROR itself fails, the workflow returns that secondary error.
func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_UpdateJobStatusErrorFails() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeCreateActivity := activities.VolumeCreateActivity{SE: mockStorage}

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-err-fail"

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeCreateActivity)

	mockStorage.On("UpdateVolumeFields", mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mockStorage.On("GetVolume", mock.Anything, mock.Anything).Return(volume, nil).Maybe()

	// GetOntapJob returns failure to trigger the error path.
	s.env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "failure",
		Error: &vsa.OntapError{Message: "split failed"},
	}, nil)

	// UpdateJobStatus: PROCESSING succeeds, ERROR fails.
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("failed to update ERROR status"))
	s.env.OnActivity(volumeCreateActivity.UpdateCloneParentStateInDB, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, ontapJobUUID)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

// Test_VolumePollSplitWorkflow_DeferSkipsWhenNoCloneParentInfo covers line 130: the defer
// returns early when VolumeAttributes or CloneParentInfo is nil.
func (s *VolumePollSplitUnitTestSuite) Test_VolumePollSplitWorkflow_DeferSkipsWhenNoCloneParentInfo() {
	mockStorage := database.NewMockStorage(s.T())
	commonActivity := activities.CommonActivities{SE: mockStorage}
	volumeSplitActivity := activities.VolumeSplitActivity{SE: mockStorage}

	// Volume without CloneParentInfo — defer should skip UpdateCloneParentStateInDB.
	volume := &datamodel.Volume{
		BaseModel: datamodel.BaseModel{ID: 1, UUID: "volume-uuid"},
		VolumeAttributes: &datamodel.VolumeAttributes{
			ExternalUUID:    "external-volume-uuid",
			CloneParentInfo: nil, // nil triggers the early return on line 130
		},
		Pool: &datamodel.Pool{
			BaseModel:       datamodel.BaseModel{ID: 1},
			PoolCredentials: &datamodel.PoolCredentials{AuthType: env.USERNAME_PWD},
			DeploymentName:  "test-deployment",
		},
		Account: &datamodel.Account{Name: "test-account"},
	}
	node := testSplitNode()

	s.env.RegisterActivity(&commonActivity)
	s.env.RegisterActivity(&volumeSplitActivity)

	s.env.OnActivity(volumeSplitActivity.CleanupSplitSnapshot, mock.Anything, mock.Anything).Return(nil)
	s.env.OnActivity(commonActivity.UpdateJobStatus, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
	// UpdateCloneParentStateInDB must NOT be called.

	s.env.ExecuteWorkflow(VolumePollSplitWorkflow, volume, node, "")

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}


// TestPollONTAPSplitJobInternal_FailureWithErrorCode covers line 219: ONTAP job failure
// where the error has both a message and a non-empty code.
func TestPollONTAPSplitJobInternal_FailureWithErrorCode(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{"logParam": encodedValue}})

	mockStorage := database.NewMockStorage(t)
	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(&commonActivity)

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-errcode"

	env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "failure",
		Error: &vsa.OntapError{
			Message: "out of space",
			Code:    "ONTAP-1234",
		},
	}, nil)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return pollONTAPSplitJobInternal(ctx, volume, node, ontapJobUUID, -1, workflow.Now(ctx))
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ONTAP-1234")
	assert.Contains(t, err.Error(), "out of space")
}

// TestPollONTAPSplitJobInternal_FailureNoMessage covers line 223: ONTAP job failure
// where the error field is nil (no message, no code).
func TestPollONTAPSplitJobInternal_FailureNoMessage(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{"logParam": encodedValue}})

	mockStorage := database.NewMockStorage(t)
	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(&commonActivity)

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-nomsg"

	env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "failure",
		Error: nil, // nil error — triggers the "no error message" fallback on line 223
	}, nil)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return pollONTAPSplitJobInternal(ctx, volume, node, ontapJobUUID, -1, workflow.Now(ctx))
	})

	assert.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no error message")
}

// TestPollONTAPSplitJobInternal_SleepError covers lines 227-228: Sleep returns an error
// (e.g. workflow cancelled) while waiting between polls.
func TestPollONTAPSplitJobInternal_SleepError(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{"logParam": encodedValue}})

	mockStorage := database.NewMockStorage(t)
	commonActivity := activities.CommonActivities{SE: mockStorage}
	env.RegisterActivity(&commonActivity)

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-sleep-err"

	// GetOntapJob returns a running state so the loop proceeds to Sleep.
	env.OnActivity(commonActivity.GetOntapJob, mock.Anything, ontapJobUUID, node).Return(&vsa.OntapJob{
		UUID:  ontapJobUUID,
		State: "running",
	}, nil)

	// Cancel the workflow context after the activity completes so Sleep returns an error.
	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 0)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return pollONTAPSplitJobInternal(ctx, volume, node, ontapJobUUID, -1, workflow.Now(ctx))
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.Error(t, env.GetWorkflowError())
}

func TestPollONTAPSplitJobContinueAsNew(t *testing.T) {
	// pollONTAPSplitJobInternal should return a ContinueAsNewError when the fallback
	// maxHistoryLength threshold is reached. We use maxHistoryLength=0 so the check fires
	// immediately on the first iteration (history length starts at 0 in the test env),
	// before any GetOntapJob activity is called.
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{
		Fields: map[string]*commonpb.Payload{"logParam": encodedValue},
	})
	env.RegisterWorkflow(VolumePollSplitWorkflow)

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-can"

	// Use maxHistoryLength=0 so ContinueAsNew fires immediately (history length starts at 0).
	// runStart = workflow.Now(ctx) so elapsed ~0 and the time-based check does not interfere.
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)
		return pollONTAPSplitJobInternal(ctx, volume, node, ontapJobUUID, 0, workflow.Now(ctx))
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()),
		"expected ContinueAsNewError, got: %v", env.GetWorkflowError())
}

// TestPollONTAPSplitJobContinueAsNew_TimeBased verifies that the time-based ContinueAsNew
// trigger fires when the run has been alive longer than the configured threshold, even when
// the history-size check would not fire. We pass a runStart that is already 2 hours in the
// past so that elapsed > GetSplitVolumeRunContinueAsNewDuration() on the very first loop
// iteration, before any GetOntapJob activity is called.
func TestPollONTAPSplitJobContinueAsNew_TimeBased(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	encodedValue, _ := converter.GetDefaultDataConverter().ToPayload(log.Fields{})
	env.SetHeader(&commonpb.Header{
		Fields: map[string]*commonpb.Payload{"logParam": encodedValue},
	})
	env.RegisterWorkflow(VolumePollSplitWorkflow)

	volume := testSplitVolume()
	node := testSplitNode()
	ontapJobUUID := "ontap-job-uuid-time-can"

	// runStart is 2 hours before workflow.Now(ctx), so elapsed > 60m threshold immediately.
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ao := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second}
		ctx = workflow.WithActivityOptions(ctx, ao)
		pastRunStart := workflow.Now(ctx).Add(-2 * time.Hour)
		return pollONTAPSplitJobInternal(ctx, volume, node, ontapJobUUID, -1, pastRunStart)
	})

	assert.True(t, env.IsWorkflowCompleted())
	assert.True(t, workflow.IsContinueAsNewError(env.GetWorkflowError()),
		"expected ContinueAsNewError from time-based trigger, got: %v", env.GetWorkflowError())
}

func TestVolumePollSplitUnitTestSuite(t *testing.T) {
	suite.Run(t, new(VolumePollSplitUnitTestSuite))
}
