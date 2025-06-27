package backgroundworkflows

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

type SyncSnapshotsTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func (s *SyncSnapshotsTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetContextPropagators([]workflow.ContextPropagator{util.NewContextMapPropagator()})
	s.env.RegisterWorkflow(SyncVSASnapshotsWorkflow)
}

func (s *SyncSnapshotsTestSuite) AfterTest() {
	s.env.AssertExpectations(s.T())
}

func TestJobManagerTestSuiteTestSuite(t *testing.T) {
	suite.Run(t, new(SyncSnapshotsTestSuite))
}

func (s *SyncSnapshotsTestSuite) TestSyncSnapshotsWorkflow_Success() {
	mockStorage := database.NewMockStorage(s.T())
	syncSnapshotsActivity := &backgroundactivities.SyncSnapshotActivity{SE: mockStorage}

	s.env.RegisterActivity(syncSnapshotsActivity.ListPools)
	s.env.RegisterActivity(syncSnapshotsActivity.SynchronizeSnapshots)

	s.env.OnActivity(syncSnapshotsActivity.ListPools, mock.Anything).Return([]*datamodel.Pool{}, nil)
	s.env.OnActivity(syncSnapshotsActivity.SynchronizeSnapshots, mock.Anything, mock.Anything).Return(nil)

	s.env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.NoError(s.T(), s.env.GetWorkflowError())
}

func (s *SyncSnapshotsTestSuite) TestSyncSnapshotsWorkflow_ListPoolsFailure() {
	mockStorage := database.NewMockStorage(s.T())
	syncSnapshotsActivity := &backgroundactivities.SyncSnapshotActivity{SE: mockStorage}

	s.env.RegisterActivity(syncSnapshotsActivity.ListPools)
	s.env.RegisterActivity(syncSnapshotsActivity.SynchronizeSnapshots)

	s.env.OnActivity(syncSnapshotsActivity.ListPools, mock.Anything).Return(nil, errors.New("could not fetch pools"))

	s.env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}

func (s *SyncSnapshotsTestSuite) TestSyncSnapshotsWorkflow_SynchronizeSnapshotsFailure() {
	mockStorage := database.NewMockStorage(s.T())
	syncSnapshotsActivity := &backgroundactivities.SyncSnapshotActivity{SE: mockStorage}

	s.env.RegisterActivity(syncSnapshotsActivity.ListPools)
	s.env.RegisterActivity(syncSnapshotsActivity.SynchronizeSnapshots)

	s.env.OnActivity(syncSnapshotsActivity.ListPools, mock.Anything).Return([]*datamodel.Pool{}, nil)
	s.env.OnActivity(syncSnapshotsActivity.SynchronizeSnapshots, mock.Anything, mock.Anything).Return(errors.New("could not sync snapshots"))

	s.env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	assert.Error(s.T(), s.env.GetWorkflowError())
}
