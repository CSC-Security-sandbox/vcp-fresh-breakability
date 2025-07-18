package backgroundworkflows

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
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

func (s *SyncSnapshotsTestSuite) TestSyncSnapshotsWorkflow_SynchronizeSnapshotsWithErrors() {
	mockStorage := database.NewMockStorage(s.T())
	syncSnapshotsActivity := &backgroundactivities.SyncSnapshotActivity{SE: mockStorage}

	s.env.RegisterActivity(syncSnapshotsActivity.ListPools)
	s.env.RegisterActivity(syncSnapshotsActivity.SynchronizeSnapshots)

	// Mock ListPools to return no error and an empty pool list
	s.env.OnActivity(syncSnapshotsActivity.ListPools, mock.Anything).Return([]*datamodel.Pool{}, nil)

	// Mock SynchronizeSnapshots to return an error
	s.env.OnActivity(syncSnapshotsActivity.SynchronizeSnapshots, mock.Anything, mock.Anything).Return(fmt.Errorf("snapshot Synchronization completed with errors: [error1, error2]"))

	s.env.ExecuteWorkflow(SyncVSASnapshotsWorkflow)

	assert.True(s.T(), s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	assert.Error(s.T(), err)
	assert.Contains(s.T(), err.Error(), "snapshot Synchronization completed with errors")
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
