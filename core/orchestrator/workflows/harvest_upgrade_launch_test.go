package workflows

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
	workflowenginetemporal "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/api/enums/v1"
	temporalclient "go.temporal.io/sdk/client"
)

type mockWorkflowRun struct {
	getErr error
}

func (m *mockWorkflowRun) Get(ctx context.Context, valuePtr interface{}) error {
	return m.getErr
}

func (m *mockWorkflowRun) GetWithOptions(ctx context.Context, valuePtr interface{}, options temporalclient.WorkflowRunGetOptions) error {
	return m.getErr
}

func (m *mockWorkflowRun) GetID() string {
	return HarvestPollerUpgradeWorkflowID
}

func (m *mockWorkflowRun) GetRunID() string {
	return "test-run-id"
}

func TestLaunchHarvestRefreshIfNeeded(t *testing.T) {
	currentHarvestSHA := utils.HarvestTemplateSHA

	t.Run("SkipsWhenFeatureDisabled", func(tt *testing.T) {
		cfg := &common.Config{RefreshHarvestOnUpgrade: false}
		logger := log.NewLogger()

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, nil, nil, logger)
		assert.NoError(tt, err)
	})

	t.Run("TriggersWorkflowOnFirstDeploy_NoExistingRow", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		run := &mockWorkflowRun{}
		notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found"))
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, notFoundErr)
		// Workflow function cannot be passed into mock.On (testify treats it as unsupported Func); assert options via MatchedBy.
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything,
			mock.MatchedBy(func(opts temporalclient.StartWorkflowOptions) bool {
				return opts.TaskQueue == workflowenginetemporal.CustomerTaskQueue &&
					opts.ID == HarvestPollerUpgradeWorkflowID &&
					opts.WorkflowIDConflictPolicy == enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			}),
			mock.Anything,
			mock.Anything,
		).Return(run, nil)
		mockDB.EXPECT().UpsertAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA).
			Return(nil)

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.NoError(tt, err)

		mockDB.AssertCalled(tt, "UpsertAppConfig", mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA)
	})

	t.Run("TriggersWorkflowWhenTemplateChanged", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		run := &mockWorkflowRun{}
		oldHash := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(&datamodel.AppConfig{Key: HarvestTemplateSHAAppConfigKey, Value: oldHash}, nil)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything,
			mock.MatchedBy(func(opts temporalclient.StartWorkflowOptions) bool {
				return opts.TaskQueue == workflowenginetemporal.CustomerTaskQueue &&
					opts.ID == HarvestPollerUpgradeWorkflowID &&
					opts.WorkflowIDConflictPolicy == enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING
			}),
			mock.Anything,
			mock.Anything,
		).Return(run, nil)
		mockDB.EXPECT().UpsertAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA).
			Return(nil)

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.NoError(tt, err)

		mockDB.AssertCalled(tt, "UpsertAppConfig", mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA)
	})

	t.Run("SkipsWhenHashUnchanged", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(&datamodel.AppConfig{Key: HarvestTemplateSHAAppConfigKey, Value: currentHarvestSHA}, nil)

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.NoError(tt, err)

		mockDB.AssertNotCalled(tt, "UpsertAppConfig", mock.Anything, mock.Anything, mock.Anything)
		mockTemporal.AssertNotCalled(tt, "ExecuteWorkflow", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("ReturnsErrorWhenDBReadFails", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		dbErr := vsaerrors.NewVCPError(vsaerrors.ErrDatabaseDataReadError, errors.New("connection refused"))
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, dbErr)

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to read harvest template SHA from DB")
	})

	t.Run("ReturnsErrorWhenGetAppConfigReturnsNonNotFoundError", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, errors.New("plain error without ErrResourceNotFound"))

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to read harvest template SHA from DB")
	})

	t.Run("ReturnsErrorWhenWorkflowExecutionFails", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()

		notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found"))
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, notFoundErr)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("temporal unavailable"))

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to execute HarvestPollerUpgradeWorkFlow")
	})

	t.Run("ReturnsErrorWhenUpsertFails", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()
		run := &mockWorkflowRun{}

		notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found"))
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, notFoundErr)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(run, nil)
		mockDB.EXPECT().UpsertAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA).
			Return(errors.New("upsert failed"))

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "failed to persist harvest template SHA")
	})

	t.Run("ReturnsErrorWhenWorkflowRunFails_DoesNotPersistSHA", func(tt *testing.T) {
		mockDB := database.NewMockStorage(tt)
		mockTemporal := workflowengine.NewMockTemporalTestClient(tt)
		cfg := &common.Config{RefreshHarvestOnUpgrade: true}
		logger := log.NewLogger()
		run := &mockWorkflowRun{getErr: errors.New("workflow activity failed")}

		notFoundErr := vsaerrors.NewVCPError(vsaerrors.ErrResourceNotFound, errors.New("not found"))
		mockDB.EXPECT().GetAppConfig(mock.Anything, HarvestTemplateSHAAppConfigKey).
			Return(nil, notFoundErr)
		mockTemporal.EXPECT().ExecuteWorkflow(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(run, nil)

		err := LaunchHarvestRefreshIfNeeded(context.Background(), cfg, mockDB, mockTemporal, logger)
		assert.Error(tt, err)
		assert.Contains(tt, err.Error(), "harvest refresh workflow failed")
		mockDB.AssertNotCalled(tt, "UpsertAppConfig", mock.Anything, HarvestTemplateSHAAppConfigKey, currentHarvestSHA)
	})
}
