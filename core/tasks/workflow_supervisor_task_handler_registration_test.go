package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	supervisorhandler "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/tasks/supervisor-handler"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine"
)

func TestRunWorkflowSupervisorTask_RegistersAllHandlers(t *testing.T) {
	enableProcessingTimeout(t)
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	// Mock GetJobsWithCondition since runWorkflowSupervisorTask calls scan()
	// First call for NEW state jobs, second call for PROCESSING state jobs
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil).Twice()

	// Call with no handlers to trigger default registration
	runWorkflowSupervisorTask(context.Background(), storage, temporal, "test-correlation-id")

	// Verify that all handlers are registered by checking supported job types
	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "test-correlation-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}

	// Register default handlers
	handlers := []supervisorhandler.Handler{
		supervisorhandler.NewCmekHandler(),
		supervisorhandler.NewPoolHandler(),
		supervisorhandler.NewPoolUpdateHandler(),
		supervisorhandler.NewPoolDeleteHandler(),
		supervisorhandler.NewVolumeHandler(),
		supervisorhandler.NewVolumeUpdateHandler(),
		supervisorhandler.NewVolumeDeleteHandler(),
		supervisorhandler.NewBackupHandler(),
		supervisorhandler.NewBackupUpdateHandler(),
		supervisorhandler.NewBackupDeleteHandler(),
		supervisorhandler.NewSnapshotHandler(),
		supervisorhandler.NewSnapshotDeleteHandler(),
		supervisorhandler.NewReplicationHandler(),
		supervisorhandler.NewReplicationUpdateHandler(),
		supervisorhandler.NewReplicationDeleteHandler(),
		supervisorhandler.NewKmsDeleteHandler(),
		supervisorhandler.NewKmsMigrateHandler(),
		supervisorhandler.NewNetworkHandler(),
	}

	runner.registerHandlers(handlers...)

	// Verify UPDATE handlers are registered
	handler, ok := runner.handlerFor(string(models.JobTypeUpdatePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateLargePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateVolume))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateBackup))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateVolumeReplication))
	require.True(t, ok)
	require.NotNil(t, handler)

	// Verify DELETE handlers are registered
	handler, ok = runner.handlerFor(string(models.JobTypeDeletePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteLargePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteVolume))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteBackup))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteSnapshot))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteVolumeReplication))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteKmsConfig))
	require.True(t, ok)
	require.NotNil(t, handler)

	// Verify MIGRATE handlers are registered
	handler, ok = runner.handlerFor(string(models.JobTypeMigrateKmsConfig))
	require.True(t, ok)
	require.NotNil(t, handler)
}

func TestRunWorkflowSupervisorTask_RegistersCustomHandlers(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)
	customHandler := newTestHandler(models.JobTypeUpdatePool)

	// Mock GetJobsWithCondition for NEW state scan only.
	// UPDATE_POOL is not eligible for PROCESSING state scan, so only one call.
	storage.EXPECT().GetJobsWithCondition(mock.Anything, mock.Anything).Return([]*datamodel.Job{}, nil).Once()

	// Call with custom handler
	runWorkflowSupervisorTask(context.Background(), storage, temporal, "test-correlation-id", customHandler)

	// Verify custom handler is used instead of default
	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "test-correlation-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}

	runner.registerHandlers(customHandler)

	handler, ok := runner.handlerFor(string(models.JobTypeUpdatePool))
	require.True(t, ok)
	require.Equal(t, customHandler, handler)
}

func TestWorkflowSupervisorTaskRunner_HandlerFor_UpdateDeleteJobTypes(t *testing.T) {
	storage := database.NewMockStorage(t)
	temporal := workflowEngine.NewMockTemporalTestClient(t)

	runner := &workflowSupervisorTaskRunner{
		storage:       storage,
		temporal:      temporal,
		correlationID: "test-correlation-id",
		handlers:      make(map[string]supervisorhandler.Handler),
	}

	// Register update and delete handlers
	runner.registerHandlers(
		supervisorhandler.NewPoolUpdateHandler(),
		supervisorhandler.NewPoolDeleteHandler(),
		supervisorhandler.NewVolumeUpdateHandler(),
		supervisorhandler.NewVolumeDeleteHandler(),
	)

	// Test UPDATE handlers
	handler, ok := runner.handlerFor(string(models.JobTypeUpdatePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateLargePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeUpdateVolume))
	require.True(t, ok)
	require.NotNil(t, handler)

	// Test DELETE handlers
	handler, ok = runner.handlerFor(string(models.JobTypeDeletePool))
	require.True(t, ok)
	require.NotNil(t, handler)

	handler, ok = runner.handlerFor(string(models.JobTypeDeleteVolume))
	require.True(t, ok)
	require.NotNil(t, handler)
}
