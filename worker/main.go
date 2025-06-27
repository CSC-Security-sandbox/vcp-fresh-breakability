package main

import (
	"context"
	"os"

	"github.com/google/uuid"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/jobmanageractivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/kms_activities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/replicationActivities"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/backgroundworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/jobmanagerworkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/kms_workflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/workflows/replicationWorkflows"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/db"
	tManagerPkg "github.com/vcp-vsa-control-Plane/vsa-control-plane/worker/temporalmanager"
	workflowEngine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
)

// main is the entry point of the worker application. It initializes the Temporal worker,
// database connection, registers workflows and activities, and starts the worker.
func main() {
	ctx := context.WithValue(context.Background(), utilsmiddleware.CorrelationContextKey, uuid.NewString())
	eg, ctx := errgroup.WithContext(ctx)
	logger := log.NewLogger()
	logger.Info("Starting temporal worker")

	// Create a Temporal client
	workflowClient, err := initializeTemporalClient(logger)
	if err != nil {
		logger.Error("Failed to initialize Temporal client", "error", err.Error())
		os.Exit(1)
	}

	// create database connection
	dbConn, err := db.GetDbConnection(ctx, logger)
	if err != nil {
		logger.Error("Failed to get database connection", "error", err.Error())
		os.Exit(1)
	}
	defer db.CloseDatabase(dbConn, logger)
	logger.Info("Database connection established", "connection", dbConn)

	// Initialize the temporal server client
	temporalManager := tManagerPkg.TemporalManager{
		Client: workflowClient.GetTemporalClient(),
		Config: workflowClient.LoadConfig(),
		DBConn: dbConn,
	}
	defer workflowClient.CloseClient(workflowClient.GetTemporalClient())

	// Initialise the error handler
	errorFilePath := "/errors.json"
	// Check if the file exists
	if _, err := os.Stat(errorFilePath); err == nil {
		// TODO: add a flag to enable/disable the error handler
		// TODO: add middleware to handle error codes
		// Keeping errors.json in core for now, if needed we can merge two jsons together one in core and one in proxy layer later.
		_, err = vsaerrors.NewErrorHandler(errorFilePath)
		if err != nil {
			logger.Error("Failed to create error handler", "error", err.Error())
			os.Exit(1)
		}
	}

	// Create a new worker
	worker := tManagerPkg.NewWorker(temporalManager.GetClient(), workflowEngine.CustomerTaskQueue)

	logger.Info("registering workflows and activities")
	RegisterWorkflowsAndActivities(*worker, dbConn)

	// Start the worker
	eg.Go(func() error {
		if err := worker.Run(); err != nil {
			logger.Error("Failed to run worker", "error", err.Error())
			return err
		}
		return nil
	})

	// Create Background job worker
	backgrondjobsworker := tManagerPkg.NewWorker(temporalManager.GetClient(), workflowEngine.BackgroundTaskQueue)

	workflowClient.GetTemporalClient().ScheduleClient()
	logger.Info("registering background workflows and activities")
	RegisterBackgroundWorkflowsAndActivities(*backgrondjobsworker, workflowClient.GetTemporalClient(), dbConn)

	// Start the worker
	eg.Go(func() error {
		if err := backgrondjobsworker.Run(); err != nil {
			logger.Error("Failed to run worker", "error", err.Error())
			return err
		}
		return nil
	})

	// Wait for all goroutines to complete
	if err := eg.Wait(); err != nil {
		logger.Error("Error while running worker", "error", err.Error())
		os.Exit(1)
	}

	logger.Info("All goroutines completed successfully")
}

func RegisterBackgroundWorkflowsAndActivities(worker tManagerPkg.Worker, temporal client.Client, conn database.Storage) {
	worker.RegisterWorkflow(jobmanagerworkflows.JobManagerWorkflow)
	worker.RegisterWorkflow(backgroundworkflows.SyncVSASnapshotsWorkflow)

	temporalScheduler := scheduler.NewTemporalScheduler(temporal.ScheduleClient())
	worker.RegisterActivity(&jobmanageractivities.JobManagerActivity{SE: conn, Scheduler: temporalScheduler})
	worker.RegisterActivity(&activities.CommonActivities{SE: conn})
	worker.RegisterActivity(&backgroundactivities.SyncSnapshotActivity{SE: conn})
}

// initializeTemporalClient initializes and returns a TemporalWorkflowEngine client.
// It loads the configuration, initializes the client, and logs any errors encountered.
func initializeTemporalClient(logger log.Logger) (workflowEngine.TemporalWorkflowEngine, error) {
	workflowClient := workflowEngine.TemporalWorkflowEngine{}
	workflowCfg := workflowClient.LoadConfig()
	err := workflowClient.InitializeClient(workflowCfg, logger)
	if err != nil {
		logger.Error("client error: %w", "error", err.Error())
		return workflowClient, err
	}
	return workflowClient, nil
}

// main is the entry point of the worker application. It initializes the Temporal worker
func RegisterWorkflowsAndActivities(worker tManagerPkg.Worker, dbcon database.Storage) {
	worker.RegisterWorkflow(workflows.SequenceWorkflow)
	worker.RegisterWorkflow(workflows.CreatePoolWorkflow)
	worker.RegisterWorkflow(workflows.UpdatePoolWorkflow)
	worker.RegisterWorkflow(workflows.DeletePoolWorkflow)
	worker.RegisterWorkflow(workflows.CreateVolumeWorkflow)
	worker.RegisterWorkflow(workflows.UpdateVolumeWorkflow)
	worker.RegisterWorkflow(workflows.DeleteVolumeWorkflow)
	worker.RegisterWorkflow(workflows.DeleteSnapshotWorkflow)
	worker.RegisterWorkflow(workflows.CreateSnapshotWorkflow)
	worker.RegisterWorkflow(workflows.UpdateSnapshotWorkflow)
	worker.RegisterWorkflow(workflows.AcceptClusterPeerWorkflow)
	worker.RegisterWorkflow(kms_workflows.UpdateKmsConfigWorkflow)
	worker.RegisterWorkflow(kms_workflows.CreateKmsConfigWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.CreateInternalVolumeReplicationWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.CreateVolumeReplicationWorkflow)
	worker.RegisterWorkflow(workflows.CreateBackupWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.GetMultipleReplicationsInternalWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.PerformMountCheckWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.ResumeInternalVolumeReplicationWorkflow)
	worker.RegisterWorkflow(replicationWorkflows.ResumeReplicationWorkflow)

	worker.RegisterActivity(&activities.CommonActivities{SE: dbcon})
	worker.RegisterActivity(&activities.PoolActivity{SE: dbcon})
	worker.RegisterActivity(&activities.VolumeCreateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.VolumeUpdateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.VolumeDeleteActivity{SE: dbcon})
	worker.RegisterActivity(&activities.SnapshotCreateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.SnapshotUpdateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.SnapshotDeleteActivity{SE: dbcon})
	worker.RegisterActivity(&activities.ClusterPeerActivity{SE: dbcon})
	worker.RegisterActivity(&kms_activities.KmsConfigActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.InternalVolumeReplicationActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.VolumeReplicationCreateActivity{SE: dbcon})
	worker.RegisterActivity(&activities.BackupActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.ReplicationInternalGetMultipleActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.MountJobActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.InternalVolumeReplicationResumeActivity{SE: dbcon})
	worker.RegisterActivity(&replicationActivities.ResumeVolumeReplicationActivity{SE: dbcon})
}
