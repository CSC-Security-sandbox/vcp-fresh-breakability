package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/robfig/cron"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	coregenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/endpoints"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/tasks"
	_ "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/drivers/postgres"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
	_ "go.uber.org/automaxprocs"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.WithValue(context.Background(), utilsmiddleware.CorrelationContextKey, uuid.NewString())

	// Use signal.NotifyContext to handle termination signals
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.NewLogger()
	logger.Info("Starting VCP Core API Service")

	// Setup metrics, tracing, and context propagation
	shutdown, err := log.SetupOpenTelemetry(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "error setting up OpenTelemetry", "error", err)
		shutdown = func(ctx context.Context) error { return nil }
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			logger.ErrorContext(ctx, "error shutting down OpenTelemetry", "error", err)
		}
	}()

	cfg := common.LoadConfig()

	// initialize the database - this can be moved to a separate function
	dbCon, err := InitializeDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database", "error", err.Error())
		os.Exit(1)
	}
	defer closeDatabase(dbCon, logger)

	// Initialize Temporal client
	workflowClient, err := initializeTemporalClient(logger)
	if err != nil {
		logger.Error("Failed to initialize Temporal client", "error", err.Error())
		os.Exit(1)
	}
	defer workflowClient.CloseClient(workflowClient.GetTemporalClient())

	// Create GCP proxy server and inject required dependencies
	orch := orchestrator.GetNewOrchestrator(dbCon, workflowClient.GetTemporalClient())
	newHandler := &api.Handler{Orchestrator: orch} // inject the orchestrator into the handler
	oasserver, err := coregenserver.NewServer(newHandler)
	if err != nil {
		logger.Error("Failed to create server", "error", err.Error())
		os.Exit(1)
	}

	_, err = vsaerrors.NewErrorHandler()
	if err != nil {
		logger.Error("Failed to create error handler", "error", err.Error())
		os.Exit(1)
	}

	httpServer := setupHTTPServer(cfg, oasserver)

	// Use errgroup to manage goroutines and context
	eg, ctx := errgroup.WithContext(ctx)

	// Start HTTP server
	eg.Go(func() error {
		logger.Info("Starting HTTP server on " + cfg.CoreHost + ":" + cfg.CorePort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start HTTP server", "error", err.Error())
			return err
		}
		return nil
	})

	// Start background task scheduler
	if cfg.EnableBackgroundTask {
		eg.Go(func() error {
			return startBackgroundTaskScheduler(ctx, dbCon)
		})
	}

	// Handle graceful shutdown on signal or context cancellation
	eg.Go(func() error {
		<-ctx.Done()
		logger.Info("Shutting down server")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Failed to shut down server gracefully", "error", err.Error())
			return err
		}
		return nil
	})
	handleGracefulShutdown(eg, ctx, httpServer, logger)

	// Wait for all goroutines to finish
	if err := eg.Wait(); err != nil {
		logger.Error("Server error", "error", err.Error())
		os.Exit(1)
	}

	logger.Info("Server stopped gracefully")
}

func InitializeDatabase(ctx context.Context, cfg *common.Config, logger log.Logger) (database.Storage, error) {
	dbConfig := GetDBConfig(cfg)
	db, err := database.New(dbConfig, logger)
	if err != nil {
		return nil, err
	}
	for {
		err = db.Connect(false)
		if err == nil {
			break
		}
		logger.Error("Failed to connect to the database, retrying...", "error", err.Error())
		time.Sleep(2 * time.Second) // Add a delay between retries to avoid overwhelming the database
	}

	if cfg.RunMigrationOnStart {
		// this flag is used to run the migration on start in local env
		// this works only if the app user has the necessary permission
		if err := db.Migrate(ctx); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func closeDatabase(dbCon database.Storage, logger log.Logger) {
	if err := dbCon.Close(); err != nil {
		logger.Error("Failed to close database connection", "error", err.Error())
	}
}

func initializeTemporalClient(logger log.Logger) (workflowengine.WorkflowEngine, error) {
	workflowClient := workflowengine.WorkflowEngine{}
	workflowCfg := workflowClient.LoadConfig()
	err := workflowClient.InitializeClient(workflowCfg, logger)
	if err != nil {
		logger.Error("client error: %w", "error", err.Error())
		return workflowClient, err
	}
	return workflowClient, nil
}

func GetDBConfig(cfg *common.Config) dbutils.DbConfig {
	dbConfig := dbutils.DbConfig{
		Type:            cfg.DBType,
		Host:            cfg.DBHost,
		Port:            cfg.DBPort,
		User:            cfg.DBUser,
		Password:        cfg.DBPassword,
		Name:            cfg.DBName,
		SSLMode:         cfg.DBSSLMode,
		TimeZone:        cfg.DBTimeZone.String(),
		MaxOpenConns:    cfg.DBMaxOpenConns,
		MaxIdleConns:    cfg.DBMaxIdleConns,
		ConnMaxLifetime: cfg.DBConnMaxLifetime,
		AdminUser:       cfg.DBAdminUser,
		AdminPassword:   cfg.DBAdminPassword,
	}
	if cfg.MSIEnabled {
		// When MSI is enabled, set the database user credentials from MSIDBUser
		dbConfig.User = cfg.MSIDBUser
		dbConfig.AdminUser = cfg.MSIDBUser
	}
	return dbConfig
}

func setupHTTPServer(cfg *common.Config, handler http.Handler) *http.Server {
	mux := chi.NewRouter()
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(auth.AuthMiddleware(true)) // true = skip project number validation
	mux.Use(log.RecoverMiddleware)
	mux.Mount("/", handler)
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:              cfg.CoreHost + ":" + cfg.CorePort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
}

// startBackgroundTaskScheduler initializes and starts the cron scheduler for background tasks
func startBackgroundTaskScheduler(ctx context.Context, se database.Storage) error {
	logger := util.GetLogger(ctx)
	logger.Info("Starting background task scheduler  ")

	logger.InfoContext(ctx, "Starting background task scheduler")

	cronScheduler := cron.New()

	// Schedule the VSA Cluster Health Sync Task to run every 30 seconds with database-backed locking
	err := cronScheduler.AddFunc("*/30 * * * * *", func() { syncVSAClusterHealthWithLock(ctx, se, logger) })
	if err != nil {
		logger.ErrorContext(ctx, "Failed to schedule VSA Cluster Health Sync Task", "error", err)
		return err
	}

	cronScheduler.Start()
	logger.InfoContext(ctx, "Background task scheduler started successfully")

	<-ctx.Done()

	logger.InfoContext(ctx, "Stopping background task scheduler")
	cronScheduler.Stop()

	return nil
}

// syncVSAClusterHealthWithLock implements database-backed cron job with locking mechanism
func syncVSAClusterHealthWithLock(ctx context.Context, se database.Storage, logger log.Logger) {
	const jobType = "SYNC_VSA_CLUSTER_HEALTH_STATUS"
	const cronExpression = "*/30 * * * * *"
	const lockTimeoutSeconds = 30

	logger.InfoContext(ctx, "Starting VSA cluster health sync with lock")

	// Try to create the job spec atomically (pod initialization phase)
	newJobSpec := &datamodel.AdminJobSpec{
		JobType:        jobType,
		CronExpression: cronExpression,
		State:          scheduler.JobStatusScheduled,
	}

	createdJobSpec, createErr := se.CreateAdminJobSpecIfNotExists(ctx, newJobSpec)
	if createErr == nil {
		// Successfully created the job spec - we have the "lock", execute immediately
		logger.InfoContext(ctx, "Created new admin job spec, executing task immediately", "jobUUID", createdJobSpec.UUID)
		// Generate correlation ID for tracing (use job UUID if available, otherwise generate new UUID)
		correlationID := createdJobSpec.UUID
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		tasks.SyncVSAClusterHealth(ctx, se, correlationID)
		return
	}

	// Job spec already exists, try to acquire lock by updating it
	logger.InfoContext(ctx, "Job spec already exists, attempting to acquire lock by updating", "jobType", jobType)

	currentTime := time.Now()
	// Lock threshold: only allow lock acquisition if last update was >= 30 seconds ago
	// This handles the case where a pod crashes - after 30 seconds, other pods can take over
	lockThreshold := currentTime.Add(-time.Duration(lockTimeoutSeconds) * time.Second)

	// Try to acquire the lock by updating the job spec
	rowsAffected, updateErr := se.UpdateAdminJobSpecWithLock(ctx, jobType, scheduler.JobStatusScheduled, lockThreshold, currentTime)

	if updateErr != nil {
		logger.ErrorContext(ctx, "Failed to acquire lock for admin job spec", "error", updateErr, "jobType", jobType)
		return
	}

	if rowsAffected > 0 {
		// Successfully acquired the lock, now get the job spec to retrieve UUID
		jobSpec, getErr := se.GetAdminJobSpecByJobType(ctx, jobType)
		if getErr != nil {
			logger.ErrorContext(ctx, "Failed to retrieve job spec after acquiring lock", "error", getErr, "jobType", jobType)
			return
		}

		logger.InfoContext(ctx, "Successfully acquired lock, executing task", "jobUUID", jobSpec.UUID)
		// Generate correlation ID for tracing (use job UUID if available, otherwise generate new UUID)
		correlationID := jobSpec.UUID
		if correlationID == "" {
			correlationID = uuid.NewString()
		}
		tasks.SyncVSAClusterHealth(ctx, se, correlationID)
	} else {
		logger.InfoContext(ctx, "Could not acquire lock - another pod is currently executing the task or not enough time has passed since last execution")
	}
}

func handleGracefulShutdown(eg *errgroup.Group, ctx context.Context, httpServer *http.Server, logger log.Logger) {
	eg.Go(func() error {
		<-ctx.Done()
		logger.Info("Shutting down server")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Failed to shut down server gracefully", "error", err.Error())
			return err
		}
		return nil
	})
}
