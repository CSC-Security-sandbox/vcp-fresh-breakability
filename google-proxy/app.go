package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	vsaerrors "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/scheduler/adminbackgroundjobs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	_ "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/postgres"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/endpoints"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"go.temporal.io/sdk/client"
	"golang.org/x/sync/errgroup"
)

var errorFilePath = env.GetString("ERROR_FILE_PATH", "/errors.json")

func main() {
	ctx := context.WithValue(context.Background(), utilsmiddleware.CorrelationContextKey, uuid.NewString())

	// Use signal.NotifyContext to handle termination signals
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.NewLogger()
	logger.Info("Starting gcp proxy API")

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

	err = refreshAdminJobSpecs(ctx, cfg, dbCon, workflowClient.GetTemporalClient(), logger)
	if err != nil {
		logger.Errorf("Failed to refresh admin job specs: %v", err)
		os.Exit(1)
	}

	// Create GCP proxy server and inject required dependencies
	orch := orchestrator.GetNewOrchestrator(dbCon, workflowClient.GetTemporalClient())
	newHandler := api.Handler{Orchestrator: orch} // inject the orchestrator into the handler
	gcpServer, err := gcpgenserver.NewServer(newHandler)
	if err != nil {
		logger.Error("Failed to create server", "error", err.Error())
		os.Exit(1)
	}

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
	httpServer := setupHTTPServer(cfg, gcpServer)

	// Use errgroup to manage goroutines and context
	eg, ctx := errgroup.WithContext(ctx)
	// Start HTTP server
	eg.Go(func() error {
		logger.Info("Starting HTTP server on " + cfg.GCPHost + ":" + cfg.GCPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start HTTP server", "error", err.Error())
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

func GetDBConfig(cfg *common.Config) database.DbConfig {
	dbConfig := database.DbConfig{
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

func refreshAdminJobSpecs(ctx context.Context, cfg *common.Config, db database.Storage, temporal client.Client, logger log.Logger) error {
	if cfg.RefreshAdminJobSpecs {
		err := adminbackgroundjobs.LoadJobSpecs()
		if err != nil {
			logger.Errorf("Failed to load admin job specs: %v", err)
			return err
		}

		shouldRefreshAdminJobSpecs, err := adminbackgroundjobs.IsJobSpecRefreshNeeded(ctx, db, logger)
		if err != nil {
			logger.Errorf("Failed to check if job specs refresh is needed: %v", err)
			return err
		}

		if shouldRefreshAdminJobSpecs {
			err = adminbackgroundjobs.LaunchJobManagerWorkflow(ctx, temporal, logger)
			if err != nil {
				return err
			}
			logger.Info("Admin job specs have been refreshed successfully")
		}
	}
	return nil
}

func setupHTTPServer(cfg *common.Config, handler http.Handler) *http.Server {
	mux := chi.NewRouter()
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(middleware.AuthMiddleware)
	mux.Use(chimiddleware.Recoverer)
	mux.Mount("/", handler)
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Addr:              cfg.GCPHost + ":" + cfg.GCPPort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
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
