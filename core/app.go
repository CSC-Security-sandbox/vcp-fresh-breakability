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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	coregenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/handler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	dbutils "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/auth"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	workflowengine "github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/temporal"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, uuid.NewString())

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

	// Create GCP proxy server and inject required dependencies
	orch := orchestrator.GetNewOrchestrator(dbCon, workflowClient.GetTemporalClient())
	newHandler := api.Handler{Orchestrator: orch} // inject the orchestrator into the handler
	oasserver, err := coregenserver.NewServer(newHandler)
	if err != nil {
		logger.Error("Failed to create server", "error", err.Error())
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
	// Setup HTTP router
	mux := chi.NewRouter()
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(auth.AuthMiddleware(true)) // true = skip project number validation
	mux.Use(log.RecoverMiddleware)

	// Mount the generated API handler
	mux.Mount("/", http.Handler(handler))
	mux.Handle("/metrics", promhttp.Handler())

	// Setup HTTP server with proper timeouts
	return &http.Server{
		Addr:              cfg.CoreHost + ":" + cfg.CorePort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
}
