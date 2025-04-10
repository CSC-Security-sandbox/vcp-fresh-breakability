package main

import (
	"context"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database"
	_ "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/postgres"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/endpoints"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/middleware"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"

	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx := context.WithValue(context.Background(), common.CorrelationContextKey, uuid.NewString())

	// Use signal.NotifyContext to handle termination signals
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Starting gcp proxy API")
	cfg := common.LoadConfig()

	// initialize the database - this can be moved to a separate function
	dbCon, err := initializeDatabase(ctx, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize database", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer closeDatabase(dbCon, logger)

	// Create GCP proxy server and inject required dependencies
	orch := orchestrator.NewOrchestrator(dbCon)
	newHandler := api.Handler{Orchestrator: orch} // inject the orchestrator into the handler
	gcpServer, err := gcpgenserver.NewServer(newHandler)
	if err != nil {
		logger.Error("Failed to create server", slog.String("error", err.Error()))
		os.Exit(1)
	}
	httpServer := setupHTTPServer(cfg, gcpServer)
	// Use errgroup to manage goroutines and context
	eg, ctx := errgroup.WithContext(ctx)

	// Start HTTP server
	eg.Go(func() error {
		logger.Info("Starting HTTP server on localhost:" + cfg.GCPPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start HTTP server", slog.String("error", err.Error()))
			return err
		}
		return nil
	})

	handleGracefulShutdown(eg, ctx, httpServer, logger)
	// Wait for all goroutines to finish
	if err := eg.Wait(); err != nil {
		logger.Error("Server error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("Server stopped gracefully")
}

func initializeDatabase(ctx context.Context, cfg *common.Config, logger *slog.Logger) (database.Storage, error) {
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
		MigrationPath:   cfg.MigrationPath,
		AdminUser:       cfg.DBAdminUser,
		AdminPassword:   cfg.DBAdminPassword,
	}
	db, err := database.New(dbConfig, logger)
	if err != nil {
		return nil, err
	}
	for {
		err = db.Connect()
		if err == nil {
			break
		}
		logger.Error("Failed to connect to the database, retrying...", slog.String("error", err.Error()))
		time.Sleep(2 * time.Second) // Add a delay between retries to avoid overwhelming the database
	}

	if cfg.RunMigrationOnStart {
		if err := db.Migrate(ctx); err != nil {
			return nil, err
		}
	}
	return db, nil
}

func closeDatabase(dbCon database.Storage, logger *slog.Logger) {
	if err := dbCon.Close(); err != nil {
		logger.Error("Failed to close database connection", slog.String("error", err.Error()))
	}
}

func setupHTTPServer(cfg *common.Config, handler http.Handler) *http.Server {
	mux := chi.NewRouter()
	mux.Use(middleware.AuthMiddleware)
	mux.Use(chimiddleware.Logger)
	mux.Use(chimiddleware.Recoverer)
	mux.Mount("/", handler)

	return &http.Server{
		Addr:              "localhost:" + cfg.GCPPort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
}

func handleGracefulShutdown(eg *errgroup.Group, ctx context.Context, httpServer *http.Server, logger *slog.Logger) {
	eg.Go(func() error {
		<-ctx.Done()
		logger.Info("Shutting down server")

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Failed to shut down server gracefully", slog.String("error", err.Error()))
			return err
		}
		return nil
	})
}
