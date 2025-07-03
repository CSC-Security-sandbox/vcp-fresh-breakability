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
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/endpoints"
	coreapiserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/database"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/sync/errgroup"
)

func main() {
	logger := log.NewLogger()
	logger.Info("Starting Telemetry Server")
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, uuid.NewString())
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	// TODO SHIVVAT defer DB connection close
	// defer cleanup(ctx)

	logger.Info("Initializing database connections...")
	// Initialize the VCP database connection
	VCPDbConn, err := database.InitializeDatabase(ctx, &database.VCPDatabaseImpl, logger)
	if err != nil {
		logger.Error("Failed to initialize VCP database connection", "error", err.Error())
		return
	}

	// Initialize the telemetry database connection
	telemetryDbConn, err := database.InitializeDatabase(ctx, &database.TelemetryDatabaseImpl, logger)
	if err != nil {
		logger.Error("Failed to initialize Telemetry database connection", "error", err.Error())
		return
	}

	googleSink := performance.NewSink(ctx, common.LoadConfig())
	metricsProcessor := processor.NewMetricsProcessor(VCPDbConn, telemetryDbConn, googleSink)

	// Create a new server instance with the API handler
	var gcpServer *coreapiserver.Server
	if gcpServer, err = coreapiserver.NewServer(api.NewHandler(VCPDbConn, telemetryDbConn, metricsProcessor)); err != nil {
		logger.Error("Fatal error occurred", "error", err.Error())
		os.Exit(1)
	}

	// prometheus metrics endpoint
	mux := chi.NewRouter()
	mux.Use(chimiddleware.Recoverer)
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Mount("/", http.Handler(gcpServer))
	mux.Handle("/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	eg, _ := errgroup.WithContext(ctx)

	eg.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	// Wait for the context to be done, it's an infinite loop.
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
