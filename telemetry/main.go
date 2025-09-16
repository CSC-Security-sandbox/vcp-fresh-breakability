package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/connection"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/aggregator"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/endpoints"
	coreapiserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/collector"
	metricscommon "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/jobs"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/performance"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/processor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/usage"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/utils"
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
	VCPDbConn, err := connection.GetVcpDbConnection(ctx, logger)
	if err != nil {
		logger.Error("Failed to initialize VCP database connection", "error", err.Error())
		return
	}

	logger.Info("Successfully connected to VCP database...")

	logger.Info("Initializing metrics database...")
	// Initialize the telemetry database connection
	telemetryDbConn, err := connection.GetTelemetryDbConnection(ctx, logger)
	if err != nil {
		logger.Error("Failed to initialize Telemetry database connection", "error", err.Error())
		return
	}

	logger.Info("Successfully connected to Telemetry database...")

	googleSink := performance.NewSink(ctx, metricscommon.LoadConfig())
	billingSink := usage.NewSink(ctx, metricscommon.LoadConfig(), telemetryDbConn)
	tenantProvider := collector.NewGoogleTenantProjectProvider(VCPDbConn)
	client, err := monitoring.NewMetricClient(ctx)
	if err != nil {
		logger.Warnf("Failed to create MetricClient: %v", err)
	}
	wrapper := collector.NewMetricClientWrapper(client)
	config := metricscommon.LoadMetricsConfigFromBytes()
	provider := collector.NewGoogleProvider(tenantProvider, wrapper, config.VolumeMetrics)
	billingProvidor := aggregator.NewBillingProvider(telemetryDbConn, metricscommon.LoadConfig(), billingSink)
	metricsProcessor := processor.NewMetricsProcessor(VCPDbConn, telemetryDbConn, googleSink, provider, billingProvidor)
	tdb := telemetryDbConn.SQLDB()

	// Create a new server instance with the API handler
	var gcpServer *coreapiserver.Server
	if gcpServer, err = coreapiserver.NewServer(api.NewHandler(VCPDbConn, telemetryDbConn, metricsProcessor)); err != nil {
		logger.Error("Fatal error occurred", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("Successfully initialized Telemetry server...")

	// prometheus metrics endpoint
	mux := chi.NewRouter()
	mux.Use(chimiddleware.Recoverer)
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Mount("/", http.Handler(gcpServer))
	mux.Handle("/metrics", promhttp.Handler())

	cfg := common.LoadConfig()
	// Setup HTTP server with proper timeouts
	httpServer := &http.Server{
		Addr:              ":" + cfg.MetricsServerPort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	eg, _ := errgroup.WithContext(ctx)

	eg.Go(func() error {
		logger.Info("Starting Telemetry server", "port", cfg.MetricsServerPort)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})

	queue := utils.NewQueue(tdb, &metricsProcessor)
	queues := []string{"performance"}
	if err := queue.Worker(context.Background(), queues, &jobs.ProcessPerformanceMetrics{}); err != nil {
		logger.Errorf(err.Error())
	}

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
