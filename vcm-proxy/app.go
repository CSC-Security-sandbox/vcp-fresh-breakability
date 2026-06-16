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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	utilsmiddleware "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	vcmapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcm-proxy/api/endpoints"
	vcmserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcm-proxy/api/vcm-servergen"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.WithValue(context.Background(), utilsmiddleware.CorrelationContextKey, uuid.NewString())
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.NewLogger()
	logger.Info("Starting VCM proxy API")
	cfg := common.LoadConfig()
	if cfg == nil {
		logger.Error("Failed to load configuration")
		os.Exit(1)
	}

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

	orch := factory.GetOrchestratorForProvider(nil, nil)

	serverState := vcmapi.NewServerState()
	handler := &vcmapi.Handler{
		Orchestrator: orch,
		ServerState:  serverState,
	}

	vcmServer, err := vcmserver.NewServer(handler)
	if err != nil {
		logger.Error("Failed to create VCM server", "error", err.Error())
		os.Exit(1)
	}

	httpServer := setupHTTPServer(cfg, vcmServer)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		logger.Info("Starting VCM HTTP server on " + cfg.VCMHost + ":" + cfg.VCMPort)
		httpServer.Addr = cfg.VCMHost + ":" + cfg.VCMPort
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("VCM HTTP server error", "error", err.Error())
			return err
		}
		return nil
	})

	handleGracefulShutdown(eg, ctx, cfg, httpServer, serverState, logger)

	if err := eg.Wait(); err != nil {
		logger.Error("Server error", "error", err.Error())
		os.Exit(1)
	}
	logger.Info("VCM server stopped gracefully")
}

func setupHTTPServer(cfg *common.Config, handler http.Handler) *http.Server {
	mux := chi.NewRouter()
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(log.RecoverMiddleware)
	mux.Mount("/", handler)
	mux.Handle("/metrics", promhttp.Handler())

	return &http.Server{
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
}

func handleGracefulShutdown(eg *errgroup.Group, ctx context.Context, cfg *common.Config, httpServer *http.Server, serverState *vcmapi.ServerState, logger log.Logger) {
	eg.Go(func() error {
		<-ctx.Done()
		logger.Info("Received shutdown signal, marking VCM server as not ready")
		serverState.SetShuttingDown()

		logger.Info("Shutting down VCM HTTP server")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer shutdownCancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("Failed to shut down VCM server gracefully", "error", err.Error())
			return err
		}
		logger.Info("VCM HTTP server shut down successfully")
		return nil
	})
}
