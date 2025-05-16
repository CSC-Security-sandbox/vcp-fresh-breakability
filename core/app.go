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
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/common"
	coregenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/core-servergen"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/handler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"golang.org/x/sync/errgroup"
)

// github.com/vcp-vsa-control-Plane/vsa-control-plane/core
func main() {
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, uuid.NewString())

	// Use signal.NotifyContext to handle termination signals
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.NewLogger()
	logger.Info("Starting VCP Core API")

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

	oasserver, err := coregenserver.NewServer(api.Handler{})
	if err != nil {
		logger.Error("Failed to create server", "error", err.Error())
		os.Exit(1)
	}

	// Setup HTTP router
	mux := chi.NewRouter()
	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(chimiddleware.Recoverer)

	// Mount the generated API handler
	mux.Mount("/", http.Handler(oasserver))

	cfg := common.LoadConfig()
	// Setup HTTP server with proper timeouts
	httpServer := &http.Server{
		Addr:              "localhost:" + cfg.CorePort,
		Handler:           mux,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	// Use errgroup to manage goroutines and context
	eg, ctx := errgroup.WithContext(ctx)

	// Start HTTP server
	eg.Go(func() error {
		logger.Info("Starting HTTP server on localhost:" + cfg.CorePort)
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
