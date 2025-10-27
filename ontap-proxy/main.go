package main

import (
	"context"
	"errors"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func main() {
	// Use signal.NotifyContext to handle termination signals
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.NewLogger()
	logger.Info("Starting ONTAP Proxy Service")

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

	handler := setupHTTPServer()
	port := getPort()
	httpServer := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
	}

	// Start HTTP server in a goroutine for graceful shutdown
	go func() {
		logger.Info("Starting HTTP server on " + port)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Failed to start HTTP server", "error", err.Error())
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()
	logger.Info("Shutting down server...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server shutdown error", "error", err.Error())
	}

	logger.Info("Server stopped gracefully")
}

func setupHTTPServer() http.Handler {
	mux := chi.NewRouter()

	mux.Use(httphelpers.LoggingHttpHandler)
	mux.Use(log.LoggingMiddleware)
	mux.Use(log.RecoverMiddleware)
	mux.Use(middleware.AuthMiddleware)

	ontapProxy := BuildOntapRESTProxy()

	mux.Route("/v1beta/projects/{projectId}/locations/{locationId}/pools/{poolId}/ontap-api", func(r chi.Router) {
		r.Use(middleware.CredentialMiddleware())
		r.Use(middleware.RuleEngineMiddleware())
		r.Use(middleware.CertificateMiddleware())
		r.Handle("/*", ontapProxy)
	})

	return mux
}

func getPort() string {
	port := env.GetString("PORT", "8080")
	return port
}
