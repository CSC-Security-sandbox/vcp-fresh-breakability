package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/endpoints"
	coreapiserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/api/telemetry-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.WithValue(context.Background(), middleware.CorrelationContextKey, uuid.NewString())

	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	gcpServer, err := coreapiserver.NewServer(api.Handler{})
	if err != nil {
		os.Exit(1)
	}
	errorFilePath := "/errors.json"
	if _, err := os.Stat(errorFilePath); err == nil {
		if err != nil {
			os.Exit(1)
		}
	}

	mux := chi.NewRouter()
	mux.Use(chimiddleware.Recoverer)

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

	if err := eg.Wait(); err != nil {
		os.Exit(1)
	}
}
