package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/util/middleware"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-faster/errors"
	"github.com/google/uuid"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/common"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/endpoints"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/servergen"
	"golang.org/x/sync/errgroup"
)

// github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api
func main() {
	ctx := context.WithValue(context.Background(), common.CorrelationContextKey, uuid.NewString())
	ctx, cancelFunc := context.WithCancel(ctx)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger.Info("Starting VCP Core API")

	oasserver, err := oasgenserver.NewServer(api.Handler{})
	if err != nil {
		panic(err)
	}

	mux := chi.NewRouter()
	mux.Use(chimiddleware.Logger)
	mux.Use(middleware.CustomLoggingMiddleware)
	mux.Use(chimiddleware.Recoverer)
	//mux.Use(middleware.Auth)
	mux.Mount("/", http.Handler(oasserver))

	//routeFinder := httpmiddleware.MakeRouteFinder(oasserver)
	httpServer := http.Server{
		Addr:              "localhost:8080",
		ReadHeaderTimeout: time.Second,
		Handler:           mux,
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	eg := &errgroup.Group{}
	eg.Go(func() error {
		select {
		case <-signalChan:
			cancelFunc()
			return nil
		case <-ctx.Done():
			return nil
		}
	})

	eg.Go(func() error {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Error in starting server", slog.Any("err", err))
			return err
		}
		cancelFunc()
		return err
	})

	err = eg.Wait()
	if err != nil {

		logger.Error("Error in server", slog.Any("err", err))
		os.Exit(-1)
		return
	}

	os.Exit(1)
}
