package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func main() {
	logger := log.NewLogger()
	logger.Info("ONTAP Proxy Server starting...")

	handler := setupHTTPServer()
	port := getPort()
	logger.Info("Server starting", "port", port, "url", fmt.Sprintf("http://localhost:%s", port))

	err := http.ListenAndServe(":"+port, handler)
	if err != nil {
		logger := log.NewLogger()
		logger.Error("Failed to start server", "error", err)
		os.Exit(1)
	}
}

func setupHTTPServer() http.Handler {
	mux := chi.NewRouter()

	mux.Use(chimiddleware.Logger)
	mux.Use(chimiddleware.Recoverer)
	mux.Use(chimiddleware.RequestID)
	mux.Use(middleware.AuthMiddleware)

	ontapProxy := BuildOntapRESTProxy()

	mux.Route("/v1beta/projects/{projectId}/locations/{locationId}/pools/{poolId}/ontap-api", func(r chi.Router) {
		r.Use(middleware.RuleEngineMiddleware())
		r.Handle("/*", ontapProxy)
	})

	return mux
}

func getPort() string {
	port := env.GetString("PORT", "8080")
	return port
}
