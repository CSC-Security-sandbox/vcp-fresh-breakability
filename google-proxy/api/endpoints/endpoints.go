package api

import (
	coreapi "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/core-api/handler"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
)

type Handler struct {
	oasgenserver.UnimplementedHandler
	CoreHandler *coreapi.Handler
}

func NewHandler(coreHandler *coreapi.Handler) *Handler {
	return &Handler{
		CoreHandler: coreHandler,
	}
}
