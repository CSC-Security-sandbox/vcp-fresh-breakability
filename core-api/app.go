package main

import (
	"log"

	"github.com/labstack/echo/v4"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/datastores"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/server"
)

// github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api
func main() {
	// create a type that satisfies the `api.ServerInterface`, which contains an implementation of every operation from the generated code
	apiServer := server.New(datastores.NewFireStoreDatastore("sridhar-yalla", "vsa-control-plane"))

	e := echo.New()

	api.RegisterHandlers(e, apiServer)

	// And we serve HTTP until the world ends.
	log.Fatal(e.Start("0.0.0.0:50051"))
}
