package main

import (
	"github.com/go-faster/errors"
	api "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/endpoints"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/api/oasserver"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api/util/httpmiddleware"
	"net/http"
	"time"
)

// github.com/vcp-vsa-control-Plane/vsa-control-plane/core-api
func main() {
	//// create a type that satisfies the `api.ServerInterface`, which contains an implementation of every operation from the generated code
	//apiServer := server.New(datastores.NewFireStoreDatastore("sridhar-yalla", "vsa-control-plane"))
	//
	//e := echo.New()
	//
	//api.RegisterHandlers(e, apiServer)
	//
	//// And we serve HTTP until the world ends.
	//log.Fatal(e.Start("0.0.0.0:50051"))

	oasserver, err := oasgenserver.NewServer(api.Handler{})
	if err != nil {
		panic(err)
	}

	routeFinder := httpmiddleware.MakeRouteFinder(oasserver)
	httpServer := http.Server{
		Addr:              "localhost:8080",
		ReadHeaderTimeout: time.Second,
		Handler:           httpmiddleware.Wrap(oasserver, httpmiddleware.LogRequests(routeFinder)),
	}

	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return
	}
}
