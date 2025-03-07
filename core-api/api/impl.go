package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// ensure that we've conformed to the `ServerInterface` with a compile-time check
var _ ServerInterface = (*Server)(nil)

type Server struct{}

func (s Server) GetV1Pools(ctx echo.Context) error {
	resp := "Hello"
	return ctx.JSON(http.StatusOK, resp)
}

func NewServer() Server {
	return Server{}
}
