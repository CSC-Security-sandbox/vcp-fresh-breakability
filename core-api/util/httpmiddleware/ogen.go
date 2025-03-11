package httpmiddleware

import (
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"
	"net/http"
	"net/url"
)

// Middleware is a net/http middleware.
type Middleware = func(http.Handler) http.Handler

// Server is a generic ogen server type.
type Server[R Route] interface {
	FindPath(method string, u *url.URL) (r R, _ bool)
}

// Route is a generic ogen route type.
type Route interface {
	Name() string
	OperationID() string
	PathPattern() string
}

// RouteFinder finds Route by given URL.
type RouteFinder func(method string, u *url.URL) (Route, bool)

// MakeRouteFinder creates RouteFinder from given server.
func MakeRouteFinder[R Route, S Server[R]](server S) RouteFinder {
	return func(method string, u *url.URL) (Route, bool) {
		return server.FindPath(method, u)
	}
}

// LogRequests logs incoming requests using context logger.
func LogRequests(find RouteFinder) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			lg := zctx.From(ctx)
			var (
				opName = zap.Skip()
				opID   = zap.Skip()
			)
			if route, ok := find(r.Method, r.URL); ok {
				opName = zap.String("operationName", route.Name())
				opID = zap.String("operationId", route.OperationID())
			}
			lg.Info("Got request",
				zap.String("method", r.Method),
				zap.Stringer("url", r.URL),
				opID,
				opName,
			)
			next.ServeHTTP(w, r)
		})
	}
}

// Wrap handler using given middlewares.
func Wrap(h http.Handler, middlewares ...Middleware) http.Handler {
	switch len(middlewares) {
	case 0:
		return h
	case 1:
		return middlewares[0](h)
	default:
		for i := len(middlewares) - 1; i >= 0; i-- {
			h = middlewares[i](h)
		}
		return h
	}
}
