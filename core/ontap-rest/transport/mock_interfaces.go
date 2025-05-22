package transport

import (
	"net/http"

	"github.com/go-openapi/runtime"
)

// RoundTripper Equivalent interface to http.RoundTripper. Used to generate mocks for test
type RoundTripper interface {
	RoundTrip(req *http.Request) (*http.Response, error)
}

// ClientTransport Equivalent interface to runtime.ClientTransport. Used to generate mocks for test
type ClientTransport interface {
	Submit(operation *runtime.ClientOperation) (interface{}, error)
}

// ClientResponseReader Equivalent interface to runtime.ClientResponseReader. Used to generate mocks for test
type ClientResponseReader interface {
	ReadResponse(rsp runtime.ClientResponse, cns runtime.Consumer) (interface{}, error)
}
