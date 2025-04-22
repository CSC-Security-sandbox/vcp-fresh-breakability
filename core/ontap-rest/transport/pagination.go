package transport

import (
	"net/http"
	"net/url"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

type paginationContextKey struct{}

var (
	// NextContextKey is the key to use when fetching the next URL value stored in  context
	NextContextKey paginationContextKey = struct{}{}
	urlParse                            = url.Parse
)

// NewPaginationRoundTripper creates a new PaginationRoundTripper
func NewPaginationRoundTripper(roundTripper http.RoundTripper) *PaginationRoundTripper {
	return &PaginationRoundTripper{roundTripper: roundTripper}
}

// PaginationRoundTripper has pagination superpowers
type PaginationRoundTripper struct {
	roundTripper http.RoundTripper
}

// RoundTrip performs the round trip for this RoundTripper
func (prt *PaginationRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	next, ok := req.Context().Value(NextContextKey).(string)
	if ok && next != "" {
		u, err := urlParse(next)
		if err != nil {
			return nil, errors.Errorf("failed to parse next link from ontap: '%s' '%s'", next, err.Error())
		}

		if u.RawQuery == "" {
			return nil, errors.Errorf("query is empty in next link from ontap: '%s'", next)
		}

		req.URL.RawQuery = u.RawQuery
	}

	return prt.roundTripper.RoundTrip(req)
}
