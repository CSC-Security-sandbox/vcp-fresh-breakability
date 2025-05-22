package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
)

func TestNewPaginationRoundTripper(t *testing.T) {
	mrt := NewMockRoundTripper(t)
	prt := NewPaginationRoundTripper(mrt)

	assert.IsType(t, &PaginationRoundTripper{}, prt)
	assert.Equal(t, mrt, prt.roundTripper)
}

func TestPaginationRoundTripperRoundTrip(t *testing.T) {
	t.Run("EnsureErrorIsPropagated", func(t *testing.T) {
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)

		eErr := errors.New("expected error")

		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		mrt.On("RoundTrip", req).Return(nil, eErr)

		rsp, err := prt.RoundTrip(req)
		assert.Nil(t, rsp)
		assert.EqualError(t, eErr, err.Error())

		mrt.AssertCalled(t, "RoundTrip", req)
		mrt.AssertExpectations(t)
	})

	t.Run("WhenNoNextLink", func(t *testing.T) {
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)
		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header: map[string][]string{
				"Www-authenticate": {"?", "!"},
				"Some-header-key":  {"Some-header-value"},
				"Some-header-key2": {"Some-header-value-p1", "Some-header-value-p2"},
			},
			Body: io.NopCloser(bytes.NewReader([]byte("this-is\n-the-body \n "))),
		}

		mrt.On("RoundTrip", req).Return(resp, nil)

		rsp, err := prt.RoundTrip(req)
		assert.Equal(t, resp, rsp)
		assert.Nil(t, err)
		mrt.AssertCalled(t, "RoundTrip", req)
		mrt.AssertExpectations(t)
	})
	t.Run("WhenNextLinkOfWrongType", func(t *testing.T) {
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)
		ctx := context.WithValue(context.Background(), NextContextKey, 1)
		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		req = req.WithContext(ctx)
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header: map[string][]string{
				"Www-authenticate": {"?", "!"},
				"Some-header-key":  {"Some-header-value"},
				"Some-header-key2": {"Some-header-value-p1", "Some-header-value-p2"},
			},
			Body: io.NopCloser(bytes.NewReader([]byte("this-is\n-the-body \n "))),
		}
		mrt.On("RoundTrip", req).Return(resp, nil)

		rsp, err := prt.RoundTrip(req)

		assert.Equal(t, resp, rsp)
		assert.Nil(t, err)
		mrt.AssertCalled(t, "RoundTrip", req)
		mrt.AssertExpectations(t)
	})
	t.Run("WhenNextLinkIsAnInvalidURL", func(t *testing.T) {
		parseErr := errors.New("parserrr")
		defer func() { urlParse = url.Parse }()
		urlParse = func(rawURL string) (*url.URL, error) {
			return nil, parseErr
		}
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)
		ctx := context.WithValue(context.Background(), NextContextKey, "invalid")
		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		req = req.WithContext(ctx)

		rsp, err := prt.RoundTrip(req)

		assert.Nil(t, rsp)
		assert.EqualError(t, err, "failed to parse next link from ontap: 'invalid' 'parserrr'")
	})
	t.Run("WhenFailingToParse", func(t *testing.T) {
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)

		ctx := context.WithValue(context.Background(), NextContextKey, "/api/next")

		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		req = req.WithContext(ctx)

		rsp, err := prt.RoundTrip(req)

		assert.Nil(t, rsp)
		assert.Equal(t, errors.New("query is empty in next link from ontap: '/api/next'"), err)
		mrt.AssertExpectations(t)
	})
	t.Run("WhenSucceeds", func(t *testing.T) {
		mrt := NewMockRoundTripper(t)
		prt := NewPaginationRoundTripper(mrt)
		link := "/api/storage/volumes?start.uuid=2b0a1121-3808-4926-aaa4-093fe6e29f70&is_constituent=false"
		ctx := context.WithValue(context.Background(), NextContextKey, link)
		req := &http.Request{
			URL: &url.URL{
				Host:     "best host",
				Path:     "best path",
				RawQuery: "a=b&c=d",
			},
			Header: map[string][]string{
				"Authorization":     {"Basic Secret"},
				"First header key":  {"First header value"},
				"Second header key": {"Second header value first part", "Second header value second part"},
			},
		}
		req = req.WithContext(ctx)
		resp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header: map[string][]string{
				"Www-authenticate": {"?", "!"},
				"Some-header-key":  {"Some-header-value"},
				"Some-header-key2": {"Some-header-value-p1", "Some-header-value-p2"},
			},
			Body: io.NopCloser(bytes.NewReader([]byte("this-is\n-the-body \n "))),
		}
		mrt.On("RoundTrip", req).Return(resp, nil)

		rsp, err := prt.RoundTrip(req)

		assert.Equal(t, resp, rsp)
		assert.Nil(t, err)
		mrt.AssertCalled(t, "RoundTrip", req)
		mrt.AssertExpectations(t)
	})
}
