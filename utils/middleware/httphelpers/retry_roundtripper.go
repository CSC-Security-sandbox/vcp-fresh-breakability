package httphelpers

import (
	"bytes"
	goerrors "errors"
	"io"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	timeSleep = time.Sleep
	ioReadAll = io.ReadAll
	// retryableStatusCodes contains HTTP response codes that should trigger a retry.
	// Update this list when new transient scenarios need to be retried.
	retryableStatusCodes = map[int]struct{}{
		http.StatusInternalServerError: {},
		http.StatusBadGateway:          {},
		http.StatusServiceUnavailable:  {},
		http.StatusGatewayTimeout:      {},
	}
)

type retryRoundTripper struct {
	retryDelay   time.Duration
	maxRetries   int
	logger       log.Logger
	roundTripper http.RoundTripper
}

// RoundTrip is the implementation of the http.RoundTripper interface
func (c *retryRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()
	shouldSleep := false

	var response *http.Response
	var err error
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, err = ioReadAll(r.Body)
		if err != nil {
			c.logger.With(log.Fields{
				"error": err,
			}).ErrorContext(ctx, "Error while reading request body")
			return nil, err
		}
	}
	for i := 0; i < c.maxRetries; i++ {
		if err = ctx.Err(); err != nil {
			c.logger.With(log.Fields{
				"error": err,
			}).WarnContext(ctx, "Context cancelled")
			break
		}

		if response != nil {
			err := response.Body.Close()
			if err != nil {
				c.logger.With(log.Fields{
					"error": err,
				}).WarnContext(ctx, "Error while closing response body before retrying")
			}
		}
		if shouldSleep {
			timeSleep(c.retryDelay)
			url := ""
			if r.URL != nil {
				url = r.URL.RequestURI()
			}
			c.logger.With(log.Fields{
				"method": r.Method,
				"url":    url,
			}).WarnContext(ctx, "Retrying server call")
		}
		shouldSleep = true
		cloneReq := r.Clone(ctx)
		if r.Body != nil {
			cloneReq.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		response, err = c.roundTripper.RoundTrip(cloneReq)
		if err != nil {
			if goerrors.Is(err, syscall.ECONNREFUSED) ||
				goerrors.Is(err, syscall.ETIMEDOUT) ||
				goerrors.Is(err, io.ErrUnexpectedEOF) {
				c.logger.With(log.Fields{
					"error":      err,
					"try_number": i + 1,
				}).WarnContext(ctx, "Got an error while calling server")
				continue
			}
			if neterror, ok := err.(net.Error); ok && neterror.Timeout() {
				c.logger.With(log.Fields{
					"try_number": i + 1,
				}).WarnContext(ctx, "Got an timeout while calling server")
				continue
			}
			break
		}
		if shouldRetryResponse(response) {
			if response.Body != nil {
				if errClose := response.Body.Close(); errClose != nil {
					c.logger.With(log.Fields{
						"error": errClose,
					}).WarnContext(ctx, "Error while closing response body for retry")
				}
			}
			c.logger.With(log.Fields{
				"status_code": response.StatusCode,
				"try_number":  i + 1,
			}).WarnContext(ctx, "Retrying server call due to response status code")
			response = nil
			continue
		}
		return response, err
	}
	return response, err
}

func shouldRetryResponse(response *http.Response) bool {
	if response == nil {
		return false
	}
	_, ok := retryableStatusCodes[response.StatusCode]
	return ok
}

func NewRetryRoundTripper(retryDelay time.Duration, maxRetries int, logger log.Logger, nextRoundTripper http.RoundTripper) http.RoundTripper {
	return &retryRoundTripper{
		retryDelay:   retryDelay,
		maxRetries:   maxRetries,
		logger:       logger,
		roundTripper: nextRoundTripper,
	}
}
