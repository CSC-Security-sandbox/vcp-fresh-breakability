package googleproxyclient

import (
	"bytes"
	goerrors "errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"syscall"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

//go:generate go run github.com/ogen-go/ogen/cmd/ogen@latest --clean --package googleproxyclient --config .ogenserver.yml --target . ../../google-proxy/api/gcp-api.yaml

type ContextKey int

const (
	CorrelationContextKey ContextKey = iota
)

var (
	GetGProxyClient                   = getGProxyClient
	addConsumersToTransport           = _addConsumersToTransport
	httpTransport                     http.RoundTripper
	httphelpersGetLoggingRoundTripper = httphelpers.GetLoggingRoundTripper
	transportSchema                   = env.GetString("PROXY_CLIENT_TRANSPORT_SCHEMA", "http")
	apiIdleTimeout                    = env.GetUint("PROXY_CLIENT_API_IDLE_TIMEOUT", 8)
	ApiRetryDelay                     = time.Duration(env.GetInt("PROXY_CLIENT_API_RETRY_DELAY", 5)) * time.Second
	ApiMaxRetries                     = max(1, env.GetInt("PROXY_CLIENT_API_MAX_RETRIES", 10))
	RetryErrors                       = []int{403, 429, 500, 503, 504}
	timeSleep                         = time.Sleep
	ioReadAll                         = io.ReadAll
)

func init() {
	httpTransportClone := http.DefaultTransport.(*http.Transport).Clone()
	if apiIdleTimeout > 0 {
		httpTransportClone.IdleConnTimeout = time.Second * ((time.Duration)(apiIdleTimeout))
	} else {
		httpTransportClone.DisableKeepAlives = true
	}
	httpTransport = httpTransportClone
}

type vcpRoundTripper struct {
	JWT          string
	RoundTripper http.RoundTripper
	retryDelay   time.Duration
	maxRetries   int
	logger       slogger.Logger
}

// RoundTrip is the implementation of the http.RoundTripper interface
func (c *vcpRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()
	shouldSleep := false

	var response *http.Response
	var err error
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, err = ioReadAll(r.Body)
		if err != nil {
			c.logger.With(slogger.Fields{
				"error": err,
			}).ErrorContext(ctx, "Error while reading request body")
			_ = r.Body.Close() // Close body even on error
			return nil, err
		}
		// Close the original request body after reading it, as per http.RoundTripper contract
		if closeErr := r.Body.Close(); closeErr != nil {
			c.logger.With(slogger.Fields{
				"error": closeErr,
			}).WarnContext(ctx, "Error while closing request body")
		}
	}

	for i := 0; i < c.maxRetries; i++ {
		if err = ctx.Err(); err != nil {
			c.logger.With(slogger.Fields{
				"error": err,
			}).WarnContext(ctx, "Context cancelled")
			// Close any existing response from previous attempts before returning
			if response != nil {
				if closeErr := response.Body.Close(); closeErr != nil {
					c.logger.With(slogger.Fields{
						"error": closeErr,
					}).WarnContext(ctx, "Error while closing response body on context cancellation")
				}
			}
			// Return nil response with context error to avoid returning stale response
			return nil, err
		}

		if response != nil {
			err := response.Body.Close()
			if err != nil {
				c.logger.With(slogger.Fields{
					"error": err,
				}).WarnContext(ctx, "Error while closing response body before retrying")
			}
		}

		if shouldSleep {
			timeSleep(c.retryDelay)
			c.logger.WarnContext(ctx, "Retrying request")
		}
		shouldSleep = true

		cloneReq := r.Clone(ctx)
		if r.Body != nil {
			cloneReq.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		}

		// Add headers to the cloned request
		cloneReq.Header.Add("Authorization", c.JWT)
		currCorrId := cloneReq.Header.Get(slogger.RequestCorrelationID)
		if currCorrId == "" {
			if ctxCorrId, ok := ctx.Value(CorrelationContextKey).(string); ok {
				cloneReq.Header.Set(slogger.RequestCorrelationID, ctxCorrId)
			}
		}

		response, err = c.RoundTripper.RoundTrip(cloneReq)
		if err != nil {
			if goerrors.Is(err, syscall.ECONNREFUSED) ||
				goerrors.Is(err, syscall.ETIMEDOUT) ||
				goerrors.Is(err, io.ErrUnexpectedEOF) {
				c.logger.With(slogger.Fields{
					"error":      err,
					"try_number": i + 1,
				}).WarnContext(ctx, "Got an error while calling remote")
				continue
			}
			if neterror, ok := err.(net.Error); ok && neterror.Timeout() {
				c.logger.With(slogger.Fields{
					"try_number": i + 1,
				}).WarnContext(ctx, "Got an timeout while calling remote")
				continue
			}
			break
		}

		if utils.ContainsInt(RetryErrors, response.StatusCode) {
			continue
		}

		// Check content-type for non-204 responses
		// 204 No Content responses don't have content-type, so we allow them
		if response.StatusCode != 204 {
			contentType := response.Header.Get(runtime.HeaderContentType)
			// Parse content-type to handle charset parameters (e.g., "application/json; charset=utf-8")
			mediaType, _, parseErr := mime.ParseMediaType(contentType)
			if parseErr != nil || mediaType != runtime.JSONMime {
				// If content-type is not JSON, retry (this handles cases where server returns unexpected content-type)
				continue
			}
		}

		return response, err
	}

	return response, err
}

// getGProxyClient creates a new Google Proxy client with the specified base path and JWT token
func getGProxyClient(basePath string, jwt string, logger slogger.Logger) *ProxyClient {
	transport := httptransport.New(basePath, "", []string{transportSchema})

	loggingRoundTripper := httphelpersGetLoggingRoundTripper("Google-Proxy", logger, httpTransport)
	rr := &vcpRoundTripper{
		JWT:          jwt,
		RoundTripper: loggingRoundTripper,
		retryDelay:   ApiRetryDelay,
		maxRetries:   ApiMaxRetries,
		logger:       logger,
	}
	transport.Transport = rr
	addConsumersToTransport(transport)

	httpClient := &http.Client{
		Transport: transport.Transport,
	}

	serverURL := fmt.Sprintf("%s://%s", transportSchema, basePath)

	client := new(ProxyClient)
	var err error
	client.Invoker, err = NewClient(serverURL, WithClient(httpClient))
	if err != nil {
		return nil
	}

	return client
}

type ProxyClient struct {
	Invoker Invoker
	Client  Client
}

// _addConsumersToTransport adds custom consumers to the transport
func _addConsumersToTransport(transport *httptransport.Runtime) {
	var consumerBuilder = func(contentType string) runtime.ConsumerFunc {
		return func(r io.Reader, i interface{}) error {
			content, err := io.ReadAll(r)
			if err != nil {
				return err
			}
			return errors.New(fmt.Sprintf("content-type %s ", string(content)))
		}
	}
	transport.Consumers = make(map[string]runtime.Consumer)
	transport.Consumers[runtime.JSONMime] = runtime.JSONConsumer()
	transport.Consumers[runtime.XMLMime] = consumerBuilder(runtime.XMLMime)
	transport.Consumers[runtime.TextMime] = consumerBuilder(runtime.TextMime)
	transport.Consumers[runtime.HTMLMime] = consumerBuilder(runtime.HTMLMime)
	transport.Consumers[runtime.CSVMime] = consumerBuilder(runtime.CSVMime)
	transport.Consumers[runtime.DefaultMime] = consumerBuilder(runtime.DefaultMime)
	transport.Consumers["*/*"] = consumerBuilder("*/*")
}
