package coreapi

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-openapi/runtime"
	httptransport "github.com/go-openapi/runtime/client"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/errors"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	slogger "github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type ContextKey int

const (
	CorrelationContextKey ContextKey = iota
)

var (
	GetCoreAPIClient                  = getCoreAPIClient
	GetCoreAPIClientWithoutRetry      = getCoreAPIClientWithoutRetry
	addConsumersToTransport           = _addConsumersToTransport
	httpTransport                     http.RoundTripper
	httphelpersGetLoggingRoundTripper = httphelpers.GetLoggingRoundTripper
	httphelpersNewRetryRoundTripper   = httphelpers.NewRetryRoundTripper
	transportSchema                   = env.GetString("CORE_API_CLIENT_TRANSPORT_SCHEMA", "http")
	apiIdleTimeout                    = env.GetUint("CORE_API_CLIENT_API_IDLE_TIMEOUT", 8)
	ApiRetryDelay                     = time.Duration(env.GetInt("CORE_API_CLIENT_API_RETRY_DELAY", 5)) * time.Second
	ApiMaxRetries                     = max(1, env.GetInt("CORE_API_CLIENT_API_MAX_RETRIES", 10))
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
}

// RoundTrip is the implementation of the http.RoundTripper interface
func (c *vcpRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Add("Authorization", c.JWT)
	currCorrId := r.Header.Get(slogger.RequestCorrelationID)
	if currCorrId == "" {
		if ctxCorrId, ok := r.Context().Value(CorrelationContextKey).(string); ok {
			r.Header.Set(slogger.RequestCorrelationID, ctxCorrId)
		}
	}
	return c.RoundTripper.RoundTrip(r)
}

// getCoreAPIClient creates a new Core API client with the specified base path and JWT token
func getCoreAPIClient(basePath string, jwt string, logger slogger.Logger) *CoreAPIClient {
	transport := httptransport.New(basePath, "", []string{transportSchema})

	loggingRoundTripper := httphelpersGetLoggingRoundTripper("Core-API", logger, httpTransport)
	retryRoundTripper := httphelpersNewRetryRoundTripper(ApiRetryDelay, ApiMaxRetries, logger, loggingRoundTripper)
	rr := &vcpRoundTripper{
		JWT:          jwt,
		RoundTripper: retryRoundTripper,
	}
	transport.Transport = rr
	addConsumersToTransport(transport)

	httpClient := &http.Client{
		Transport: transport.Transport,
	}

	serverURL := fmt.Sprintf("%s://%s", transportSchema, basePath)

	client := new(CoreAPIClient)
	var err error
	client.Invoker, err = NewClient(serverURL, WithClient(httpClient))
	if err != nil {
		return nil
	}

	return client
}

// getCoreAPIClientWithoutRetry creates a new Core API client without retry logic
func getCoreAPIClientWithoutRetry(basePath string, jwt string, logger slogger.Logger) *CoreAPIClient {
	transport := httptransport.New(basePath, "", []string{transportSchema})

	// Skip retry round tripper, use logging round tripper directly
	loggingRoundTripper := httphelpersGetLoggingRoundTripper("Core-API", logger, httpTransport)
	rr := &vcpRoundTripper{
		JWT:          jwt,
		RoundTripper: loggingRoundTripper,
	}
	transport.Transport = rr
	addConsumersToTransport(transport)

	httpClient := &http.Client{
		Transport: transport.Transport,
	}

	serverURL := fmt.Sprintf("%s://%s", transportSchema, basePath)

	client := new(CoreAPIClient)
	var err error
	client.Invoker, err = NewClient(serverURL, WithClient(httpClient))
	if err != nil {
		return nil
	}

	return client
}

type CoreAPIClient struct {
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
