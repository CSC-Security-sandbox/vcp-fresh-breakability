package cvp

import (
	"net/http"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/clients/cvp/cvpapi"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/httphelpers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	CVP_HOST         = env.GetString("CVP_HOST", "")
	apiIdleTimeout   = env.GetUint("API_CVP_IDLE_TIMEOUT", 8)
	ApiCvpRetryDelay = time.Duration(env.GetInt("API_CVP_RETRY_DELAY", 5)) * time.Second
	ApiCvpMaxRetries = max(1, env.GetInt("API_CVP_MAX_RETRIES", 10))
	httpTransport    http.RoundTripper
)

// SetCVPHost updates the CVP_HOST value at runtime (mainly for testing)
func SetCVPHost(host string) {
	CVP_HOST = host
}

type cvpRoundTripper struct {
	jwt    string
	logger log.Logger
	rt     http.RoundTripper
}

// RoundTrip is the implementation of the http.RoundTripper interface
func (c *cvpRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Add("Authorization", c.jwt)
	currCorrId := r.Header.Get(string(middleware.CorrelationIDName))
	if currCorrId == "" {
		if ctxCorrId, ok := r.Context().Value(middleware.CorrelationContextKey).(string); ok {
			r.Header.Set(string(middleware.CorrelationIDName), ctxCorrId)
		}
	}
	return c.rt.RoundTrip(r)
}

func init() {
	httpTransportClone := http.DefaultTransport.(*http.Transport).Clone()
	if apiIdleTimeout > 0 {
		httpTransportClone.IdleConnTimeout = time.Second * ((time.Duration)(apiIdleTimeout))
	} else {
		httpTransportClone.DisableKeepAlives = true
	}
	httpTransport = httpTransportClone
}

// CreateClient creates a new client to the CVP
func CreateClient(logger log.Logger, JWT string) cvpapi.Cvp {
	transport := httptransport.New(CVP_HOST, cvpapi.DefaultBasePath, []string{"http"})
	loggingRoundTripper := httphelpers.GetLoggingRoundTripper("CVP", logger, httpTransport)
	retryRoundTripper := httphelpers.NewRetryRoundTripper(ApiCvpRetryDelay, ApiCvpMaxRetries, logger, loggingRoundTripper)
	transport.Transport = &cvpRoundTripper{
		jwt:    JWT,
		logger: logger,
		rt:     retryRoundTripper,
	}
	return *cvpapi.New(transport, strfmt.Default)
}
