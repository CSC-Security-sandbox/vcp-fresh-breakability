package httphelpers

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

var (
	httputilDumpRequestOut = httputil.DumpRequestOut
	httputilDumpResponse   = httputil.DumpResponse
	timeNow                = time.Now
	timeSince              = time.Since
)

type requestResponseLogger struct {
	callerInfo   string
	logger       log.Logger
	roundTripper http.RoundTripper
}

// Supported fields for google cloud logging
// https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry#httprequest
type httpRequest struct {
	// RequestMethod The request method. Examples: "GET", "HEAD", "PUT", "POST".
	RequestMethod string `json:"requestMethod"`
	// RequestUrl The scheme (http, https), the host name, the path and the query portion of the URL that was requested. Example: "http://example.com/some/info?color=red".
	RequestUrl string `json:"requestUrl"`
	// RequestSize The size of the HTTP request message in bytes, including the request headers and the request body.
	RequestSize string `json:"requestSize"`
	// Status The response code indicating the status of response. Examples: 200, 404.
	Status int `json:"status"`
	// ResponseSize The size of the HTTP response message sent back to the client, in bytes, including the response headers and the response body.
	ResponseSize string `json:"responseSize"`
	// UserAgent The user agent sent by the client. Example: "Mozilla/4.0 (compatible; MSIE 6.0; Windows 98; Q312461; .NET CLR 1.0.3705)".
	UserAgent string `json:"userAgent"`
	// RemoteIp The IP address (IPv4 or IPv6) of the client that issued the HTTP request. This field can include port information. Examples: "192.168.1.1", "10.0.0.1:80", "FE80::0202:B3FF:FE1E:8329".
	RemoteIp string `json:"remoteIp,omitempty"`
	// ServerIp The IP address (IPv4 or IPv6) of the origin server that the request was sent to. This field can include port information. Examples: "192.168.1.1", "10.0.0.1:80", "FE80::0202:B3FF:FE1E:8329".
	ServerIp string `json:"serverIp,omitempty"`
	// Referer The referer URL of the request, as defined in HTTP/1.1 Header Field Definitions.
	Referer string `json:"referer,omitempty"`
	// Latency The request processing latency on the server, from the time the request was received until the response was sent. A duration in seconds with up to nine fractional digits, ending with 's'. Example: "3.5s".
	Latency string `json:"latency"`
	// CacheLookup Whether a cache lookup was attempted.
	CacheLookup bool `json:"cacheLookup,omitempty"`
	// CacheHit Whether an entity was served from cache (with or without validation).
	CacheHit bool `json:"cacheHit,omitempty"`
	// CacheValidatedWithOriginServer Whether the response was validated with the origin server before being served from cache. This field is only meaningful if cacheHit is True.
	CacheValidatedWithOriginServer bool `json:"cacheValidatedWithOriginServer,omitempty"`
	// CacheFillBytesA The number of HTTP response bytes inserted into cache. Set only when a cache fill was attempted.
	CacheFillBytesA string `json:"cacheFillBytes,omitempty"`
	// Protocol Protocol used for the request. Examples: "HTTP/1.1", "HTTP/2", "websocket"
	Protocol string `json:"protocol"`
}

func GetLoggingRoundTripper(callerInfo string, logger log.Logger, roundTripper http.RoundTripper) http.RoundTripper {
	return &requestResponseLogger{
		callerInfo:   callerInfo,
		logger:       logger,
		roundTripper: roundTripper,
	}
}

func (c *requestResponseLogger) RoundTrip(r *http.Request) (*http.Response, error) {
	callerInfo := fmt.Sprintf("VCP -> %s", c.callerInfo)
	ctxCallerInfo := r.Context().Value(middleware.CallerInfoContextKey)
	if ctxCallerInfo != nil {
		ctxCallerInfoVal, ok := ctxCallerInfo.(string)
		if ok {
			callerInfo = fmt.Sprintf("%s -> %s", ctxCallerInfoVal, c.callerInfo)
		}
	}

	requestURL := r.RequestURI
	serverIP := ""
	if r.URL != nil {
		requestURL = r.URL.String()
		serverIP = r.URL.Host
		callerInfo = fmt.Sprintf("%s - %s", callerInfo, r.URL.Path)
	}

	req := httpRequest{
		RequestMethod: r.Method,
		RequestUrl:    requestURL,
		UserAgent:     r.UserAgent(),
		ServerIp:      serverIP,
		Protocol:      r.Proto,
	}

	reqDump, err := httputilDumpRequestOut(r, true)
	if err != nil {
		c.logger.With(log.Fields{
			"httpRequest": req,
			"error":       err,
		}).ErrorContext(r.Context(), fmt.Sprintf("%s - Error while reading request body", callerInfo))
		return nil, err
	}
	req.RequestSize = fmt.Sprintf("%v", len(reqDump))

	startTime := timeNow()
	httpResponse, err := c.roundTripper.RoundTrip(r)
	if err != nil {
		c.logger.With(log.Fields{
			"httpRequest": req,
			"requestBody": string(reqDump),
			"error":       err,
		}).ErrorContext(r.Context(), fmt.Sprintf("%s - Error during request", callerInfo))
		return httpResponse, err
	}
	elapsedTime := timeSince(startTime)
	req.Latency = elapsedTime.String()
	req.Status = httpResponse.StatusCode

	responseDump, err := httputilDumpResponse(httpResponse, true)
	if err != nil {
		c.logger.With(log.Fields{
			"httpRequest": req,
			"requestBody": string(reqDump),
			"error":       err,
		}).ErrorContext(r.Context(), fmt.Sprintf("%s - Error while reading response body", callerInfo))
		return nil, err
	}
	req.ResponseSize = fmt.Sprintf("%v", len(responseDump))

	c.logger.With(log.Fields{
		"httpRequest":  req,
		"requestBody":  string(reqDump),
		"responseBody": string(responseDump),
	}).ErrorContext(r.Context(), callerInfo)
	return httpResponse, err
}
