package transport

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

const (
	xDotClientAppHeaderKey = "X-Dot-Client-App"
	xDotSvmNameHeaderKey   = "X-Dot-Svm-Name"
	traceParentHeaderKey   = "traceparent"
	traceParentHeaderValue = "00-00000000000000000000000000000000-0000000000000000-00"
	traceStateHeaderKey    = "tracestate"
)

var (
	// MD: FIXME: Source this value from context instead of the environment
	xDotClientAppHeaderValue = env.GetString("ONTAP_REST_X_DOT_CLIENT_APP", "")
	slowThreshold            = time.Second * time.Duration(env.GetUint("ONTAP_REST_SYNC_SLOW_TOLERANCE_SECONDS", 15))

	utilsRandomUUID = utils.RandomUUID
	ioReadAll       = io.ReadAll
	timeNow         = time.Now
)

// NewLoggingRoundTripper creates a new LoggingRoundTripper
func NewLoggingRoundTripper(trace log.Logger, logVerbose, useCert bool, roundTripper http.RoundTripper) *LoggingRoundTripper {
	authStyle := "basic"
	if ontapRestOAuthEnabled {
		authStyle = "oauth"
	} else {
		if useCert {
			authStyle = "cert"
		}
	}

	return &LoggingRoundTripper{
		trace:        trace, // MD: Due to logging requirements and instability of ontap rest this must be hardcoded for now
		logVerbose:   logVerbose,
		roundTripper: roundTripper,
		authStyle:    authStyle,
	}
}

// LoggingRoundTripper logs outgoing and incoming HTTP requests and responses
type LoggingRoundTripper struct {
	trace        log.Logger
	logVerbose   bool
	authStyle    string
	roundTripper http.RoundTripper
}

// RoundTrip performs the round trip for this RoundTripper
func (lrt *LoggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add(xDotClientAppHeaderKey, xDotClientAppHeaderValue)
	req.Header.Add(traceParentHeaderKey, traceParentHeaderValue)
	// FixMe: implementation of trace is pending
	// req.Header.Add(traceStateHeaderKey, lrt.trace.GetRequestID(lrt.trace, ""))

	externalRequestID := utilsRandomUUID()
	params := req.URL.Query()
	requestFields := log.Fields{
		"host":              req.URL.Host,
		"path":              req.URL.Path,
		"headers":           prettify(req.Header),
		"externalRequestID": externalRequestID,
		"method":            req.Method,
		"params":            prettify(params),
		"auth style":        lrt.authStyle,
	}

	if req.Body != nil {
		body, err := ioReadAll(req.Body)
		if err != nil {
			requestFields["err"] = "Failed to read body from request: " + err.Error()
			lrt.trace.With(requestFields).WarnContext(context.TODO(), "ontap-rest error")
			return nil, err
		}

		requestFields["body"] = removeWhiteSpaceButNotBetweenDoubleQuotes(string(body))
		req.Body = io.NopCloser(bytes.NewReader(body))
	}

	if lrt.logVerbose {
		lrt.trace.With(requestFields).InfoContext(context.TODO(), "ontap-rest request")
	}

	t1 := timeNow()
	res, err := lrt.roundTripper.RoundTrip(req)
	if err != nil {
		requestFields["err"] = "lrt.roundTripper.RoundTrip failure: " + err.Error()
		lrt.trace.With(requestFields).WarnContext(context.TODO(), "ontap-rest error")
		return nil, err
	}
	duration := timeNow().Sub(t1)

	body, err := ioReadAll(res.Body)
	if err != nil {
		requestFields["err"] = "Failed to read body from response: " + err.Error()
		lrt.trace.With(requestFields).WarnContext(context.TODO(), "ontap-rest error")
		return nil, err
	}
	defer func() {
		res.Body = io.NopCloser(bytes.NewReader(body))
	}()

	if svmName := req.Header.Get(xDotSvmNameHeaderKey); svmName != "" {
		res.Header.Set(xDotSvmNameHeaderKey, svmName)
	}
	res.Header.Set(xDotClientAppHeaderKey, xDotClientAppHeaderValue)

	content := removeWhiteSpaceButNotBetweenDoubleQuotes(string(body))
	responseFields := log.Fields{
		"host":                  req.URL.Host,
		"path":                  req.URL.Path,
		"headers":               prettify(res.Header),
		"externalRequestID":     externalRequestID,
		"method":                req.Method,
		"http status code":      res.StatusCode,
		"body":                  content,
		"auth style":            lrt.authStyle,
		"duration.Milliseconds": duration.Milliseconds(),
	}

	if duration > slowThreshold && !params.Has("return_timeout") {
		responseFields["slow"] = "true"
	}

	isJobFailure := false
	isJob := strings.HasPrefix(req.URL.Path, "/api/cluster/jobs")
	if isJob {
		isJobFailure = strings.Contains(content, "failure")
	}

	if res.StatusCode >= http.StatusBadRequest || isJobFailure {
		if !lrt.logVerbose {
			// MD: request was not logged out earlier. Force it here
			lrt.trace.With(requestFields).InfoContext(context.TODO(), "ontap-rest request")
		}

		lrt.trace.With(responseFields).WarnContext(context.TODO(), "ontap-rest error")
		return res, nil
	}

	if res.StatusCode == http.StatusAccepted || isJob {
		if !lrt.logVerbose {
			// MD: request was not logged out earlier. Force it here
			lrt.trace.With(requestFields).InfoContext(context.TODO(), "ontap-rest request")
		}

		lrt.trace.With(responseFields).InfoContext(context.TODO(), "ontap-rest response")
		return res, nil
	}

	if lrt.logVerbose {
		lrt.trace.With(responseFields).InfoContext(context.TODO(), "ontap-rest response")
	}

	return res, nil
}

var prettify = _prettify

func _prettify(query map[string][]string) string {
	var sb strings.Builder

	var index []string
	for i := range query {
		index = append(index, i)
	}
	sort.Strings(index)

	for i := range index {
		lower := strings.ToLower(index[i])
		if lower == "www-authenticate" || lower == "authorization" || lower == "traceparent" || lower == "tracestate" {
			continue
		}

		_, _ = sb.WriteString(index[i])
		_, _ = sb.WriteString("=")

		v := query[index[i]]
		if len(v) != 0 {
			if len(v) == 1 {
				_, _ = sb.WriteString(v[0])
			} else {
				_, _ = sb.WriteString("[")
				_, _ = sb.WriteString(strings.Join(v, ","))
				_, _ = sb.WriteString("]")
			}
		}

		_, _ = sb.WriteString(",")
	}

	return strings.TrimSuffix(sb.String(), ",")
}

var removeWhiteSpaceButNotBetweenDoubleQuotes = _removeWhiteSpaceButNotBetweenDoubleQuotes

func _removeWhiteSpaceButNotBetweenDoubleQuotes(body string) string {
	rs := make([]rune, 0, len(body))
	betweenQuotes := false

	for _, r := range body {
		if r == '"' {
			betweenQuotes = !betweenQuotes
		}
		if !betweenQuotes && (r == ' ' || r == '\n' || r == '\t') {
			continue
		}
		rs = append(rs, r)
	}

	return string(rs)
}
