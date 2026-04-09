package middleware

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/env"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// ontapClientErrorCodes lists ONTAP error codes that represent client validation, failures but are incorrectly returned with HTTP 500. The proxy rewrites these to 400.
// Loaded from ONTAP_CLIENT_ERROR_CODES env var (comma-separated) during init.
var ontapClientErrorCodes map[string]bool

func init() {
	ontapClientErrorCodes = parseOntapClientErrorCodes(
		env.GetString("ONTAP_CLIENT_ERROR_CODES", ""),
	)
}

// ProcessResponseModification processes response modifications based on the action
// stored in the request context. This is typically called from the reverse proxy's
// ModifyResponse hook.
func ProcessResponseModification(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}

	logger := util.GetLogger(resp.Request.Context())

	ctx := resp.Request.Context().Value(models.RuleContextKey)
	if ctx == nil {
		logger.InfoContext(resp.Request.Context(), "No ruleContext found in response context")
		return nil
	}

	action, ok := ctx.(dsl.IAction)
	if !ok {
		logger.InfoContext(resp.Request.Context(), "Context value is not an IAction", "type", fmt.Sprintf("%T", ctx))
		return nil
	}

	actionName, err := action.ProcessResponse(resp)
	if err != nil {
		logger.ErrorContext(resp.Request.Context(), "Error applying response modifications", "error", err, "action", actionName)
		return err
	}

	logger.InfoContext(resp.Request.Context(), "Successfully processed response", "action", actionName)
	return nil
}

func parseOntapClientErrorCodes(csv string) map[string]bool {
	codes := map[string]bool{}
	for _, code := range strings.Split(csv, ",") {
		code = strings.TrimSpace(code)
		if code != "" {
			codes[code] = true
		}
	}
	return codes
}

// rewriteOntap500ToClientError checks whether an ONTAP 500 response carries a
// structured error body whose code is in ontapClientErrorCodes. If so, the HTTP
// status is rewritten to 400 Bad Request (the body is left intact).
func rewriteOntap500ToClientError(resp *http.Response) {
	if resp.StatusCode != http.StatusInternalServerError {
		return
	}

	if resp.Body == nil {
		return
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	// Original Body is consumed; always give downstream a new reader. Do not drop
	// successfully read bytes when Close fails, and preserve partial reads on ReadAll error.
	resp.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	if err != nil {
		return
	}
	if closeErr != nil {
		logger := util.GetLogger(resp.Request.Context())
		logger.WarnContext(resp.Request.Context(), "rewriteOntap500ToClientError: response body Close failed after full read",
			"error", closeErr, "path", resp.Request.URL.Path)
	}

	var parsed struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bodyBytes, &parsed); err != nil {
		return
	}

	if ontapClientErrorCodes[parsed.Error.Code] {
		logger := util.GetLogger(resp.Request.Context())
		logger.DebugContext(resp.Request.Context(), "Rewriting ONTAP 500 to 400 for client error code",
			"ontapErrorCode", parsed.Error.Code,
			"path", resp.Request.URL.Path)
		resp.StatusCode = http.StatusBadRequest
		resp.Status = "400 Bad Request"
	}
}

// ProcessResponseAndRecordBackendMetrics runs ProcessResponseModification then records backend metrics.
// Used as the reverse proxy's ModifyResponse hook. Metrics are best-effort; we never return an error due to metrics.
func ProcessResponseAndRecordBackendMetrics(resp *http.Response) error {
	err := ProcessResponseModification(resp)

	if resp == nil || resp.Request == nil {
		return err
	}

	rewriteOntap500ToClientError(resp)

	ctx := resp.Request.Context()
	start, ok := GetBackendRequestStartFromContext(ctx)
	if !ok {
		util.GetLogger(ctx).InfoContext(ctx, "Backend metrics skipped: no start time in request context")
		// Do not error out when metrics are broken; return nil so the request still succeeds.
		return nil
	}
	// Record metrics in a best-effort way: do not fail the request if metrics break.
	func() {
		defer func() {
			if r := recover(); r != nil {
				util.GetLogger(ctx).ErrorContext(ctx, "Backend metrics recording panicked; request still succeeded", "panic", r)
			}
		}()
		duration := time.Since(start).Seconds()
		projectID, poolID, path := GetBackendMetricsFromContext(ctx)
		method := resp.Request.Method
		statusCode := resp.StatusCode
		RecordBackendRequest(ctx, method, projectID, poolID, path, statusCode)
		RecordBackendDuration(ctx, duration, method, projectID, poolID, path, statusCode)
		if statusCode >= 400 {
			code := BackendErrorCodeForMetric(statusCode, nil)
			if code != "" {
				RecordBackendError(ctx, method, projectID, poolID, path, code)
			}
		}
	}()
	return err
}
