package middleware

import (
	"fmt"
	"net/http"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

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

// ProcessResponseAndRecordBackendMetrics runs ProcessResponseModification then records backend metrics.
// Used as the reverse proxy's ModifyResponse hook. Metrics are best-effort; we never return an error due to metrics.
func ProcessResponseAndRecordBackendMetrics(resp *http.Response) error {
	err := ProcessResponseModification(resp)
	if resp == nil || resp.Request == nil {
		return err
	}
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
