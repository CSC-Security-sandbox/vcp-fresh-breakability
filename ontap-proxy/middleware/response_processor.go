package middleware

import (
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
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
