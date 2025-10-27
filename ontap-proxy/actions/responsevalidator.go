package actions

import (
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// ProcessResponseModification processes response modifications using the action from context
func ProcessResponseModification(resp *http.Response) error {
	if resp == nil || resp.Request == nil {
		return nil
	}

	logger := util.GetLogger(resp.Request.Context())

	if ctx := resp.Request.Context().Value(models.RuleContextKey); ctx != nil {
		if action, ok := ctx.(RequestProcessor); ok {
			logger.InfoContext(resp.Request.Context(), "Processing response with action", "action", action)
			if err := action.ProcessResponse(resp); err != nil {
				logger.ErrorContext(resp.Request.Context(), "Error applying modifications", "error", err)
				return err
			}
			logger.InfoContext(resp.Request.Context(), "Successfully processed response")
		} else {
			logger.InfoContext(resp.Request.Context(), "Context value is not a RequestProcessor", "type", fmt.Sprintf("%T", ctx))
		}
	} else {
		logger.InfoContext(resp.Request.Context(), "No ruleContext found in response context")
	}
	return nil
}
