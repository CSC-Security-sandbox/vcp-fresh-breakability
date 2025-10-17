package actions

import (
	"fmt"
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// ProcessResponseModification processes response modifications using the action from context
func ProcessResponseModification(resp *http.Response) error {
	logger := log.NewLogger()

	if resp.Request == nil {
		logger.Info("Response request is nil, skipping processing")
		return nil
	}

	if ctx := resp.Request.Context().Value("ruleContext"); ctx != nil {
		if action, ok := ctx.(RequestProcessor); ok {
			logger.Info("Processing response with action", "action", action)
			if err := action.ProcessResponse(resp); err != nil {
				logger.Error("Error applying modifications", "error", err)
				return err
			}
			logger.Info("Successfully processed response")
		} else {
			logger.Info("Context value is not a RequestProcessor", "type", fmt.Sprintf("%T", ctx))
		}
	} else {
		logger.Info("No ruleContext found in response context")
	}
	return nil
}
