package actions

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

// ProcessResponseModification processes response modifications using the action from context
func ProcessResponseModification(resp *http.Response) error {
	logger := log.NewLogger()

	if ctx := resp.Request.Context().Value("ruleContext"); ctx != nil {
		if action, ok := ctx.(RequestProcessor); ok {
			if err := action.ProcessResponse(resp); err != nil {
				logger.Error("Error applying modifications", "error", err)
				return err
			}
		}
	}
	return nil
}
