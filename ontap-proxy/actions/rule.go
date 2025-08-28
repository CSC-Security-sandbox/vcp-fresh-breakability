package actions

import "net/http"

// Rule maps HTTP methods to actions
type Rule struct {
	GET    IAction
	POST   IAction
	PATCH  IAction
	DELETE IAction
}

// GetAction returns the appropriate action for the HTTP method
func (rule Rule) GetAction(r *http.Request) IAction {
	switch r.Method {
	case http.MethodGet:
		return rule.GET
	case http.MethodPost:
		return rule.POST
	case http.MethodPatch:
		return rule.PATCH
	case http.MethodDelete:
		return rule.DELETE
	default:
		return nil
	}
}
