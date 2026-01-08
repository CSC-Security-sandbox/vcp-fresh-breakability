package dsl

import "net/http"

// IAction defines the interface for processing HTTP requests and responses.
// This interface is the core abstraction for the rule engine DSL.
type IAction interface {
	// ShouldAllow determines if the request should be permitted.
	// Returns (true, "") if allowed, or (false, "reason") if denied.
	// The reason is used to provide detailed error messages to the client.
	ShouldAllow(r *http.Request) (allowed bool, reason string)

	// ProcessRequest applies modifications to the request before forwarding.
	// The actionName return value is used for logging/debugging.
	ProcessRequest(r *http.Request, w http.ResponseWriter) (actionName string, err error)

	// ProcessResponse applies modifications to the response before returning to client.
	// The actionName return value is used for logging/debugging.
	ProcessResponse(resp *http.Response) (actionName string, err error)
}

// Rule maps HTTP methods to actions.
// Each field corresponds to an HTTP method and contains the action to execute.
type Rule struct {
	GET    IAction
	POST   IAction
	PUT    IAction
	PATCH  IAction
	DELETE IAction
	HEAD   IAction
}

// GetAction returns the appropriate action for the HTTP method.
// Returns nil if the method is not supported or if the request is nil.
func (rule Rule) GetAction(r *http.Request) IAction {
	if r == nil {
		return nil
	}

	switch r.Method {
	case http.MethodGet:
		return rule.GET
	case http.MethodPost:
		return rule.POST
	case http.MethodPut:
		return rule.PUT
	case http.MethodPatch:
		return rule.PATCH
	case http.MethodDelete:
		return rule.DELETE
	case http.MethodHead:
		return rule.HEAD
	default:
		return nil
	}
}
