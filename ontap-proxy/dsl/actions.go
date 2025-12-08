package dsl

import (
	"net/http"
)

// ResolveAction evaluates any action and returns the leaf action to use.
// For When actions, this evaluates the condition once and returns the resolved branch.
// For other actions (Allow, Deny, etc.), returns them as-is.
// Returns: (resolvedAction, allowed, reason)
func ResolveAction(action IAction, r *http.Request) (IAction, bool, string) {
	if action == nil {
		return nil, false, "No action defined"
	}

	// If it's a When, use its Resolve method
	if when, ok := action.(When); ok {
		return when.Resolve(r)
	}

	// For all other actions, check ShouldAllow and return as-is
	allowed, reason := action.ShouldAllow(r)
	if !allowed {
		return nil, false, reason
	}
	return action, true, ""
}

// Allow is an action that permits requests and optionally applies modifications.
type Allow struct {
	Name           string       // Human-readable name for logging
	ModifyRequest  Modification // Optional request modification
	ModifyResponse Modification // Optional response modification
}

func (a Allow) ShouldAllow(r *http.Request) (bool, string) {
	return true, ""
}

func (a Allow) ProcessRequest(r *http.Request, w http.ResponseWriter) (string, error) {
	if a.ModifyRequest == nil {
		return a.Name, nil
	}

	if err := a.ModifyRequest.Apply(r); err != nil {
		return a.Name, err
	}

	return a.Name, nil
}

func (a Allow) ProcessResponse(resp *http.Response) (string, error) {
	if a.ModifyResponse == nil {
		return a.Name, nil
	}

	if err := a.ModifyResponse.Apply(resp); err != nil {
		return a.Name, err
	}

	return a.Name, nil
}

// AllowAll is a simple action that permits requests without any modifications.
// Use this when you want to allow a request to pass through unchanged.
type AllowAll struct{}

func (a AllowAll) ShouldAllow(r *http.Request) (bool, string) {
	return true, ""
}

func (a AllowAll) ProcessRequest(r *http.Request, w http.ResponseWriter) (string, error) {
	return "AllowAll", nil
}

func (a AllowAll) ProcessResponse(resp *http.Response) (string, error) {
	return "AllowAll", nil
}

// Deny is an action that blocks requests with HTTP 400 Bad Request.
type Deny struct {
	Name string // Human-readable name for logging
}

func (d Deny) ShouldAllow(r *http.Request) (bool, string) {
	return false, d.Name
}

func (d Deny) ProcessRequest(r *http.Request, w http.ResponseWriter) (string, error) {
	// Never reached since ShouldAllow returns false
	return d.Name, nil
}

func (d Deny) ProcessResponse(resp *http.Response) (string, error) {
	// Never reached since requests are blocked
	return d.Name, nil
}

// DenyAll is a shorthand for Deny{} that blocks all requests.
type DenyAll struct{}

func (d DenyAll) ShouldAllow(r *http.Request) (bool, string) {
	return false, "Access denied"
}

func (d DenyAll) ProcessRequest(r *http.Request, w http.ResponseWriter) (string, error) {
	return "DenyAll", nil
}

func (d DenyAll) ProcessResponse(resp *http.Response) (string, error) {
	return "DenyAll", nil
}

// When is a conditional action that branches based on a Condition.
// The Condition returns (bool, string) - if it fails, the reason is used directly.
// If IsTrue/IsFalse actions are provided, they are executed based on the condition result.
//
// IMPORTANT: Use Resolve() to evaluate the condition once and get the resolved action.
// The resolved action should be stored in context for ProcessRequest/ProcessResponse.
type When struct {
	Name      string    // Human-readable name for logging
	Condition Condition // Condition to evaluate - returns (bool, reason)
	IsTrue    IAction   // Action if condition is true (optional)
	IsFalse   IAction   // Action if condition is false (optional - uses condition's reason if not set)
}

// Resolve evaluates the condition and returns the resolved action and allow status.
// This should be called once, and the resolved action stored for later use.
// Returns: (resolvedAction, allowed, reason)
// - If allowed, resolvedAction is the action to use for ProcessRequest/ProcessResponse
// - If not allowed, reason explains why
func (w When) Resolve(r *http.Request) (IAction, bool, string) {
	if w.Condition == nil {
		return nil, false, "No condition defined"
	}

	ok, reason := w.Condition(r)
	if ok {
		if w.IsTrue != nil {
			// Recursively resolve if IsTrue is also a When
			if nested, isWhen := w.IsTrue.(When); isWhen {
				return nested.Resolve(r)
			}
			// Check if the resolved action allows
			allowed, allowReason := w.IsTrue.ShouldAllow(r)
			if !allowed {
				return nil, false, allowReason
			}
			return w.IsTrue, true, ""
		}
		// No IsTrue action, use implicit AllowAll
		return AllowAll{}, true, ""
	}

	// Condition failed
	if w.IsFalse != nil {
		// Recursively resolve if IsFalse is also a When
		if nested, isWhen := w.IsFalse.(When); isWhen {
			return nested.Resolve(r)
		}
		// Check if the resolved action allows
		allowed, allowReason := w.IsFalse.ShouldAllow(r)
		if !allowed {
			return nil, false, allowReason
		}
		return w.IsFalse, true, ""
	}
	return nil, false, reason // Use the condition's reason directly
}

func (w When) ShouldAllow(r *http.Request) (bool, string) {
	_, allowed, reason := w.Resolve(r)
	return allowed, reason
}

// ProcessRequest delegates to the resolved action.
// Note: Prefer using Resolve() once and calling ProcessRequest on the resolved action directly.
func (w When) ProcessRequest(r *http.Request, w2 http.ResponseWriter) (string, error) {
	resolved, allowed, _ := w.Resolve(r)
	if !allowed || resolved == nil {
		return w.Name, nil
	}
	return resolved.ProcessRequest(r, w2)
}

// ProcessResponse delegates to the resolved action.
// Note: Prefer using Resolve() once and calling ProcessResponse on the resolved action directly.
func (w When) ProcessResponse(resp *http.Response) (string, error) {
	if resp.Request == nil {
		return w.Name, nil
	}
	resolved, allowed, _ := w.Resolve(resp.Request)
	if !allowed || resolved == nil {
		return w.Name, nil
	}
	return resolved.ProcessResponse(resp)
}
