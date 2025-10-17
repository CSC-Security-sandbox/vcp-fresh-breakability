package actions

import "net/http"

// RequestProcessor defines the interface for processing HTTP requests and responses
type RequestProcessor interface {
	ShouldAllow(r *http.Request) (bool, error)
	ProcessRequest(r *http.Request, w http.ResponseWriter) error
	ProcessResponse(resp *http.Response) error
}

// Rule maps HTTP methods to actions
type Rule struct {
	GET    RequestProcessor
	POST   RequestProcessor
	PATCH  RequestProcessor
	DELETE RequestProcessor
}

// GetAction returns the appropriate action for the HTTP method
func (rule Rule) GetAction(r *http.Request) RequestProcessor {
	if r == nil {
		return nil
	}

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

// ValidationRule defines a validation rule for a field
type ValidationRule struct {
	FieldPath string        // JSON path to the field (e.g., "size", "guarantee.type")
	Required  bool          // Whether the field is required
	MinValue  interface{}   // Minimum value for numeric fields
	MaxValue  interface{}   // Maximum value for numeric fields
	Values    []interface{} // Allowed values for the field
}

// InjectionRule defines a field injection rule
type InjectionRule struct {
	FieldPath string      // JSON path to the field
	Value     interface{} // Value to inject
}

// RemovalRule defines a field removal rule for response
type RemovalRule struct {
	FieldPath string // JSON path to the field to remove
}

// RequestRule contains rules for request processing
type RequestRule struct {
	ValidationRules []ValidationRule // Fields to validate
	InjectionRules  []InjectionRule  // Fields to inject
}

// ResponseRule contains rules for response processing
type ResponseRule struct {
	InjectionRules []InjectionRule // Fields to inject in response
	RemovalRules   []RemovalRule   // Fields to remove from response
}
