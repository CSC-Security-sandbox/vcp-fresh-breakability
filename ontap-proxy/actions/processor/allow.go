package processor

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

// Allow is an action that allows requests with optional validation and field manipulation
type Allow struct {
	Name         string
	RequestRule  actions.RequestRule
	ResponseRule actions.ResponseRule
}

func (a *Allow) ShouldAllow(r *http.Request) (bool, error) {
	if len(a.RequestRule.ValidationRules) == 0 {
		return true, nil
	}

	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		return true, nil
	}

	requestBody, err := UnmarshalRequestBody(r)
	if err != nil {
		return false, err
	}

	if err := ApplyValidationRules(requestBody, a.RequestRule.ValidationRules); err != nil {
		return false, err
	}
	return true, nil
}

func (a *Allow) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	if len(a.RequestRule.InjectionRules) == 0 {
		return nil
	}

	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		return nil
	}

	requestBody, err := UnmarshalRequestBody(r)
	if err != nil {
		return err
	}

	for _, rule := range a.RequestRule.InjectionRules {
		injectField(requestBody, rule.FieldPath, rule.Value)
	}

	return MarshalAndRestoreBody(r, w, requestBody)
}

func (a *Allow) ProcessResponse(resp *http.Response) error {
	// If no response rules, do nothing
	if len(a.ResponseRule.InjectionRules) == 0 && len(a.ResponseRule.RemovalRules) == 0 {
		return nil
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return nil
	}

	responseData, err := UnmarshalResponseBody(resp)
	if err != nil {
		return err
	}

	ApplyResponseRules(responseData, a.ResponseRule.InjectionRules, a.ResponseRule.RemovalRules)

	return MarshalAndRestoreResponse(resp, responseData)
}
