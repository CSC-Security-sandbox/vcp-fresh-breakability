package processor

import (
	"net/http"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/actions"
)

// VolumeAction implements RequestProcessor for volume operations
type VolumeAction struct {
	Name         string
	RequestRule  actions.RequestRule
	ResponseRule actions.ResponseRule
}

func (v *VolumeAction) ShouldAllow(r *http.Request) (bool, error) {
	if r.Method == http.MethodGet {
		return true, nil
	}

	if r.Method == http.MethodDelete {
		// TODO : Call Reconciliation API
		return true, nil
	}

	requestBody, err := UnmarshalRequestBody(r)
	if err != nil {
		return false, err
	}

	if err := ApplyValidationRules(requestBody, v.RequestRule.ValidationRules); err != nil {
		return false, err
	}
	// TODO : Call Reconciliation API
	return true, nil
}

func (v *VolumeAction) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		return nil
	}

	requestBody, err := UnmarshalRequestBody(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return err
	}

	// Apply injection rules
	for _, rule := range v.RequestRule.InjectionRules {
		injectField(requestBody, rule.FieldPath, rule.Value)
	}

	return MarshalAndRestoreBody(r, w, requestBody)
}

func (v *VolumeAction) ProcessResponse(resp *http.Response) error {
	if len(v.ResponseRule.InjectionRules) == 0 && len(v.ResponseRule.RemovalRules) == 0 {
		return nil
	}

	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return nil
	}

	responseData, err := UnmarshalResponseBody(resp)
	if err != nil {
		return err
	}

	ApplyResponseRules(responseData, v.ResponseRule.InjectionRules, v.ResponseRule.RemovalRules)

	return MarshalAndRestoreResponse(resp, responseData)
}
