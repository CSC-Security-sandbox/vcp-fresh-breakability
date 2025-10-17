package processor

import (
	"net/http"
)

// Deny is an action that denies all requests (ShouldAllow always returns false)
type Deny struct {
	Name string
}

func (d *Deny) ShouldAllow(r *http.Request) (bool, error) {
	return false, nil // Always deny
}

func (d *Deny) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	// Never reached since ShouldAllow returns false
	return nil
}

func (d *Deny) ProcessResponse(resp *http.Response) error {
	// Never reached since requests are blocked
	return nil
}
