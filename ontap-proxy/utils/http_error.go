package utils

import "net/http"

// HTTPError carries an HTTP status and message for errors.As from Core credential fetches,
// DNS reconcile, and transport paths so callers can map them to client responses.
type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	return http.StatusText(e.Status)
}
