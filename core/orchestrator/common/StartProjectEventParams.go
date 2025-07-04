package common

// StartProjectEventParams describes parameters supplied to StartProjectEventActivity
type StartProjectEventParams struct {
	Description    string
	LocationID     string
	State          string
	ProjectNumber  string
	XCorrelationID string
}

type StartProjectEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}
