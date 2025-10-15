package common

type StartProjectEventParams struct {
	LocationId     string
	State          string
	ProjectNumber  string
	XCorrelationID string
	Zone           string
}

type StartProjectEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}

type SDEOperationStatus struct {
	Completed bool
	Error     error
}

type FinishProjectEventParams struct {
	LocationId     string
	State          string
	ProjectNumber  string
	XCorrelationID string
	Zone           string
}

type FinishProjectEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}
