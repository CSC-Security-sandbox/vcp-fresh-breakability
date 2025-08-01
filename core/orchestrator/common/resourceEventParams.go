package common

type StartProjectEventParams struct {
	LocationId     string
	State          string
	ProjectNumber  string
	XCorrelationID string
}

type StartProjectEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}
type FinishProjectEventParams struct {
	LocationId     string
	State          string
	ProjectNumber  string
	XCorrelationID string
}

type FinishProjectEventResult struct {
	Done *bool   `json:"done,omitempty"`
	Name *string `json:"name,omitempty"`
}
