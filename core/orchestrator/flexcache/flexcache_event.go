package flexcache

type CreateFlexCacheEvent struct {
	LocationID    string  `json:"LocationID,omitempty"`
	ProjectNumber string  `json:"ProjectNumber,omitempty"`
	CorrelationID *string `json:"XCorrelationID,omitempty"`
	RequestUri    string  `json:"RequestUri,omitempty"`
}
