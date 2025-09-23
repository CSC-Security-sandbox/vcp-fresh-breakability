package models

// AggregateDistributionResult represents the result of CV distribution across aggregates
type AggregateDistributionResult struct {
	Aggregates     []string `json:"aggregates"`
	AggrMultiplier int64    `json:"aggr_multiplier"`
}
