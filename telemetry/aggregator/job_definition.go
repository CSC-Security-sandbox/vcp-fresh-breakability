package aggregator

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

// AggregationJobDefinition is a description of an aggregation job that cvt is expected to run.
type AggregationJobDefinition struct {
	MeasuredType    metadata.MeasuredType
	ResourceType    metadata.ResourceType
	AggregationType JobType
	IsBillable      bool
}

var defaultAggregationJobDefinitions = map[metadata.CombinedKeyResourceTypeMeasuredType]AggregationJobDefinition{
	{ResourceType: metadata.Volume, MeasuredType: metadata.AllocatedSize}: {
		AggregationType: IntegralAggregation,
		IsBillable:      false,
	},
}
