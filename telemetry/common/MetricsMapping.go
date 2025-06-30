package common

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"

type CombinedKeyResourceTypeMeasuredType struct {
	ResourceType metadata.ResourceType
	MeasuredType metadata.MeasuredType
}

type Triple struct {
	Left   string
	Middle string
	Right  string
}

func CreateMetricsMappingMap() map[CombinedKeyResourceTypeMeasuredType]Triple {
	metricsMappingMap := map[CombinedKeyResourceTypeMeasuredType]Triple{
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolAllocatedSize}: {
			Left: "capacity", Middle: "", Right: "",
		},
	}
	return metricsMappingMap
}
