package common

import "github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"

type Triple struct {
	Left   string
	Middle string
	Right  string
}

func CreateMetricsMappingMap() map[metadata.CombinedKeyResourceTypeMeasuredType]Triple {
	metricsMappingMap := map[metadata.CombinedKeyResourceTypeMeasuredType]Triple{
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.PoolAllocatedSize}: {
			Left: "capacity", Middle: "", Right: "",
		},
		{ResourceType: metadata.VolumePool, MeasuredType: metadata.AllocatedUsed}: {
			Left: "allocated", Middle: "", Right: "",
		},
	}
	return metricsMappingMap
}
