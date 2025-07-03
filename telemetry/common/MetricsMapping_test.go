package common

import (
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
)

func TestReturnsCorrectMappingForValidKey(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	key := CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.PoolAllocatedSize,
	}
	expected := Triple{Left: "capacity", Middle: "", Right: ""}
	result, exists := metricsMappingMap[key]
	if !exists {
		t.Fatalf("Expected key to exist in the map")
	}
	if result != expected {
		t.Fatalf("Expected %v, got %v", expected, result)
	}
}

func TestReturnsEmptyMapWhenNoMappingsExist(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	if len(metricsMappingMap) == 0 {
		t.Fatalf("Expected non-empty map, got empty map")
	}
}

func TestHandlesNonExistentKeyGracefully(t *testing.T) {
	metricsMappingMap := CreateMetricsMappingMap()
	key := CombinedKeyResourceTypeMeasuredType{
		ResourceType: metadata.VolumePool,
		MeasuredType: metadata.UnknownMeasuredType,
	}
	_, exists := metricsMappingMap[key]
	if exists {
		t.Fatalf("Expected key to not exist in the map")
	}
}
