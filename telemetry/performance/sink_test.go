package performance

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"strconv"
	"testing"
	"time"
)

// TestDeliverMetrics_ValidMetrics tests the DeliverMetrics function with valid metrics.
func TestDeliverMetrics_ValidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()

	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.PoolAllocatedSize,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 1, count)
}

// TestDeliverMetrics_InvalidMetrics tests the DeliverMetrics function with valid metrics.
func TestDeliverMetrics_InvalidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()

	sink := NewSink(ctx, config)
	var hydratedM []entity.HydratedMetric
	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.UnknownMeasuredType,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	count := sink.DeliverMetrics(ctx, hydratedM)
	assert.Equal(t, 0, count)
}

// TestFilterAcceptedMetrics_ValidMetrics tests the FilterAcceptedMetrics function with valid metrics.
func TestFilterAcceptedMetrics_ValidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	var hydratedM []entity.HydratedMetric
	sink := NewSink(ctx, config)

	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.PoolAllocatedSize,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	validMetrics := sink.FilterAcceptedMetrics(hydratedM)

	assert.Len(t, validMetrics, 1)
}

// TestFilterAcceptedMetrics_InvalidMetrics tests the FilterAcceptedMetrics function with invalid metrics.
func TestFilterAcceptedMetrics_InvalidMetrics(t *testing.T) {
	ctx := context.Background()
	config := common.LoadConfig()
	var hydratedM []entity.HydratedMetric
	sink := NewSink(ctx, config)

	rm := metadata.ResourceMetadata{
		ResourceUUID:        nillable.ToPointer(uuid.New().String()),
		ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
		ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
		AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
		RegionName:          nillable.ToPointer("us-central1"),
		ResourceType:        metadata.VolumePool,
	}

	hydratedM = append(hydratedM, entity.HydratedMetric{
		Metadata:     rm,
		MeasuredType: metadata.UnknownMeasuredType,
		Quantity:     float64(1234),
		Timestamp:    entity.UnixNano(time.Now().UnixNano()),
	})

	validMetrics := sink.FilterAcceptedMetrics(hydratedM)

	assert.Len(t, validMetrics, 0)
}
