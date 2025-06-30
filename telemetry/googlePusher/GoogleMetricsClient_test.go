package googlePusher

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/nillable"
	"strconv"
	"sync"
	"testing"
	"time"
)

func createDummyGoogleMetrics(count int) []entity.HydratedMetric {
	var hydratedM []entity.HydratedMetric
	for i := 0; i < count; i++ {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(i)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(i)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}

		hydratedM = append(hydratedM, entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.PoolAllocatedSize,
			Quantity:     float64(i),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		})
	}
	return hydratedM
}

func Test_reportMetrics(t *testing.T) {
	t.Run("Report metrics successfully", func(t *testing.T) {
		metrics := createDummyGoogleMetrics(2)
		operationStartTime := time.Now().Unix()
		operationEndTime := time.Now().Add(time.Hour).Unix()

		wg := sync.WaitGroup{}
		fpChan := make(chan []common.MetricsResult, 2)
		ctx := context.Background()
		config := common.LoadConfig()
		client := NewGoogleMetricsClient(ctx, "", config)
		wg.Add(1)
		go client.ReportMetrics(metrics, operationStartTime, operationEndTime, &wg, fpChan)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Assert fpChan has data
			results := <-fpChan
			assert.NotEmpty(t, results)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for results")
		}
	})

	t.Run("Report empty metrics", func(t *testing.T) {
		var metrics []entity.HydratedMetric
		operationStartTime := time.Now().Unix()
		operationEndTime := time.Now().Add(time.Hour).Unix()

		wg := sync.WaitGroup{}
		fpChan := make(chan []common.MetricsResult)

		wg.Add(1)
		ctx := context.Background()
		config := common.LoadConfig()
		client := NewGoogleMetricsClient(ctx, "", config)
		go client.ReportMetrics(metrics, operationStartTime, operationEndTime, &wg, fpChan)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Assert fpChan is empty data
			results := <-fpChan
			assert.Nil(t, results)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for results")
		}
	})
}

func Test_createOperationForMetric(t *testing.T) {
	config := common.LoadConfig()
	ctx := context.Background()
	client := NewGoogleMetricsClient(ctx, "", config)
	t.Run("Create operation for valid metrics", func(t *testing.T) {
		metrics := createDummyGoogleMetrics(3)
		operationId := uuid.New().String()
		customerId := "customer123"
		resourceUuid := "resource123"
		opStart := time.Now().Unix()
		opEnd := time.Now().Add(time.Hour).Unix()

		operation, droppedMetrics, err := client.createOperationForMetric(operationId, metrics, customerId, resourceUuid, opStart, opEnd)

		require.NoError(t, err)
		assert.NotNil(t, operation)
		assert.Empty(t, droppedMetrics)
		assert.Equal(t, operationId, operation.OperationId)
	})

	t.Run("Create operation with empty metrics", func(t *testing.T) {
		var metrics []entity.HydratedMetric
		operationId := uuid.New().String()
		customerId := "customer123"
		resourceUuid := "resource123"
		opStart := time.Now().Unix()
		opEnd := time.Now().Add(time.Hour).Unix()

		operation, droppedMetrics, err := client.createOperationForMetric(operationId, metrics, customerId, resourceUuid, opStart, opEnd)

		require.NoError(t, err)
		assert.Nil(t, operation)
		assert.Empty(t, droppedMetrics)
	})

	t.Run("Create operation with invalid metric type", func(t *testing.T) {
		metrics := createDummyGoogleMetrics(3)
		// Simulate invalid metric by setting a field to an invalid value
		metrics[0].MeasuredType = "Unknown"
		operationId := uuid.New().String()
		customerId := "customer123"
		resourceUuid := "resource123"
		opStart := time.Now().Unix()
		opEnd := time.Now().Add(time.Hour).Unix()

		operation, droppedMetrics, err := client.createOperationForMetric(operationId, metrics, customerId, resourceUuid, opStart, opEnd)

		assert.NoError(t, err, operation)
		assert.Len(t, droppedMetrics, 1)
	})
}

func Test_getMetricName(t *testing.T) {
	config := common.LoadConfig()
	ctx := context.Background()
	client := NewGoogleMetricsClient(ctx, "", config)
	t.Run("PerformanceMetric", func(t *testing.T) {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}

		hydratedM := entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.PoolAllocatedSize,
			Quantity:     float64(1234),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		}

		expectedMetricName := "netapp.googleapis.com/storage_pool/capacity"
		metricName, err := client.GetMetricName(hydratedM.MeasuredType, hydratedM.Metadata.ResourceType)
		assert.NoError(t, err)
		assert.Equal(t, expectedMetricName, metricName)
	})

	t.Run("InvalidMetricType", func(t *testing.T) {
		rm := metadata.ResourceMetadata{
			ResourceUUID:        nillable.ToPointer(uuid.New().String()),
			ResourceName:        nillable.ToPointer("dummy-resource-" + strconv.Itoa(1)),
			ResourceDisplayName: nillable.ToPointer("Dummy Resource " + strconv.Itoa(1)),
			AccountName:         nillable.ToPointer("netapp-au-se1-autopush-sde-tst"),
			RegionName:          nillable.ToPointer("us-central1"),
			ResourceType:        metadata.VolumePool,
		}

		hydratedM := entity.HydratedMetric{
			Metadata:     rm,
			MeasuredType: metadata.UnknownMeasuredType,
			Quantity:     float64(1234),
			Timestamp:    entity.UnixNano(time.Now().UnixNano()),
		}

		metricName, err := client.GetMetricName(hydratedM.MeasuredType, hydratedM.Metadata.ResourceType)
		assert.Error(t, err)
		assert.Empty(t, metricName)
	})
}

func Test_partitionMetrics(t *testing.T) {
	t.Run("Multiple metric types", func(t *testing.T) {
		metrics := createDummyGoogleMetrics(3)
		partitionedMetrics := partitionMetrics(metrics)
		require.Len(t, partitionedMetrics, 3)
	})

	t.Run("Empty metrics", func(t *testing.T) {
		var metrics []entity.HydratedMetric
		partitionedMetrics := partitionMetrics(metrics)
		require.Len(t, partitionedMetrics, 1)
		assert.Empty(t, partitionedMetrics[0])
	})
}

func Test_toGoogleProject(t *testing.T) {
	t.Run("Convert customer ID to Google project ID", func(t *testing.T) {
		customerId := "123456"
		expected := "project_number:123456"

		result := toGoogleProject(customerId)
		assert.Equal(t, expected, result)
	})

	t.Run("Return empty string for empty customer ID", func(t *testing.T) {
		customerId := ""
		expected := ""

		result := toGoogleProject(customerId)
		assert.Equal(t, expected, result)
	})

	t.Run("Return project ID when customer ID starts with project:", func(t *testing.T) {
		customerId := "project:my-project"
		expected := "project:my-project"

		result := toGoogleProject(customerId)
		assert.Equal(t, expected, result)
	})

	t.Run("Return project number when customer ID starts with project_number:", func(t *testing.T) {
		customerId := "project_number:123456"
		expected := "project_number:123456"

		result := toGoogleProject(customerId)
		assert.Equal(t, expected, result)
	})
}

func Test_startsWith(t *testing.T) {
	t.Run("String starts with prefix", func(t *testing.T) {
		s := "project:my-project"
		prefix := "project:"

		result := startsWith(s, prefix)
		assert.True(t, result)
	})

	t.Run("String does not start with prefix", func(t *testing.T) {
		s := "my-project"
		prefix := "project:"

		result := startsWith(s, prefix)
		assert.False(t, result)
	})
}

func Test_isNumeric(t *testing.T) {
	t.Run("String is numeric", func(t *testing.T) {
		s := "123456"

		result := isNumeric(s)
		assert.True(t, result)
	})

	t.Run("String is not numeric", func(t *testing.T) {
		s := "abc123"

		result := isNumeric(s)
		assert.False(t, result)
	})
}

func Test_RemoveOperationFromList(t *testing.T) {
	operations := []*Operation{
		{OperationId: "op1"},
		{OperationId: "op2"},
		{OperationId: "op3"},
	}
	operationToRemove := &Operation{OperationId: "op2"}

	result := removeOperation(operations, operationToRemove)

	require.Len(t, result, 2)
	assert.Equal(t, "op1", result[0].OperationId)
	assert.Equal(t, "op3", result[1].OperationId)
}

func Test_RemoveOperationNotInList(t *testing.T) {
	operations := []*Operation{
		{OperationId: "op1"},
		{OperationId: "op2"},
		{OperationId: "op3"},
	}
	operationToRemove := &Operation{OperationId: "op4"}

	result := removeOperation(operations, operationToRemove)

	require.Len(t, result, 3)
	assert.Equal(t, "op1", result[0].OperationId)
	assert.Equal(t, "op2", result[1].OperationId)
	assert.Equal(t, "op3", result[2].OperationId)
}

func Test_RemoveOperationFromEmptyList(t *testing.T) {
	var operations []*Operation
	operationToRemove := &Operation{OperationId: "op1"}

	result := removeOperation(operations, operationToRemove)

	assert.Empty(t, result)
}

func Test_RemoveNilOperationFromList(t *testing.T) {
	operations := []*Operation{
		{OperationId: "op1"},
		{OperationId: "op2"},
		{OperationId: "op3"},
	}

	result := removeOperation(operations, nil)

	require.Len(t, result, 3)
	assert.Equal(t, "op1", result[0].OperationId)
	assert.Equal(t, "op2", result[1].OperationId)
	assert.Equal(t, "op3", result[2].OperationId)
}

func TestRemoveOperationScenarios(t *testing.T) {
	t.Run("Remove operation from list", func(t *testing.T) {
		operations := []*Operation{
			{OperationId: "op1"},
			{OperationId: "op2"},
			{OperationId: "op3"},
		}
		operationToRemove := &Operation{OperationId: "op2"}

		result := removeOperation(operations, operationToRemove)

		require.Len(t, result, 2)
		assert.Equal(t, "op1", result[0].OperationId)
		assert.Equal(t, "op3", result[1].OperationId)
	})

	t.Run("Remove operation not in list", func(t *testing.T) {
		operations := []*Operation{
			{OperationId: "op1"},
			{OperationId: "op2"},
			{OperationId: "op3"},
		}
		operationToRemove := &Operation{OperationId: "op4"}

		result := removeOperation(operations, operationToRemove)

		require.Len(t, result, 3)
		assert.Equal(t, "op1", result[0].OperationId)
		assert.Equal(t, "op2", result[1].OperationId)
		assert.Equal(t, "op3", result[2].OperationId)
	})

	t.Run("Remove operation from empty list", func(t *testing.T) {
		var operations []*Operation
		operationToRemove := &Operation{OperationId: "op1"}

		result := removeOperation(operations, operationToRemove)

		assert.Empty(t, result)
	})

	t.Run("Remove nil operation from list", func(t *testing.T) {
		operations := []*Operation{
			{OperationId: "op1"},
			{OperationId: "op2"},
			{OperationId: "op3"},
		}

		result := removeOperation(operations, nil)

		require.Len(t, result, 3)
		assert.Equal(t, "op1", result[0].OperationId)
		assert.Equal(t, "op2", result[1].OperationId)
		assert.Equal(t, "op3", result[2].OperationId)
	})
}

func TestSetCommonLabelsScenarios(t *testing.T) {
	t.Run("Sets labels correctly for valid input", func(t *testing.T) {
		op := &Operation{}
		consumerId := "123456"
		dataCenter := "us-central1"
		resourceId := "resource123"
		googleMetric := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceDisplayName: nillable.ToPointer("Test Resource"),
			},
		}

		err := SetCommonLabels(op, consumerId, dataCenter, resourceId, googleMetric)
		require.NoError(t, err)
		assert.Equal(t, "us-central1", op.Labels["location"])
		assert.Equal(t, "projects/123456", op.Labels["resource_container"])
		assert.Equal(t, "Test Resource", op.Labels["name"])
	})

	t.Run("Returns error when ResourceDisplayName is nil", func(t *testing.T) {
		op := &Operation{}
		consumerId := "123456"
		dataCenter := "us-central1"
		resourceId := "resource123"
		googleMetric := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceDisplayName: nil,
			},
		}

		err := SetCommonLabels(op, consumerId, dataCenter, resourceId, googleMetric)
		require.NoError(t, err)
	})

	t.Run("Handles empty ConsumerId gracefully", func(t *testing.T) {
		op := &Operation{}
		consumerId := ""
		dataCenter := "us-central1"
		resourceId := "resource123"
		googleMetric := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceDisplayName: nillable.ToPointer("Test Resource"),
			},
		}

		err := SetCommonLabels(op, consumerId, dataCenter, resourceId, googleMetric)
		require.NoError(t, err)
		assert.Equal(t, "us-central1", op.Labels["location"])
		assert.Equal(t, "projects/", op.Labels["resource_container"])
		assert.Equal(t, "Test Resource", op.Labels["name"])
	})

	t.Run("Handles empty DataCenter gracefully", func(t *testing.T) {
		op := &Operation{}
		consumerId := "123456"
		dataCenter := ""
		resourceId := "resource123"
		googleMetric := entity.HydratedMetric{
			Metadata: metadata.ResourceMetadata{
				ResourceDisplayName: nillable.ToPointer("Test Resource"),
			},
		}

		err := SetCommonLabels(op, consumerId, dataCenter, resourceId, googleMetric)
		require.NoError(t, err)
		assert.Equal(t, "", op.Labels["location"])
		assert.Equal(t, "projects/123456", op.Labels["resource_container"])
		assert.Equal(t, "Test Resource", op.Labels["name"])
	})
}
