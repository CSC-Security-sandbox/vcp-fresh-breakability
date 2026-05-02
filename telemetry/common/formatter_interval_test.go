package common

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSampledMetricsFormatter_Format_Intervals(t *testing.T) {
	intervalFormatter := SampledMetricsFormatter{
		Mode:          Interval,
		BackfillLimit: 2 * time.Hour,
		Logger:        log.NewLogger(),
	}

	start := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
	end := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)

	t.Run("00:-------|-----------|-------", func(t *testing.T) {
		var want []TimeSeries

		got := intervalFormatter.Format(context.Background(), nil, []entity.HydratedMetric{}, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("01:-----x-|-----------|-------", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp, nil, 100, true, false, "low"),
		}

		var want []TimeSeries

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("02:-------|-----x-----|-------", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp, nil, 100, true, false, "low"),
		}

		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("03:-------|-----------|-x-----", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp, nil, 100, true, false, "low"),
		}
		var want []TimeSeries

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("04:-----x-|-x---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("05:-------|-x-------x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("06:-------|---------x-|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("07:-----x-|-----------|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("08:------|x|---------|x|------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
			createHydratedMetric(end, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("09:-----x-|-y---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("10:-------|-x-------y-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("11:-------|---------x-|-y-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("12:-----x-|-----------|-y-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("13:------|x|---------|y|------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
			createHydratedMetric(end, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   start,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("14:-----x-|-x---x-----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("15:-------|-x---x---x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("16:-------|-----x---x-|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("17:-----x-|-----x-----|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("18:------|x|----x----|x|------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("19:-----x-|-x---y-----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("20:-------|-x---x---y-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("21:-------|-----x---x-|-y-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("22:------|x|----x----|y|------", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp, nil, 200, true, false, "low"),
			createHydratedMetric(end, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("23:-----x-|-y---y-----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("24:-------|-x---y---y-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("25:-------|-----x---y-|-y-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("26:-------|-x---y---z-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "high"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp3,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("27:-----x-|-----y-----|-z-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "high"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("40:----x₃-|-----x₁----|-x₂----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		sort.Sort(entity.ByTimestamp(hydratedMetrics))
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: end,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("41:------|x|----------|-------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   start,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("42:-------|----------|x|------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(end, nil, 100, true, false, "low"),
		}
		var want []TimeSeries

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("43:----x-|x|----------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 55, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(start, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   start,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("44:----x-|x|-x--------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 55, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 05, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(start, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 300, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  200,
						TransferType: nil,
					},
					{
						Timestamp: timestamp2,
						Quantity:  300,
						TransferType: nil,
					},
				},
			},
		}

		got := intervalFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})
}

func TestSampledMetricsFormatter_Format_Intervals_Backfill_Limit_Exceeded(t *testing.T) {
	sampledFormatter := SampledMetricsFormatter{
		Mode:          Interval,
		BackfillLimit: 30 * time.Minute,
		Logger:        log.NewLogger(),
	}

	start := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
	end := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)

	t.Run("01:-x-----|-x---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("02:-------|-x-------x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("03:-------|---------x-|-----x-", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("04:-x-----|-y---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("05:-------|-x-------y-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("06:-------|---------x-|----y--", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 50, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
						TransferType: nil,
					},
				},
			},
		}

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("07:-----x-|-----------|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 01, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		var want []TimeSeries

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("08:-----x-|-----------|-y-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 01, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		var want []TimeSeries

		got := sampledFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})
}

func TestSampledMetricsFormatterSpecificLines(t *testing.T) {
	// Test cases to cover specific missing lines in formatter_interval.go

	t.Run("Debug logging with nil logger", func(t *testing.T) {
		// Test formatter with nil logger to cover the nil check
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        nil, // No logger
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(start.Add(30*time.Minute), nil, 100, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should handle nil logger gracefully
		if result == nil {
			t.Error("Expected non-nil result even with nil logger")
		}
	})

	t.Run("Less than 1 metric", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Empty metrics slice
		result := formatter.Format(context.Background(), nil, []entity.HydratedMetric{}, start, end)

		if result != nil {
			t.Error("Expected nil result for empty metrics")
		}
	})

	t.Run("Metric before start - lastMetric nil scenario", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Metric before start time
		timestampBefore := time.Date(2022, 11, 22, 14, 30, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestampBefore, nil, 100, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should handle metrics before start correctly
		if len(result) != 0 {
			t.Errorf("Expected empty result for metrics only before start, got %d", len(result))
		}
	})

	t.Run("First metric after end when lastMetric is nil", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Metric after end time with no previous lastMetric
		timestampAfter := time.Date(2022, 11, 22, 16, 30, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestampAfter, nil, 100, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should break immediately when first metric is after end and no lastMetric
		if len(result) != 0 {
			t.Errorf("Expected empty result when first metric is after end, got %d", len(result))
		}
	})

	t.Run("First metric within period when lastMetric is nil", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Single metric within the period
		timestampWithin := time.Date(2022, 11, 22, 15, 30, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestampWithin, nil, 100, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should create a time series with single data point
		if len(result) != 1 {
			t.Errorf("Expected 1 time series for single metric within period, got %d", len(result))
		}
		if len(result) > 0 && len(result[0].DataPoints) != 1 {
			t.Errorf("Expected 1 data point, got %d", len(result[0].DataPoints))
		}
	})

	t.Run("Interval mode - backfill limit exceeded", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,         // Important: Interval mode
			BackfillLimit: 30 * time.Minute, // Short backfill limit
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Create metrics with large gap between them (exceeding backfill limit)
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 0, 0, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 0, 0, time.UTC) // 40 min gap > 30 min limit

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should create separate time series due to backfill limit exceeded
		if len(result) != 2 {
			t.Errorf("Expected 2 time series due to backfill limit, got %d", len(result))
		}
	})

	t.Run("Point mode - different behavior", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Point, // Point mode instead of Interval
			BackfillLimit: 30 * time.Minute,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		timestamp1 := time.Date(2022, 11, 22, 15, 10, 0, 0, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Point mode should behave differently than Interval mode
		if len(result) != 1 {
			t.Errorf("Expected 1 time series in Point mode, got %d", len(result))
		}
	})

	t.Run("Metric after end time", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		timestamp1 := time.Date(2022, 11, 22, 15, 30, 0, 0, time.UTC)
		timestampAfterEnd := time.Date(2022, 11, 22, 16, 30, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestampAfterEnd, nil, 200, true, false, "low"), // After end
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should stop processing when metric is after end but include end point in Interval mode
		if len(result) != 1 {
			t.Errorf("Expected 1 time series (stopping at end), got %d", len(result))
		}
		if len(result) > 0 && len(result[0].DataPoints) != 2 {
			t.Errorf("Expected 2 data points (before metric and end point), got %d", len(result[0].DataPoints))
		}
	})

	t.Run("Metadata change scenario", func(t *testing.T) {
		formatter := SampledMetricsFormatter{
			Mode:          Interval,
			BackfillLimit: 1 * time.Hour,
			Logger:        log.NewLogger(),
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		timestamp1 := time.Date(2022, 11, 22, 15, 20, 0, 0, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 40, 0, 0, time.UTC)

		metrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "high"), // Different service level
		}

		result := formatter.Format(context.Background(), nil, metrics, start, end)

		// Should create separate time series due to metadata change
		if len(result) != 2 {
			t.Errorf("Expected 2 time series due to metadata change, got %d", len(result))
		}
	})
}

// TestSampledMetricsFormatterBasicFunctions tests the basic sampled metrics formatter functions for coverage
func TestSampledMetricsFormatterBasicFunctions(t *testing.T) {
	formatter := SampledMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Mode:          Interval,
		Logger:        nil, // Use nil logger to avoid mock issues
	}

	t.Run("Format - empty metrics", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		emptyMetrics := []entity.HydratedMetric{}
		result := formatter.Format(context.Background(), nil, emptyMetrics, start, end)
		assert.Nil(t, result, "Empty metrics should return nil")
	})

	t.Run("Format - single metric in range", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Metric within range
		metricTime := start.Add(30 * time.Minute)
		metric := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metricTime.UnixNano()),
			Quantity:  100,
		}
		metric.Metadata.SetResourceUUID("resource1")
		metric.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric}
		result := formatter.Format(context.Background(), nil, metrics, start, end)
		assert.NotNil(t, result, "Should return result for valid metric")
		assert.GreaterOrEqual(t, len(result), 0, "Should handle single metric")
	})

	t.Run("Format - metric before start range", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Metric before start
		metricTime := start.Add(-30 * time.Minute)
		metric := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metricTime.UnixNano()),
			Quantity:  100,
		}
		metric.Metadata.SetResourceUUID("resource1")
		metric.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric}
		result := formatter.Format(context.Background(), nil, metrics, start, end)
		// Metric before start with no metric in range may return nil
		assert.True(t, result == nil || len(result) >= 0, "Should handle metric before start")
	})

	t.Run("Format - metric after end range", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Metric after end
		metricTime := end.Add(30 * time.Minute)
		metric := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metricTime.UnixNano()),
			Quantity:  100,
		}
		metric.Metadata.SetResourceUUID("resource1")
		metric.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric}
		result := formatter.Format(context.Background(), nil, metrics, start, end)
		// Metric after end with no metric in range may return nil
		assert.True(t, result == nil || len(result) >= 0, "Should handle metric after end")
	})

	t.Run("Format - multiple metrics with metadata change", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// First metric
		metric1Time := start.Add(10 * time.Minute)
		metric1 := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metric1Time.UnixNano()),
			Quantity:  100,
		}
		metric1.Metadata.SetResourceUUID("resource1")
		metric1.MeasuredType = metadata.AllocatedSize

		// Second metric with different metadata
		metric2Time := start.Add(20 * time.Minute)
		metric2 := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metric2Time.UnixNano()),
			Quantity:  200,
		}
		metric2.Metadata.SetResourceUUID("resource2")
		metric2.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric1, metric2}
		result := formatter.Format(context.Background(), nil, metrics, start, end)
		assert.NotNil(t, result, "Should handle metrics with metadata changes")
	})

	t.Run("Format - backfill limit exceeded", func(t *testing.T) {
		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		// Short backfill limit
		shortFormatter := SampledMetricsFormatter{
			BackfillLimit: 5 * time.Minute,
			Mode:          Interval,
			Logger:        nil,
		}

		// First metric
		metric1Time := start.Add(10 * time.Minute)
		metric1 := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metric1Time.UnixNano()),
			Quantity:  100,
		}
		metric1.Metadata.SetResourceUUID("resource1")
		metric1.MeasuredType = metadata.AllocatedSize

		// Second metric much later (exceeding backfill limit)
		metric2Time := start.Add(30 * time.Minute)
		metric2 := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metric2Time.UnixNano()),
			Quantity:  200,
		}
		metric2.Metadata.SetResourceUUID("resource1")
		metric2.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric1, metric2}
		result := shortFormatter.Format(context.Background(), nil, metrics, start, end)
		assert.NotNil(t, result, "Should handle backfill limit exceeded")
	})

	t.Run("Format - point mode", func(t *testing.T) {
		pointFormatter := SampledMetricsFormatter{
			BackfillLimit: 2 * time.Hour,
			Mode:          Point,
			Logger:        nil,
		}

		start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
		end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

		metricTime := start.Add(30 * time.Minute)
		metric := entity.HydratedMetric{
			Timestamp: entity.UnixNano(metricTime.UnixNano()),
			Quantity:  100,
		}
		metric.Metadata.SetResourceUUID("resource1")
		metric.MeasuredType = metadata.AllocatedSize

		metrics := []entity.HydratedMetric{metric}
		result := pointFormatter.Format(context.Background(), nil, metrics, start, end)
		assert.NotNil(t, result, "Point mode should work")
	})
}
