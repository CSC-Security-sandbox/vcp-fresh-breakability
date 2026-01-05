package common

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

func TestSampledMetricsFormatter_Format_Points(t *testing.T) {
	pointFormatter := SampledMetricsFormatter{
		Mode:   Point,
		Logger: log.NewLogger(),
	}

	start := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
	end := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)

	t.Run("00:-------|-----------|-------", func(t *testing.T) {
		var want []TimeSeries

		got := pointFormatter.Format(context.Background(), nil, []entity.HydratedMetric{}, start, end)

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

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   timestamp1,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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

		var want []TimeSeries

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   start,
				Metadata:         hydratedMetrics[0].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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

		var want []TimeSeries

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp1,
						Quantity:  100,
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("18:------|x|----x----|x|------", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)

		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp, nil, 200, true, false, "low"),
			createHydratedMetric(end, nil, 300, true, false, "low"),
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
					},
					{
						Timestamp: timestamp,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
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
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
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
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   timestamp2,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("42:-------|----------|x|------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(end, nil, 100, true, false, "low"),
		}

		var want []TimeSeries

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  300,
					},
				},
			},
		}

		got := pointFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})
}
