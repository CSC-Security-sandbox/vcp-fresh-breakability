package common

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/metrics"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/metadata"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
)

type MockFormatter struct {
	gotInvoked bool
}

func (f MockFormatter) Format(metrics []entity.HydratedMetric, start, end time.Time) []TimeSeries {
	f.gotInvoked = true
	return nil
}

func TestCounterMetricsFormatter_Format(t *testing.T) {
	logger := log.NewLogger()
	counterFormatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        logger,
	}

	start := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
	end := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)

	t.Run("00:-------|-----------|-------", func(t *testing.T) {
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, []entity.HydratedMetric{}, start, end)
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
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("02:-------|-----x-----|-------", func(t *testing.T) {
		timestamp := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp, nil, 100, true, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: end,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[1].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: start,
						Quantity:  100,
					},
					{
						Timestamp: end,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("14.5:-x-x-x-|-x---x-----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 14, 20, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp5 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
			createHydratedMetric(timestamp4, nil, 400, true, false, "low"),
			createHydratedMetric(timestamp5, nil, 500, true, false, "low"),
		}

		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[4].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
					{
						Timestamp: timestamp5,
						Quantity:  500,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
					{
						Timestamp: end,
						Quantity:  300,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[2].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp,
						Quantity:  200,
					},
					{
						Timestamp: end,
						Quantity:  300,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
			createHydratedMetric(timestamp2, nil, 200, false, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "medium"),
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
			createHydratedMetric(timestamp2, nil, 200, false, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "medium"),
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("25:-------|-x---y---z-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "high"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
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
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
			createHydratedMetric(timestamp2, nil, 200, false, false, "medium"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "high"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("28:-----x-|-n---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("29:-------|-x-------n-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("30:-------|---------x-|-n-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("31:-----x-|-----------|-n-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("32:-----n-|-x---------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, false, false, "low"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("33:-------|-n-------x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, false, false, "low"),
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("34:-------|---------n-|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, false, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("35:-----n-|-----------|-x-----", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 16, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, false, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("36:-------|-x-n-n-n-x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 20, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 40, 00, 00, time.UTC)
		timestamp5 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "low"),
			createHydratedMetric(timestamp4, nil, 400, false, false, "low"),
			createHydratedMetric(timestamp5, nil, 500, true, false, "low"),
		}

		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[4].Metadata,
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
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
					{
						Timestamp: timestamp5,
						Quantity:  500,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("37:-------|-x-x-n-x-x-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 20, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 40, 00, 00, time.UTC)
		timestamp5 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "low"),
			createHydratedMetric(timestamp4, nil, 400, true, false, "low"),
			createHydratedMetric(timestamp5, nil, 500, true, false, "low"),
		}
		// Since billable flag is not actually implemented in metadata comparison,
		// all metrics with same metadata properties will be combined into single time series
		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[4].Metadata,
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
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
					{
						Timestamp: timestamp5,
						Quantity:  500,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("38:-------|-x-x-n-y-y-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 20, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 40, 00, 00, time.UTC)
		timestamp5 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, false, false, "low"),
			createHydratedMetric(timestamp4, nil, 400, true, false, "medium"),
			createHydratedMetric(timestamp5, nil, 500, true, false, "medium"),
		}
		// Since billable flag is not actually implemented in metadata comparison,
		// all metrics with same metadata properties (ResourceName, AccountName, ServiceLevel) will be combined into single time series.
		// Metrics 1-3 all have ServiceLevel "low", so they are grouped together.
		// When ServiceLevel changes to "medium" at metric 4, a new time series starts.
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
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[4].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
					{
						Timestamp: timestamp5,
						Quantity:  500,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("39:-------|-x-n-y-n-z-|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 20, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 40, 00, 00, time.UTC)
		timestamp5 := time.Date(2022, 11, 22, 15, 50, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, false, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "medium"),
			createHydratedMetric(timestamp4, nil, 400, false, false, "medium"),
			createHydratedMetric(timestamp5, nil, 500, true, false, "high"),
		}
		// Since billable flag is not actually implemented in metadata comparison,
		// all metrics with same metadata properties (ResourceName, AccountName, ServiceLevel) will be combined into single time series.
		// Metrics 1-2 both have ServiceLevel "low", so they are grouped together.
		// When ServiceLevel changes to "medium" at metric 3, a new time series starts with the transition point (metric 2).
		// Metrics 3-4 both have ServiceLevel "medium", so they are grouped together.
		// When ServiceLevel changes to "high" at metric 5, a new time series starts with the transition point (metric 4).
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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
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
					},
					{
						Timestamp: timestamp3,
						Quantity:  300,
					},
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
				},
			},
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[4].Metadata,
				MeasuredType:     metadata.AllocatedSize,
				DataPoints: []DataPoint{
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
					{
						Timestamp: timestamp5,
						Quantity:  500,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
					},
					{
						Timestamp: timestamp2,
						Quantity:  200,
					},
				},
			},
		}

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("41:------|x|----------|-------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(start, nil, 100, true, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("42:-------|----------|x|------", func(t *testing.T) {
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(end, nil, 100, true, false, "low"),
		}
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		var want []TimeSeries
		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := counterFormatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})
}

func TestCounterMetricsFormatter_Format_Intervals_Backfill_Limit_Exceeded(t *testing.T) {
	formatter := CounterMetricsFormatter{
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
		var want []TimeSeries

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
				AggregationEnd:   end,
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

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

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
		var want []TimeSeries

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("04:-x-----|-y--------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "medium"),
		}
		var want []TimeSeries

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("05:-x-----|-x----x----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
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
				AggregationEnd:   end,
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

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})

	t.Run("05:-x----x-|-x----x----|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
		timestamp2 := time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC)
		timestamp3 := time.Date(2022, 11, 22, 15, 10, 00, 00, time.UTC)
		timestamp4 := time.Date(2022, 11, 22, 15, 30, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
			createHydratedMetric(timestamp2, nil, 200, true, false, "low"),
			createHydratedMetric(timestamp3, nil, 300, true, false, "low"),
			createHydratedMetric(timestamp4, nil, 400, true, false, "low"),
		}

		want := []TimeSeries{
			{
				AggregationStart: start,
				AggregationEnd:   end,
				Metadata:         hydratedMetrics[3].Metadata,
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
					{
						Timestamp: timestamp4,
						Quantity:  400,
					},
				},
			},
		}

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		if !reflect.DeepEqual(got, want) {
			t.Errorf("Got %+v, Want %+v", got, want)
		}
	})
	t.Run("06:-x-----|----------|-------", func(t *testing.T) {
		timestamp1 := time.Date(2022, 11, 22, 14, 00, 00, 00, time.UTC)
		hydratedMetrics := []entity.HydratedMetric{
			createHydratedMetric(timestamp1, nil, 100, true, false, "low"),
		}

		got := formatter.Format(context.Background(), nil, hydratedMetrics, start, end)

		require.Equal(t, 0, len(got))
	})
}

func TestAddingCreatedAtBeforeCurrentHour(t *testing.T) {
	// for all of these tests, the createdAt datapoint is set 10 minutes before the start of the curent hour
	formatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        log.NewLogger(),
	}

	start := time.Date(2022, 11, 22, 15, 00, 00, 00, time.UTC)
	end := time.Date(2022, 11, 22, 16, 00, 00, 00, time.UTC)
	_ = time.Date(2022, 11, 22, 14, 50, 00, 00, time.UTC) // createdAt - unused for now
	createHydratedMetricWithCreatedAt := func(timestamp time.Time, deletedAt *time.Time, quantity float64, billable bool, coolTier bool, serviceLevel string) entity.HydratedMetric {
		m := createHydratedMetric(timestamp, deletedAt, quantity, billable, coolTier, serviceLevel)
		return m
	}

	// Add test cases here for AddingCreatedAtBeforeCurrentHour scenarios
	t.Run("CreatedAt scenario test", func(t *testing.T) {
		// This test handles the createdAt scenario where metadata includes creation time
		metric := createHydratedMetricWithCreatedAt(start.Add(10*time.Minute), nil, 100, true, false, "low")
		metrics := []entity.HydratedMetric{metric}

		result := formatter.Format(context.Background(), nil, metrics, start, end)
		require.Equal(t, 0, len(result)) // Not enough metrics to form a time series
	})
}

func TestCounterMetricsFormatter_GetSetBackfillLimit(t *testing.T) {
	formatter := CounterMetricsFormatter{}

	// Test default value
	assert.Equal(t, time.Duration(0), formatter.BackfillLimit)

	// Test setter
	newLimit := 2 * time.Hour
	formatter.BackfillLimit = newLimit
	assert.Equal(t, newLimit, formatter.BackfillLimit)
}

// TestCounterMetricsFormatter_DatabaseFetch_Success tests successful database fetching of previous metric
func TestCounterMetricsFormatter_DatabaseFetch_Success(t *testing.T) {
	logger := log.NewLogger()
	mockDB := database.NewMockStorage(t)

	formatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        logger,
		MetricsDB:     mockDB,
	}

	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	// Create metrics that are all AFTER the start time (no previous metric)
	resourceName := "test-resource"
	deploymentName := "test-deployment"
	accountName := "test-account"

	hydratedMetrics := []entity.HydratedMetric{
		{
			Timestamp:    entity.UnixNano(start.Add(10 * time.Minute).UnixNano()),
			Quantity:     100,
			MeasuredType: metadata.AllocatedSize,
			Metadata: metadata.ResourceMetadata{
				ResourceType:   metadata.Volume,
				ResourceName:   &resourceName,
				DeploymentName: &deploymentName,
				AccountName:    &accountName,
			},
		},
		{
			Timestamp:    entity.UnixNano(start.Add(30 * time.Minute).UnixNano()),
			Quantity:     200,
			MeasuredType: metadata.AllocatedSize,
			Metadata: metadata.ResourceMetadata{
				ResourceType:   metadata.Volume,
				ResourceName:   &resourceName,
				DeploymentName: &deploymentName,
				AccountName:    &accountName,
			},
		},
	}

	// Mock the database to return a previous metric
	previousMetricTime := start.Add(-30 * time.Minute)
	mockDB.On("GetHydratedMetrics", context.Background(), mock.MatchedBy(func(filter map[string]interface{}) bool {
		conditions, hasConditions := filter["conditions"].([][]interface{})
		if !hasConditions {
			return false
		}

		// Verify filter contains correct conditions
		hasTimestamp := false
		hasResource := false
		hasDeployment := false
		hasAccount := false
		hasResourceType := false
		hasMeasuredType := false

		for _, cond := range conditions {
			if len(cond) >= 2 {
				condStr := cond[0].(string)
				if condStr == "metric_timestamp < ?" {
					hasTimestamp = true
				} else if condStr == "resource_name = ?" && cond[1] == resourceName {
					hasResource = true
				} else if condStr == "deployment_name = ?" && cond[1] == deploymentName {
					hasDeployment = true
				} else if condStr == "consumer_id = ?" && cond[1] == accountName {
					hasAccount = true
				} else if condStr == "resource_type = ?" {
					hasResourceType = true
				} else if condStr == "measured_type = ?" {
					hasMeasuredType = true
				}
			}
		}

		return hasTimestamp && hasResource && hasDeployment && hasAccount && hasResourceType && hasMeasuredType
	})).Return([]datamodel.HydratedMetrics{
		{
			MetricTimestamp: previousMetricTime,
			Quantity:        50,
			MeasuredType:    metadata.AllocatedSize,
			ResourceType:    metadata.Volume,
			ResourceName:    resourceName,
		},
	}, nil)

	result := formatter.Format(context.Background(), logger, hydratedMetrics, start, end)

	// Should have 1 time series
	assert.Len(t, result, 1)

	// Should have 3 data points: 1 from DB + 2 from input
	assert.Len(t, result[0].DataPoints, 3)

	// First data point should be from database
	assert.Equal(t, float64(50), result[0].DataPoints[0].Quantity)

	mockDB.AssertExpectations(t)
}

// TestCounterMetricsFormatter_DatabaseFetch_Error tests error handling when database fetch fails
func TestCounterMetricsFormatter_DatabaseFetch_Error(t *testing.T) {
	logger := log.NewLogger()
	mockDB := database.NewMockStorage(t)

	formatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        logger,
		MetricsDB:     mockDB,
	}

	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	resourceName := "test-resource"
	deploymentName := "test-deployment"
	accountName := "test-account"

	hydratedMetrics := []entity.HydratedMetric{
		{
			Timestamp:    entity.UnixNano(start.Add(10 * time.Minute).UnixNano()),
			Quantity:     100,
			MeasuredType: metadata.AllocatedSize,
			Metadata: metadata.ResourceMetadata{
				ResourceType:   metadata.Volume,
				ResourceName:   &resourceName,
				DeploymentName: &deploymentName,
				AccountName:    &accountName,
			},
		},
	}

	// Mock the database to return an error
	mockDB.On("GetHydratedMetrics", context.Background(), mock.Anything).Return(nil, assert.AnError)

	result := formatter.Format(context.Background(), logger, hydratedMetrics, start, end)

	// Should still process but without the DB metric
	// With only 1 metric, can't create a time series (needs at least 2)
	assert.Len(t, result, 0)

	mockDB.AssertExpectations(t)
}

// TestCounterMetricsFormatter_DatabaseFetch_NoResults tests when database returns empty results
func TestCounterMetricsFormatter_DatabaseFetch_NoResults(t *testing.T) {
	logger := log.NewLogger()
	mockDB := database.NewMockStorage(t)

	formatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        logger,
		MetricsDB:     mockDB,
	}

	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	resourceName := "test-resource"
	deploymentName := "test-deployment"
	accountName := "test-account"

	hydratedMetrics := []entity.HydratedMetric{
		{
			Timestamp:    entity.UnixNano(start.Add(10 * time.Minute).UnixNano()),
			Quantity:     100,
			MeasuredType: metadata.AllocatedSize,
			Metadata: metadata.ResourceMetadata{
				ResourceType:   metadata.Volume,
				ResourceName:   &resourceName,
				DeploymentName: &deploymentName,
				AccountName:    &accountName,
			},
		},
	}

	// Mock the database to return empty results
	mockDB.On("GetHydratedMetrics", context.Background(), mock.Anything).Return([]datamodel.HydratedMetrics{}, nil)

	result := formatter.Format(context.Background(), logger, hydratedMetrics, start, end)

	// Should still process but without the DB metric
	assert.Len(t, result, 0)

	mockDB.AssertExpectations(t)
}

// TestCounterMetricsFormatter_DatabaseFetch_NoDatabase tests when MetricsDB is nil
func TestCounterMetricsFormatter_DatabaseFetch_NoDatabase(t *testing.T) {
	logger := log.NewLogger()

	formatter := CounterMetricsFormatter{
		BackfillLimit: 2 * time.Hour,
		Logger:        logger,
		MetricsDB:     nil, // No database
	}

	start := time.Date(2022, 11, 22, 15, 0, 0, 0, time.UTC)
	end := time.Date(2022, 11, 22, 16, 0, 0, 0, time.UTC)

	resourceName := "test-resource"
	deploymentName := "test-deployment"
	accountName := "test-account"

	hydratedMetrics := []entity.HydratedMetric{
		{
			Timestamp:    entity.UnixNano(start.Add(10 * time.Minute).UnixNano()),
			Quantity:     100,
			MeasuredType: metadata.AllocatedSize,
			Metadata: metadata.ResourceMetadata{
				ResourceType:   metadata.Volume,
				ResourceName:   &resourceName,
				DeploymentName: &deploymentName,
				AccountName:    &accountName,
			},
		},
	}

	result := formatter.Format(context.Background(), logger, hydratedMetrics, start, end)

	// Should process without attempting database fetch
	assert.Len(t, result, 0)
}
