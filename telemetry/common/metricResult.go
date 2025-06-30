package common

import (
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
	"google.golang.org/api/servicecontrol/v1"
)

type ReportResponse servicecontrol.ReportResponse

type MetricsResult struct {
	GoogleMetric   entity.HydratedMetric
	ReportResponse *ReportResponse
	OperationID    string
	OperationName  string
	Exception      error
}

func NewGoogleMetricsResultWithResponse(googleMetric entity.HydratedMetric, reportResponse *ReportResponse, operationID string) *MetricsResult {
	return &MetricsResult{
		GoogleMetric:   googleMetric,
		ReportResponse: reportResponse,
		OperationID:    operationID,
	}
}

func NewGoogleMetricsResultWithException(googleMetric entity.HydratedMetric, exception error, operationID string) *MetricsResult {
	return &MetricsResult{
		GoogleMetric: googleMetric,
		OperationID:  operationID,
		Exception:    exception,
	}
}

func (g *MetricsResult) GetGoogleMetric() entity.HydratedMetric {
	return g.GoogleMetric
}

func (g *MetricsResult) GetReportResponse() *ReportResponse {
	return g.ReportResponse
}

func (g *MetricsResult) GetOperationID() string {
	return g.OperationID
}

func (g *MetricsResult) SetOperationName(operationName string) {
	g.OperationName = operationName
}

func (g *MetricsResult) GetOperationName() string {
	return g.OperationName
}

func (g *MetricsResult) GetException() error {
	return g.Exception
}
