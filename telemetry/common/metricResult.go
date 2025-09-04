package common

import (
	"google.golang.org/api/servicecontrol/v1"
)

type ReportResponse servicecontrol.ReportResponse

type MetricsResult struct {
	GoogleMetric   GoogleMetric
	ReportResponse *ReportResponse
	OperationID    string
	OperationName  string
	Exception      error
}

func NewGoogleMetricsResultWithResponse(googleMetric GoogleMetric, reportResponse *ReportResponse, operationID string) *MetricsResult {
	return &MetricsResult{
		GoogleMetric:   googleMetric,
		ReportResponse: reportResponse,
		OperationID:    operationID,
	}
}

func NewGoogleMetricsResultWithException(googleMetric GoogleMetric, exception error, operationID string) *MetricsResult {
	return &MetricsResult{
		GoogleMetric: googleMetric,
		OperationID:  operationID,
		Exception:    exception,
	}
}

func (g *MetricsResult) GetGoogleMetric() GoogleMetric {
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
