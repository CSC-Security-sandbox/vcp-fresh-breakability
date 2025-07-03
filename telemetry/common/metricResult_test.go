package common

import (
	"fmt"
	"testing"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/telemetry/entity"
)

func TestCreatesMetricsResultWithValidResponse(t *testing.T) {
	googleMetric := entity.HydratedMetric{}
	reportResponse := &ReportResponse{}
	operationID := "operation-123"

	result := NewGoogleMetricsResultWithResponse(googleMetric, reportResponse, operationID)

	if result.ReportResponse != reportResponse {
		t.Fatalf("Expected ReportResponse to be %v, got %v", reportResponse, result.ReportResponse)
	}
	if result.OperationID != operationID {
		t.Fatalf("Expected OperationID to be %s, got %s", operationID, result.OperationID)
	}
}

func TestCreatesMetricsResultWithValidException(t *testing.T) {
	googleMetric := entity.HydratedMetric{}
	exception := fmt.Errorf("sample exception")
	operationID := "operation-456"

	result := NewGoogleMetricsResultWithException(googleMetric, exception, operationID)

	if result.Exception != exception {
		t.Fatalf("Expected Exception to be %v, got %v", exception, result.Exception)
	}
	if result.OperationID != operationID {
		t.Fatalf("Expected OperationID to be %s, got %s", operationID, result.OperationID)
	}
}

func TestRetrievesReportResponseSuccessfully(t *testing.T) {
	reportResponse := &ReportResponse{}
	result := &MetricsResult{ReportResponse: reportResponse}

	retrievedResponse := result.GetReportResponse()

	if retrievedResponse != reportResponse {
		t.Fatalf("Expected ReportResponse to be %v, got %v", reportResponse, retrievedResponse)
	}
}

func TestRetrievesOperationIDSuccessfully(t *testing.T) {
	operationID := "operation-789"
	result := &MetricsResult{OperationID: operationID}

	retrievedID := result.GetOperationID()

	if retrievedID != operationID {
		t.Fatalf("Expected OperationID to be %s, got %s", operationID, retrievedID)
	}
}

func TestSetsAndRetrievesOperationNameSuccessfully(t *testing.T) {
	operationName := "operation-name"
	result := &MetricsResult{}

	result.SetOperationName(operationName)
	retrievedName := result.GetOperationName()

	if retrievedName != operationName {
		t.Fatalf("Expected OperationName to be %s, got %s", operationName, retrievedName)
	}
}

func TestRetrievesExceptionSuccessfully(t *testing.T) {
	exception := fmt.Errorf("sample exception")
	result := &MetricsResult{Exception: exception}

	retrievedException := result.GetException()

	if retrievedException != exception {
		t.Fatalf("Expected Exception to be %v, got %v", exception, retrievedException)
	}
}
