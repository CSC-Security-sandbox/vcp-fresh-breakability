package common

import (
	"fmt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicecontrol/v1"
	"testing"
)

func TestReturnsErrorMessageForValidReportResponse(t *testing.T) {
	reportErrors := []*servicecontrol.ReportError{
		{Status: &servicecontrol.Status{Code: 400, Message: "Bad Request"}},
		{Status: &servicecontrol.Status{Code: 404, Message: "Not Found"}},
	}
	result := &MetricsResult{ReportResponse: &ReportResponse{ReportErrors: reportErrors}}
	expected := "400 - Bad Request,404 - Not Found"
	actual := GetErrorMessage(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestReturnsExceptionMessageWhenExceptionExists(t *testing.T) {
	exception := fmt.Errorf("Sample exception")
	result := &MetricsResult{Exception: exception}
	expected := "Sample exception"
	actual := GetErrorMessage(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestReturnsUnknownExceptionMessageWhenNoResponseOrExceptionExists(t *testing.T) {
	result := &MetricsResult{}
	expected := "Unknown exception when pushing Google billing usage."
	actual := GetErrorMessage(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestFiltersResultsIntoCorrectCategories(t *testing.T) {
	goodResult := MetricsResult{}
	errorResult := MetricsResult{ReportResponse: &ReportResponse{ReportErrors: []*servicecontrol.ReportError{{Status: &servicecontrol.Status{Code: 400, Message: "Bad Request"}}}}}
	exceptionResult := MetricsResult{Exception: fmt.Errorf("Sample exception")}
	results := []MetricsResult{goodResult, errorResult, exceptionResult}

	goodResults, errorResults, exceptionResults := FilterErrorResults(results)

	if len(goodResults) != 1 || len(errorResults) != 1 || len(exceptionResults) != 1 {
		t.Fatalf("Expected 1 good result, 1 error result, and 1 exception result, got %d, %d, %d", len(goodResults), len(errorResults), len(exceptionResults))
	}
}

func TestReturnsCodeForValidGoogleApiError(t *testing.T) {
	exception := &googleapi.Error{Code: 400}
	result := &MetricsResult{Exception: exception}
	expected := "400"
	actual := GetCode(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestReturnsCodeForValidReportResponse(t *testing.T) {
	reportErrors := []*servicecontrol.ReportError{{Status: &servicecontrol.Status{Code: 404, Message: "Not Found"}}}
	result := &MetricsResult{ReportResponse: &ReportResponse{ReportErrors: reportErrors}}
	expected := "404"
	actual := GetCode(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}

func TestReturnsOtherCodeWhenNoErrorsExist(t *testing.T) {
	result := &MetricsResult{}
	expected := "OTHER"
	actual := GetCode(result)
	if actual != expected {
		t.Fatalf("Expected %s, got %s", expected, actual)
	}
}
