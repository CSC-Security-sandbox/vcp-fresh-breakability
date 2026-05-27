package common

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/vcp-vsa-control-Plane/vsa-control-plane/lib/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/servicecontrol/v1"
)

// GetErrorMessage returns the error message from the result.
// Parameters:
// - result: The result to get the error message from.
// Returns:
// - The error message.
func GetErrorMessage(result *MetricsResult) string {
	var response = result.ReportResponse
	if response != nil {
		return strings.Join(getErrorMessages(response.ReportErrors), ",")
	} else if result.Exception != nil {
		return result.Exception.Error()
	} else {
		return "Unknown exception when pushing Google billing usage."
	}
}

// getErrorMessages returns the error messages from the report errors.
// Parameters:
// - reportErrors: The report errors to get the error messages from.
// Returns:
// - Slice of error messages.
func getErrorMessages(reportErrors []*servicecontrol.ReportError) []string {
	size := len(reportErrors)
	errorMessages := make([]string, 0, size)

	for _, reportError := range reportErrors {
		errorMessages = append(errorMessages, fmt.Sprintf("%d - %s", reportError.Status.Code, reportError.Status.Message))
	}

	return errorMessages
}

// FilterErrorResults filters the results into good results, error results, and exception results.
// Parameters:
// - results: The results to filter.
// Returns:
// - Slice of good results.
// - Slice of error results.
// - Slice of exception results.
func FilterErrorResults(results []MetricsResult) ([]MetricsResult, []MetricsResult, []MetricsResult) {
	var goodResults []MetricsResult
	var errorResults []MetricsResult
	var exceptionResults []MetricsResult

	for _, result := range results {
		if result.Exception != nil {
			exceptionResults = append(exceptionResults, result)
		} else if result.ReportResponse != nil && result.ReportResponse.ReportErrors != nil && len(result.ReportResponse.ReportErrors) > 0 {
			errorResults = append(errorResults, result)
		} else {
			goodResults = append(goodResults, result)
		}
	}
	return goodResults, errorResults, exceptionResults
}

// GetCode returns the code from the result if it exists, otherwise it returns "OTHER".
// Parameters:
// - result: The result to get the code from.
// Returns:
// - The code in string format.
func GetCode(result *MetricsResult) string {
	if result.Exception != nil {
		var googleApiError *googleapi.Error
		if errors.As(result.Exception, &googleApiError) {
			return strconv.Itoa(googleApiError.Code)
		}
	} else if result.ReportResponse != nil {
		return getCodeResponse(result.ReportResponse)
	}
	return "OTHER"
}

// getCodeResponse returns 200 if there are no errors, otherwise it returns the first error code.
// Parameters:
// - response: The response to get the code from.
// Returns:
// - The code in string format.
func getCodeResponse(response *ReportResponse) string {
	if len(response.ReportErrors) == 0 {
		return "200"
	}
	return strconv.FormatInt(response.ReportErrors[0].Status.Code, 10)
}
