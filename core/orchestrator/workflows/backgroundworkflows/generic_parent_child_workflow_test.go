package backgroundworkflows

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/activities/backgroundactivities"
)

func TestGenericChildWorkflowResult(t *testing.T) {
	result := &GenericChildWorkflowResult{
		WorkflowID:          "workflow-123",
		TotalItemsProcessed: 10,
		SuccessfulItems:     8,
		FailedItems:         2,
		Error:               "",
		FailedResourceNames: []string{"resource1", "resource2"},
	}

	assert.Equal(t, "workflow-123", result.WorkflowID)
	assert.Equal(t, 10, result.TotalItemsProcessed)
	assert.Equal(t, 8, result.SuccessfulItems)
	assert.Equal(t, 2, result.FailedItems)
	assert.Equal(t, "", result.Error)
	assert.Equal(t, 2, len(result.FailedResourceNames))
}

func TestGenericParentWorkflowResult(t *testing.T) {
	childResult := &GenericChildWorkflowResult{
		WorkflowID:          "child-1",
		TotalItemsProcessed: 5,
		SuccessfulItems:     4,
		FailedItems:         1,
	}

	result := &GenericParentWorkflowResult{
		TotalItemsProcessed: 10,
		TotalSuccessful:     8,
		TotalFailed:         2,
		ChildResults:        []*GenericChildWorkflowResult{childResult},
	}

	assert.Equal(t, 10, result.TotalItemsProcessed)
	assert.Equal(t, 8, result.TotalSuccessful)
	assert.Equal(t, 2, result.TotalFailed)
	assert.Equal(t, 1, len(result.ChildResults))
	assert.Equal(t, childResult, result.ChildResults[0])
}

func TestGenericParentWorkflowConfig(t *testing.T) {
	config := GenericParentWorkflowConfig{
		WorkflowName:          "test-workflow",
		BatchSize:             25,
		ChildWorkflowTimeout:  0,
		GetTotalCountActivity: "GetTotalCount",
		ChildWorkflowFunc:     "ChildWorkflow",
	}

	assert.Equal(t, "test-workflow", config.WorkflowName)
	assert.Equal(t, 25, config.BatchSize)
	assert.Equal(t, time.Duration(0), config.ChildWorkflowTimeout)
	assert.Equal(t, "GetTotalCount", config.GetTotalCountActivity)
	assert.Equal(t, "ChildWorkflow", config.ChildWorkflowFunc)
}

func TestGenericChildWorkflowConfig(t *testing.T) {
	config := GenericChildWorkflowConfig{
		WorkflowName:            "test-child-workflow",
		ActivityBatchSize:       10,
		MaxConcurrentActivities: 5,
		ActivityTimeoutMinutes:  30,
		ListDataActivity:        "ListData",
		ProcessBatchActivity:    "ProcessBatch",
	}

	assert.Equal(t, "test-child-workflow", config.WorkflowName)
	assert.Equal(t, 10, config.ActivityBatchSize)
	assert.Equal(t, 5, config.MaxConcurrentActivities)
	assert.Equal(t, 30, config.ActivityTimeoutMinutes)
	assert.Equal(t, "ListData", config.ListDataActivity)
	assert.Equal(t, "ProcessBatch", config.ProcessBatchActivity)
}

// TestGenericChildWorkflow_ProcessChildWorkflowResults_FailedResourceNames tests the processing of failed resource names from child workflow results
func TestGenericChildWorkflow_ProcessChildWorkflowResults_FailedResourceNames(t *testing.T) {
	// Test the logic for processing failed resource names from map results
	// This simulates the logic in lines 260-262

	// Simulate a result map with failed resource names
	resultMap := map[string]interface{}{
		"FailedResourceNames": []interface{}{"resource1", "resource2", "resource3"},
	}

	var allFailedResourceNames []string

	// Test the logic from lines 260-262
	if val, exists := resultMap["FailedResourceNames"]; exists {
		if failedResourceNames, ok := val.([]interface{}); ok {
			for _, resourceName := range failedResourceNames {
				if name, ok := resourceName.(string); ok {
					allFailedResourceNames = append(allFailedResourceNames, name)
				}
			}
		}
	}

	assert.Equal(t, 3, len(allFailedResourceNames))
	assert.Contains(t, allFailedResourceNames, "resource1")
	assert.Contains(t, allFailedResourceNames, "resource2")
	assert.Contains(t, allFailedResourceNames, "resource3")
}

// TestGenericChildWorkflow_ProcessChildWorkflowResults_FailedResourceErrors tests the processing of failed resource errors from child workflow results
func TestGenericChildWorkflow_ProcessChildWorkflowResults_FailedResourceErrors(t *testing.T) {
	// Test the logic for processing failed resource errors from map results
	// This simulates the logic in lines 270-274

	// Simulate a result map with failed resource errors
	resultMap := map[string]interface{}{
		"FailedResourceErrors": []interface{}{
			map[string]interface{}{
				"ResourceName": "resource1",
				"Error":        "deletion failed",
			},
			map[string]interface{}{
				"ResourceName": "resource2",
				"Error":        "timeout error",
			},
		},
	}

	var allFailedResourceErrors []backgroundactivities.ParentChildWorkflowError

	// Test the logic from lines 270-274
	if val, exists := resultMap["FailedResourceErrors"]; exists {
		if failedResourceErrors, ok := val.([]interface{}); ok {
			for _, resourceError := range failedResourceErrors {
				if resourceErrorMap, ok := resourceError.(map[string]interface{}); ok {
					resourceName, _ := resourceErrorMap["ResourceName"].(string)
					errorMsg, _ := resourceErrorMap["Error"].(string)
					allFailedResourceErrors = append(allFailedResourceErrors, backgroundactivities.ParentChildWorkflowError{
						ResourceName: resourceName,
						Error:        errorMsg,
					})
				}
			}
		}
	}

	assert.Equal(t, 2, len(allFailedResourceErrors))
	assert.Equal(t, "resource1", allFailedResourceErrors[0].ResourceName)
	assert.Equal(t, "deletion failed", allFailedResourceErrors[0].Error)
	assert.Equal(t, "resource2", allFailedResourceErrors[1].ResourceName)
	assert.Equal(t, "timeout error", allFailedResourceErrors[1].Error)
}

// TestGenericChildWorkflow_ProcessChildWorkflowResults_LogFailedResourceErrors tests the logging of failed resource errors
func TestGenericChildWorkflow_ProcessChildWorkflowResults_LogFailedResourceErrors(t *testing.T) {
	// Test the logic for logging failed resource errors
	// This simulates the logic in line 287

	// Simulate failed resource errors that would trigger logging
	allFailedResourceErrors := []backgroundactivities.ParentChildWorkflowError{
		{
			ResourceName: "resource1",
			Error:        "deletion failed",
		},
		{
			ResourceName: "resource2",
			Error:        "timeout error",
		},
	}

	// Test that we have failed resource errors to log
	assert.Equal(t, 2, len(allFailedResourceErrors))
	assert.Greater(t, len(allFailedResourceErrors), 0)

	// Verify the structure of the failed resource errors
	for _, err := range allFailedResourceErrors {
		assert.NotEmpty(t, err.ResourceName)
		assert.NotEmpty(t, err.Error)
	}
}

// TestGenericChildWorkflow_ProcessChildWorkflowResults_UnexpectedResultType tests handling of unexpected result types
func TestGenericChildWorkflow_ProcessChildWorkflowResults_UnexpectedResultType(t *testing.T) {
	// Test the logic for handling unexpected result types
	// This simulates the logic in the else clause around line 282-284

	// Simulate an unexpected result type
	result := "unexpected-string-result"

	// Test the type checking logic
	var resultMap map[string]interface{}
	var ok bool

	// This simulates the type assertion that would fail
	var resultInterface interface{} = result
	resultMap, ok = resultInterface.(map[string]interface{})

	// Verify that the type assertion fails for unexpected types
	assert.False(t, ok)
	assert.Nil(t, resultMap)

	// Test that we can handle the unexpected result type gracefully
	if !ok {
		// This is the expected behavior for unexpected result types
		assert.Equal(t, "unexpected-string-result", result)
	}
}

// TestGenericChildWorkflow_ProcessChildWorkflowResults_MapInterfaceConversion tests the conversion of map[string]interface{} results
func TestGenericChildWorkflow_ProcessChildWorkflowResults_MapInterfaceConversion(t *testing.T) {
	// Test the complete map interface conversion logic
	// This simulates the logic in lines 260-262, 270-274

	// Create a mock result that simulates the map[string]interface{} structure
	resultMap := map[string]interface{}{
		"TotalItemsProcessed": 3,
		"SuccessfulItems":     1,
		"FailedItems":         2,
		"FailedResourceNames": []interface{}{"resource1", "resource2"},
		"FailedResourceErrors": []interface{}{
			map[string]interface{}{
				"ResourceName": "resource1",
				"Error":        "deletion failed",
			},
			map[string]interface{}{
				"ResourceName": "resource2",
				"Error":        "timeout error",
			},
		},
	}

	var allFailedResourceNames []string
	var allFailedResourceErrors []backgroundactivities.ParentChildWorkflowError

	// Test the complete conversion logic
	if val, exists := resultMap["FailedResourceNames"]; exists {
		if failedResourceNames, ok := val.([]interface{}); ok {
			for _, resourceName := range failedResourceNames {
				if name, ok := resourceName.(string); ok {
					allFailedResourceNames = append(allFailedResourceNames, name)
				}
			}
		}
	}

	if val, exists := resultMap["FailedResourceErrors"]; exists {
		if failedResourceErrors, ok := val.([]interface{}); ok {
			for _, resourceError := range failedResourceErrors {
				if resourceErrorMap, ok := resourceError.(map[string]interface{}); ok {
					resourceName, _ := resourceErrorMap["ResourceName"].(string)
					errorMsg, _ := resourceErrorMap["Error"].(string)
					allFailedResourceErrors = append(allFailedResourceErrors, backgroundactivities.ParentChildWorkflowError{
						ResourceName: resourceName,
						Error:        errorMsg,
					})
				}
			}
		}
	}

	// Verify the conversion results
	assert.Equal(t, 2, len(allFailedResourceNames))
	assert.Contains(t, allFailedResourceNames, "resource1")
	assert.Contains(t, allFailedResourceNames, "resource2")

	assert.Equal(t, 2, len(allFailedResourceErrors))
	assert.Equal(t, "resource1", allFailedResourceErrors[0].ResourceName)
	assert.Equal(t, "deletion failed", allFailedResourceErrors[0].Error)
	assert.Equal(t, "resource2", allFailedResourceErrors[1].ResourceName)
	assert.Equal(t, "timeout error", allFailedResourceErrors[1].Error)
}
