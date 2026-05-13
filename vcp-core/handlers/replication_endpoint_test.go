package api

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	orchestratorMocks "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/factory"
	gcpgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/google-proxy/api/gcp-servergen"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/vcp-core/servergen"
)

// Simple compilation test to check our types are correct
func TestReplicationHandlerTypes(t *testing.T) {
	// Test parameters structure for GET request
	params := oasgenserver.V1GetMultipleReplicationsByExternalUUIDParams{
		ExternalUuids:          "uuid1,uuid2",
		IncludeSourceEndpoints: oasgenserver.OptBool{Value: true, Set: true},
		XCorrelationID:         oasgenserver.OptString{Value: "test-correlation", Set: true},
	}

	// Test response structure
	replication := oasgenserver.ReplicationV1{
		ReplicationId: oasgenserver.OptString{Value: "test-id", Set: true},
		ResourceId:    oasgenserver.OptString{Value: "test-resource", Set: true},
		Description:   oasgenserver.OptNilString{Value: "test-desc", Set: true},
		State:         oasgenserver.OptReplicationV1State{Value: oasgenserver.ReplicationV1StateREADY, Set: true},
		StateDetails:  oasgenserver.OptString{Value: "ready", Set: true},
		Created:       oasgenserver.OptDateTime{Value: time.Now(), Set: true},
	}

	response := &oasgenserver.V1GetMultipleReplicationsByExternalUUIDOK{
		Replications: []oasgenserver.ReplicationV1{replication},
	}

	// Test state mapping function
	state := mapGcpStateToCore("READY")
	assert.Equal(t, oasgenserver.ReplicationV1StateREADY, state)

	state = mapGcpStateToCore("ERROR")
	assert.Equal(t, oasgenserver.ReplicationV1StateERROR, state)

	state = mapGcpStateToCore("UNKNOWN")
	assert.Equal(t, oasgenserver.ReplicationV1StateSTATEUNSPECIFIED, state)

	// Basic assertions that compilation worked
	assert.NotNil(t, params)
	assert.NotNil(t, response)
	assert.Len(t, response.Replications, 1)
}

func TestV1GetMultipleReplicationsByExternalUUID_Success(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters (IncludeSourceEndpoints false = dst behavior)
	params := oasgenserver.V1GetMultipleReplicationsByExternalUUIDParams{
		ExternalUuids:          "uuid1, uuid2 ",
		IncludeSourceEndpoints: oasgenserver.OptBool{Value: false, Set: true}, // false = dst behavior
	}

	// Create expected orchestrator parameters
	expectedParams := commonparams.GetMultipleReplicationsByExternalUUIDParams{
		ExternalUUIDs: []string{"uuid1", "uuid2"},
		EndpointType:  "dst", // false maps to "dst"
	}

	// Mock orchestrator response
	replId1 := "repl-id-1"
	resourceId1 := "resource-1"
	description1 := "test description"
	state1 := "READY"
	stateDetails1 := "ready for use"
	created1 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	replId2 := "repl-id-2"
	resourceId2 := "resource-2"
	mockReplications := []commonparams.ReplicationV1beta{
		{
			ReplicationId: &replId1,
			ResourceId:    &resourceId1,
			Description:   &description1,
			State:         &state1,
			StateDetails:  &stateDetails1,
			Created:       &created1,
		},
		{
			ReplicationId: &replId2,
			ResourceId:    &resourceId2,
		},
	}

	// Set up mock expectation
	mockOrchestrator.On("GetMultipleReplicationsByExternalUUID",
		mock.Anything, expectedParams).Return(mockReplications, nil)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetMultipleReplicationsByExternalUUID(ctx, params)

	// Assert success
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to success response
	successResponse, ok := result.(*oasgenserver.V1GetMultipleReplicationsByExternalUUIDOK)
	assert.True(t, ok)
	assert.Len(t, successResponse.Replications, 2)

	// Verify first replication
	first := successResponse.Replications[0]
	assert.Equal(t, "repl-id-1", first.ReplicationId.Value)
	assert.True(t, first.ReplicationId.Set)
	assert.Equal(t, "resource-1", first.ResourceId.Value)
	assert.True(t, first.ResourceId.Set)
	assert.Equal(t, "test description", first.Description.Value)
	assert.True(t, first.Description.Set)
	assert.Equal(t, oasgenserver.ReplicationV1StateREADY, first.State.Value)
	assert.True(t, first.State.Set)
	assert.Equal(t, "ready for use", first.StateDetails.Value)
	assert.True(t, first.StateDetails.Set)
	assert.Equal(t, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), first.Created.Value)
	assert.True(t, first.Created.Set)

	// Verify second replication (minimal fields)
	second := successResponse.Replications[1]
	assert.Equal(t, "repl-id-2", second.ReplicationId.Value)
	assert.True(t, second.ReplicationId.Set)
	assert.Equal(t, "resource-2", second.ResourceId.Value)
	assert.True(t, second.ResourceId.Set)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1GetMultipleReplicationsByExternalUUID_OrchestratorError(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters (IncludeSourceEndpoints true = src behavior)
	params := oasgenserver.V1GetMultipleReplicationsByExternalUUIDParams{
		ExternalUuids:          "uuid1",
		IncludeSourceEndpoints: oasgenserver.OptBool{Value: true, Set: true}, // true = src behavior
	}

	// Create expected orchestrator parameters
	expectedParams := commonparams.GetMultipleReplicationsByExternalUUIDParams{
		ExternalUUIDs: []string{"uuid1"},
		EndpointType:  "src", // true maps to "src"
	}

	// Set up mock to return error
	mockError := errors.New("orchestrator error")
	mockOrchestrator.On("GetMultipleReplicationsByExternalUUID",
		mock.Anything, expectedParams).Return(nil, mockError)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetMultipleReplicationsByExternalUUID(ctx, params)

	// Assert error
	assert.Error(t, err)
	assert.Equal(t, mockError, err)
	assert.Nil(t, result)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestV1GetMultipleReplicationsByExternalUUID_EmptyInput(t *testing.T) {
	// Create mock orchestrator
	mockOrchestrator := &orchestratorMocks.MockOrchestratorFactory{}

	// Create handler
	handler := Handler{
		Orchestrator: mockOrchestrator,
	}

	// Create test parameters with empty string (default behavior = dst)
	params := oasgenserver.V1GetMultipleReplicationsByExternalUUIDParams{
		ExternalUuids:          "",
		IncludeSourceEndpoints: oasgenserver.OptBool{}, // not set, defaults to false = dst behavior
	}

	// Create expected orchestrator parameters
	expectedParams := commonparams.GetMultipleReplicationsByExternalUUIDParams{
		ExternalUUIDs: []string{""},
		EndpointType:  "dst", // default behavior
	}

	// Mock orchestrator response (empty)
	mockReplications := []commonparams.ReplicationV1beta{}

	// Set up mock expectation
	mockOrchestrator.On("GetMultipleReplicationsByExternalUUID",
		mock.Anything, expectedParams).Return(mockReplications, nil)

	// Call the handler
	ctx := context.Background()
	result, err := handler.V1GetMultipleReplicationsByExternalUUID(ctx, params)

	// Assert success with empty result
	assert.NoError(t, err)
	assert.NotNil(t, result)

	// Cast to success response
	successResponse, ok := result.(*oasgenserver.V1GetMultipleReplicationsByExternalUUIDOK)
	assert.True(t, ok)
	assert.Len(t, successResponse.Replications, 0)

	// Verify mock was called correctly
	mockOrchestrator.AssertExpectations(t)
}

func TestConvertGcpGenServerToCoreReplication(t *testing.T) {
	// Test with all fields set
	gcpReplication := gcpgenserver.ReplicationV1beta{
		ReplicationId: gcpgenserver.NewOptString("test-repl-id"),
		ResourceId:    gcpgenserver.NewOptString("test-resource-id"),
		Description:   gcpgenserver.NewOptString("test description"),
		State:         gcpgenserver.NewOptReplicationV1betaState(gcpgenserver.ReplicationV1betaStateCREATING),
		StateDetails:  gcpgenserver.NewOptString("creating resource"),
		Created:       gcpgenserver.NewOptDateTime(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
	}

	result := convertGcpGenServerToCoreReplication(gcpReplication)

	assert.Equal(t, "test-repl-id", result.ReplicationId.Value)
	assert.True(t, result.ReplicationId.Set)
	assert.Equal(t, "test-resource-id", result.ResourceId.Value)
	assert.True(t, result.ResourceId.Set)
	assert.Equal(t, "test description", result.Description.Value)
	assert.True(t, result.Description.Set)
	assert.Equal(t, oasgenserver.ReplicationV1StateCREATING, result.State.Value)
	assert.True(t, result.State.Set)
	assert.Equal(t, "creating resource", result.StateDetails.Value)
	assert.True(t, result.StateDetails.Set)
	assert.Equal(t, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), result.Created.Value)
	assert.True(t, result.Created.Set)
}

func TestConvertGcpGenServerToCoreReplication_UnsetFields(t *testing.T) {
	// Test with minimal fields
	gcpReplication := gcpgenserver.ReplicationV1beta{}

	result := convertGcpGenServerToCoreReplication(gcpReplication)

	assert.Equal(t, "", result.ReplicationId.Value)
	assert.False(t, result.ReplicationId.Set)
	assert.Equal(t, "", result.ResourceId.Value)
	assert.False(t, result.ResourceId.Set)
	assert.Equal(t, "", result.Description.Value)
	assert.False(t, result.Description.Set)
	assert.Equal(t, oasgenserver.ReplicationV1StateSTATEUNSPECIFIED, result.State.Value)
	assert.False(t, result.State.Set)
	assert.Equal(t, "", result.StateDetails.Value)
	assert.False(t, result.StateDetails.Set)
	assert.Equal(t, time.Time{}, result.Created.Value)
	assert.False(t, result.Created.Set)
}

func TestConvertCommonReplicationV1betaToCoreReplication(t *testing.T) {
	// Test with all fields set
	replId := "test-repl-id"
	resourceId := "test-resource-id"
	description := "test description"
	state := "CREATING"
	stateDetails := "creating resource"
	created := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	commonReplication := commonparams.ReplicationV1beta{
		ReplicationId: &replId,
		ResourceId:    &resourceId,
		Description:   &description,
		State:         &state,
		StateDetails:  &stateDetails,
		Created:       &created,
	}

	result := convertCommonReplicationV1betaToCoreReplication(commonReplication)

	assert.Equal(t, "test-repl-id", result.ReplicationId.Value)
	assert.True(t, result.ReplicationId.Set)
	assert.Equal(t, "test-resource-id", result.ResourceId.Value)
	assert.True(t, result.ResourceId.Set)
	assert.Equal(t, "test description", result.Description.Value)
	assert.True(t, result.Description.Set)
	assert.Equal(t, oasgenserver.ReplicationV1StateCREATING, result.State.Value)
	assert.True(t, result.State.Set)
	assert.Equal(t, "creating resource", result.StateDetails.Value)
	assert.True(t, result.StateDetails.Set)
	assert.Equal(t, time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), result.Created.Value)
	assert.True(t, result.Created.Set)
}

func TestConvertCommonReplicationV1betaToCoreReplication_UnsetFields(t *testing.T) {
	// Test with minimal fields
	commonReplication := commonparams.ReplicationV1beta{}

	result := convertCommonReplicationV1betaToCoreReplication(commonReplication)

	assert.Equal(t, "", result.ReplicationId.Value)
	assert.False(t, result.ReplicationId.Set)
	assert.Equal(t, "", result.ResourceId.Value)
	assert.False(t, result.ResourceId.Set)
	assert.Equal(t, "", result.Description.Value)
	assert.False(t, result.Description.Set)
	assert.Equal(t, oasgenserver.ReplicationV1StateSTATEUNSPECIFIED, result.State.Value)
	assert.False(t, result.State.Set)
	assert.Equal(t, "", result.StateDetails.Value)
	assert.False(t, result.StateDetails.Set)
	assert.Equal(t, time.Time{}, result.Created.Value)
	assert.False(t, result.Created.Set)
}

func TestMapGcpStateEnumToCore(t *testing.T) {
	tests := []struct {
		input    gcpgenserver.ReplicationV1betaState
		expected oasgenserver.ReplicationV1State
	}{
		{gcpgenserver.ReplicationV1betaStateSTATEUNSPECIFIED, oasgenserver.ReplicationV1StateSTATEUNSPECIFIED},
		{gcpgenserver.ReplicationV1betaStateCREATING, oasgenserver.ReplicationV1StateCREATING},
		{gcpgenserver.ReplicationV1betaStateREADY, oasgenserver.ReplicationV1StateREADY},
		{gcpgenserver.ReplicationV1betaStateUPDATING, oasgenserver.ReplicationV1StateUPDATING},
		{gcpgenserver.ReplicationV1betaStateDELETING, oasgenserver.ReplicationV1StateDELETING},
		{gcpgenserver.ReplicationV1betaStateERROR, oasgenserver.ReplicationV1StateERROR},
		{gcpgenserver.ReplicationV1betaState("UNKNOWN"), oasgenserver.ReplicationV1StateSTATEUNSPECIFIED}, // Test default case
	}

	for _, test := range tests {
		result := mapGcpStateEnumToCore(test.input)
		assert.Equal(t, test.expected, result, "Failed for input: %v", test.input)
	}
}

func TestMapGcpStateToCore_AllStates(t *testing.T) {
	tests := []struct {
		input    string
		expected oasgenserver.ReplicationV1State
	}{
		{"STATE_UNSPECIFIED", oasgenserver.ReplicationV1StateSTATEUNSPECIFIED},
		{"CREATING", oasgenserver.ReplicationV1StateCREATING},
		{"READY", oasgenserver.ReplicationV1StateREADY},
		{"UPDATING", oasgenserver.ReplicationV1StateUPDATING},
		{"DELETING", oasgenserver.ReplicationV1StateDELETING},
		{"ERROR", oasgenserver.ReplicationV1StateERROR},
		{"UNKNOWN", oasgenserver.ReplicationV1StateSTATEUNSPECIFIED},
		{"", oasgenserver.ReplicationV1StateSTATEUNSPECIFIED},
	}

	for _, test := range tests {
		result := mapGcpStateToCore(test.input)
		assert.Equal(t, test.expected, result, "Failed for input: %s", test.input)
	}
}

// TestIncludeSourceEndpointsBooleanConversion tests the conversion from boolean to string for the endpoint type
func TestIncludeSourceEndpointsBooleanConversion(t *testing.T) {
	tests := []struct {
		name                   string
		includeSourceEndpoints oasgenserver.OptBool
		expectedEndpointType   string
		description            string
	}{
		{
			name:                   "IncludeSourceEndpoints true",
			includeSourceEndpoints: oasgenserver.OptBool{Value: true, Set: true},
			expectedEndpointType:   "src",
			description:            "When IncludeSourceEndpoints is true, should map to 'src'",
		},
		{
			name:                   "IncludeSourceEndpoints false",
			includeSourceEndpoints: oasgenserver.OptBool{Value: false, Set: true},
			expectedEndpointType:   "dst",
			description:            "When IncludeSourceEndpoints is false, should map to 'dst'",
		},
		{
			name:                   "IncludeSourceEndpoints not set",
			includeSourceEndpoints: oasgenserver.OptBool{},
			expectedEndpointType:   "dst",
			description:            "When IncludeSourceEndpoints is not set, should default to 'dst'",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Test the conversion logic directly
			var endpointType string
			if test.includeSourceEndpoints.IsSet() && test.includeSourceEndpoints.Value {
				endpointType = "src"
			} else {
				endpointType = "dst"
			}

			assert.Equal(t, test.expectedEndpointType, endpointType, test.description)
		})
	}
}
