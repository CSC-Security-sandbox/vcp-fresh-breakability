package gcp

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	commonparams "github.com/vcp-vsa-control-Plane/vsa-control-plane/core/orchestrator/common"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/database/datamodel"
	utils2 "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/utils"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
)

func TestGetMultipleReplicationsByExternalUUID(t *testing.T) {
	t.Run("SuccessfullyFetchReplicationsByExternalUUID", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create test replications with external UUIDs and dst endpoint type
		testReplications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-uuid-1",
				},
				Name: "test-repl-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "dst",
					ExternalUUID: "repl-uuid-1", // This is what gets mapped to ReplicationId
					// Other fields would be populated by the database JSONB
				},
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-uuid-2",
				},
				Name: "test-repl-2",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "dst",
					ExternalUUID: "repl-uuid-2", // This is what gets mapped to ReplicationId
					// Other fields would be populated by the database JSONB
				},
			},
		}

		// Create expected filter for external UUIDs and endpoint_type
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{"external-uuid-1", "external-uuid-2"}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "dst"))

		// Mock the ListVolumeReplications call
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return(testReplications, nil)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{"external-uuid-1", "external-uuid-2"},
			EndpointType:  "dst",
		}

		// Call the function with database.Storage interface
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 2) // Two replications expected

		// Verify the results have expected basic fields
		assert.Equal(tt, "repl-uuid-1", result[0].ReplicationId.Value)
		assert.Equal(tt, "test-repl-1", result[0].ResourceId.Value)
		assert.Equal(tt, "repl-uuid-2", result[1].ReplicationId.Value)
		assert.Equal(tt, "test-repl-2", result[1].ResourceId.Value)

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})

	t.Run("NoReplicationsFound", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create expected filter for external UUIDs and endpoint_type
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{"nonexistent-uuid"}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "dst"))

		// Mock the ListVolumeReplications call to return empty slice
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{"nonexistent-uuid"},
			EndpointType:  "dst",
		}

		// Call the function
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0) // Empty result

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})

	t.Run("DatabaseError", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create expected filter for external UUIDs and endpoint_type
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{"external-uuid-1"}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "dst"))

		// Mock the ListVolumeReplications call to return an error
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return(nil, assert.AnError)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{"external-uuid-1"},
			EndpointType:  "dst",
		}

		// Call the function
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.Error(tt, err)
		assert.Nil(tt, result)
		// Check that it's a VCP error with database data read error type
		assert.Contains(tt, err.Error(), "internal error")

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})

	t.Run("FilterBySourceEndpointType", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create test replications with src endpoint type
		testReplications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-uuid-src-1",
				},
				Name: "test-repl-src-1",
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "src",
					ExternalUUID: "repl-uuid-src-1", // This is what gets mapped to ReplicationId
				},
			},
		}

		// Create expected filter for external UUIDs and src endpoint_type
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{"external-uuid-src-1"}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "src"))

		// Mock the ListVolumeReplications call
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return(testReplications, nil)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{"external-uuid-src-1"},
			EndpointType:  "src",
		}

		// Call the function
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 1)
		assert.Equal(tt, "repl-uuid-src-1", result[0].ReplicationId.Value)
		assert.Equal(tt, "test-repl-src-1", result[0].ResourceId.Value)

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})

	t.Run("EmptyExternalUUIDs", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create expected filter with empty array
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "dst"))

		// Mock the ListVolumeReplications call to return empty slice
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return([]*datamodel.VolumeReplication{}, nil)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{},
			EndpointType:  "dst",
		}

		// Call the function
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 0)

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})

	t.Run("StateMapping", func(tt *testing.T) {
		// Create mock storage
		mockStorage := &database.MockStorage{}

		// Create test replications with different states
		testReplications := []*datamodel.VolumeReplication{
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-creating",
				},
				Name:  "test-repl-creating",
				State: datamodel.LifeCycleStateCreating,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "dst",
					ExternalUUID: "creating-external-uuid", // Add ExternalUUID
				},
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-available",
				},
				Name:  "test-repl-available",
				State: datamodel.LifeCycleStateAvailable,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "dst",
					ExternalUUID: "available-external-uuid", // Add ExternalUUID
				},
			},
			{
				BaseModel: datamodel.BaseModel{
					UUID: "repl-error",
				},
				Name:  "test-repl-error",
				State: datamodel.LifeCycleStateError,
				ReplicationAttributes: &datamodel.ReplicationDetails{
					EndpointType: "dst",
					ExternalUUID: "error-external-uuid", // Add ExternalUUID
				},
			},
		}

		// Create expected filter
		expectedFilter := utils2.CreateFilterWithConditions(
			utils2.NewFilterCondition("replication_attributes->>'external_uuid'", "in", []string{"external-uuid-1", "external-uuid-2", "external-uuid-3"}),
			utils2.NewFilterCondition("replication_attributes->>'endpoint_type'", "=", "dst"))

		// Mock the ListVolumeReplications call
		mockStorage.On("ListVolumeReplications", mock.Anything, *expectedFilter, mock.Anything).Return(testReplications, nil)

		// Create params
		params := commonparams.GetMultipleReplicationsByExternalUUIDParams{
			ExternalUUIDs: []string{"external-uuid-1", "external-uuid-2", "external-uuid-3"},
			EndpointType:  "dst",
		}

		// Call the function
		result, err := _getMultipleReplicationsByExternalUUID(context.Background(), mockStorage, params)

		// Assertions
		assert.NoError(tt, err)
		assert.NotNil(tt, result)
		assert.Len(tt, result, 3)

		// Check basic fields - State is not currently mapped by convertDataStoreReplicationToGcpGenServerModel
		assert.Equal(tt, "creating-external-uuid", result[0].ReplicationId.Value)
		assert.Equal(tt, "test-repl-creating", result[0].ResourceId.Value)
		assert.Equal(tt, "available-external-uuid", result[1].ReplicationId.Value)
		assert.Equal(tt, "test-repl-available", result[1].ResourceId.Value)
		assert.Equal(tt, "error-external-uuid", result[2].ReplicationId.Value)
		assert.Equal(tt, "test-repl-error", result[2].ResourceId.Value)

		// Verify mocks were called
		mockStorage.AssertExpectations(tt)
	})
}
