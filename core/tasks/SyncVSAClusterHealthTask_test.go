package tasks

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/datamodel"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/inmemotasksprocessor"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/core/vsa"
	database "github.com/vcp-vsa-control-Plane/vsa-control-plane/database/vcp"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/hyperscaler"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func TestDetermineJSwapAction(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	tests := []struct {
		name          string
		clusterHealth *vsa.ClusterHealthStatusResponse
		expected      JSwapAction
	}{
		{
			name: "Should swap to disk for unplanned failover scenario",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							Takeover: &vsa.TakeoverState{
								State: vsa.TakeoverStateNotPossible,
							},
							TakeoverCheck: &vsa.TakeoverCheck{
								TakeoverPossible: false,
								Reasons:          []string{UnplannedFailoverTakeoverReason},
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to disk when takeover_possible is false",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							TakeoverCheck: &vsa.TakeoverCheck{
								TakeoverPossible: false,
								Reasons:          []string{"Negotiated takeover is not possible. Partner is not UP."},
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to disk when takeover not_possible with required reason",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							Takeover: &vsa.TakeoverState{
								State: vsa.TakeoverStateNotPossible,
							},
							TakeoverCheck: &vsa.TakeoverCheck{
								TakeoverPossible: true,
								Reasons:          []string{"disabled"}, // This is a required reason
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to disk when takeover in_takeover",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							Takeover: &vsa.TakeoverState{
								State: vsa.TakeoverStateInTakeover,
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to disk when takeover in_progress",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							Takeover: &vsa.TakeoverState{
								State: vsa.TakeoverStateInProgress,
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to disk when takeover failed",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							Takeover: &vsa.TakeoverState{
								State: vsa.TakeoverStateFailed,
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 1,
			},
			expected: JSwapActionToDisk,
		},
		{
			name: "Should swap to memory when takeover_possible is true for both nodes",
			clusterHealth: &vsa.ClusterHealthStatusResponse{
				Records: []vsa.NodeHealthStatus{
					{
						UUID: "node1",
						Name: "node1",
						Ha: &vsa.HAHealthInfo{
							TakeoverCheck: &vsa.TakeoverCheck{
								TakeoverPossible: true,
								Reasons:          []string{},
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
					{
						UUID: "node2",
						Name: "node2",
						Ha: &vsa.HAHealthInfo{
							TakeoverCheck: &vsa.TakeoverCheck{
								TakeoverPossible: true,
								Reasons:          []string{},
							},
						},
						NVLog: &vsa.NVLog{
							BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
						},
					},
				},
				NumRecords: 2,
			},
			expected: JSwapActionToMemory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := determineJSwapAction(tt.clusterHealth, poolUUID, logger, correlationID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldJSwapToDiskForTakeoverNotPossible(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	tests := []struct {
		name     string
		nodes    []vsa.NodeHealthStatus
		expected bool
	}{
		{
			name: "Should return true when takeover_possible is false",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{"Negotiated takeover is not possible. Partner is not UP."},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false when takeover_possible is true",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when TakeoverCheck is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: nil,
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when Ha is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha:   nil,
				},
			},
			expected: false,
		},
		{
			name:     "Should return false for empty nodes",
			nodes:    []vsa.NodeHealthStatus{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldJSwapToDiskForTakeoverNotPossible(tt.nodes, poolUUID, logger, correlationID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldJSwapToMemoryForTakeoverNotPossible(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	tests := []struct {
		name     string
		nodes    []vsa.NodeHealthStatus
		expected bool
	}{
		{
			name: "Should return true when both nodes have takeover_possible true",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{},
						},
					},
				},
				{
					UUID: "node2",
					Name: "node2",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false when one node has takeover_possible false",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{},
						},
					},
				},
				{
					UUID: "node2",
					Name: "node2",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false, // This node has false
							Reasons:          []string{"Partner is not UP."},
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when TakeoverCheck is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: nil,
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when Ha is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha:   nil,
				},
			},
			expected: false,
		},
		{
			name:     "Should return false for empty nodes",
			nodes:    []vsa.NodeHealthStatus{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldJSwapToMemoryForTakeoverPossible(tt.nodes, poolUUID, logger, correlationID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldJSwapToDiskForTakeoverStates(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	tests := []struct {
		name     string
		nodes    []vsa.NodeHealthStatus
		expected bool
	}{
		{
			name: "Should return true for not_possible with required reason",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{"disabled"}, // Required reason
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false for not_possible without required reason",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: true,
							Reasons:          []string{"some other reason"}, // Not a required reason
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return true for in_takeover state",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateInTakeover,
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return true for in_progress state",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateInProgress,
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return true for failed state",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateFailed,
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false for not_attempted state",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotAttempted,
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when ha is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha:   nil,
				},
			},
			expected: false,
		},
		{
			name: "Should return false when takeover is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: nil,
					},
				},
			},
			expected: false,
		},
		{
			name:     "Should return false for empty nodes",
			nodes:    []vsa.NodeHealthStatus{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldJSwapToDiskForTakeoverStates(tt.nodes, poolUUID, logger, correlationID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShouldJSwapToDiskForUnplannedFailover(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	tests := []struct {
		name     string
		nodes    []vsa.NodeHealthStatus
		expected bool
	}{
		{
			name: "Should return true for unplanned failover scenario",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{UnplannedFailoverTakeoverReason},
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false when backing type is not ephemeral_memory",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{UnplannedFailoverTakeoverReason},
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: string(vsa.JSWAPBackingTypeEphemeralDisk),
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when reason is different",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{"disabled"},
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when takeover state is not not_possible",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateInProgress,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{UnplannedFailoverTakeoverReason},
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when ha is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha:   nil,
					NVLog: &vsa.NVLog{
						BackingType: string(vsa.JSWAPBackingTypeEphemeralMemory),
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when nvlog is nil",
			nodes: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false,
							Reasons:          []string{UnplannedFailoverTakeoverReason},
						},
					},
					NVLog: nil,
				},
			},
			expected: false,
		},
		{
			name:     "Should return false for empty nodes",
			nodes:    []vsa.NodeHealthStatus{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShouldJSwapToDiskForUnplannedFailover(tt.nodes, poolUUID, logger, correlationID)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsRequiredTakeoverReason(t *testing.T) {
	tests := []struct {
		name     string
		reason   string
		expected bool
	}{
		{
			name:     "Should return true for disabled reason",
			reason:   "disabled",
			expected: true,
		},
		{
			name:     "Should return true for mailbox disks degraded",
			reason:   "Storage failover mailbox disks are in a degraded state",
			expected: true,
		},
		{
			name:     "Should return true for partner mailbox read error",
			reason:   "Local node has encountered errors while reading the storage failover partner's mailbox disks",
			expected: true,
		},
		{
			name:     "Should return true for interconnect error",
			reason:   "Storage failover interconnect error",
			expected: true,
		},
		{
			name:     "Should return true for partner halted after disabling",
			reason:   "Partner node halted after disabling takeover",
			expected: true,
		},
		{
			name:     "Should return true for mailbox disks unhealthy",
			reason:   "Mailbox disks are not healthy",
			expected: true,
		},
		{
			name:     "Should return true for local node missing partner disks",
			reason:   "Local node missing partner disks",
			expected: true,
		},
		{
			name:     "Should return true for default reason",
			reason:   "Default",
			expected: true,
		},
		{
			name:     "Should return false for other reasons",
			reason:   "some_other_reason",
			expected: false,
		},
		{
			name:     "Should return false for empty reason",
			reason:   "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsRequiredTakeoverReason(tt.reason)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasNodeRequiredTakeoverReasonFromHealth(t *testing.T) {
	tests := []struct {
		name     string
		node     vsa.NodeHealthStatus
		expected bool
	}{
		{
			name: "Should return true when node has required takeover reason",
			node: vsa.NodeHealthStatus{
				UUID: "node1",
				Name: "node1",
				Ha: &vsa.HAHealthInfo{
					TakeoverCheck: &vsa.TakeoverCheck{
						TakeoverPossible: false,
						Reasons:          []string{"disabled", "other_reason"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Should return false when node doesn't have required takeover reason",
			node: vsa.NodeHealthStatus{
				UUID: "node1",
				Name: "node1",
				Ha: &vsa.HAHealthInfo{
					TakeoverCheck: &vsa.TakeoverCheck{
						TakeoverPossible: true,
						Reasons:          []string{"some_other_reason"},
					},
				},
			},
			expected: false,
		},
		{
			name: "Should return false when takeover check is nil",
			node: vsa.NodeHealthStatus{
				UUID: "node1",
				Name: "node1",
				Ha: &vsa.HAHealthInfo{
					TakeoverCheck: nil,
				},
			},
			expected: false,
		},
		{
			name: "Should return false when ha field is nil",
			node: vsa.NodeHealthStatus{
				UUID: "node1",
				Name: "node1",
				Ha:   nil,
			},
			expected: false,
		},
		{
			name: "Should return false when reasons are empty",
			node: vsa.NodeHealthStatus{
				UUID: "node1",
				Name: "node1",
				Ha: &vsa.HAHealthInfo{
					TakeoverCheck: &vsa.TakeoverCheck{
						TakeoverPossible: false,
						Reasons:          []string{},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := HasNodeRequiredTakeoverReasonFromHealth(tt.node)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetVSAProviderUnit(t *testing.T) {
	t.Run("GetVSAProviderUnit with valid inputs", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "test-pool-uuid",
			AccountID: int64(123),
		}

		// Mock GetPool to return error for simplicity
		mockStorage.On("GetPool", mock.Anything, poolIdentifier.UUID, poolIdentifier.AccountID).Return(nil, errors.New("pool not found"))

		// Act - Pass context as first parameter, then poolIdentifier, mockStorage, and context again
		result, err := GetVSAProviderUnit(ctx, poolIdentifier, mockStorage, ctx)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get pool")
		mockStorage.AssertExpectations(t)
	})

	t.Run("GetVSAProviderUnit with insufficient parameters", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		// Act - Test insufficient parameters (less than 3, after context)
		result, err := GetVSAProviderUnit(ctx, "only-one-param", "only-two-params")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient parameters")
	})
}

func TestGetClusterHealthStatusUnit(t *testing.T) {
	t.Run("GetClusterHealthStatusUnit with valid inputs", func(t *testing.T) {
		// Arrange
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		poolUUID := "test-pool-uuid"

		expectedResponse := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
				},
			},
			NumRecords: 1,
		}

		mockProvider.On("GetClusterHealthStatus").Return(expectedResponse, nil)

		// Act - Pass context as first parameter, then mockProvider, poolUUID, and context again
		result, err := GetClusterHealthStatusUnit(ctx, mockProvider, poolUUID, ctx)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, expectedResponse, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetClusterHealthStatusUnit with provider error", func(t *testing.T) {
		// Arrange
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		poolUUID := "test-pool-uuid"

		mockProvider.On("GetClusterHealthStatus").Return(nil, errors.New("cluster health error"))

		// Act
		result, err := GetClusterHealthStatusUnit(ctx, mockProvider, poolUUID, ctx)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get cluster health status")
		mockProvider.AssertExpectations(t)
	})

	t.Run("GetClusterHealthStatusUnit with insufficient parameters", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		// Act - Test insufficient parameters (less than 3, after context)
		result, err := GetClusterHealthStatusUnit(ctx, "only-one-param", "only-two-params")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient parameters")
	})
}

func TestJSwapUnit(t *testing.T) {
	t.Run("JSwapUnit with successful operation", func(t *testing.T) {
		// Arrange
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		nodeUUID := "test-node-uuid"
		backingType := vsa.JSWAPBackingTypeEphemeralDisk

		mockProvider.On("UpdateJSwapMode", nodeUUID, backingType).Return(true, nil)

		// Act - Pass context, then mockProvider, nodeUUID, backingType, and context again
		result, err := JSwapUnit(ctx, mockProvider, nodeUUID, backingType, ctx)

		// Assert
		assert.NoError(t, err)
		assert.Equal(t, true, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("JSwapUnit with provider error", func(t *testing.T) {
		// Arrange
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		nodeUUID := "test-node-uuid"
		backingType := vsa.JSWAPBackingTypeEphemeralMemory

		mockProvider.On("UpdateJSwapMode", nodeUUID, backingType).Return(false, errors.New("jswap failed"))

		// Act - Pass context, then mockProvider, nodeUUID, backingType, and context again
		result, err := JSwapUnit(ctx, mockProvider, nodeUUID, backingType, ctx)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to perform JSWAP")
		mockProvider.AssertExpectations(t)
	})

	t.Run("JSwapUnit with unsuccessful operation", func(t *testing.T) {
		// Arrange
		mockProvider := vsa.NewMockProvider(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		nodeUUID := "test-node-uuid"
		backingType := vsa.JSWAPBackingTypeEphemeralDisk

		mockProvider.On("UpdateJSwapMode", nodeUUID, backingType).Return(false, nil)

		// Act - Pass context, then mockProvider, nodeUUID, backingType, and context again
		result, err := JSwapUnit(ctx, mockProvider, nodeUUID, backingType, ctx)

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "JSWAP operation failed")
		mockProvider.AssertExpectations(t)
	})

	t.Run("JSwapUnit with insufficient parameters", func(t *testing.T) {
		// Arrange
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		// Act - Test insufficient parameters (less than 4, after context)
		result, err := JSwapUnit(ctx, "param1", "param2", "param3")

		// Assert
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient parameters")
	})
}

func TestSyncVSAClusterHealth(t *testing.T) {
	t.Run("SyncVSAClusterHealth_With_Successful_Execution", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"

		pools := []*database.PoolIdentifier{
			{UUID: "pool-1", AccountID: int64(1)},
			{UUID: "pool-2", AccountID: int64(2)},
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "pool-1",
				},
				State: models.LifeCycleStateREADY,
				PoolCredentials: &datamodel.PoolCredentials{
					SecretID: "test-secret",
					Password: "test-password",
					AuthType: 1,
				},
			},
		}

		// Sample nodes for the pool
		nodes := []*datamodel.Node{
			{
				BaseModel: datamodel.BaseModel{
					ID:   1,
					UUID: "node-1",
				},
				Name:            "node-1",
				State:           "READY",
				EndpointAddress: "192.168.1.10",
				PoolID:          1,
				AccountID:       1,
			},
			{
				BaseModel: datamodel.BaseModel{
					ID:   2,
					UUID: "node-2",
				},
				Name:            "node-2",
				State:           "READY",
				EndpointAddress: "192.168.1.11",
				PoolID:          1,
				AccountID:       1,
			},
		}

		// Mock cluster health response
		clusterHealthResponse := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{
				{
					UUID: "node-1",
					Name: "node-1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotAttempted,
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: "ephemeral_memory",
					},
				},
				{
					UUID: "node-2",
					Name: "node-2",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotAttempted,
						},
					},
					NVLog: &vsa.NVLog{
						BackingType: "ephemeral_memory",
					},
				},
			},
			NumRecords: 2,
		}

		// Create mock VSA provider
		mockProvider := new(vsa.MockProvider)

		// Mock nodes for TriggerTakeoverCheck
		vsaNodes := []*vsa.Node{
			{ExternalUUID: "node-1"},
			{ExternalUUID: "node-2"},
		}
		mockProvider.On("GetNodes").Return(vsaNodes, nil)
		mockProvider.On("TriggerTakeoverCheck", "node-1").Return(true, nil)
		mockProvider.On("TriggerTakeoverCheck", "node-2").Return(true, nil)

		mockProvider.On("GetClusterHealthStatus").Return(clusterHealthResponse, nil)

		mockStorage.On("GetPool", mock.Anything, mock.Anything, mock.Anything).Return(poolView, nil)
		mockStorage.On("ListPoolUUIDs", mock.Anything, mock.Anything).Return(pools, nil)
		mockStorage.On("GetNodesByPoolID", mock.Anything, mock.Anything).Return(nodes, nil)
		// Note: UpdatePoolFields expectation removed as pool is already READY and no state change should occur due to optimization

		// Patch hyperscaler.GetProviderByNode to return mock provider
		originalGetProviderByNode := hyperscaler.GetProviderByNode
		defer func() { hyperscaler.GetProviderByNode = originalGetProviderByNode }()
		hyperscaler.GetProviderByNode = func(ctx context.Context, node *models.Node) (vsa.Provider, error) {
			return mockProvider, nil
		}

		// Act
		SyncVSAClusterHealth(ctx, mockStorage, correlationID)

		// Assert
		mockStorage.AssertExpectations(t)
		mockProvider.AssertExpectations(t)
	})

	t.Run("SyncVSAClusterHealth with ListPoolUUIDs error", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"

		mockStorage.On("ListPoolUUIDs", mock.Anything, mock.Anything).Return(nil, errors.New("list pools failed"))

		// Act
		SyncVSAClusterHealth(ctx, mockStorage, correlationID)

		// Assert
		mockStorage.AssertExpectations(t)
	})

	t.Run("SyncVSAClusterHealth with empty pools list", func(t *testing.T) {
		// Arrange
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"

		mockStorage.On("ListPoolUUIDs", mock.Anything, mock.Anything).Return([]*database.PoolIdentifier{}, nil)

		// Act
		SyncVSAClusterHealth(ctx, mockStorage, correlationID)

		// Assert
		mockStorage.AssertExpectations(t)
	})
}

func TestEdgeCases(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	t.Run("Complex mixed node states for comprehensive coverage", func(t *testing.T) {
		// Mix of different node states to test complex conditions
		complexNodes := []vsa.NodeHealthStatus{
			{
				UUID: "node1",
				Name: "node1",
				Ha: &vsa.HAHealthInfo{
					Takeover: &vsa.TakeoverState{
						State: vsa.TakeoverStateNotAttempted,
					},
				},
				NVLog: &vsa.NVLog{
					BackingType: "ephemeral_memory",
				},
			},
			{
				UUID: "node2",
				Name: "node2",
				Ha: &vsa.HAHealthInfo{
					Takeover: &vsa.TakeoverState{
						State: vsa.TakeoverStateInProgress,
					},
				},
				NVLog: nil, // Missing nvlog
			},
			{
				UUID: "node3",
				Name: "node3",
				Ha:   nil, // Missing ha
			},
		}

		// This complex test verifies that mixed states work correctly
		result := DetermineJSwapAction(&vsa.ClusterHealthStatusResponse{
			Records:    complexNodes,
			NumRecords: len(complexNodes),
		}, poolUUID, logger, correlationID)
		assert.Equal(t, JSwapActionToDisk, result) // Should prioritize disk swap due to takeover_possible false
	})

	t.Run("Test all default takeover reason constants coverage", func(t *testing.T) {
		allDefaultReasons := []string{
			"disabled",
			"Storage failover mailbox disks are in a degraded state",
			"Local node has encountered errors while reading the storage failover partner's mailbox disks",
			"Storage failover interconnect error",
			"Partner node halted after disabling takeover",
			"Mailbox disks are not healthy",
			"Local node missing partner disks",
			"Default",
		}

		for _, reason := range allDefaultReasons {
			assert.True(t, IsRequiredTakeoverReason(reason), "Reason %s should be required", reason)
		}
	})

	t.Run("Test getRequiredTakeoverReasons with environment variable", func(t *testing.T) {
		// Test with custom environment variable
		originalEnv := os.Getenv("REQUIRED_TAKEOVER_REASONS")
		defer func() {
			err := os.Setenv("REQUIRED_TAKEOVER_REASONS", originalEnv)
			assert.NoError(t, err)
		}()

		err := os.Setenv("REQUIRED_TAKEOVER_REASONS", "disabled,custom reason,another reason")
		assert.NoError(t, err)

		reasons := getRequiredTakeoverReasons()
		expected := []string{"disabled", "custom reason", "another reason"}
		assert.Equal(t, expected, reasons)
	})

	t.Run("Test getRequiredTakeoverReasons with empty environment variable", func(t *testing.T) {
		// Test with empty environment variable (should use defaults)
		originalEnv := os.Getenv("REQUIRED_TAKEOVER_REASONS")
		defer func() {
			err := os.Setenv("REQUIRED_TAKEOVER_REASONS", originalEnv)
			assert.NoError(t, err)
		}()

		err := os.Setenv("REQUIRED_TAKEOVER_REASONS", "")
		assert.NoError(t, err)

		reasons := getRequiredTakeoverReasons()
		// Should return default reasons
		assert.Contains(t, reasons, "disabled")
		assert.Contains(t, reasons, "Default")
		assert.Len(t, reasons, 8) // Default has 8 reasons
	})

	t.Run("Test multiple required reasons in node", func(t *testing.T) {
		node := vsa.NodeHealthStatus{
			UUID: "node1",
			Name: "node1",
			Ha: &vsa.HAHealthInfo{
				TakeoverCheck: &vsa.TakeoverCheck{
					TakeoverPossible: false,
					Reasons:          []string{"other_reason", "Storage failover mailbox disks are in a degraded state", "another_reason"},
				},
			},
		}

		assert.True(t, HasNodeRequiredTakeoverReasonFromHealth(node))
	})

	t.Run("Comprehensive cluster health response edge cases", func(t *testing.T) {
		testCases := []struct {
			name     string
			response *vsa.ClusterHealthStatusResponse
			expected JSwapAction
		}{
			{
				name: "Empty records",
				response: &vsa.ClusterHealthStatusResponse{
					Records:    []vsa.NodeHealthStatus{},
					NumRecords: 0,
				},
				expected: JSwapActionNone,
			},
			{
				name: "Nil records slice",
				response: &vsa.ClusterHealthStatusResponse{
					Records:    nil,
					NumRecords: 0,
				},
				expected: JSwapActionNone,
			},
			{
				name: "Mixed states prioritizing disk swap",
				response: &vsa.ClusterHealthStatusResponse{
					Records: []vsa.NodeHealthStatus{
						{
							UUID: "node1",
							Name: "node1",
							Ha: &vsa.HAHealthInfo{
								Takeover: &vsa.TakeoverState{
									State: vsa.TakeoverStateNotPossible,
								},
								TakeoverCheck: &vsa.TakeoverCheck{
									TakeoverPossible: true,
									Reasons:          []string{"disabled"}, // Required reason that triggers disk swap
								},
							},
							NVLog: &vsa.NVLog{BackingType: "ephemeral_memory"},
						},
						{
							UUID: "node2",
							Name: "node2",
							Ha: &vsa.HAHealthInfo{
								TakeoverCheck: &vsa.TakeoverCheck{
									TakeoverPossible: true,
									Reasons:          []string{},
								},
							},
							NVLog: &vsa.NVLog{BackingType: "ephemeral_memory"},
						},
					},
					NumRecords: 2,
				},
				expected: JSwapActionToDisk, // Should prioritize disk due to required takeover reason
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := determineJSwapAction(tc.response, poolUUID, logger, correlationID)
				assert.Equal(t, tc.expected, result)
			})
		}
	})
}

// New test functions for additional coverage
func TestUpdatePoolToReadyState(t *testing.T) {
	t.Run("successful update", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"
		logger := util.GetLogger(ctx)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateDegraded, // Changed to DEGRADED so update to READY actually happens
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-1", mock.Anything).Return(nil)

		UpdatePoolToReadyState(mockStorage, poolIdentifier, logger, correlationID)

		mockStorage.AssertExpectations(t)
	})

	t.Run("update with database error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"
		logger := util.GetLogger(ctx)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateDegraded, // Changed to DEGRADED so update to READY actually happens
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-1", mock.Anything).Return(errors.New("database error"))

		UpdatePoolToReadyState(mockStorage, poolIdentifier, logger, correlationID)

		mockStorage.AssertExpectations(t)
	})
}

func TestUpdatePoolState(t *testing.T) {
	t.Run("successful update from READY to DEGRADED", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-1", mock.Anything).Return(nil)

		err := UpdatePoolState(mockStorage, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("skip update when new state same as current state", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		// Note: No UpdatePoolFields call expected since state is already READY

		err := UpdatePoolState(mockStorage, poolIdentifier, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("skip update when pool in DELETING state", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     "DELETING", // Not READY or DEGRADED
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		// Note: No UpdatePoolFields call expected

		err := UpdatePoolState(mockStorage, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails)
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
	})

	t.Run("error getting pool", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(nil, errors.New("pool not found"))

		err := UpdatePoolState(mockStorage, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get pool for state update")
		mockStorage.AssertExpectations(t)
	})

	t.Run("error updating pool fields", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-1", mock.Anything).Return(errors.New("update failed"))

		err := UpdatePoolState(mockStorage, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to conditionally update pool state")
		mockStorage.AssertExpectations(t)
	})
}

func TestExecuteJSwapAction_AdditionalCoverage(t *testing.T) {
	t.Run("execute JSwapActionToDisk", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"
		logger := util.GetLogger(ctx)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		clusterHealth := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		mockStorage.On("UpdatePoolFields", mock.Anything, "pool-1", mock.Anything).Return(nil)

		// Create IMTPContext mock
		imtpCtx := &inmemotasksprocessor.IMTPContext{}
		mockProvider := new(vsa.MockProvider)

		// Patch PerformJSwapToDisk to avoid context issues
		originalPerformJSwapToDisk := PerformJSwapToDisk
		defer func() { PerformJSwapToDisk = originalPerformJSwapToDisk }()
		PerformJSwapToDisk = func(ctx *inmemotasksprocessor.IMTPContext, clusterHealth *vsa.ClusterHealthStatusResponse, provider vsa.Provider, se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string, bgCtx context.Context) {
			// Mock implementation that just calls updatePoolState
			_ = updatePoolState(se, poolIdentifier, models.LifeCycleStateDegraded, models.LifeCycleStateDegradedDetails) // Ignore error in test mock
		}

		// Create background context for the updated function signature
		bgCtx := context.Background()
		ExecuteJSwapAction(imtpCtx, JSwapActionToDisk, clusterHealth, mockProvider, mockStorage, poolIdentifier, logger, correlationID, bgCtx)

		mockStorage.AssertExpectations(t)
	})

	t.Run("execute JSwapActionToMemory", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"
		logger := util.GetLogger(ctx)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		clusterHealth := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		// Note: UpdatePoolFields expectation removed as this tries to update READY->READY, which is skipped by optimization

		// Create IMTPContext mock
		imtpCtx := &inmemotasksprocessor.IMTPContext{}
		mockProvider := new(vsa.MockProvider)

		// Patch PerformJSwapToMemory to avoid context issues
		originalPerformJSwapToMemory := PerformJSwapToMemory
		defer func() { PerformJSwapToMemory = originalPerformJSwapToMemory }()
		PerformJSwapToMemory = func(ctx *inmemotasksprocessor.IMTPContext, clusterHealth *vsa.ClusterHealthStatusResponse, provider vsa.Provider, se database.Storage, poolIdentifier *database.PoolIdentifier, logger log.Logger, correlationID string, bgCtx context.Context) {
			// Mock implementation that just calls updatePoolState
			_ = updatePoolState(se, poolIdentifier, models.LifeCycleStateREADY, models.LifeCycleStateReadyDetails) // Ignore error in test mock
		}

		// Create background context for the updated function signature
		bgCtx := context.Background()
		ExecuteJSwapAction(imtpCtx, JSwapActionToMemory, clusterHealth, mockProvider, mockStorage, poolIdentifier, logger, correlationID, bgCtx)

		mockStorage.AssertExpectations(t)
	})

	t.Run("execute JSwapActionNone", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		correlationID := "test-correlation-id"
		logger := util.GetLogger(ctx)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		poolView := &datamodel.PoolView{
			Pool: datamodel.Pool{
				BaseModel: datamodel.BaseModel{ID: 1, UUID: "pool-1"},
				State:     models.LifeCycleStateREADY,
			},
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(poolView, nil)
		// Note: UpdatePoolFields expectation removed as JSwapActionNone doesn't call updatePoolState

		// Create IMTPContext mock
		imtpCtx := &inmemotasksprocessor.IMTPContext{}
		clusterHealth := &vsa.ClusterHealthStatusResponse{}
		mockProvider := new(vsa.MockProvider)

		// Create background context for the updated function signature
		bgCtx := context.Background()
		ExecuteJSwapAction(imtpCtx, JSwapActionNone, clusterHealth, mockProvider, mockStorage, poolIdentifier, logger, correlationID, bgCtx)

		mockStorage.AssertExpectations(t)
	})
}

func TestGetVSAProviderUnit_ErrorCases(t *testing.T) {
	t.Run("insufficient parameters", func(t *testing.T) {
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		result, err := GetVSAProviderUnit(ctx, nil, nil) // Only 2 parameters instead of 3
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient parameters for GetVSAProviderUnit")
		assert.Nil(t, result)
	})

	t.Run("get pool error", func(t *testing.T) {
		mockStorage := database.NewMockStorage(t)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		poolIdentifier := &database.PoolIdentifier{
			UUID:      "pool-1",
			AccountID: 1,
		}

		mockStorage.On("GetPool", mock.Anything, "pool-1", int64(1)).Return(nil, errors.New("pool not found"))

		result, err := GetVSAProviderUnit(ctx, poolIdentifier, mockStorage, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get pool")
		assert.Nil(t, result)
		mockStorage.AssertExpectations(t)
	})
}

func TestGetClusterHealthStatusUnit_ErrorCases(t *testing.T) {
	t.Run("insufficient parameters", func(t *testing.T) {
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		result, err := GetClusterHealthStatusUnit(ctx, nil, nil) // Only 2 parameters instead of 3
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient parameters for GetClusterHealthStatusUnit")
		assert.Nil(t, result)
	})

	t.Run("provider error", func(t *testing.T) {
		mockProvider := new(vsa.MockProvider)
		ctx := context.Background()
		// Add correlation ID to context
		ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

		poolUUID := "pool-1"

		mockProvider.On("GetClusterHealthStatus").Return(nil, errors.New("provider error"))

		result, err := GetClusterHealthStatusUnit(ctx, mockProvider, poolUUID, ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get cluster health status")
		assert.Nil(t, result)
		mockProvider.AssertExpectations(t)
	})
}

func TestDetermineJSwapAction_EdgeCases(t *testing.T) {
	ctx := context.Background()
	logger := util.GetLogger(ctx)
	poolUUID := "test-pool-uuid"
	correlationID := "test-correlation-id"

	t.Run("prioritizes disk swap over memory swap", func(t *testing.T) {
		// Test case where a node has both not_attempted state AND takeover_possible false
		// Should prioritize disk swap due to takeover_possible false
		clusterHealth := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						TakeoverCheck: &vsa.TakeoverCheck{
							TakeoverPossible: false, // Should trigger disk swap
							Reasons:          []string{"Partner is not UP."},
						},
					},
					NVLog: &vsa.NVLog{BackingType: "ephemeral_memory"},
				},
				{
					UUID: "node2",
					Name: "node2",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotAttempted, // Would trigger memory swap
						},
					},
					NVLog: &vsa.NVLog{BackingType: "ephemeral_memory"},
				},
			},
			NumRecords: 2,
		}

		result := determineJSwapAction(clusterHealth, poolUUID, logger, correlationID)
		assert.Equal(t, JSwapActionToDisk, result)
	})

	t.Run("returns none when no action needed", func(t *testing.T) {
		// All nodes are healthy with no issues - no TakeoverCheck means no action needed
		clusterHealth := &vsa.ClusterHealthStatusResponse{
			Records: []vsa.NodeHealthStatus{
				{
					UUID: "node1",
					Name: "node1",
					Ha: &vsa.HAHealthInfo{
						Takeover: &vsa.TakeoverState{
							State: vsa.TakeoverStateNotPossible,
						},
						// No TakeoverCheck means no action needed
					},
					NVLog: &vsa.NVLog{BackingType: "ephemeral_memory"},
				},
			},
			NumRecords: 1,
		}

		result := determineJSwapAction(clusterHealth, poolUUID, logger, correlationID)
		assert.Equal(t, JSwapActionNone, result)
	})
}

func TestTriggerTakeoverCheckUnit(t *testing.T) {
	ctx := context.Background()
	// Add correlation ID to context
	ctx = context.WithValue(ctx, middleware.CorrelationContextKey, "test-correlation-id")
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	ctx = context.WithValue(ctx, middleware.TemporalSLoggerKey, logger)

	poolUUID := "test-pool-uuid"

	t.Run("Success - triggers takeover check for all nodes", func(t *testing.T) {
		mockProvider := &vsa.MockProvider{}

		// Mock cluster nodes
		nodes := []*vsa.Node{
			{ExternalUUID: "node1-uuid"},
			{ExternalUUID: "node2-uuid"},
		}

		// Mock GetNodes
		mockProvider.On("GetNodes").Return(nodes, nil)

		// Mock successful takeover check for each node
		mockProvider.On("TriggerTakeoverCheck", "node1-uuid").Return(true, nil)
		mockProvider.On("TriggerTakeoverCheck", "node2-uuid").Return(true, nil)

		result, err := TriggerTakeoverCheckUnit(ctx, mockProvider, poolUUID, ctx)

		assert.NoError(t, err)
		assert.Equal(t, true, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error - GetNodes fails", func(t *testing.T) {
		mockProvider := &vsa.MockProvider{}

		// Mock GetNodes failure
		mockProvider.On("GetNodes").Return(nil, errors.New("get nodes error"))

		result, err := TriggerTakeoverCheckUnit(ctx, mockProvider, poolUUID, ctx)

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "failed to get nodes for pool")
		mockProvider.AssertExpectations(t)
	})

	t.Run("Warning - TriggerTakeoverCheck fails for one node but continues", func(t *testing.T) {
		mockProvider := &vsa.MockProvider{}

		// Mock cluster nodes
		nodes := []*vsa.Node{
			{ExternalUUID: "node1-uuid"},
			{ExternalUUID: "node2-uuid"},
		}

		// Mock GetNodes
		mockProvider.On("GetNodes").Return(nodes, nil)

		// Mock successful takeover check for first node
		mockProvider.On("TriggerTakeoverCheck", "node1-uuid").Return(true, nil)
		// Mock failure for second node - but function continues
		mockProvider.On("TriggerTakeoverCheck", "node2-uuid").Return(false, errors.New("takeover check failed"))

		result, err := TriggerTakeoverCheckUnit(ctx, mockProvider, poolUUID, ctx)

		// Function should still succeed even if one node fails
		assert.NoError(t, err)
		assert.Equal(t, true, result)
		mockProvider.AssertExpectations(t)
	})

	t.Run("Error - insufficient parameters", func(t *testing.T) {
		result, err := TriggerTakeoverCheckUnit(ctx, "only-one-param", "only-two-params")

		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "insufficient parameters")
	})
}
